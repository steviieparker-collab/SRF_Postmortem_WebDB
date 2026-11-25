-- SRF Postmortem feature/vector store (proposed)
-- SQLite, read-only at MCP runtime.  One file, copied from the batch host.
-- Vectors are float32 little-endian BLOBs; dim/model are in model_versions.

PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id   TEXT PRIMARY KEY,          -- e.g. 20261118T090000Z
    created_at    TEXT NOT NULL,
    db_sha256     TEXT NOT NULL,
    event_count   INTEGER NOT NULL,
    parquet_count INTEGER NOT NULL,
    report_count  INTEGER NOT NULL,
    notes         TEXT
);

CREATE TABLE IF NOT EXISTS model_versions (
    model_key     TEXT PRIMARY KEY,          -- text_bge_m3, image_clip, waveform_v1
    model_name    TEXT NOT NULL,             -- e.g. BAAI/bge-m3
    version       TEXT NOT NULL,
    dim           INTEGER NOT NULL,
    normalize     INTEGER NOT NULL DEFAULT 1, -- 1 = L2-normalized
    created_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_features (
    event_id      TEXT PRIMARY KEY,
    snapshot_id   TEXT NOT NULL REFERENCES snapshots(snapshot_id),
    timestamp     TEXT NOT NULL,
    t0_offset_s   REAL,
    quench_unit   TEXT,
    has_waveform  INTEGER NOT NULL DEFAULT 0,
    features_json TEXT NOT NULL,             -- sag/reflect/timing/digital/BCL03
    input_hash    TEXT NOT NULL,             -- parquet+report hash
    updated_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS waveform_vectors (
    event_id      TEXT NOT NULL,
    model_key     TEXT NOT NULL REFERENCES model_versions(model_key),
    vector        BLOB NOT NULL,
    meta_json     TEXT,                      -- window, channels, normalization
    PRIMARY KEY (event_id, model_key)
);

-- Fixed-length vector definition: ordered feature names, imputation values,
-- robust scaler (median/MAD), clipping and normalisation.  Stored so a vector
-- can be reproduced or a query vector can be built with the same transform.
CREATE TABLE IF NOT EXISTS feature_vector_specs (
    model_key     TEXT PRIMARY KEY REFERENCES model_versions(model_key),
    spec_json     TEXT NOT NULL,
    created_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS report_documents (
    doc_id        TEXT PRIMARY KEY,          -- sha256 of source file
    event_id      TEXT,
    source_path   TEXT NOT NULL,
    mime_type     TEXT,
    sha256        TEXT NOT NULL,
    title         TEXT,
    text          TEXT,                      -- extracted text (may be empty)
    meta_json     TEXT,
    created_at    TEXT NOT NULL
);

-- A byte-identical report can be attached to several events (5 copies of one
-- PDF, 2 copies of one HWP in the 2026-11 snapshot).  doc_id is content-based,
-- so the event association lives here instead of being overwritten.
CREATE TABLE IF NOT EXISTS report_document_events (
    doc_id        TEXT NOT NULL REFERENCES report_documents(doc_id),
    event_id      TEXT NOT NULL,
    PRIMARY KEY (doc_id, event_id)
);

CREATE TABLE IF NOT EXISTS report_chunks (
    chunk_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id        TEXT NOT NULL REFERENCES report_documents(doc_id),
    ord           INTEGER NOT NULL,
    text          TEXT NOT NULL,
    token_count   INTEGER,
    UNIQUE (doc_id, ord)
);

CREATE TABLE IF NOT EXISTS report_chunk_vectors (
    chunk_id      INTEGER NOT NULL REFERENCES report_chunks(chunk_id),
    model_key     TEXT NOT NULL REFERENCES model_versions(model_key),
    vector        BLOB NOT NULL,
    PRIMARY KEY (chunk_id, model_key)
);

CREATE TABLE IF NOT EXISTS report_images (
    image_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id        TEXT NOT NULL REFERENCES report_documents(doc_id),
    ord           INTEGER NOT NULL,
    source_path   TEXT,
    caption       TEXT,                      -- VLM caption (searchable)
    ocr_text      TEXT,
    meta_json     TEXT,
    UNIQUE (doc_id, ord)
);

CREATE TABLE IF NOT EXISTS report_image_vectors (
    image_id      INTEGER NOT NULL REFERENCES report_images(image_id),
    model_key     TEXT NOT NULL REFERENCES model_versions(model_key),
    vector        BLOB NOT NULL,
    PRIMARY KEY (image_id, model_key)
);

-- Optional FAQ cache: question vector + paired answer.
CREATE TABLE IF NOT EXISTS qna_cache (
    qna_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    question      TEXT NOT NULL,
    question_vec  BLOB NOT NULL,
    model_key     TEXT NOT NULL REFERENCES model_versions(model_key),
    answer        TEXT NOT NULL,
    evidence_json TEXT,
    created_at    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_event_features_time ON event_features(timestamp);
CREATE INDEX IF NOT EXISTS idx_waveform_vectors_model ON waveform_vectors(model_key);
CREATE INDEX IF NOT EXISTS idx_report_documents_event ON report_documents(event_id);
CREATE INDEX IF NOT EXISTS idx_report_document_events_event ON report_document_events(event_id);
CREATE INDEX IF NOT EXISTS idx_report_chunks_doc ON report_chunks(doc_id);
CREATE INDEX IF NOT EXISTS idx_report_images_doc ON report_images(doc_id);

-- Lexical fallback / hybrid search over report text and captions.
CREATE VIRTUAL TABLE IF NOT EXISTS report_fts USING fts5(
    text,
    content='',           -- external content populated by the batch writer
    tokenize='trigram'
);

-- Image captions/OCR are searched separately; rowid maps to report_images.
CREATE VIRTUAL TABLE IF NOT EXISTS report_image_fts USING fts5(
    text,
    content='',
    tokenize='trigram'
);
