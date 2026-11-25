#!/usr/bin/env python3
"""Load extracted reports into the feature store and embed chunks (Task 4).

Inputs:
    data/reports/report_documents.jsonl   from extract_report_text.py
    data/reports/report_images.jsonl
    data/models/multilingual-e5-small/    int8 ONNX + tokenizer.json
    data/feature_store.sqlite3            built by build_feature_store.py

The script copies the feature store, fills the report_* tables and FTS index,
optionally computes e5-small embeddings, validates, then atomically replaces
the original file.  Event/waveform tables are not touched.

Usage:
    venv-report/bin/python scripts/build_report_index.py --force
    venv-report/bin/python scripts/build_report_index.py --no-embeddings
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.features.embedding import (  # noqa: E402
    E5Embedder, MAX_TOKENS, TEXT_DIM, TEXT_MODEL_KEY, TEXT_MODEL_NAME,
    TEXT_MODEL_VERSION,
)

CHUNK_CHARS = 900
CHUNK_OVERLAP = 120


# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------

def chunk_text(text: str, max_chars: int = CHUNK_CHARS,
               overlap: int = CHUNK_OVERLAP) -> list[str]:
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= max_chars:
        return [text]
    out: list[str] = []
    start = 0
    while start < len(text):
        end = min(len(text), start + max_chars)
        if end < len(text):
            cut = max(
                text.rfind("\n", start, end),
                text.rfind(". ", start, end),
                text.rfind("다.", start, end),
                text.rfind("다.\n", start, end),
            )
            if cut > start + max_chars // 2:
                end = cut + 1
        piece = text[start:end].strip()
        if piece:
            out.append(piece)
        if end >= len(text):
            break
        start = max(end - overlap, start + 1)
    return out


def token_count(embedder: Optional[E5Embedder], text: str) -> Optional[int]:
    if embedder is None:
        return None
    return len(embedder.tokenizer.encode(text).ids)


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    records = []
    with path.open(encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{lineno}: {exc}") from exc
    return records


def _clear_report_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS report_document_events (
            doc_id   TEXT NOT NULL REFERENCES report_documents(doc_id),
            event_id TEXT NOT NULL,
            PRIMARY KEY (doc_id, event_id)
        );
        CREATE INDEX IF NOT EXISTS idx_report_document_events_event
            ON report_document_events(event_id);
        """
    )
    conn.execute("DELETE FROM report_document_events")
    conn.execute("DELETE FROM report_chunk_vectors")
    conn.execute("DELETE FROM report_chunks")
    conn.execute("DELETE FROM report_image_vectors")
    conn.execute("DELETE FROM report_images")
    conn.execute("DELETE FROM report_documents")
    conn.execute("DROP TABLE IF EXISTS report_fts")
    conn.execute(
        "CREATE VIRTUAL TABLE report_fts USING fts5("
        "text, content='', tokenize='trigram')"
    )
    conn.execute("DROP TABLE IF EXISTS report_image_fts")
    conn.execute(
        "CREATE VIRTUAL TABLE report_image_fts USING fts5("
        "text, content='', tokenize='trigram')"
    )


def build(
    feature_store: Path,
    reports_dir: Path,
    model_dir: Optional[Path],
    chunk_chars: int,
    overlap: int,
) -> dict[str, Any]:
    from datetime import datetime
    started = time.time()
    created_at = datetime.now(timezone.utc).isoformat()

    docs = load_jsonl(reports_dir / "report_documents.jsonl")
    image_rows = load_jsonl(reports_dir / "report_images.jsonl")
    if not docs:
        raise ValueError("no report documents")

    # doc_id is the file content hash, so byte-identical attachments collapse
    # to one document while keeping every event association.
    docs_by_id: dict[str, dict[str, Any]] = {}
    events_by_doc: dict[str, list[str]] = {}
    for rec in docs:
        doc_id = rec["doc_id"]
        if not doc_id:
            continue
        events_by_doc.setdefault(doc_id, [])
        event_id = rec.get("event_id", "")
        if event_id and event_id not in events_by_doc[doc_id]:
            events_by_doc[doc_id].append(event_id)
        docs_by_id.setdefault(doc_id, rec)
    images_by_key: dict[tuple[str, int], dict[str, Any]] = {}
    for img in image_rows:
        images_by_key[(img["doc_id"], img["ord"])] = img

    captions: dict[tuple[str, int], dict[str, str]] = {}
    captions_path = reports_dir / "image_captions.jsonl"
    if captions_path.is_file():
        for rec in load_jsonl(captions_path):
            if rec.get("error"):
                continue
            key = (rec["doc_id"], int(rec["ord"]))
            cap = (rec.get("caption") or "").strip()
            ocr = (rec.get("ocr_text") or "").strip()
            if cap or ocr:
                captions[key] = {"caption": cap, "ocr_text": ocr}

    embedder = E5Embedder(model_dir) if model_dir else None

    tmp = feature_store.with_name(feature_store.name + ".tmp")
    for suffix in ("", "-wal", "-shm"):
        side = Path(str(tmp) + suffix)
        if side.exists():
            side.unlink()
    shutil.copy2(feature_store, tmp)

    conn = sqlite3.connect(str(tmp))
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        _clear_report_tables(conn)

        chunk_total = 0
        image_total = 0
        caption_total = 0
        vectors: list[tuple[int, np.ndarray]] = []
        image_vectors: list[tuple[int, np.ndarray]] = []
        with conn:
            for doc_id, rec in docs_by_id.items():
                event_ids = events_by_doc.get(doc_id, [])
                meta = {
                    "event_id": rec.get("event_id", ""),
                    "event_ids": event_ids,
                    "original_name": rec.get("original_name", ""),
                    "ext": rec.get("ext", ""),
                    "status": rec.get("status", ""),
                    "page_count": rec.get("page_count"),
                    "image_count": rec.get("image_count", 0),
                    "warnings": rec.get("warnings") or [],
                    "parser": rec.get("parser", ""),
                }
                conn.execute(
                    """
                    INSERT OR REPLACE INTO report_documents
                        (doc_id, event_id, source_path, mime_type, sha256,
                         title, text, meta_json, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        doc_id, rec.get("event_id", ""), rec.get("source_path", ""),
                        rec.get("mime_type", ""), rec.get("sha256", ""),
                        rec.get("title", ""), rec.get("text", ""),
                        json.dumps(meta, ensure_ascii=False, sort_keys=True),
                        created_at,
                    ),
                )
                for event_id in event_ids:
                    conn.execute(
                        "INSERT OR IGNORE INTO report_document_events "
                        "(doc_id, event_id) VALUES (?, ?)",
                        (doc_id, event_id),
                    )

                chunks = chunk_text(rec.get("text", ""), chunk_chars, overlap)
                for ord_, chunk in enumerate(chunks):
                    cur = conn.execute(
                        "INSERT INTO report_chunks (doc_id, ord, text, token_count) "
                        "VALUES (?, ?, ?, ?)",
                        (doc_id, ord_, chunk, token_count(embedder, chunk)),
                    )
                    chunk_id = int(cur.lastrowid)
                    # Index title + chunk so a title-only match still works.
                    conn.execute(
                        "INSERT INTO report_fts (rowid, text) VALUES (?, ?)",
                        (chunk_id, f"{rec.get('title', '')}\n{chunk}"),
                    )
                    if embedder is not None:
                        vec = embedder.encode(
                            [f"{rec.get('title', '')}\n{chunk}"], "passage: "
                        )[0]
                        vectors.append((chunk_id, vec))
                    chunk_total += 1

            for img in images_by_key.values():
                key = (img["doc_id"], int(img["ord"]))
                cap = captions.get(key)
                caption = cap["caption"] if cap else ""
                ocr = cap["ocr_text"] if cap else ""
                if cap:
                    caption_status = "ok"
                elif captions:
                    caption_status = "missing"
                else:
                    caption_status = "pending_gpu"
                meta = {
                    "source": img.get("source", ""),
                    "event_id": img.get("event_id", ""),
                    "format": img.get("format", ""),
                    "width": img.get("width"),
                    "height": img.get("height"),
                    "bytes": img.get("bytes"),
                    "sha256": img.get("sha256"),
                    "caption_status": caption_status,
                }
                cur = conn.execute(
                    """
                    INSERT INTO report_images
                        (doc_id, ord, source_path, caption, ocr_text, meta_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        img["doc_id"], int(img["ord"]), img.get("path", ""),
                        caption or None, ocr or None,
                        json.dumps(meta, ensure_ascii=False, sort_keys=True),
                    ),
                )
                image_id = int(cur.lastrowid)
                image_total += 1
                if cap:
                    caption_total += 1
                    fts_text = " ".join(
                        x for x in (img.get("original_name", ""), caption, ocr)
                        if x
                    )
                    conn.execute(
                        "INSERT INTO report_image_fts (rowid, text) VALUES (?, ?)",
                        (image_id, fts_text),
                    )
                    if embedder is not None:
                        vec = embedder.encode_one(
                            f"{img.get('original_name', '')}\n{caption}\n{ocr}",
                            "passage: ",
                        )
                        image_vectors.append((image_id, vec))
            if embedder is not None and image_vectors:
                conn.executemany(
                    "INSERT INTO report_image_vectors (image_id, model_key, vector) "
                    "VALUES (?, ?, ?)",
                    [(iid, TEXT_MODEL_KEY, v.astype("<f4").tobytes())
                     for iid, v in image_vectors],
                )

            if embedder is not None:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO model_versions
                        (model_key, model_name, version, dim, normalize, created_at)
                    VALUES (?, ?, ?, ?, 1, ?)
                    """,
                    (TEXT_MODEL_KEY, TEXT_MODEL_NAME, TEXT_MODEL_VERSION,
                     TEXT_DIM, created_at),
                )
                spec = {
                    "kind": "text",
                    "model_name": TEXT_MODEL_NAME,
                    "model_file": Path(embedder.model_file).name,
                    "dim": TEXT_DIM,
                    "pooling": "mean",
                    "normalize": True,
                    "max_tokens": MAX_TOKENS,
                    "prefix_passage": "passage: ",
                    "prefix_query": "query: ",
                    "chunk_chars": chunk_chars,
                    "chunk_overlap": overlap,
                }
                conn.execute(
                    "INSERT OR REPLACE INTO feature_vector_specs "
                    "(model_key, spec_json, created_at) VALUES (?, ?, ?)",
                    (TEXT_MODEL_KEY,
                     json.dumps(spec, ensure_ascii=False, sort_keys=True),
                     created_at),
                )
                conn.executemany(
                    "INSERT INTO report_chunk_vectors (chunk_id, model_key, vector) "
                    "VALUES (?, ?, ?)",
                    [(cid, TEXT_MODEL_KEY, v.astype("<f4").tobytes())
                     for cid, v in vectors],
                )

            conn.execute(
                "UPDATE snapshots SET report_count = ?", (len(docs),)
            )

        # ---- validation -------------------------------------------------
        checks: dict[str, Any] = {}
        expected_events = sum(len(v) for v in events_by_doc.values())
        checks["documents"] = (
            conn.execute("SELECT COUNT(*) FROM report_documents").fetchone()[0]
            == len(docs_by_id)
        )
        checks["document_events"] = (
            conn.execute(
                "SELECT COUNT(*) FROM report_document_events"
            ).fetchone()[0] == expected_events
        )
        checks["chunks"] = (
            conn.execute("SELECT COUNT(*) FROM report_chunks").fetchone()[0]
            == chunk_total
        )
        checks["images"] = (
            conn.execute("SELECT COUNT(*) FROM report_images").fetchone()[0]
            == len(images_by_key)
        )
        checks["image_fts_rows"] = (
            conn.execute("SELECT COUNT(*) FROM report_image_fts").fetchone()[0]
            == caption_total
        )
        if embedder is not None:
            checks["image_vectors"] = (
                conn.execute(
                    "SELECT COUNT(*) FROM report_image_vectors WHERE model_key=?",
                    (TEXT_MODEL_KEY,),
                ).fetchone()[0] == caption_total
            )
            image_norms = [
                float(np.linalg.norm(np.frombuffer(blob, dtype="<f4")))
                for (blob,) in conn.execute(
                    "SELECT vector FROM report_image_vectors WHERE model_key=?",
                    (TEXT_MODEL_KEY,),
                )
            ]
            checks["image_vectors_normalized"] = (
                caption_total == 0
                or all(abs(x - 1.0) <= 1e-4 for x in image_norms)
            )
        checks["fts_rows"] = (
            conn.execute("SELECT COUNT(*) FROM report_fts").fetchone()[0]
            == chunk_total
        )
        checks["foreign_key_check"] = not list(
            conn.execute("PRAGMA foreign_key_check")
        )
        if embedder is not None:
            checks["vectors"] = (
                conn.execute(
                    "SELECT COUNT(*) FROM report_chunk_vectors WHERE model_key=?",
                    (TEXT_MODEL_KEY,),
                ).fetchone()[0] == chunk_total
            )
            norms = [
                float(np.linalg.norm(np.frombuffer(blob, dtype="<f4")))
                for (blob,) in conn.execute(
                    "SELECT vector FROM report_chunk_vectors WHERE model_key=?",
                    (TEXT_MODEL_KEY,),
                )
            ]
            checks["vector_dim"] = all(
                len(np.frombuffer(blob, dtype="<f4")) == TEXT_DIM
                for (blob,) in conn.execute(
                    "SELECT vector FROM report_chunk_vectors WHERE model_key=?",
                    (TEXT_MODEL_KEY,),
                )
            )
            checks["l2_normalized"] = all(abs(x - 1.0) <= 1e-4 for x in norms)
            checks["no_nan"] = all(math.isfinite(x) for x in norms)
        else:
            norms = []

        fts_hits = conn.execute(
            "SELECT COUNT(*) FROM report_fts WHERE report_fts MATCH ?",
            ('"유지보수"',),
        ).fetchone()[0]
        checks["fts_smoke"] = fts_hits > 0

        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("PRAGMA optimize")
    finally:
        conn.close()

    os.replace(tmp, feature_store)
    for suffix in ("-wal", "-shm"):
        side = Path(str(feature_store) + suffix)
        if side.exists():
            side.unlink()

    return {
        "ok": all(bool(v) for v in checks.values()),
        "checks": checks,
        "documents": len(docs_by_id),
        "attachments": len(docs),
        "chunks": chunk_total,
        "images": image_total,
        "image_captions": caption_total,
        "vectors": len(vectors),
        "image_vectors": len(image_vectors),
        "dim": TEXT_DIM if embedder else None,
        "model": TEXT_MODEL_KEY if embedder else None,
        "fts_smoke_hits": fts_hits,
        "chunk_chars": chunk_chars,
        "chunk_overlap": overlap,
        "output": {
            "path": str(feature_store),
            "size_bytes": feature_store.stat().st_size,
            "sha256": _sha256_file(feature_store),
        },
        "elapsed_seconds": round(time.time() - started, 2),
    }


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--feature-store",
                    default=str(ROOT / "data" / "feature_store.sqlite3"))
    ap.add_argument("--reports-dir", default=str(ROOT / "data" / "reports"))
    ap.add_argument("--model-dir",
                    default=str(ROOT / "data" / "models" / "multilingual-e5-small"))
    ap.add_argument("--chunk-chars", type=int, default=CHUNK_CHARS)
    ap.add_argument("--overlap", type=int, default=CHUNK_OVERLAP)
    ap.add_argument("--no-embeddings", action="store_true")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    feature_store = Path(args.feature_store)
    reports_dir = Path(args.reports_dir)
    model_dir = None if args.no_embeddings else Path(args.model_dir)
    if not feature_store.is_file():
        sys.stderr.write(f"feature store not found: {feature_store}\n")
        return 2
    for name in ("report_documents.jsonl", "report_images.jsonl"):
        if not (reports_dir / name).is_file():
            sys.stderr.write(f"missing {reports_dir / name}\n")
            return 2

    result = build(
        feature_store, reports_dir, model_dir,
        max(200, args.chunk_chars), max(0, args.overlap),
    )
    validation_path = feature_store.parent / "report_index_validation.json"
    validation_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps({
        "ok": result["ok"], "documents": result["documents"],
        "attachments": result["attachments"],
        "chunks": result["chunks"], "images": result["images"],
        "image_captions": result["image_captions"],
        "vectors": result["vectors"], "image_vectors": result["image_vectors"],
        "dim": result["dim"],
        "fts_smoke_hits": result["fts_smoke_hits"],
        "elapsed_seconds": result["elapsed_seconds"],
    }, ensure_ascii=False))
    if not result["ok"]:
        bad = [k for k, v in result["checks"].items() if not v]
        sys.stderr.write("failed checks: " + ", ".join(bad) + "\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
