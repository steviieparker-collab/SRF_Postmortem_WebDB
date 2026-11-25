"""Build and query the local FTS5 index for the RAG assistant.

The MVP uses SQLite FTS5 with the ``trigram`` tokenizer.  It handles Korean
substrings, English technical terms and PV names without extra dependencies.
Dense embeddings can be added later without changing the document model.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from .config import get_rag_config
from .db import connect_events, connect_index, index_exists
from .documents import RagDocument, build_documents

SYNONYMS = {
    "쿼치": "quench",
    "퀜치": "quench",
    "유량계": "flowmeter",
    "유량": "water flow",
    "이온펌프": "ion pump",
    "이온 펌프": "ion pump",
    "빔덤프": "beam dump",
    "빔 덤프": "beam dump",
    "알람": "alarm",
    "인터록": "interlock",
    "냉각": "cooling",
    "진공": "vacuum",
    "온도": "temperature",
    "압력": "pressure",
    "전원": "power",
    "고주파": "RF",
}


def expand_query(query: str) -> str:
    """Add Korean/English synonyms and normalize common subsystem spellings."""
    # Split script boundaries so "KSU관련" -> "KSU 관련" and "2026년" -> "2026 년".
    out = re.sub(r"([A-Za-z0-9])([가-힣])", r"\1 \2", query)
    out = re.sub(r"([가-힣])([A-Za-z0-9])", r"\1 \2", out)
    for ko, en in SYNONYMS.items():
        if ko in query:
            out += " " + en
    out = re.sub(r"\b(KSU|CM|CRM)\s*#?\s*(\d)\b", r"\1\2", out, flags=re.IGNORECASE)
    # The event notes use both KSU3 and KSU#3 spellings; CM events use
    # CM#1/CRM1. Add variants so trigram search matches either form.
    for m in re.finditer(r"\b(KSU|CRM|CM)\s*#?\s*(\d)\b", query, re.IGNORECASE):
        base = "KSU" if m.group(1).upper() == "KSU" else "CRM"
        n = m.group(2)
        out += f" {base}{n} {base}#{n}"
        if base == "CRM":
            out += f" CM{n} CM#{n}"
    if re.search(r"KSU", query, re.IGNORECASE):
        out += " KSU1 KSU2 KSU3 KSU#1 KSU#2 KSU#3"
    if re.search(r"\b(CM|CRM)", query, re.IGNORECASE):
        out += " CM1 CM2 CM3 CRM1 CRM2 CRM3 SRF1 SRF2 SRF3"
    return out


SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_key       TEXT UNIQUE NOT NULL,
    doc_type      TEXT NOT NULL,
    event_id      TEXT DEFAULT '',
    pv            TEXT DEFAULT '',
    year          TEXT DEFAULT '',
    fault_type    TEXT DEFAULT '',
    subsystem     TEXT DEFAULT '',
    title         TEXT DEFAULT '',
    text          TEXT NOT NULL,
    metadata_json TEXT DEFAULT '{}',
    content_hash  TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rag_doc_type ON documents(doc_type);
CREATE INDEX IF NOT EXISTS idx_rag_event ON documents(event_id);
CREATE INDEX IF NOT EXISTS idx_rag_pv ON documents(pv);

CREATE TABLE IF NOT EXISTS chunks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    text        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rag_chunk_doc ON chunks(document_id);

CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
    text,
    content='chunks',
    content_rowid='id',
    tokenize='trigram'
);

CREATE TRIGGER IF NOT EXISTS chunks_ai AFTER INSERT ON chunks BEGIN
    INSERT INTO chunks_fts(rowid, text) VALUES (new.id, new.text);
END;
CREATE TRIGGER IF NOT EXISTS chunks_ad AFTER DELETE ON chunks BEGIN
    INSERT INTO chunks_fts(chunks_fts, rowid, text) VALUES('delete', old.id, old.text);
END;
CREATE TRIGGER IF NOT EXISTS chunks_au AFTER UPDATE ON chunks BEGIN
    INSERT INTO chunks_fts(chunks_fts, rowid, text) VALUES('delete', old.id, old.text);
    INSERT INTO chunks_fts(rowid, text) VALUES (new.id, new.text);
END;
"""


def _chunks(text: str, max_len: int = 1600, overlap: int = 120) -> list[str]:
    text = text.strip()
    if not text:
        return [""]
    if len(text) <= max_len:
        return [text]
    out: list[str] = []
    start = 0
    while start < len(text):
        end = min(len(text), start + max_len)
        if end < len(text):
            # prefer a line/sentence boundary near the end
            cut = max(text.rfind("\n", start, end), text.rfind(". ", start, end))
            if cut > start + max_len // 2:
                end = cut + 1
        out.append(text[start:end].strip())
        if end >= len(text):
            break
        start = max(end - overlap, start + 1)
    return out


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


def _insert_document(conn: sqlite3.Connection, doc: RagDocument) -> None:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO documents
            (doc_key, doc_type, event_id, pv, year, fault_type, subsystem,
             title, text, metadata_json, content_hash, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            doc.doc_key,
            doc.doc_type,
            doc.event_id,
            doc.pv,
            doc.year,
            doc.fault_type,
            doc.subsystem,
            doc.title,
            doc.text,
            json.dumps(doc.metadata, ensure_ascii=False),
            _hash(doc.text),
            now,
        ),
    )
    doc_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    for i, chunk in enumerate(_chunks(doc.text)):
        conn.execute(
            "INSERT INTO chunks (document_id, chunk_index, text) VALUES (?, ?, ?)",
            (doc_id, i, chunk),
        )


def _delete_document(conn: sqlite3.Connection, doc_key: str) -> None:
    row = conn.execute(
        "SELECT id FROM documents WHERE doc_key = ?", (doc_key,)
    ).fetchone()
    if not row:
        return
    doc_id = row[0]
    conn.execute("DELETE FROM chunks WHERE document_id = ?", (doc_id,))
    conn.execute("DELETE FROM documents WHERE id = ?", (doc_id,))


def ingest(full: bool = True, db_path: Optional[Path] = None) -> dict[str, Any]:
    """Build or incrementally refresh the RAG index from the live DB."""
    cfg = get_rag_config()
    events = connect_events(readonly=True)
    try:
        docs = build_documents(events)
    finally:
        events.close()

    idx = connect_index(readonly=False)
    try:
        _ensure_schema(idx)
        if full:
            idx.execute("DELETE FROM chunks")
            idx.execute("DELETE FROM documents")
            idx.commit()

        existing = {
            r["doc_key"]: r["content_hash"]
            for r in idx.execute("SELECT doc_key, content_hash FROM documents")
        }
        current_keys = {d.doc_key for d in docs}
        added = updated = skipped = removed = 0
        for doc in docs:
            h = _hash(doc.text)
            if existing.get(doc.doc_key) == h:
                skipped += 1
                continue
            if doc.doc_key in existing:
                _delete_document(idx, doc.doc_key)
                updated += 1
            else:
                added += 1
            _insert_document(idx, doc)
        # Drop documents that no longer exist in the source DB.
        for stale in set(existing) - current_keys:
            _delete_document(idx, stale)
            removed += 1
        idx.commit()
        try:
            idx.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.Error:
            pass
        return {
            "ok": True,
            "documents": len(docs),
            "added": added,
            "updated": updated,
            "skipped": skipped,
            "removed": removed,
            "index_path": str(cfg.index_path),
            "index_size": cfg.index_path.stat().st_size if cfg.index_path.exists() else 0,
        }
    finally:
        idx.close()


def _fts_query(query: str) -> tuple[str, list[str]]:
    """Return (fts_match_or_empty, short_terms_for_like)."""
    cleaned = re.sub(r"[^\w가-힣:#./-]+", " ", query).strip()
    tokens = [t for t in cleaned.split() if t]
    long_terms = [t for t in tokens if len(t) >= 3]
    short_terms = [t for t in tokens if 0 < len(t) < 3]
    if not long_terms:
        return "", short_terms
    # Quote each term and OR them so BM25 can rank partial matches.
    quoted = []
    for t in long_terms:
        t = t.replace('"', "")
        if t:
            quoted.append(f'"{t}"')
    return " OR ".join(quoted), short_terms


def _filter_sql(filters: Optional[dict[str, Any]]) -> tuple[str, list[Any]]:
    filters = filters or {}
    clauses: list[str] = []
    params: list[Any] = []
    if filters.get("doc_type"):
        clauses.append("d.doc_type = ?")
        params.append(filters["doc_type"])
    if filters.get("event_id"):
        clauses.append("d.event_id = ?")
        params.append(filters["event_id"])
    if filters.get("pv"):
        clauses.append("d.pv = ?")
        params.append(filters["pv"])
    if filters.get("year"):
        clauses.append("d.year = ?")
        params.append(str(filters["year"]))
    if filters.get("year_from"):
        clauses.append("d.year >= ?")
        params.append(str(filters["year_from"]))
    if filters.get("year_to"):
        clauses.append("d.year <= ?")
        params.append(str(filters["year_to"]))
    if filters.get("fault_type"):
        clauses.append("d.fault_type LIKE ?")
        params.append(f"%{filters['fault_type']}%")
    if filters.get("subsystem"):
        clauses.append("(d.subsystem = ? OR d.subsystem LIKE ?)")
        params.extend([filters["subsystem"], f"%{filters['subsystem']}%"])
    return (" AND " + " AND ".join(clauses)) if clauses else "", params


def _row_to_hit(r: sqlite3.Row) -> dict[str, Any]:
    try:
        meta = json.loads(r["metadata_json"] or "{}")
    except json.JSONDecodeError:
        meta = {}
    return {
        "doc_key": r["doc_key"],
        "doc_type": r["doc_type"],
        "event_id": r["event_id"],
        "pv": r["pv"],
        "year": r["year"],
        "fault_type": r["fault_type"],
        "subsystem": r["subsystem"],
        "title": r["title"],
        "text": r["text"],
        "metadata": meta,
        "score": r["score"] if "score" in r.keys() else None,
    }


def search(query: str, filters: Optional[dict[str, Any]] = None,
           top_k: Optional[int] = None) -> list[dict[str, Any]]:
    cfg = get_rag_config()
    top_k = top_k or cfg.top_k
    if not index_exists():
        return []
    conn = connect_index(readonly=True)
    try:
        where, params = _filter_sql(filters)
        expanded = expand_query(query)
        fts, short_terms = _fts_query(expanded)
        hits: list[dict[str, Any]] = []
        if fts:
            sql = (
                "SELECT c.text, d.doc_key, d.doc_type, d.event_id, d.pv, d.year, "
                "d.fault_type, d.subsystem, d.title, d.metadata_json, "
                "bm25(chunks_fts) AS score "
                "FROM chunks_fts "
                "JOIN chunks c ON c.id = chunks_fts.rowid "
                "JOIN documents d ON d.id = c.document_id "
                "WHERE chunks_fts MATCH ?" + where +
                " ORDER BY score - CASE WHEN d.doc_type='event_card' THEN 0.5 ELSE 0 END "
                "LIMIT ?"
            )
            hits = [_row_to_hit(r) for r in conn.execute(sql, [fts, *params, top_k])]
        if not hits:
            # LIKE fallback for short terms, punctuation-heavy queries, or
            # when FTS5's trigram tokenizer has no match.
            terms = [t for t in re.split(r"\s+", expanded.strip()) if t] or [query.strip()]
            if not terms:
                return []
            like_clauses = []
            like_params: list[Any] = []
            for t in terms[:6]:
                like_clauses.append("(c.text LIKE ? OR d.text LIKE ?)")
                like_params.extend([f"%{t}%", f"%{t}%"])
            sql = (
                "SELECT c.text, d.doc_key, d.doc_type, d.event_id, d.pv, d.year, "
                "d.fault_type, d.subsystem, d.title, d.metadata_json, "
                "0.0 AS score "
                "FROM chunks c JOIN documents d ON d.id = c.document_id "
                "WHERE (" + " OR ".join(like_clauses) + ")" + where +
                " ORDER BY CASE d.doc_type WHEN 'event_card' THEN 0 ELSE 1 END, "
                "d.year DESC LIMIT ?"
            )
            hits = [_row_to_hit(r) for r in conn.execute(sql, [*like_params, *params, top_k])]
        return hits
    finally:
        conn.close()


def stats() -> dict[str, Any]:
    cfg = get_rag_config()
    if not index_exists():
        return {
            "available": False,
            "index_path": str(cfg.index_path),
            "model": cfg.llm_model,
            "api_key_configured": bool(cfg.api_key),
        }
    conn = connect_index(readonly=True)
    try:
        by_type = {
            r["doc_type"]: r["n"]
            for r in conn.execute(
                "SELECT doc_type, count(*) AS n FROM documents GROUP BY doc_type"
            )
        }
        total_docs = conn.execute("SELECT count(*) FROM documents").fetchone()[0]
        total_chunks = conn.execute("SELECT count(*) FROM chunks").fetchone()[0]
        last = conn.execute("SELECT max(updated_at) FROM documents").fetchone()[0]
        return {
            "available": True,
            "index_path": str(cfg.index_path),
            "index_size": cfg.index_path.stat().st_size,
            "documents": total_docs,
            "chunks": total_chunks,
            "by_type": by_type,
            "last_ingest": last,
            "model": cfg.llm_model,
            "api_key_configured": bool(cfg.api_key),
        }
    finally:
        conn.close()
