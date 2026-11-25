"""Report search over the static feature store (Task 4).

Retrieval is hybrid:
  * FTS5/trigram lexical search (title + chunk text)
  * dense cosine over ``text_e5small_v1`` chunk vectors
  * reciprocal rank fusion (RRF) combines the two rankings

Everything runs on CPU.  If the embedding model or onnxruntime is unavailable,
the tool degrades to FTS-only and says so in ``dense_available``.
"""

from __future__ import annotations

import json
import math
import re
import sqlite3
from pathlib import Path
from typing import Any, Optional

import numpy as np

from .embedding import E5Embedder, TEXT_DIM, TEXT_MODEL_KEY
from .store import connect_feature_store, feature_store_path

_CACHE: Optional[dict[str, Any]] = None
_IMAGE_CACHE: Optional[dict[str, Any]] = None
_EMBEDDER: Any = None
_EMBEDDER_ERROR: Optional[str] = None

SYNONYMS = {
    "켄치": "quench",
    "퀜치": "quench",
    "쿼치": "quench",
    "이온펌프": "ion pump",
    "이온 펌프": "ion pump",
    "빔덤프": "beam dump",
    "빔 덤프": "beam dump",
    "캐비티": "cavity",
    "반사파": "reflected power",
    "인터록": "interlock",
    "알람": "alarm",
    "진공": "vacuum",
    "냉각": "cooling",
    "온도": "temperature",
    "압력": "pressure",
    "유량": "water flow",
    "전원": "power",
    "고주파": "RF",
}

RRF_K = 60
# e5-small has a high similarity floor; with no lexical hit at all, anything
# below this is usually an unrelated nearest neighbour.
MIN_DENSE_NO_FTS = 0.85


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

def report_store_stats() -> dict[str, Any]:
    """Counts for postmortem_get_capabilities."""
    path = feature_store_path()
    if not path.is_file():
        return {"available": False, "path": str(path)}
    try:
        conn = connect_feature_store()
        try:
            counts = {
                "documents": conn.execute(
                    "SELECT COUNT(*) FROM report_documents"
                ).fetchone()[0],
                "attachments": conn.execute(
                    "SELECT COUNT(*) FROM report_document_events"
                ).fetchone()[0],
                "chunks": conn.execute(
                    "SELECT COUNT(*) FROM report_chunks"
                ).fetchone()[0],
                "images": conn.execute(
                    "SELECT COUNT(*) FROM report_images"
                ).fetchone()[0],
                "vectors": conn.execute(
                    "SELECT COUNT(*) FROM report_chunk_vectors WHERE model_key=?",
                    (TEXT_MODEL_KEY,),
                ).fetchone()[0],
                "image_captions": conn.execute(
                    "SELECT COUNT(*) FROM report_images "
                    "WHERE trim(coalesce(caption,'')) <> '' "
                    "OR trim(coalesce(ocr_text,'')) <> ''"
                ).fetchone()[0],
                "image_vectors": conn.execute(
                    "SELECT COUNT(*) FROM report_image_vectors WHERE model_key=?",
                    (TEXT_MODEL_KEY,),
                ).fetchone()[0],
            }
            model = conn.execute(
                "SELECT model_key, model_name, version, dim, normalize "
                "FROM model_versions WHERE model_key=?",
                (TEXT_MODEL_KEY,),
            ).fetchone()
        finally:
            conn.close()
    except sqlite3.Error as exc:
        return {"available": False, "path": str(path), "error": str(exc)}
    return {
        "available": True,
        "path": str(path),
        "counts": counts,
        "model": dict(model) if model else None,
        "text_only": False,
        "caption_model": "Qwen/Qwen2.5-VL-3B-Instruct",
    }


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def expand_query(query: str) -> str:
    out = query
    lowered = query.lower()
    for ko, en in SYNONYMS.items():
        if ko in query or ko.lower() in lowered:
            out += " " + en
    out = re.sub(r"([A-Za-z0-9])([가-힣])", r"\1 \2", out)
    out = re.sub(r"([가-힣])([A-Za-z0-9])", r"\1 \2", out)
    out = re.sub(r"\b(KSU|CM|CRM)\s*#?\s*(\d)\b", r"\1\2", out,
                 flags=re.IGNORECASE)
    return out


def _fts_match(query: str) -> tuple[str, list[str]]:
    cleaned = re.sub(r"[^\w가-힣:#./-]+", " ", query).strip()
    tokens = [t for t in cleaned.split() if t]
    long_terms = [t for t in tokens if len(t) >= 3]
    short_terms = [t for t in tokens if 0 < len(t) < 3]
    if not long_terms:
        return "", short_terms
    quoted = []
    for term in long_terms:
        term = term.replace('"', "")
        if term:
            quoted.append(f'"{term}"')
    return " OR ".join(quoted), short_terms


def _event_clause(
    event_id: Optional[str], alias: str = "c"
) -> tuple[str, list[Any]]:
    if not event_id:
        return "", []
    return (
        " AND EXISTS (SELECT 1 FROM report_document_events m "
        f"WHERE m.doc_id = {alias}.doc_id AND m.event_id = ?)",
        [event_id],
    )


def _fts_search(
    conn: sqlite3.Connection, query: str, event_id: Optional[str], limit: int
) -> list[dict[str, Any]]:
    expanded = expand_query(query)
    match, short_terms = _fts_match(expanded)
    clause, clause_params = _event_clause(event_id, "c")
    rows: list[dict[str, Any]] = []
    if match:
        sql = (
            "SELECT c.chunk_id, c.doc_id, c.ord, c.text, d.title, d.event_id, "
            "bm25(report_fts) AS score "
            "FROM report_fts "
            "JOIN report_chunks c ON c.chunk_id = report_fts.rowid "
            "JOIN report_documents d ON d.doc_id = c.doc_id "
            "WHERE report_fts MATCH ?" + clause +
            " ORDER BY score LIMIT ?"
        )
        rows = [dict(r) for r in conn.execute(
            sql, [match, *clause_params, limit]
        )]
    if rows:
        return rows

    # LIKE fallback: short terms, punctuation-heavy queries, or no trigram hit.
    terms = [t for t in re.split(r"\s+", expanded.strip()) if t] or [query.strip()]
    if not terms:
        return []
    like = []
    params: list[Any] = []
    for term in terms[:6]:
        like.append("(c.text LIKE ? OR d.title LIKE ?)")
        params.extend([f"%{term}%", f"%{term}%"])
    sql = (
        "SELECT c.chunk_id, c.doc_id, c.ord, c.text, d.title, d.event_id, "
        "0.0 AS score "
        "FROM report_chunks c JOIN report_documents d ON d.doc_id = c.doc_id "
        "WHERE (" + " OR ".join(like) + ")" + clause +
        " LIMIT ?"
    )
    return [dict(r) for r in conn.execute(sql, [*params, *clause_params, limit])]


def _load_chunks() -> dict[str, Any]:
    global _CACHE
    path = feature_store_path()
    stat = path.stat()
    if (
        _CACHE is not None
        and _CACHE["path"] == str(path)
        and _CACHE["mtime_ns"] == stat.st_mtime_ns
        and _CACHE["size"] == stat.st_size
    ):
        return _CACHE

    conn = connect_feature_store()
    try:
        rows = list(conn.execute(
            "SELECT c.chunk_id, c.doc_id, c.ord, c.text, d.title, d.event_id, "
            "d.meta_json, v.vector "
            "FROM report_chunk_vectors v "
            "JOIN report_chunks c ON c.chunk_id = v.chunk_id "
            "JOIN report_documents d ON d.doc_id = c.doc_id "
            "WHERE v.model_key=?",
            (TEXT_MODEL_KEY,),
        ))
    finally:
        conn.close()

    ids: list[int] = []
    doc_ids: list[str] = []
    ords: list[int] = []
    texts: list[str] = []
    titles: list[str] = []
    event_ids: list[str] = []
    event_lists: list[list[str]] = []
    vectors = np.empty((len(rows), TEXT_DIM), dtype=np.float32)
    for i, row in enumerate(rows):
        ids.append(row["chunk_id"])
        doc_ids.append(row["doc_id"])
        ords.append(row["ord"])
        texts.append(row["text"])
        titles.append(row["title"])
        event_ids.append(row["event_id"])
        try:
            meta = json.loads(row["meta_json"] or "{}")
        except json.JSONDecodeError:
            meta = {}
        event_lists.append(meta.get("event_ids") or [row["event_id"]])
        vectors[i] = np.frombuffer(row["vector"], dtype="<f4")

    _CACHE = {
        "path": str(path),
        "mtime_ns": stat.st_mtime_ns,
        "size": stat.st_size,
        "ids": ids,
        "doc_ids": doc_ids,
        "ords": ords,
        "texts": texts,
        "titles": titles,
        "event_ids": event_ids,
        "event_lists": event_lists,
        "vectors": vectors,
        "index": {chunk_id: i for i, chunk_id in enumerate(ids)},
    }
    return _CACHE


def _load_images() -> dict[str, Any]:
    """Captioned images (caption FTS + e5 vector), cached by DB mtime."""
    global _IMAGE_CACHE
    path = feature_store_path()
    stat = path.stat()
    if (
        _IMAGE_CACHE is not None
        and _IMAGE_CACHE["path"] == str(path)
        and _IMAGE_CACHE["mtime_ns"] == stat.st_mtime_ns
        and _IMAGE_CACHE["size"] == stat.st_size
    ):
        return _IMAGE_CACHE

    conn = connect_feature_store()
    try:
        rows = list(conn.execute(
            "SELECT i.image_id, i.doc_id, i.ord, i.source_path, i.caption, "
            "i.ocr_text, d.title, d.event_id, d.meta_json, v.vector "
            "FROM report_image_vectors v "
            "JOIN report_images i ON i.image_id = v.image_id "
            "JOIN report_documents d ON d.doc_id = i.doc_id "
            "WHERE v.model_key=?",
            (TEXT_MODEL_KEY,),
        ))
    finally:
        conn.close()

    image_ids: list[int] = []
    doc_ids: list[str] = []
    ords: list[int] = []
    source_paths: list[str] = []
    captions: list[str] = []
    ocr_texts: list[str] = []
    titles: list[str] = []
    event_ids: list[str] = []
    event_lists: list[list[str]] = []
    vectors = np.empty((len(rows), TEXT_DIM), dtype=np.float32)
    for i, row in enumerate(rows):
        image_ids.append(row["image_id"])
        doc_ids.append(row["doc_id"])
        ords.append(row["ord"])
        source_paths.append(row["source_path"])
        captions.append(row["caption"] or "")
        ocr_texts.append(row["ocr_text"] or "")
        titles.append(row["title"])
        event_ids.append(row["event_id"])
        try:
            meta = json.loads(row["meta_json"] or "{}")
        except json.JSONDecodeError:
            meta = {}
        event_lists.append(meta.get("event_ids") or [row["event_id"]])
        vectors[i] = np.frombuffer(row["vector"], dtype="<f4")

    _IMAGE_CACHE = {
        "path": str(path),
        "mtime_ns": stat.st_mtime_ns,
        "size": stat.st_size,
        "image_ids": image_ids,
        "doc_ids": doc_ids,
        "ords": ords,
        "source_paths": source_paths,
        "captions": captions,
        "ocr_texts": ocr_texts,
        "titles": titles,
        "event_ids": event_ids,
        "event_lists": event_lists,
        "vectors": vectors,
        "index": {image_id: i for i, image_id in enumerate(image_ids)},
    }
    return _IMAGE_CACHE


def _fts_image_search(
    conn: sqlite3.Connection, query: str, event_id: Optional[str], limit: int
) -> list[dict[str, Any]]:
    expanded = expand_query(query)
    match, short_terms = _fts_match(expanded)
    clause, clause_params = _event_clause(event_id, "i")
    rows: list[dict[str, Any]] = []
    if match:
        sql = (
            "SELECT i.image_id, i.doc_id, i.ord, i.source_path, i.caption, "
            "i.ocr_text, d.title, d.event_id, bm25(report_image_fts) AS score "
            "FROM report_image_fts "
            "JOIN report_images i ON i.image_id = report_image_fts.rowid "
            "JOIN report_documents d ON d.doc_id = i.doc_id "
            "WHERE report_image_fts MATCH ?" + clause +
            " ORDER BY score LIMIT ?"
        )
        rows = [dict(r) for r in conn.execute(
            sql, [match, *clause_params, limit]
        )]
    if rows:
        return rows

    terms = [t for t in re.split(r"\s+", expanded.strip()) if t] or [query.strip()]
    if not terms:
        return []
    like = []
    params: list[Any] = []
    for term in terms[:6]:
        like.append("(i.caption LIKE ? OR i.ocr_text LIKE ? OR d.title LIKE ?)")
        params.extend([f"%{term}%", f"%{term}%", f"%{term}%"])
    sql = (
        "SELECT i.image_id, i.doc_id, i.ord, i.source_path, i.caption, "
        "i.ocr_text, d.title, d.event_id, 0.0 AS score "
        "FROM report_images i JOIN report_documents d ON d.doc_id = i.doc_id "
        "WHERE (" + " OR ".join(like) + ")" + clause +
        " LIMIT ?"
    )
    return [dict(r) for r in conn.execute(sql, [*params, *clause_params, limit])]


def _get_embedder() -> Optional[E5Embedder]:
    global _EMBEDDER, _EMBEDDER_ERROR
    if _EMBEDDER is not None:
        return _EMBEDDER
    if _EMBEDDER_ERROR is not None:
        return None
    try:
        _EMBEDDER = E5Embedder()
    except Exception as exc:  # noqa: BLE001 - degrade to FTS-only
        _EMBEDDER_ERROR = f"{type(exc).__name__}: {exc}"
        return None
    return _EMBEDDER


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

def search_reports(
    query: str, event_id: Optional[str] = None, top_k: int = 8
) -> dict[str, Any]:
    query = (query or "").strip()
    top_k = max(1, min(int(top_k or 8), 30))
    path = feature_store_path()
    if not path.is_file():
        return {
            "response_type": "report_search",
            "available": False,
            "query": query,
            "error": f"feature store not found: {path}",
        }
    try:
        conn = connect_feature_store()
        try:
            doc_count = conn.execute(
                "SELECT COUNT(*) FROM report_documents"
            ).fetchone()[0]
            fts_rows = _fts_search(conn, query, event_id, limit=top_k * 4)
            fts_image_rows = _fts_image_search(
                conn, query, event_id, limit=top_k * 4
            )
        finally:
            conn.close()
        chunks = _load_chunks()
        images = _load_images()
    except sqlite3.Error as exc:
        return {
            "response_type": "report_search",
            "available": False,
            "query": query,
            "error": str(exc),
        }

    if doc_count == 0:
        return {
            "response_type": "report_search",
            "available": False,
            "query": query,
            "error": "no reports are indexed",
        }

    embedder = _get_embedder()
    dense_available = embedder is not None
    qvec = embedder.encode_one(query, "query: ") if dense_available else None

    def _mask(scores: np.ndarray, event_lists: list[list[str]]) -> np.ndarray:
        if not event_id:
            return scores
        allowed = np.array(
            [event_id in eids for eids in event_lists], dtype=bool
        )
        if allowed.any():
            return np.where(allowed, scores, -np.inf)
        return np.full_like(scores, -np.inf)

    dense_scores: Optional[np.ndarray] = None
    if dense_available and len(chunks["ids"]):
        dense_scores = _mask(chunks["vectors"] @ qvec, chunks["event_lists"])

    image_dense: Optional[np.ndarray] = None
    if dense_available and len(images["image_ids"]):
        image_dense = _mask(images["vectors"] @ qvec, images["event_lists"])

    # RRF over chunk text and image captions, dense and lexical rankings.
    rrf: dict[tuple[str, int], float] = {}
    dense_by_key: dict[tuple[str, int], Optional[float]] = {}

    if dense_scores is not None:
        order = np.argsort(-dense_scores)
        rank = 0
        for idx in order:
            value = float(dense_scores[idx])
            if not math.isfinite(value):
                continue
            rank += 1
            key = ("chunk", int(chunks["ids"][idx]))
            rrf[key] = rrf.get(key, 0.0) + 1.0 / (RRF_K + rank)
            dense_by_key[key] = value
            if rank >= top_k * 4:
                break
    for rank, row in enumerate(fts_rows, 1):
        key = ("chunk", int(row["chunk_id"]))
        rrf[key] = rrf.get(key, 0.0) + 1.0 / (RRF_K + rank)

    if image_dense is not None:
        order = np.argsort(-image_dense)
        rank = 0
        for idx in order:
            value = float(image_dense[idx])
            if not math.isfinite(value):
                continue
            rank += 1
            key = ("image", int(images["image_ids"][idx]))
            rrf[key] = rrf.get(key, 0.0) + 1.0 / (RRF_K + rank)
            dense_by_key[key] = value
            if rank >= top_k * 4:
                break
    for rank, row in enumerate(fts_image_rows, 1):
        key = ("image", int(row["image_id"]))
        rrf[key] = rrf.get(key, 0.0) + 1.0 / (RRF_K + rank)

    fts_chunk_scores = {
        int(r["chunk_id"]): float(r["score"]) for r in fts_rows
    }
    fts_image_scores = {
        int(r["image_id"]): float(r["score"]) for r in fts_image_rows
    }

    ranked = sorted(rrf.items(), key=lambda kv: (-kv[1], kv[0]))
    items = []
    for (kind, ref_id), score in ranked[:top_k]:
        if kind == "chunk":
            idx = chunks["index"].get(ref_id)
            if idx is None:
                continue
            event_ids = chunks["event_lists"][idx]
            primary_event = chunks["event_ids"][idx] or (
                event_ids[0] if event_ids else ""
            )
            dense_score = dense_by_key.get(("chunk", ref_id))
            items.append({
                "kind": "chunk",
                "chunk_id": ref_id,
                "doc_id": chunks["doc_ids"][idx],
                "event_id": primary_event,
                "event_ids": event_ids,
                "title": chunks["titles"][idx],
                "ord": chunks["ords"][idx],
                "text": chunks["texts"][idx],
                "score": round(score, 6),
                "dense_score": (
                    round(dense_score, 6) if dense_score is not None else None
                ),
                "fts_score": (
                    round(fts_chunk_scores[ref_id], 6)
                    if ref_id in fts_chunk_scores else None
                ),
                "citation": f"[R:{primary_event}/{chunks['doc_ids'][idx][:12]}]",
                "url": f"/events/{primary_event}" if primary_event else "",
            })
        else:
            idx = images["index"].get(ref_id)
            if idx is None:
                continue
            event_ids = images["event_lists"][idx]
            primary_event = images["event_ids"][idx] or (
                event_ids[0] if event_ids else ""
            )
            dense_score = dense_by_key.get(("image", ref_id))
            items.append({
                "kind": "image",
                "image_id": ref_id,
                "doc_id": images["doc_ids"][idx],
                "event_id": primary_event,
                "event_ids": event_ids,
                "title": images["titles"][idx],
                "ord": images["ords"][idx],
                "caption": images["captions"][idx],
                "ocr_text": images["ocr_texts"][idx],
                "source_path": images["source_paths"][idx],
                "score": round(score, 6),
                "dense_score": (
                    round(dense_score, 6) if dense_score is not None else None
                ),
                "fts_score": (
                    round(fts_image_scores[ref_id], 6)
                    if ref_id in fts_image_scores else None
                ),
                "citation": (
                    f"[R:{primary_event}/{images['doc_ids'][idx][:12]}"
                    f"#img{images['ords'][idx]}]"
                ),
                "url": f"/events/{primary_event}" if primary_event else "",
            })

    suppressed = False
    if (
        items
        and dense_available
        and all(item["fts_score"] is None for item in items)
        and max((item["dense_score"] or 0.0) for item in items)
        < MIN_DENSE_NO_FTS
    ):
        items = []
        suppressed = True

    caveats = [
        "Text chunks and image captions/OCR are both searchable. Captions were "
        "generated with Qwen2.5-VL-3B on a local GPU; verify values against the "
        "source image before quoting them.",
        "91 unique documents from 96 attachments; byte-identical copies "
        "(5 PDFs, 2 HWPs) share one doc_id and keep all event links.",
        "HWP text includes tables via hwp5html; PDF text requires an embedded "
        "text layer (scanned pages contribute images only).",
        "Scores are reciprocal-rank fusion of cosine (e5-small) and "
        "FTS5 trigram; they are ranking scores, not probabilities.",
    ]
    if not dense_available:
        caveats.append(
            f"Dense search unavailable ({_EMBEDDER_ERROR}); FTS-only results."
        )
    if event_id:
        caveats.append(
            f"Filtered to reports linked to event {event_id} via "
            "report_document_events."
        )
    if suppressed:
        caveats.append(
            "No lexical match and the best dense score was below "
            f"{MIN_DENSE_NO_FTS}; low-confidence neighbour results were hidden."
        )

    return {
        "response_type": "report_search",
        "available": True,
        "query": query,
        "event_id": event_id or "",
        "top_k": top_k,
        "returned": len(items),
        "suppressed": suppressed,
        "dense_available": dense_available,
        "documents_indexed": doc_count,
        "chunks_indexed": len(chunks["ids"]),
        "images_indexed": len(images["image_ids"]),
        "items": items,
        "caveats": caveats,
    }
