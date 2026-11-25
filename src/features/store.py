"""Read-only access to the static waveform feature store.

The MCP runtime reads ``data/feature_store.sqlite3`` (built offline by
``scripts/build_feature_store.py``).  Nothing here writes, trains, or calls an
external service.

The similarity path is deliberately pure Python: 311 x 78 float32 is tiny, and
it keeps the MCP venv free of numpy.  Vectors are cached until the file's mtime
changes, so a rebuilt store is picked up without restarting the server.
"""

from __future__ import annotations

import json
import math
import os
import sqlite3
import sys
from array import array
from pathlib import Path
from typing import Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
MODEL_KEY = "waveform_manual_v1"

_CACHE: Optional[dict[str, Any]] = None


# ---------------------------------------------------------------------------
# Connection / metadata
# ---------------------------------------------------------------------------

def feature_store_path() -> Path:
    """Feature store location; override with ``FEATURE_STORE_PATH``."""
    return Path(
        os.getenv(
            "FEATURE_STORE_PATH",
            str(PROJECT_ROOT / "data" / "feature_store.sqlite3"),
        )
    )


def feature_store_available() -> bool:
    path = feature_store_path()
    return path.is_file() and path.stat().st_size > 0


def connect_feature_store() -> sqlite3.Connection:
    path = feature_store_path()
    if not path.is_file():
        raise FileNotFoundError(f"feature store not found: {path}")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def feature_store_stats() -> dict[str, Any]:
    """Availability, snapshot, model and row counts for capabilities."""
    path = feature_store_path()
    if not feature_store_available():
        return {"available": False, "path": str(path)}
    try:
        conn = connect_feature_store()
        try:
            snapshot = conn.execute(
                "SELECT snapshot_id, created_at, db_sha256, event_count, "
                "parquet_count, report_count FROM snapshots LIMIT 1"
            ).fetchone()
            model = conn.execute(
                "SELECT model_key, model_name, version, dim, normalize, created_at "
                "FROM model_versions WHERE model_key=?",
                (MODEL_KEY,),
            ).fetchone()
            counts = {
                "event_features": conn.execute(
                    "SELECT COUNT(*) FROM event_features"
                ).fetchone()[0],
                "waveform_vectors": conn.execute(
                    "SELECT COUNT(*) FROM waveform_vectors WHERE model_key=?",
                    (MODEL_KEY,),
                ).fetchone()[0],
                "report_documents": conn.execute(
                    "SELECT COUNT(*) FROM report_documents"
                ).fetchone()[0],
            }
        finally:
            conn.close()
    except sqlite3.Error as exc:  # stale/partial file
        return {"available": False, "path": str(path), "error": str(exc)}
    return {
        "available": True,
        "path": str(path),
        "size_bytes": path.stat().st_size,
        "snapshot": dict(snapshot) if snapshot else None,
        "model": dict(model) if model else None,
        "counts": counts,
    }


# ---------------------------------------------------------------------------
# Per-event features
# ---------------------------------------------------------------------------

def get_waveform_features(event_id: str) -> Optional[dict[str, Any]]:
    """Return one event's stored record, or None if it has no parquet."""
    conn = connect_feature_store()
    try:
        row = conn.execute(
            "SELECT event_id, snapshot_id, timestamp, t0_offset_s, quench_unit, "
            "has_waveform, features_json, input_hash "
            "FROM event_features WHERE event_id=?",
            (event_id,),
        ).fetchone()
        model = conn.execute(
            "SELECT model_key, model_name, version, dim, normalize "
            "FROM model_versions WHERE model_key=?",
            (MODEL_KEY,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    try:
        features = json.loads(row["features_json"])
    except json.JSONDecodeError:
        features = {}
    return {
        "event_id": row["event_id"],
        "snapshot_id": row["snapshot_id"],
        "timestamp": row["timestamp"],
        "t0_offset_s": row["t0_offset_s"],
        "quench_unit": row["quench_unit"],
        "has_waveform": bool(row["has_waveform"]),
        "input_hash": row["input_hash"],
        "model": dict(model) if model else None,
        "features": features,
    }


# ---------------------------------------------------------------------------
# Similarity (pure Python, cached)
# ---------------------------------------------------------------------------

def _decode_vector(blob: bytes) -> array:
    vec = array("f")
    vec.frombytes(blob)
    if sys.byteorder != "little":
        vec.byteswap()
    return vec


def _norm(vec: array) -> float:
    return math.sqrt(sum(x * x for x in vec))


def _dot(a: array, b: array) -> float:
    return sum(x * y for x, y in zip(a, b))


def _load_vectors() -> dict[str, Any]:
    """Load and cache all vectors; invalidated when the DB mtime changes."""
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
        model_row = conn.execute(
            "SELECT model_key, model_name, version, dim FROM model_versions "
            "WHERE model_key=?",
            (MODEL_KEY,),
        ).fetchone()
        if model_row is None:
            raise LookupError(f"model {MODEL_KEY} not found in feature store")
        dim = int(model_row["dim"])
        ids: list[str] = []
        vectors: dict[str, array] = {}
        norms: dict[str, float] = {}
        meta: dict[str, dict[str, Any]] = {}
        event_meta: dict[str, dict[str, Any]] = {}
        for row in conn.execute(
            "SELECT w.event_id, w.vector, w.meta_json, e.timestamp, e.quench_unit "
            "FROM waveform_vectors w "
            "JOIN event_features e ON e.event_id = w.event_id "
            "WHERE w.model_key=?",
            (MODEL_KEY,),
        ):
            vec = _decode_vector(row["vector"])
            if len(vec) != dim:
                raise ValueError(
                    f"vector {row['event_id']} has dim {len(vec)}, expected {dim}"
                )
            eid = row["event_id"]
            ids.append(eid)
            vectors[eid] = vec
            norms[eid] = _norm(vec)
            try:
                meta[eid] = json.loads(row["meta_json"] or "{}")
            except json.JSONDecodeError:
                meta[eid] = {}
            event_meta[eid] = {
                "timestamp": row["timestamp"],
                "quench_unit": row["quench_unit"],
            }
    finally:
        conn.close()

    _CACHE = {
        "path": str(path),
        "mtime_ns": stat.st_mtime_ns,
        "size": stat.st_size,
        "model": dict(model_row),
        "dim": dim,
        "ids": ids,
        "vectors": vectors,
        "norms": norms,
        "meta": meta,
        "event_meta": event_meta,
    }
    return _CACHE


_SIGNATURE_FIELDS = (
    "cavity_sag_depth",
    "cavity_pre_state_ratio",
    "reflect_rise_ratio",
    "forward_trip_ratio",
    "forward_trip_time_s",
)


def _signature_summary(features: dict[str, Any]) -> dict[str, Any]:
    """Compact CM1 signature plus beam fall for comparison tables."""
    signature = (features.get("signature") or {}).get("CM1") or {}
    out = {
        "quench_unit": features.get("quench_unit"),
        "quench_unit_source": features.get("quench_unit_source"),
        "beam_fall_90_10_s": (features.get("beam_fall") or {}).get("fall_90_10_s"),
    }
    for key in _SIGNATURE_FIELDS:
        out[key] = signature.get(key)
    return out


def _round(value: Optional[float], digits: int = 6) -> Optional[float]:
    if value is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    return round(value, digits)


def find_similar_waveforms(
    event_id: str, top_k: int = 5
) -> dict[str, Any]:
    """Cosine top-k over the manual waveform vectors."""
    top_k = max(1, min(int(top_k or 5), 20))
    data = _load_vectors()
    target = data["vectors"].get(event_id)
    if target is None:
        return {
            "response_type": "waveform_similarity",
            "available": False,
            "event_id": event_id,
            "error": "event has no waveform feature vector "
                     "(no parquet, or not in this snapshot)",
            "feature_store": {
                "events": len(data["ids"]),
                "model_key": data["model"]["model_key"],
            },
        }

    target_norm = data["norms"][event_id]
    scored: list[tuple[str, float]] = []
    for eid, vec in data["vectors"].items():
        if eid == event_id:
            continue
        denom = target_norm * data["norms"][eid]
        score = 0.0 if denom <= 0 else _dot(target, vec) / denom
        scored.append((eid, score))
    scored.sort(key=lambda pair: pair[1], reverse=True)
    top = scored[:top_k]

    target_features = get_waveform_features(event_id) or {}
    target_summary = _signature_summary(target_features.get("features") or {})

    conn = connect_feature_store()
    try:
        items = []
        for eid, score in top:
            row = conn.execute(
                "SELECT timestamp, quench_unit, features_json FROM event_features "
                "WHERE event_id=?",
                (eid,),
            ).fetchone()
            features: dict[str, Any] = {}
            if row is not None:
                try:
                    features = json.loads(row["features_json"])
                except json.JSONDecodeError:
                    features = {}
            summary = _signature_summary(features)
            delta = {
                key: _round(
                    (summary.get(key) - target_summary.get(key))
                    if summary.get(key) is not None
                    and target_summary.get(key) is not None
                    else None
                )
                for key in _SIGNATURE_FIELDS
            }
            meta = data["meta"].get(eid) or {}
            items.append({
                "event_id": eid,
                "similarity": round(score, 4),
                "timestamp": (row["timestamp"] if row is not None else None),
                "fault_type": features.get("fault_type", ""),
                "quench_unit": (row["quench_unit"] if row is not None else None),
                "signature": summary,
                "delta_vs_target": delta,
                "n_imputed": meta.get("n_imputed", 0),
                "missing_channels": meta.get("missing_channels", []),
                "citation": f"[E:{eid}]",
                "url": f"/events/{eid}",
            })
    finally:
        conn.close()

    caveats = [
        "Similarity is cosine distance in a 78-dim hand-crafted feature space "
        "(waveform_manual_v1), not a learned embedding or physical equivalence.",
        "Only 311 of 866 events have parquet waveforms; 2023+ only.",
        "A high score means the stored features are close, not that the "
        "underlying fault mechanism is the same.",
    ]
    target_meta = data["meta"].get(event_id) or {}
    if target_meta.get("n_imputed"):
        caveats.append(
            f"Target event has {target_meta['n_imputed']} imputed features; "
            "its similarity is less reliable."
        )

    return {
        "response_type": "waveform_similarity",
        "available": True,
        "event_id": event_id,
        "model": data["model"],
        "metric": "cosine",
        "top_k": top_k,
        "returned": len(items),
        "feature_store_events": len(data["ids"]),
        "target": {
            "timestamp": data["event_meta"][event_id]["timestamp"],
            "signature": target_summary,
            "n_imputed": target_meta.get("n_imputed", 0),
            "missing_channels": target_meta.get("missing_channels", []),
            "citation": f"[E:{event_id}]",
            "url": f"/events/{event_id}",
        },
        "items": items,
        "caveats": caveats,
    }
