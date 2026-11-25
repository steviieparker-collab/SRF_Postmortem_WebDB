#!/usr/bin/env python3
"""Build the static feature store from extracted waveform features (Task 2).

Reads ``data/features/waveform_features.jsonl`` (Task 1 output) and the
events-db snapshot, applies ``db/feature_store.sql``, and writes one
self-contained ``feature_store.sqlite3``:

    snapshots              one row: snapshot id, db hash, counts
    model_versions         waveform_manual_v1 (78-dim, L2-normalised)
    feature_vector_specs   ordered feature names + imputer/scaler parameters
    event_features         311 rows: features_json = Task 1 record
    waveform_vectors       311 float32 BLOBs for CPU cosine similarity

The manual vector is deterministic and derived only from the Task 1 features.
It is *not* a learned embedding; the spec table makes it reproducible.

Outputs:
    <out>                          feature_store.sqlite3
    <out.parent>/feature_store_validation.json

Examples:
    venv/bin/python scripts/build_feature_store.py \
        --features data/features/waveform_features.jsonl \
        --events-db /tmp/events_snapshot_20261118.db \
        --schema db/feature_store.sql \
        --out data/feature_store.sqlite3 --force
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import numpy as np

ROOT = Path(__file__).resolve().parent.parent

SCHEMA_VERSION = 1
MODEL_KEY = "waveform_manual_v1"
MODEL_NAME = "srf_manual_waveform_features"
MODEL_VERSION = "1.0.0"

ANALOG_CHANNELS = [
    "BeamCurrent",
    "Cavity_SRF1", "Cavity_SRF2", "Cavity_SRF3",
    "Forward_SRF1", "Forward_SRF2", "Forward_SRF3",
    "Reflect_SRF1", "Reflect_SRF2", "Reflect_SRF3",
]
UNITS = {"CM1": 1, "CM2": 2, "CM3": 3}

# Same polarity lists as config.classification / extract_waveform_features.py.
ACTIVE_LOW = {
    "RDY_CM3_FC3_d", "RDY_CM2_FC2_d", "RDY_CM1_FC1_d",
    "HE-PR_CM3_FC3_d", "HE-PR_CM2_FC2_d", "HE-PR_CM1_FC1_d",
    "ARC_CM3_FC3_d", "ARC_CM2_FC2_d", "ARC_CM1_FC1_d",
    "QUEN_CM3_FC3_d", "QUEN_CM2_FC2_d", "QUEN_CM1_FC1_d",
    "VAC_CM3_FC3_d", "VAC_CM2_FC2_d", "VAC_CM1_FC1_d",
    "INT_IN_FC3_d", "INT_IC_FC2_d", "INT_IC_FC1_d",
}
ACTIVE_HIGH = {
    "INT_MIS1_IC_d", "INT_MIS2_IC_d",
    "INT_PSI2_IC_d", "INT_PSI1_IC_d",
    "RDY_KSU3_IC_d", "RDY_KSU2_IC_d", "RDY_KSU1_IC_d",
}
QUEN_CHANNELS = {f"QUEN_CM{n}_FC{n}_d": unit for unit, n in UNITS.items()}
INTERLOCK_CHANNELS = sorted((ACTIVE_LOW | ACTIVE_HIGH) - set(QUEN_CHANNELS))

POST_SCALE_CLIP = 5.0
TIME_MAX_MS = 100.0
RATIO_BOUNDS = (-10.0, 20.0)


# ---------------------------------------------------------------------------
# Vector specification
# ---------------------------------------------------------------------------

def build_feature_names() -> list[str]:
    names: list[str] = []
    for ch in ANALOG_CHANNELS:
        for key in ("min_ratio", "max_ratio", "recent_ratio"):
            names.append(f"{ch}.{key}")
    names += ["beam.fall_90_ms", "beam.fall_90_10_ms"]
    for unit in UNITS:
        for key in (
            "cavity_sag_depth", "cavity_pre_state_ratio",
            "reflect_rise_ratio", "reflect_pre_state_ratio",
            "forward_trip_ratio",
        ):
            names.append(f"{unit}.{key}")
        for key in ("forward_trip_time_ms", "reflect_peak_time_ms"):
            names.append(f"{unit}.{key}")
    for unit in UNITS:
        names.append(f"quen.{unit}")
    for ch in INTERLOCK_CHANNELS:
        names.append(f"interlock.{ch}.asserted")
    return names


FEATURE_NAMES = build_feature_names()


def raw_bounds(name: str) -> tuple[float, float]:
    if name.startswith("quen.") or name.startswith("interlock."):
        return (0.0, 1.0)
    if name.endswith("_ms"):
        return (0.0, TIME_MAX_MS)
    return RATIO_BOUNDS


def _ms(value: Optional[float]) -> Optional[float]:
    return None if value is None else float(value) * 1e3


def raw_features(record: dict[str, Any]) -> dict[str, Optional[float]]:
    """Flatten one Task-1 record into the fixed vector feature dictionary."""
    out: dict[str, Optional[float]] = {}
    analog = record.get("analog", {})
    for ch in ANALOG_CHANNELS:
        block = analog.get(ch, {})
        for key in ("min_ratio", "max_ratio", "recent_ratio"):
            out[f"{ch}.{key}"] = block.get(key)

    beam = record.get("beam_fall", {})
    out["beam.fall_90_ms"] = _ms(beam.get("fall_90_s"))
    out["beam.fall_90_10_ms"] = _ms(beam.get("fall_90_10_s"))

    signature = record.get("signature", {})
    for unit in UNITS:
        block = signature.get(unit, {})
        for key in (
            "cavity_sag_depth", "cavity_pre_state_ratio",
            "reflect_rise_ratio", "reflect_pre_state_ratio",
            "forward_trip_ratio",
        ):
            out[f"{unit}.{key}"] = block.get(key)
        out[f"{unit}.forward_trip_time_ms"] = _ms(block.get("forward_trip_time_s"))
        out[f"{unit}.reflect_peak_time_ms"] = _ms(block.get("reflect_peak_time_s"))

    digital = record.get("digital", {})
    for unit, n in UNITS.items():
        block = digital.get(f"QUEN_CM{n}_FC{n}_d")
        out[f"quen.{unit}"] = (
            None if block is None else (1.0 if block.get("asserted") else 0.0)
        )
    for ch in INTERLOCK_CHANNELS:
        block = digital.get(ch)
        out[f"interlock.{ch}.asserted"] = (
            None if block is None else (1.0 if block.get("asserted") else 0.0)
        )
    return out


def raw_matrix(records: list[dict[str, Any]]) -> np.ndarray:
    x = np.full((len(records), len(FEATURE_NAMES)), np.nan, dtype=np.float64)
    for i, record in enumerate(records):
        values = raw_features(record)
        for j, name in enumerate(FEATURE_NAMES):
            value = values.get(name)
            if value is None:
                continue
            try:
                value = float(value)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(value):
                continue
            lo, hi = raw_bounds(name)
            x[i, j] = min(max(value, lo), hi)
    return x


def fit_scaler(
    x: np.ndarray, names: list[str]
) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    """Median/MAD scaler across the fit records; never returns NaN/inf."""
    medians = np.zeros(len(names), dtype=np.float64)
    scales = np.ones(len(names), dtype=np.float64)
    for j, name in enumerate(names):
        column = x[:, j]
        values = column[np.isfinite(column)]
        if values.size == 0:
            medians[j], scales[j] = 0.0, 1.0
            continue
        med = float(np.median(values))
        mad = float(np.median(np.abs(values - med))) * 1.4826
        medians[j] = med
        scales[j] = mad if mad > 1e-9 else 1.0
    spec = {
        "scale_method": "median/MAD*1.4826",
        "zero_mad_scale": 1.0,
        "post_scale_clip": [-POST_SCALE_CLIP, POST_SCALE_CLIP],
        "l2_normalize": True,
        "impute": "median",
        "raw_bounds": [list(raw_bounds(n)) for n in names],
    }
    return medians, scales, spec


def transform(
    x: np.ndarray, medians: np.ndarray, scales: np.ndarray
) -> tuple[np.ndarray, list[list[str]], list[float]]:
    """Impute -> clip -> robust z-score -> clip -> L2-normalise."""
    z = np.empty_like(x, dtype=np.float64)
    missing: list[list[str]] = []
    for i in range(x.shape[0]):
        missing_i: list[str] = []
        for j, name in enumerate(FEATURE_NAMES):
            value = x[i, j]
            if not math.isfinite(value):
                missing_i.append(name)
                value = medians[j]
            lo, hi = raw_bounds(name)
            value = min(max(value, lo), hi)
            z[i, j] = (value - medians[j]) / scales[j]
        missing.append(missing_i)
    np.clip(z, -POST_SCALE_CLIP, POST_SCALE_CLIP, out=z)
    norms = np.linalg.norm(z, axis=1)
    zero_rows = np.flatnonzero(norms <= 0)
    if zero_rows.size:
        z[zero_rows, 0] = 1.0
        norms = np.linalg.norm(z, axis=1)
    z /= norms[:, None]
    return z, missing, norms.tolist()


# ---------------------------------------------------------------------------
# Input
# ---------------------------------------------------------------------------

def _sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(chunk), b""):
            digest.update(block)
    return digest.hexdigest()


def load_features(path: Path) -> list[dict[str, Any]]:
    records = []
    with path.open(encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{lineno}: {exc}") from exc
            if record.get("schema_version") != SCHEMA_VERSION:
                raise ValueError(
                    f"{path}:{lineno}: schema_version "
                    f"{record.get('schema_version')} != {SCHEMA_VERSION}"
                )
            records.append(record)
    if not records:
        raise ValueError(f"{path}: no feature records")
    ids = [r["event_id"] for r in records]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate event_id in feature JSONL")
    return records


def db_counts(events_db: Path) -> dict[str, int]:
    conn = sqlite3.connect(f"file:{events_db}?mode=ro", uri=True)
    try:
        return {
            "event_count": conn.execute("SELECT COUNT(*) FROM events").fetchone()[0],
            "report_count": conn.execute(
                "SELECT COUNT(*) FROM event_attachments"
            ).fetchone()[0],
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build(
    records: list[dict[str, Any]],
    vectors: np.ndarray,
    missing: list[list[str]],
    medians: np.ndarray,
    scales: np.ndarray,
    spec: dict[str, Any],
    events_db: Path,
    schema_path: Path,
    out: Path,
    snapshot_id: str,
    snapshot_notes: str,
) -> dict[str, Any]:
    started = time.time()
    created_at = datetime.now(timezone.utc).isoformat()
    db_sha = _sha256_file(events_db)
    counts = db_counts(events_db)

    tmp = out.with_name(out.name + ".tmp")
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(tmp) + suffix)
        if candidate.exists():
            candidate.unlink()

    spec_out = {
        "schema_version": SCHEMA_VERSION,
        "model_key": MODEL_KEY,
        "feature_names": FEATURE_NAMES,
        "impute_values": [float(x) for x in medians],
        "scale_values": [float(x) for x in scales],
        **spec,
    }
    if len(FEATURE_NAMES) != vectors.shape[1]:
        raise ValueError("feature name / vector dimension mismatch")

    conn = sqlite3.connect(str(tmp))
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(schema_path.read_text(encoding="utf-8"))
        with conn:
            conn.execute(
                """
                INSERT INTO snapshots
                    (snapshot_id, created_at, db_sha256, event_count,
                     parquet_count, report_count, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id, created_at, db_sha,
                    counts["event_count"], len(records), counts["report_count"],
                    snapshot_notes,
                ),
            )
            conn.execute(
                """
                INSERT INTO model_versions
                    (model_key, model_name, version, dim, normalize, created_at)
                VALUES (?, ?, ?, ?, 1, ?)
                """,
                (MODEL_KEY, MODEL_NAME, MODEL_VERSION,
                 len(FEATURE_NAMES), created_at),
            )
            conn.execute(
                """
                INSERT INTO feature_vector_specs (model_key, spec_json, created_at)
                VALUES (?, ?, ?)
                """,
                (
                    MODEL_KEY,
                    json.dumps(spec_out, ensure_ascii=False, allow_nan=False,
                               sort_keys=True),
                    created_at,
                ),
            )

            for i, record in enumerate(records):
                features_json = json.dumps(
                    record, ensure_ascii=False, allow_nan=False, sort_keys=True
                )
                input_hash = record.get("source_sha256") or hashlib.sha256(
                    features_json.encode("utf-8")
                ).hexdigest()
                conn.execute(
                    """
                    INSERT INTO event_features
                        (event_id, snapshot_id, timestamp, t0_offset_s,
                         quench_unit, has_waveform, features_json,
                         input_hash, updated_at)
                    VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?)
                    """,
                    (
                        record["event_id"], snapshot_id,
                        record.get("timestamp") or "",
                        record.get("t0", {}).get("value_s"),
                        record.get("quench_unit"),
                        features_json, input_hash, created_at,
                    ),
                )
                blob = np.asarray(vectors[i], dtype="<f4").tobytes()
                meta = {
                    # Features that were absent or not applicable and were
                    # replaced by the fit median before scaling.
                    "imputed_features": missing[i],
                    "n_imputed": len(missing[i]),
                    # Channel-level gaps, distinct from per-feature N/A values.
                    "missing_channels": record.get("missing_channels", []),
                    "norm": float(np.linalg.norm(vectors[i])),
                    "spec": MODEL_KEY,
                }
                conn.execute(
                    """
                    INSERT INTO waveform_vectors
                        (event_id, model_key, vector, meta_json)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        record["event_id"], MODEL_KEY, blob,
                        json.dumps(meta, ensure_ascii=False, allow_nan=False,
                                   sort_keys=True),
                    ),
                )

        # Integrity checks inside the finished database.
        checks: dict[str, Any] = {}
        n = len(records)
        checks["event_features_rows"] = (
            conn.execute("SELECT COUNT(*) FROM event_features").fetchone()[0] == n
        )
        checks["waveform_vectors_rows"] = (
            conn.execute("SELECT COUNT(*) FROM waveform_vectors").fetchone()[0] == n
        )
        feature_ids = {
            r[0] for r in conn.execute("SELECT event_id FROM event_features")
        }
        vector_ids = {
            r[0] for r in conn.execute("SELECT event_id FROM waveform_vectors")
        }
        checks["event_ids_match"] = feature_ids == vector_ids == {
            r["event_id"] for r in records
        }
        checks["foreign_key_check"] = not list(
            conn.execute("PRAGMA foreign_key_check")
        )
        checks["snapshot_row"] = (
            conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 1
        )
        checks["model_row"] = (
            conn.execute(
                "SELECT COUNT(*) FROM model_versions WHERE model_key=?",
                (MODEL_KEY,),
            ).fetchone()[0] == 1
        )
        checks["vector_dim"] = (
            conn.execute(
                "SELECT dim FROM model_versions WHERE model_key=?", (MODEL_KEY,)
            ).fetchone()[0] == len(FEATURE_NAMES)
        )
        lengths = [
            len(blob) for (blob,) in conn.execute(
                "SELECT vector FROM waveform_vectors WHERE model_key=?",
                (MODEL_KEY,),
            )
        ]
        checks["vector_lengths_ok"] = all(
            length == len(FEATURE_NAMES) * 4 for length in lengths
        )
        norms = []
        json_ok = True
        for (blob, features_json) in conn.execute(
            "SELECT vector, features_json FROM waveform_vectors "
            "JOIN event_features USING (event_id) WHERE model_key=?",
            (MODEL_KEY,),
        ):
            vec = np.frombuffer(blob, dtype="<f4")
            norms.append(float(np.linalg.norm(vec)))
            try:
                json.loads(features_json)
            except json.JSONDecodeError:
                json_ok = False
        checks["no_nan_vectors"] = all(math.isfinite(x) for x in norms)
        checks["l2_normalized"] = all(
            abs(x - 1.0) <= 1e-4 for x in norms
        )
        checks["features_json_ok"] = json_ok
        checks["parquet_count"] = (
            conn.execute(
                "SELECT parquet_count FROM snapshots WHERE snapshot_id=?",
                (snapshot_id,),
            ).fetchone()[0] == n
        )

        # Finish with a single self-contained file (no -wal/-shm needed).
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("PRAGMA optimize")
    finally:
        conn.close()

    os.replace(tmp, out)
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(out) + suffix)
        if sidecar.exists():
            sidecar.unlink()

    return {
        "ok": all(bool(v) for v in checks.values()),
        "checks": checks,
        "snapshot": {
            "snapshot_id": snapshot_id,
            "created_at": created_at,
            "db_sha256": db_sha,
            "event_count": counts["event_count"],
            "parquet_count": n,
            "report_count": counts["report_count"],
        },
        "vector": {
            "model_key": MODEL_KEY,
            "model_name": MODEL_NAME,
            "model_version": MODEL_VERSION,
            "dim": len(FEATURE_NAMES),
            "n_vectors": int(vectors.shape[0]),
            "norm_min": float(min(norms)),
            "norm_max": float(max(norms)),
            "imputed_counts": {
                str(k): int(v)
                for k, v in sorted(
                    Counter(len(m) for m in missing).items()
                )
            },        },
        "output": {
            "path": str(out),
            "size_bytes": out.stat().st_size,
            "sha256": _sha256_file(out),
        },
        "fit_records": int(spec_out.get("fit_records", 0)),
        "elapsed_seconds": round(time.time() - started, 2),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--features", default=str(ROOT / "data" / "features" / "waveform_features.jsonl"))
    ap.add_argument("--events-db", required=True,
                    help="events.db snapshot (opened read-only)")
    ap.add_argument("--schema", default=str(ROOT / "db" / "feature_store.sql"))
    ap.add_argument("--out", default=str(ROOT / "data" / "feature_store.sqlite3"))
    ap.add_argument("--snapshot-id", default=None,
                    help="default: snap_<first 16 chars of db sha256>")
    ap.add_argument("--snapshot-notes", default="")
    ap.add_argument("--force", action="store_true",
                    help="overwrite an existing output file")
    args = ap.parse_args()

    features_path = Path(args.features)
    events_db = Path(args.events_db)
    schema_path = Path(args.schema)
    out = Path(args.out)
    for path, label in ((features_path, "features"), (events_db, "events-db"),
                        (schema_path, "schema")):
        if not path.is_file():
            sys.stderr.write(f"{label} not found: {path}\n")
            return 2
    if out.exists() and not args.force:
        sys.stderr.write(f"output exists (use --force): {out}\n")
        return 2
    out.parent.mkdir(parents=True, exist_ok=True)

    records = load_features(features_path)
    x = raw_matrix(records)

    fit_mask = np.array(
        [not r.get("missing_channels") for r in records], dtype=bool
    )
    fit_records = [r for r, keep in zip(records, fit_mask) if keep]
    if len(fit_records) < 10:
        fit_records = records
    fit_x = raw_matrix(fit_records)
    medians, scales, spec = fit_scaler(fit_x, FEATURE_NAMES)
    spec["fit_records"] = len(fit_records)
    spec["fit_event_ids_sha256"] = hashlib.sha256(
        "\n".join(sorted(r["event_id"] for r in fit_records)).encode()
    ).hexdigest()

    vectors, missing, _ = transform(x, medians, scales)

    similarity: dict[str, float] = {}
    if len(vectors) > 1:
        sim = vectors @ vectors.T
        np.fill_diagonal(sim, -1.0)
        top1 = sim.max(axis=1)
        similarity = {
            "top1_mean": float(top1.mean()),
            "top1_min": float(top1.min()),
            "top1_max": float(top1.max()),
            "pairs_above_0.9999": int((np.triu(sim, 1) > 0.9999).sum()),
        }

    snapshot_id = args.snapshot_id
    if not snapshot_id:
        snapshot_id = "snap_" + _sha256_file(events_db)[:16]
    notes = args.snapshot_notes or (
        "waveform features schema v1; manual vector %s; events.db snapshot"
        % MODEL_KEY
    )

    result = build(
        records, vectors, missing, medians, scales, spec,
        events_db, schema_path, out, snapshot_id, notes,
    )
    result["features_input"] = {
        "path": str(features_path),
        "sha256": _sha256_file(features_path),
        "records": len(records),
    }
    result["similarity"] = similarity

    validation_path = out.parent / "feature_store_validation.json"
    validation_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )

    print(json.dumps({
        "ok": result["ok"],
        "snapshot_id": result["snapshot"]["snapshot_id"],
        "events": result["snapshot"]["parquet_count"],
        "vectors": result["vector"]["n_vectors"],
        "dim": result["vector"]["dim"],
        "db_mb": round(result["output"]["size_bytes"] / 1e6, 2),
        "elapsed_seconds": result["elapsed_seconds"],
    }, ensure_ascii=False))
    if not result["ok"]:
        bad = [k for k, v in result["checks"].items() if not v]
        sys.stderr.write("failed checks: " + ", ".join(bad) + "\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
