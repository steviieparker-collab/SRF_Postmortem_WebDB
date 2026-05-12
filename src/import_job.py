"""
Import merged parquet files into the SRF Event DB from within the web server.

Provides `run_import()` which can be called from the FastAPI endpoint.
"""

import json
from pathlib import Path
from datetime import datetime

import polars as pl
import numpy as np
import pandas as pd
import yaml

from .db.schema import init_db_sync, seed_fault_types, get_sync_connection
from .db.models import EventCreate
from .db.repository import create_event, get_event
from .classifier.classifier import classify_event


# ── Analog / Digital column lists ───────────────────────────

ANALOG_COLUMNS = [
    "BeamCurrent_v",
    "Forward_SRF1_v", "Forward_SRF2_v", "Forward_SRF3_v",
    "Cavity_SRF1_v", "Cavity_SRF2_v", "Cavity_SRF3_v",
    "Reflect_SRF1_v", "Reflect_SRF2_v", "Reflect_SRF3_v",
]

DIGITAL_COLUMNS = [
    "INT_MIS1_IC_d", "INT_MIS2_IC_d",
    "INT_IN_FC3_d", "INT_IC_FC2_d", "INT_IC_FC1_d",
    "RDY_KSU3_IC_d", "RDY_KSU2_IC_d", "RDY_KSU1_IC_d",
    "INT_PSI2_IC_d", "INT_PSI1_IC_d",
    "RDY_CM3_FC3_d", "RDY_CM2_FC2_d", "RDY_CM1_FC1_d",
    "HE-PR_CM3_FC3_d", "HE-PR_CM2_FC2_d", "HE-PR_CM1_FC1_d",
    "ARC_CM3_FC3_d", "ARC_CM2_FC2_d", "ARC_CM1_FC1_d",
    "QUEN_CM3_FC3_d", "QUEN_CM2_FC2_d", "QUEN_CM1_FC1_d",
    "VAC_CM3_FC3_d", "VAC_CM2_FC2_d", "VAC_CM1_FC1_d",
]

CASE_TO_FAULT = {
    0: "Unknown",
    1: "Case 1: Beam_loss", 2: "Case 2: Beam_loss", 3: "Case 3: RF_Interlock",
    4: "Case 4: MIS", 5: "Case 5: PSI",
    6: "Case 6: Multi_interlock(same group)", 7: "Case 7: Multi_interlock(different group)",
    8: "Case 8: Cavity_blip", 9: "Case 9: Cavity_quench",
    10: "Case 10: RF_path_Forward", 11: "Case 11: RF_path_Cavity",
    12: "Case 12: Cavity_detune", 13: "Case 13: RF_source_fault",
}


# ── Helpers ─────────────────────────────────────────────────

def _compute_analog_metrics(df: pl.DataFrame) -> dict:
    metrics = {}
    for ch in ANALOG_COLUMNS:
        if ch not in df.columns:
            continue
        vals = df[ch].to_numpy()
        clean = vals[~np.isnan(vals)]
        if len(clean) == 0:
            continue
        metrics[ch.replace("_v", "")] = {
            "peak": round(float(clean.max()), 4),
            "min": round(float(clean.min()), 4),
            "mean": round(float(clean.mean()), 4),
            "std": round(float(clean.std()), 4),
            "p2p": round(float(clean.max() - clean.min()), 4),
        }
    return metrics


def _compute_digital_pattern(df: pl.DataFrame) -> dict:
    pattern = {}
    for ch in DIGITAL_COLUMNS:
        if ch not in df.columns:
            continue
        vals = df[ch].drop_nulls()
        if len(vals) == 0:
            pattern[ch.replace("_d", "")] = 0
            continue
        mode_vals = vals.mode()
        pattern[ch.replace("_d", "")] = int(mode_vals[0]) if len(mode_vals) > 0 else 0
    return pattern


def _compute_baseline_std(df: pl.DataFrame) -> dict:
    baseline = {}
    for ch in ANALOG_COLUMNS:
        col_name = ch.replace("_v", "") + "_baseline_std"
        if col_name in df.columns:
            vals = df[col_name].drop_nulls()
            if len(vals) > 0:
                baseline[ch.replace("_v", "")] = round(float(vals.mean()), 6)
    return baseline


def _extract_event_id(fp: Path) -> tuple[str, str]:
    from datetime import timezone, timedelta
    KST = timezone(timedelta(hours=9))
    df = pl.read_parquet(fp, columns=["event_timestamp"])
    ts_vals = df["event_timestamp"].drop_nulls().unique().to_list()
    if ts_vals:
        ts = ts_vals[0]
        # Convert to KST for display
        if ts.tzinfo is not None:
            ts = ts.astimezone(KST)
        event_id = ts.strftime("%Y%m%d_%H%M%S")
        return event_id, ts.strftime("%Y-%m-%dT%H:%M:%S")
    else:
        stem = fp.stem.replace("event_", "")
        return stem, (
            stem[:4] + "-" + stem[4:6] + "-" + stem[6:8] +
            "T" + stem[9:11] + ":" + stem[11:13] + ":" + stem[13:15]
        )


def _update_config_data_root(config_path: Path, merged_dir: Path):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    data_root = str(merged_dir.parent.resolve())
    config.setdefault("monitor", {})
    changed = False
    if config["monitor"].get("data_root") != data_root:
        config["monitor"]["data_root"] = data_root
        changed = True
    if changed:
        with open(config_path, "w") as f:
            yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)


# ── Main import function ────────────────────────────────────

def run_import(merged_dir: str | Path, config_path: str | Path | None = None) -> dict:
    """
    Scan merged_dir for parquet files, classify each, and store in DB.

    Returns:
        dict: {imported, skipped, total, errors, results: [{file, event_id, case, ...}]}
    """
    merged_dir = Path(merged_dir)
    if not merged_dir.exists():
        return {"error": f"Directory not found: {merged_dir}", "imported": 0, "skipped": 0, "total": 0, "errors": 0, "results": []}

    files = sorted(merged_dir.glob("event_*.parquet"))
    if not files:
        return {"error": "No parquet files found", "imported": 0, "skipped": 0, "total": 0, "errors": 0, "results": []}

    conn = init_db_sync()
    seed_fault_types(conn)

    imported = 0
    skipped = 0
    errors = 0
    results: list[dict] = []

    for fp in files:
        event_id, timestamp = _extract_event_id(fp)

        existing = get_event(conn, event_id)
        if existing:
            skipped += 1
            results.append({"file": fp.name, "event_id": event_id, "status": "skipped", "reason": "already in DB"})
            continue

        try:
            df = pl.read_parquet(fp)
        except Exception as e:
            errors += 1
            results.append({"file": fp.name, "event_id": event_id, "status": "error", "reason": str(e)})
            continue

        analog_metrics = _compute_analog_metrics(df)
        digital_pattern = _compute_digital_pattern(df)

        beam_current = None
        beam_baseline_col = f"BeamCurrent_v_baseline_mean"
        if beam_baseline_col in df.columns:
            bl_val = df[beam_baseline_col].drop_nulls().to_list()
            if len(bl_val) > 0:
                beam_current = round(float(bl_val[0]) * 50, 2)

        # Classify (drop timezone-aware event_timestamp col to avoid polars→pandas conversion error)
        try:
            df_classify = df.drop("event_timestamp") if "event_timestamp" in df.columns else df
            df_pd = df_classify.to_pandas()
            cls_result = classify_event(df_pd)
        except Exception as e:
            cls_result = {"case": 0, "case_str": "0", "case_description": "Classification error",
                         "case_fault": str(e), "case_confidence": 0.0}

        cls_fault_type = CASE_TO_FAULT.get(cls_result["case"], "Unknown")

        event = EventCreate(
            id=event_id,
            timestamp=timestamp,
            merged_file=str(fp.resolve()),
            fault_type=cls_fault_type,
            fault_confidence=round(cls_result["case_confidence"], 2),
            beam_voltage=None,
            beam_current=beam_current,
            analog_metrics=analog_metrics,
            digital_pattern=digital_pattern,
            time_groups=cls_result.get("time_groups"),
            graphs_path=None,
            report_path=None,
            report_md=(
                f"# Event Report\n"
                f"## Auto-imported from merged parquet\n"
                f"- File: {fp.name}\n"
                f"- Timestamp: {timestamp}\n"
                f"- Analog channels: {len(analog_metrics)}\n"
                f"- Digital signals: {len(digital_pattern)}\n"
                f"- Data points: {len(df)}\n"
            ),
            case_id=cls_result["case"],
            case_description=cls_result["case_description"],
            case_fault=cls_result["case_fault"],
            user_beam_time="",
        )

        try:
            create_event(conn, event)
            imported += 1
            results.append({
                "file": fp.name,
                "event_id": event_id,
                "status": "imported",
                "case": cls_result["case"],
                "case_description": cls_result["case_description"],
                "fault_type": cls_fault_type,
            })
        except Exception as e:
            errors += 1
            results.append({"file": fp.name, "event_id": event_id, "status": "error", "reason": str(e)})

    # Update config data_root
    if config_path:
        _update_config_data_root(Path(config_path), merged_dir)

    # Refresh similarity links
    linked = 0
    if imported > 0:
        from .db.similarity import update_all_similarity_links
        linked = update_all_similarity_links(conn)

    conn.close()

    return {
        "imported": imported,
        "skipped": skipped,
        "errors": errors,
        "total": imported + skipped + errors,
        "similarity_links": linked,
        "results": results,
    }


class ImportRunner:
    """
    Wrapper around run_import for the orchestrator.
    Provides ImportRunner(db_path).run(merged_dir) interface.
    """

    def __init__(self, db_path: str | Path | None = None):
        self.db_path = str(db_path) if db_path else "./db/events.db"

    def run(self, merged_dir: str | Path) -> int:
        """Import all parquet files from merged_dir into DB. Returns count."""
        result = run_import(merged_dir)
        if isinstance(result, dict):
            return result.get("imported", 0)
        return 0
