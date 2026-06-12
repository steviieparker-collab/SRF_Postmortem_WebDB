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
from .pipeline.classifier import AcceleratorEventClassifier
from .pipeline.datatypes import SignalEvent, TimeGroup


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
    0: "Case 0: Unknown",
    1: "Case 1: Beam_loss", 2: "Case 2: Beam_loss", 3: "Case 3: RF_Interlock",
    4: "Case 4: MIS", 5: "Case 5: PSI",
    6: "Case 6: Multi_interlock(same group)", 7: "Case 7: Multi_interlock(different group)",
    8: "Case 8: Cavity_blip", 9: "Case 9: Cavity_quench",
    10: "Case 10: RF_path_Forward", 11: "Case 11: RF_path_Cavity",
    12: "Case 12: Cavity_detune", 13: "Case 13: RF_source_fault",
    14: "Case 14: Utility",
    15: "Case 15: Others",
    16: "Case 16: Beam line",
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

    This is the standalone version that runs its OWN classification.
    Prefer run_import_with_classifications() when pipeline results already exist.

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

        # Delegate to shared per-file import logic
        outcome = _import_one_file(conn, fp, cls_result=None)
        if outcome["status"] == "imported":
            imported += 1
        elif outcome["status"] == "error":
            errors += 1
        results.append(outcome)

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


def run_import_with_classifications(
    merged_dir: str | Path,
    classifications: dict[str, dict],
    config_path: str | Path | None = None,
) -> dict:
    """
    Import merged parquet files into DB using pre-computed classification results.

    Unlike run_import(), this does NOT re-run the classifier. It uses the
    classifications from the pipeline's run_classifier() output.

    Args:
        merged_dir: Directory containing event_*.parquet files
        classifications: Dict mapping event_id -> {case, case_description, case_fault,
                          case_confidence, time_groups, analog_metrics, digital_pattern}
        config_path: Optional config YAML path for data_root update

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

        # Use pre-computed classification
        cls_result = classifications.get(event_id)
        outcome = _import_one_file(conn, fp, cls_result=cls_result)
        if outcome["status"] == "imported":
            imported += 1
        elif outcome["status"] == "error":
            errors += 1
        results.append(outcome)

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


def _import_one_file(
    conn,
    fp: Path,
    cls_result: dict | None = None,
) -> dict:
    """
    Import a single parquet file into DB.

    If cls_result is provided, use it as the classification. Otherwise,
    run the pipeline classifier on-the-fly.
    """
    event_id, timestamp = _extract_event_id(fp)

    try:
        df = pl.read_parquet(fp)
    except Exception as e:
        return {"file": fp.name, "event_id": event_id, "status": "error", "reason": str(e)}

    analog_metrics = _compute_analog_metrics(df)
    digital_pattern = _compute_digital_pattern(df)

    beam_current = None
    beam_baseline_col = "BeamCurrent_v_baseline_mean"
    if beam_baseline_col in df.columns:
        bl_val = df[beam_baseline_col].drop_nulls().to_list()
        if len(bl_val) > 0:
            beam_current = round(float(bl_val[0]) * 50, 2)

    time_groups = None
    case = 0
    case_str = "0"
    case_description = ""
    case_fault = ""
    case_confidence = 0.0

    if cls_result is not None:
        # Use pre-computed classification from pipeline
        case = cls_result.get("case", 0)
        case_str = str(case)
        case_description = cls_result.get("case_description", "")
        case_fault = cls_result.get("case_fault", "")
        case_confidence = cls_result.get("case_confidence", 0.0)
        time_groups = cls_result.get("time_groups")

    # If time_groups is still None, try to load from sequence_info.json
    if time_groups is None:
        from pathlib import Path
        from src.core.config import get_config
        try:
            cfg = get_config()
            # Resolve results_dir: if relative, treat as relative to project root
            # Project root = grandparent of src/ dir, or parquet file's parent's parent
            raw_results = Path(cfg.paths.results_dir)
            if not raw_results.is_absolute():
                # Try resolving relative to fp.parent (merged_dir) first, then project root
                candidates = [
                    fp.parent.parent / raw_results,           # merged_dir/../results_dir
                    Path.cwd() / raw_results,                  # CWD based
                ]
                seq_path = None
                for c in candidates:
                    trial = c / "sequence_info.json"
                    if trial.exists():
                        seq_path = trial
                        break
                if seq_path is None:
                    seq_path = candidates[0] / "sequence_info.json"
            else:
                seq_path = raw_results / "sequence_info.json"

            parquet_stem = fp.stem  # event_YYYYMMDD_HHMMSS
            if seq_path.exists():
                seq_data = json.loads(seq_path.read_text(encoding="utf-8"))
                seq_key = f"{fp.name}"
                entry = seq_data.get(seq_key) or seq_data.get(parquet_stem)
                if entry:
                    # Also extract case info from sequence_info entry if available
                    seq_cls = entry.get("classification")
                    if seq_cls and cls_result is None:
                        case = seq_cls.get("case", 0)
                        case_str = seq_cls.get("case_str", str(case))
                        case_description = seq_cls.get("description", "")
                        case_fault = seq_cls.get("fault", "")
                        case_confidence = seq_cls.get("confidence", 0.0)

                    def _to_time_group(g: list, name: str) -> dict:
                        events = []
                        for e in g:
                            events.append({
                                "channel": e["channel"],
                                "event_type": e.get("type", "unknown"),
                                "time_s": e.get("time_raw", 0.0),
                                "effective_time_s": e.get("time_effective", e.get("time_raw", 0.0)),
                                "value": e.get("value", 0.0),
                            })
                        start = events[0]["effective_time_s"] if events else 0.0
                        return {"name": name, "start_time_s": start, "events": events}
                    time_groups = {
                        "first": _to_time_group(entry.get("first_events", []), "FIRST"),
                        "second": _to_time_group(entry.get("second_events", []), "SECOND"),
                        "third": _to_time_group(entry.get("third_events", []), "THIRD"),
                    }
        except Exception:
            pass

    if time_groups is None:
        # Fallback: run classifier on-the-fly
        try:
            clf = AcceleratorEventClassifier()
            result_df = clf.run(str(fp.parent), "/tmp/srf_classifier", input_files=[str(fp)])
            if not result_df.empty:
                first = result_df.iloc[0]
                case = int(first["case"])
                case_str = str(first["case_str"])
                case_description = first["description"]
                case_fault = first["fault"]
                case_confidence = float(first["confidence"])
                # After running classifier, sequence_info.json was updated — try reading time_groups
                try:
                    from src.core.config import get_config
                    cfg_write = get_config()
                    raw_results = Path(cfg_write.paths.results_dir)
                    if not raw_results.is_absolute():
                        candidates = [
                            fp.parent.parent / raw_results,
                            Path.cwd() / raw_results,
                        ]
                        seq_path_write = None
                        for c in candidates:
                            trial = c / "sequence_info.json"
                            if trial.exists():
                                seq_path_write = trial
                                break
                        if seq_path_write is None:
                            seq_path_write = candidates[0] / "sequence_info.json"
                    else:
                        seq_path_write = raw_results / "sequence_info.json"
                    if seq_path_write.exists():
                        seq_data = json.loads(seq_path_write.read_text(encoding="utf-8"))
                        seq_key = f"{fp.name}"
                        entry = seq_data.get(seq_key) or seq_data.get(fp.stem)
                        if entry:
                            # Extract classification info from sequence_info entry
                            seq_cls = entry.get("classification")
                            if seq_cls:
                                case = seq_cls.get("case", case)
                                case_str = seq_cls.get("case_str", str(case))
                                case_description = seq_cls.get("description", case_description)
                                case_fault = seq_cls.get("fault", case_fault)
                                case_confidence = seq_cls.get("confidence", case_confidence)

                            def _to_time_group_after(g: list, name: str) -> dict:
                                evts = []
                                for e in g:
                                    evts.append({
                                        "channel": e["channel"],
                                        "event_type": e.get("type", "unknown"),
                                        "time_s": e.get("time_raw", 0.0),
                                        "effective_time_s": e.get("time_effective", e.get("time_raw", 0.0)),
                                        "value": e.get("value", 0.0),
                                    })
                                start = evts[0]["effective_time_s"] if evts else 0.0
                                return {"name": name, "start_time_s": start, "events": evts}
                            time_groups = {
                                "first": _to_time_group_after(entry.get("first_events", []), "FIRST"),
                                "second": _to_time_group_after(entry.get("second_events", []), "SECOND"),
                                "third": _to_time_group_after(entry.get("third_events", []), "THIRD"),
                            }
                except Exception:
                    pass
            else:
                raise ValueError("No classification result")
        except Exception as e:
            case = 0
            case_str = "0"
            case_description = "Classification error"
            case_fault = str(e)
            case_confidence = 0.0

    cls_fault_type = CASE_TO_FAULT.get(case, "Unknown")

    event = EventCreate(
        id=event_id,
        timestamp=timestamp,
        merged_file=str(fp.resolve()),
        fault_type=cls_fault_type,
        fault_confidence=round(case_confidence, 2),
        beam_voltage=None,
        beam_current=beam_current,
        analog_metrics=analog_metrics,
        digital_pattern=digital_pattern,
        time_groups=time_groups,
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
        case_id=case,
        case_description=case_description,
        case_fault=case_fault,
        user_beam_time="",
    )

    try:
        create_event(conn, event)
        return {
            "file": fp.name,
            "event_id": event_id,
            "status": "imported",
            "case": case,
            "case_description": case_description,
            "fault_type": cls_fault_type,
        }
    except Exception as e:
        return {"file": fp.name, "event_id": event_id, "status": "error", "reason": str(e)}


def _load_sequence_info_classifications(
    merged_dir: str | Path,
    results_dir: str | Path,
) -> dict[str, dict]:
    """
    Load pre-computed classifications from sequence_info.json.

    Reads the JSON file produced by the pipeline classifier and maps
    each parquet filename to its classification data + time_groups.

    Returns:
        dict mapping event_id -> {
            "case", "case_description", "case_fault", "case_confidence",
            "time_groups" (dict with first/second/third)
        }
    """
    results_dir = Path(results_dir)
    seq_path = results_dir / "sequence_info.json"
    if not seq_path.exists():
        return {}

    try:
        seq_data = json.loads(seq_path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    classifications: dict[str, dict] = {}
    merged_dir = Path(merged_dir)

    for fname, entry in seq_data.items():
        parquet_path = merged_dir / fname
        if not parquet_path.exists():
            continue
        event_id, _ = _extract_event_id(parquet_path)

        cls_info = entry.get("classification", {})

        def _to_time_group(g: list, name: str) -> dict:
            events = []
            for e in g:
                events.append({
                    "channel": e["channel"],
                    "event_type": e.get("type", "unknown"),
                    "time_s": e.get("time_raw", 0.0),
                    "effective_time_s": e.get("time_effective", e.get("time_raw", 0.0)),
                    "value": e.get("value", 0.0),
                })
            start = events[0]["effective_time_s"] if events else 0.0
            return {"name": name, "start_time_s": start, "events": events}

        time_groups = {
            "first": _to_time_group(entry.get("first_events", []), "FIRST"),
            "second": _to_time_group(entry.get("second_events", []), "SECOND"),
            "third": _to_time_group(entry.get("third_events", []), "THIRD"),
        }

        classifications[event_id] = {
            "case": cls_info.get("case", 0),
            "case_description": cls_info.get("description", ""),
            "case_fault": cls_info.get("fault", ""),
            "case_confidence": cls_info.get("confidence", 0.0),
            "time_groups": time_groups,
        }

    return classifications


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

    def run_with_classifications(self, merged_dir: str | Path, classifications: dict[str, dict]) -> int:
        """Import using pre-computed classifications. Returns count."""
        result = run_import_with_classifications(merged_dir, classifications)
        if isinstance(result, dict):
            return result.get("imported", 0)
        return 0
