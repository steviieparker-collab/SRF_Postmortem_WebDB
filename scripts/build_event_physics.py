#!/usr/bin/env python3
"""Build one textbook-grounded physics record per fast-scope event.

Reads ``data/features/waveform_features.jsonl`` (the Task-1 extraction) and
writes ``data/features/event_physics_interpretations.jsonl``.  Each line is::

    {
      "event_id": "...",
      "fault_type": "...",
      "user_fault_type": "...",
      "class": "rf_trip",
      "physics": { summary_ko, timeline, checks, citations, caveats, ... }
    }

The interpretation is deterministic: numbers come from the sequence block,
prose is a template, and citations come from ``config/physics_references.json``.
No LLM is called.

Usage::

    venv/bin/python scripts/build_event_physics.py \
        --features data/features/waveform_features.jsonl \
        --out data/features/event_physics_interpretations.jsonl
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.features.physics import interpret_sequence  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--features",
        default=str(ROOT / "data" / "features" / "waveform_features.jsonl"),
    )
    ap.add_argument(
        "--out",
        default=str(
            ROOT / "data" / "features" / "event_physics_interpretations.jsonl"
        ),
    )
    ap.add_argument(
        "--summary-out",
        default=str(
            ROOT / "data" / "features" / "event_physics_summary.csv"
        ),
    )
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--events-db", default=str(ROOT / "db" / "events.db"))
    args = ap.parse_args()

    meta: dict[str, dict[str, str]] = {}
    db_path = Path(args.events_db)
    if db_path.is_file():
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            for eid, ft, uft in conn.execute(
                "SELECT id, fault_type, user_fault_type FROM events"
            ):
                meta[eid] = {"fault_type": ft or "", "user_fault_type": uft or ""}
        finally:
            conn.close()

    src = Path(args.features)
    if not src.is_file():
        print(f"missing {src}", file=sys.stderr)
        return 2

    rows = []
    for line in src.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        eid = rec.get("event_id")
        m = meta.get(eid, {})
        physics = interpret_sequence(rec.get("sequence"), rec.get("beam_fall"))
        rows.append({
            "event_id": eid,
            "fault_type": rec.get("fault_type") or m.get("fault_type", ""),
            "user_fault_type": rec.get("user_fault_type") or m.get("user_fault_type", ""),
            "class": (rec.get("sequence") or {}).get("class"),
            "physics": physics,
        })
        if args.limit and len(rows) >= args.limit:
            break

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n")

    summary = Path(args.summary_out)
    summary.parent.mkdir(parents=True, exist_ok=True)
    with summary.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=[
            "event_id", "fault_type", "class", "cavity_tau_us",
            "collapse_spread_us", "delay_cav50_to_beam50_us",
            "delay_cav90_to_beam99_us", "common_cause", "cavity_first",
            "citation_ids",
        ])
        writer.writeheader()
        for row in rows:
            p = row["physics"]
            checks = p.get("checks") or {}
            timeline = p.get("timeline") or {}
            writer.writerow({
                "event_id": row["event_id"],
                "fault_type": row["fault_type"],
                "class": row["class"],
                "cavity_tau_us": checks.get("cavity_tau_observed_us"),
                "collapse_spread_us": timeline.get("cavity_collapse_spread_us"),
                "delay_cav50_to_beam50_us": timeline.get("delay_cav50_to_beam50_us"),
                "delay_cav90_to_beam99_us": timeline.get("delay_cav90_to_beam99_us"),
                "common_cause": checks.get("common_cause_3cavity"),
                "cavity_first": checks.get("cavity_first"),
                "citation_ids": ";".join(dict.fromkeys(
                    c.get("id", "") for c in p.get("citations", [])
                )),
            })

    print(f"wrote {len(rows)} records to {out} and {summary}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
