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
import json
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
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    src = Path(args.features)
    if not src.is_file():
        print(f"missing {src}", file=sys.stderr)
        return 2

    rows = []
    for line in src.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        physics = interpret_sequence(rec.get("sequence"), rec.get("beam_fall"))
        rows.append({
            "event_id": rec.get("event_id"),
            "fault_type": rec.get("fault_type", ""),
            "user_fault_type": rec.get("user_fault_type", ""),
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
    print(f"wrote {len(rows)} records to {out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
