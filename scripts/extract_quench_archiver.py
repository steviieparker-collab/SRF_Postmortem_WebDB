#!/usr/bin/env python3
"""Extract Archiver PV series around quench-caused beam dumps.

The waveform parquet files only cover -50..+50 ms.  This script pulls the
matching Archiver PVs over a wider window (default -600..+120 s around the
beam-current drop) so the slow quench precursor and the cryogenic aftermath
can be compared with the fast waveform.

Only events that have a merged waveform are selected by default.  The event
timestamp is not the dump time; t0 is resolved from SR:G00:BEAMCURRENT_T and
stored in ``events.csv`` (seconds relative to the event timestamp).

Outputs (in --out, default data/archiver_quench):
    events.csv        event metadata and t0 offset
    summary.csv       values at fixed offsets plus min/max per PV
    series_long.csv   event_id,pv,t_rel_s,value  (t_rel_s relative to t0)

Examples:
    venv/bin/python scripts/extract_quench_archiver.py
    venv/bin/python scripts/extract_quench_archiver.py --events 20240907_025313
    venv/bin/python scripts/extract_quench_archiver.py --pre 120 --post 60
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.alarm.event_link import (  # noqa: E402
    ARCHIVER_CUTOVER,
    KST,
    fetch_pv_range,
    resolve_t0,
)

# CM1 = SRF1 = LRF1.  GMES/CRFP/CRRP are the Archiver equivalents of the
# oscilloscope Cavity/Forward/Reflect channels; LRF2/3 are the healthy units.
PVS: list[str] = []
for _unit in (1, 2, 3):
    for _key in ("GMES", "CRFP", "CRRP"):
        PVS.append(f"SR:RF:LRF{_unit}:{_key}_ARCH")
PVS += [
    "SR:RF:LRF1:PMES_ARCH",   # probe phase (detuning jump)
    "SR:RF:LRF1:GLDE_ARCH",   # gradient error (available after 2025-09-18)
    "SR:G00:BEAMCURRENT_T",
    "SR:RF:CRM1:BCL01",       # CM1 cavity bottom equator temperature
    "SR:RF:CRM1:BCL03",       # CM1 cavity top equator temperature (quench heating)
    "SR:RF:CRM1:BPO01",       # tuner displacement
    "SR:RF:CRM1:BPO02",       # cavity displacement
    "SR:RF:CRM1:BPR01",       # He vessel pressure
    "SR:RF:CRM1:BFT01",       # force transducer
    "SR:RF:CRM2:BCL03",       # healthy-unit control
    "SR:RF:CRM3:BCL03",       # healthy-unit control
]

OFFSETS = (-600, -60, -10, -1, 0, 1, 5, 10, 60, 120)


def _fetch(pv: str, start: datetime, end: datetime, ref: datetime):
    """Fetch a PV, stripping the ``_ARCH`` suffix for the legacy viewer."""
    pts, meta = fetch_pv_range(pv, start, end, ref=ref)
    if not pts and ref < ARCHIVER_CUTOVER and pv.endswith("_ARCH"):
        pts, meta = fetch_pv_range(pv[:-5], start, end, ref=ref)
    return pts, meta


def _at(pts, target: float, tol: float = 4.0):
    """Value nearest to ``target``; None if the nearest sample is far away.

    Legacy data is 1 Hz, but the current API returns sparse points for some
    cryogenic PVs.  Without a tolerance a "v+1s" cell would silently show a
    sample from tens of seconds away.
    """
    if not pts:
        return None
    t, v = min(pts, key=lambda p: abs(p[0] - target))
    return v if abs(t - target) <= tol else None


def _quench_events(conn: sqlite3.Connection, only: set[str]) -> list[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT id, timestamp, fault_type, user_fault_type, notes
        FROM events
        WHERE lower(coalesce(notes,'')) LIKE '%quench%'
           OR notes LIKE '%켄치%' OR notes LIKE '%퀜치%'
           OR lower(coalesce(user_fault_type,'')) LIKE '%quench%'
        ORDER BY timestamp
        """
    ).fetchall()
    out = []
    for row in rows:
        if only and row["id"] not in only:
            continue
        parquet = ROOT / "data" / "merged" / f"event_{row['id']}.parquet"
        if parquet.is_file():
            out.append(row)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=str(ROOT / "data" / "archiver_quench"))
    ap.add_argument("--pre", type=float, default=600.0)
    ap.add_argument("--post", type=float, default=120.0)
    ap.add_argument("--events", default="",
                    help="comma-separated event IDs (default: all quench events "
                         "with a merged parquet)")
    args = ap.parse_args()

    only = {x.strip() for x in args.events.split(",") if x.strip()}
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(ROOT / "db" / "events.db")
    conn.row_factory = sqlite3.Row
    events = _quench_events(conn, only)
    if not events:
        raise SystemExit("no matching events with merged waveform")

    long_f = open(out / "series_long.csv", "w", newline="", encoding="utf-8")
    w = csv.writer(long_f)
    w.writerow(["event_id", "pv", "t_rel_s", "value"])

    ev_f = open(out / "events.csv", "w", newline="", encoding="utf-8")
    we = csv.writer(ev_f)
    we.writerow(["event_id", "event_time", "t0_offset_s", "quench_unit",
                 "fault_type", "notes"])

    sum_f = open(out / "summary.csv", "w", newline="", encoding="utf-8")
    ws = csv.writer(sum_f)
    ws.writerow(["event_id", "pv", "n", "unit"]
                + [f"v{o:+d}s" for o in OFFSETS]
                + ["min", "t_min_s", "max", "t_max_s"])

    for i, row in enumerate(events, 1):
        eid = row["id"]
        base = datetime.strptime(eid, "%Y%m%d_%H%M%S").replace(tzinfo=KST)
        try:
            t0, _ = resolve_t0(base.isoformat())
        except Exception:
            t0 = 0.0
        t0dt = base + timedelta(seconds=t0)
        start = t0dt - timedelta(seconds=args.pre)
        end = t0dt + timedelta(seconds=args.post)
        t0_epoch = t0dt.timestamp()
        we.writerow([eid, base.isoformat(), t0, "SRF1/CM1",
                     row["fault_type"], (row["notes"] or "").replace("\n", " ")])
        print(f"[{i}/{len(events)}] {eid} t0={t0:+.3f}s", flush=True)

        for pv in PVS:
            try:
                pts, meta = _fetch(pv, start, end, base)
            except Exception as exc:  # keep going; record availability via n=0
                print(f"   ERR {pv}: {exc}", flush=True)
                pts, meta = [], {}
            for t, v in pts:
                w.writerow([eid, pv, round(t - t0_epoch, 6), v])
            values = ["" if (v := _at(pts, t0_epoch + o)) is None else v
                      for o in OFFSETS]
            if pts:
                tmin, vmin = min(pts, key=lambda p: p[1])
                tmax, vmax = max(pts, key=lambda p: p[1])
                extremes = [vmin, round(tmin - t0_epoch, 3),
                            vmax, round(tmax - t0_epoch, 3)]
            else:
                extremes = ["", "", "", ""]
            ws.writerow([eid, pv, len(pts), (meta or {}).get("unit", "")]
                        + values + extremes)
        long_f.flush()
        sum_f.flush()

    long_f.close()
    ev_f.close()
    sum_f.close()
    print(f"wrote {out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
