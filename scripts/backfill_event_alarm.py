#!/usr/bin/env python3
"""Attach a real alarm to a historical event (backfill / demo).

Events recorded before the alarm monitor was running have no
``alarm_transitions`` rows.  This helper writes the same ``event_alarms`` row
and JSON sidecar that the live path writes, but derives the timing from PLS
Archiver:

* t0     — first beam-current drop below 99 % of the recent maximum
           (same rule as the event page / pv-explorer)
* onset  — ``auto`` detects the strongest deviation of the mapped graph PV
           from its window median; or pass ``--onset-rel`` explicitly.

The Archiver series for the graph PV is prefetched into the usual
``data/alarm_series/<event>.json`` cache, so the page opens without waiting.

Example (the KSU#3 HVPS water-flow interlock event):

    python scripts/backfill_event_alarm.py 20260623_110438 \
        --alarm-pv SR:RF:KSU3:AI_LOW_FAULT40

That resolves the catalog mapping to SR:RF:KSU3:AI24 and places the alarm
onset on the real flow dip in the Archiver data.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.alarm.event_link import (  # noqa: E402
    SNAPSHOT_DIR,
    SERIES_DIR,
    WINDOW_POST_S,
    WINDOW_PRE_S,
    _fetch_pv_range,
    _split,
    load_alarm_series,
    load_event_alarms,
    resolve_t0,
    save_alarm_series,
)
from src.db.schema import get_sync_connection  # noqa: E402

KST = timezone(timedelta(hours=9))


def _apply_config(path: str | None) -> None:
    if not path:
        return
    import src.core.config as config_mod

    config_mod._config = config_mod.load_config(path)


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def _detect_onset(points: list[tuple[float, float]], t0_epoch: float) -> tuple[float, bool]:
    """Return (onset_rel_sec, confident).

    Looks for the sample that deviates most from the window median.  Only
    trusted when the deviation is >20 % of the median magnitude; otherwise the
    onset falls back to 0 s.
    """
    if not points:
        return 0.0, False
    vals = [v for _, v in points]
    med = statistics.median(vals)
    peak_t, peak_v = max(points, key=lambda tv: abs(tv[1] - med))
    dev = abs(peak_v - med)
    scale = abs(med)
    confident = dev > max(scale * 0.2, 1e-9)
    rel = round(peak_t - t0_epoch, 3) if confident else 0.0
    rel = max(WINDOW_PRE_S * -1.0, min(WINDOW_POST_S, rel))
    return rel, confident


def _series_entry(points: list[tuple[float, float]], t0_epoch: float) -> dict:
    times = [t for t, _ in points]
    diffs = sorted(b - a for a, b in zip(times, times[1:]) if b > a)
    return {
        "points": [[round(t - t0_epoch, 3), v] for t, v in points],
        "unit": "",
        "resolution_s": round(diffs[len(diffs) // 2], 3) if diffs else None,
        "count": len(points),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("event_id")
    ap.add_argument("--alarm-pv", action="append", required=True,
                    help="alarm PV from the OPI catalog (repeatable)")
    ap.add_argument("--graph-pv", action="append", default=[],
                    help="override graph PV for every --alarm-pv")
    ap.add_argument("--onset", default="auto",
                    help="'auto' (default) or a fixed onset in seconds from t0")
    ap.add_argument("--source", default="backfill",
                    help="label written into the sidecar (default: backfill)")
    ap.add_argument("--config", default=None)
    ap.add_argument("--db", default=None, help="override db path")
    args = ap.parse_args()

    os.chdir(ROOT)
    _apply_config(args.config)

    if args.db:
        import src.core.config as config_mod

        if config_mod._config is None:
            config_mod._config = config_mod.load_config(args.config)
        config_mod._config.db.path = Path(args.db)

    conn = get_sync_connection()
    try:
        event = conn.execute(
            "SELECT id, timestamp FROM events WHERE id = ?", (args.event_id,)
        ).fetchone()
        if not event:
            print(f"event not found: {args.event_id}", file=sys.stderr)
            return 2

        t0, base = resolve_t0(event["timestamp"])
        t0_abs = base + timedelta(seconds=t0)
        start = t0_abs - timedelta(seconds=WINDOW_PRE_S)
        end = t0_abs + timedelta(seconds=WINDOW_POST_S)
        print(f"event {event['id']} ts={event['timestamp']} t0_offset={t0:+.3f}s")

        existing = load_event_alarms(args.event_id) or {}
        items = [it for it in existing.get("items", [])
                 if it.get("pv") not in set(args.alarm_pv)]
        cache = load_alarm_series(args.event_id) or {}
        series = cache.get("series", {}) if isinstance(cache.get("series"), dict) else {}

        for pv in args.alarm_pv:
            cat = conn.execute(
                """SELECT description, subsystem, graph_pv, graph_description, severity
                     FROM alarm_pv_catalog WHERE pv = ?""",
                (pv,),
            ).fetchone()
            if not cat:
                print(f"alarm PV not in catalog: {pv}", file=sys.stderr)
                return 3

            graph_pvs = args.graph_pv or _split(cat["graph_pv"])
            graph_descs = _split(cat["graph_description"])
            if args.graph_pv:
                graph_descs = args.graph_pv
            if not graph_pvs:
                print(f"alarm PV has no mapped graph PV: {pv}", file=sys.stderr)
                return 4

            onset_rel = 0.0
            fixed_onset = None if args.onset == "auto" else float(args.onset)
            for gpv in graph_pvs:
                try:
                    pts, _meta = _fetch_pv_range(gpv, start, end)
                except Exception as e:  # noqa: BLE001
                    print(f"  {gpv}: archiver error {e}", file=sys.stderr)
                    continue
                series[gpv] = _series_entry(pts, t0_abs.timestamp())
                if fixed_onset is not None:
                    onset_rel = fixed_onset
                elif len(graph_pvs) == 1:
                    onset_rel, confident = _detect_onset(pts, t0_abs.timestamp())
                    if not confident:
                        print("  onset: no clear deviation, using 0 s")
                print(f"  {gpv}: {len(pts)} points, onset {onset_rel:+.3f}s")

            first_seen = _iso_utc(t0_abs + timedelta(seconds=onset_rel))
            active_at_start = onset_rel < -WINDOW_PRE_S
            conn.execute(
                "DELETE FROM event_alarms WHERE event_id = ? AND pv = ?",
                (args.event_id, pv),
            )
            conn.execute(
                """INSERT INTO event_alarms
                       (event_id, pv, description, subsystem, graph_pv,
                        graph_description, first_seen_utc, first_seen_rel,
                        active_at_start, severity)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (args.event_id, pv, cat["description"], cat["subsystem"],
                 ";".join(graph_pvs), ";".join(graph_descs), first_seen,
                 onset_rel, 1 if active_at_start else 0, cat["severity"]),
            )
            items.append({
                "pv": pv,
                "description": cat["description"],
                "subsystem": cat["subsystem"],
                "graph_pv": graph_pvs,
                "graph_description": graph_descs,
                "first_seen_utc": first_seen,
                "first_seen_rel": onset_rel,
                "active_at_start": active_at_start,
                "severity": cat["severity"],
            })
        conn.commit()
    finally:
        conn.close()

    items.sort(key=lambda x: (x["first_seen_rel"], x["pv"]))
    doc = {
        "event_id": args.event_id,
        "available": True,
        "reason": None,
        "source": args.source,
        "t0_offset_sec": t0,
        "window": {"pre": WINDOW_PRE_S, "post": WINDOW_POST_S},
        "items": items,
    }
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    (SNAPSHOT_DIR / f"{args.event_id}.json").write_text(
        json.dumps(doc, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    save_alarm_series(args.event_id, {
        "event_id": args.event_id,
        "t0_offset_sec": t0,
        "window": {"pre": WINDOW_PRE_S, "post": WINDOW_POST_S},
        "series": series,
    })
    print(f"wrote {SNAPSHOT_DIR / (args.event_id + '.json')}")
    print(f"wrote {SERIES_DIR / (args.event_id + '.json')} ({len(series)} series)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
