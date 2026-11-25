#!/usr/bin/env python3
"""Pull the PVs actually selected in event_pv_selection for quench events.

Only events that have BOTH a merged waveform and at least one
``event_pv_selection`` row are selected.  For every selected PV the script
resolves t0 from the beam-current 50% drop and fetches [t0-pre, t0+post].

Outputs (default data/archiver_rf):
    quench_pv_selection_events.csv    event list + selected PVs + t0
    quench_pv_selection_summary.csv   per event/PV pre/t0/post/max/delta
    quench_pv_selection_long.csv      raw points
    quench_pv_selection_bcl.png       per-event BCL01/BCL03 panels

Example:
    venv/bin/python scripts/pull_pv_selection_series.py
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import statistics
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.alarm.event_link import KST, fetch_pv_range, resolve_t0  # noqa: E402


def _events() -> list[dict]:
    con = sqlite3.connect(ROOT / "db" / "events.db")
    con.row_factory = sqlite3.Row
    rows = con.execute(
        """
        SELECT e.id, e.timestamp, s.pv, s.description, s.position
        FROM events e
        JOIN event_pv_selection s ON s.event_id = e.id
        WHERE lower(coalesce(e.notes,'')) LIKE '%quench%'
           OR e.notes LIKE '%켄치%' OR e.notes LIKE '%퀜치%'
           OR lower(coalesce(e.user_fault_type,'')) LIKE '%quench%'
        ORDER BY e.timestamp, s.position
        """
    ).fetchall()
    con.close()
    out: dict[str, dict] = {}
    for r in rows:
        eid = r["id"]
        parquet = ROOT / "data" / "merged" / f"event_{eid}.parquet"
        if not parquet.is_file():
            continue
        ev = out.setdefault(eid, {"event_id": eid, "timestamp": r["timestamp"],
                                  "selected": []})
        ev["selected"].append({"pv": r["pv"], "description": r["description"],
                               "position": r["position"]})
    return list(out.values())


def _median(pts, lo, hi):
    xs = [v for t, v in pts if lo <= t <= hi]
    return statistics.median(xs) if xs else None


def _nearest(pts, target, tol=4.0):
    if not pts:
        return None
    t, v = min(pts, key=lambda p: abs(p[0] - target))
    return v if abs(t - target) <= tol else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--pre", type=float, default=60.0)
    ap.add_argument("--post", type=float, default=10.0)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--out", default=str(ROOT / "data" / "archiver_rf"))
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    events = _events()
    print(f"events with waveform + pv_selection: {len(events)}")
    for ev in events:
        print(f"  {ev['event_id']}: " + ", ".join(
            s["pv"] for s in ev["selected"]))

    long_rows = []
    summary_rows = []
    plot_data = {}
    for idx, ev in enumerate(events, 1):
        base = datetime.strptime(ev["timestamp"],
                                 "%Y-%m-%dT%H:%M:%S").replace(tzinfo=KST)
        t0, _ = resolve_t0(base.isoformat())
        t0dt = base + timedelta(seconds=t0)
        t0_epoch = t0dt.timestamp()
        start = t0dt - timedelta(seconds=args.pre)
        end = t0dt + timedelta(seconds=args.post)
        ev["t0_offset_s"] = t0
        print(f"[{idx}/{len(events)}] {ev['event_id']} t0={t0:+.3f}s",
              flush=True)

        def call(pv):
            try:
                return pv, fetch_pv_range(pv, start, end, ref=base), ""
            except Exception as exc:  # noqa: BLE001
                return pv, ([], {}), str(exc)

        results = {}
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(call, s["pv"]): s for s in ev["selected"]}
            for fut in as_completed(futures):
                s = futures[fut]
                pv, (pts, _meta), err = fut.result()
                results[pv] = pts
                dts = sorted(b - a for a, b in zip([t for t, _ in pts],
                                                   [t for t, _ in pts][1:])
                             if b > a)
                pre = _median(pts, t0_epoch - args.pre, t0_epoch - 5)
                post = _median(pts, t0_epoch + 1, t0_epoch + args.post)
                t0v = _nearest(pts, t0_epoch)
                mx = max(pts, key=lambda p: p[1]) if pts else (None, None)
                summary_rows.append({
                    "event_id": ev["event_id"], "timestamp": ev["timestamp"],
                    "pv": pv, "description": s["description"], "n": len(pts),
                    "dt_med_s": round(statistics.median(dts), 3) if dts else "",
                    "t0_offset_s": t0, "pre_med": pre, "t0_val": t0v,
                    "post_med": post, "max": mx[1],
                    "max_t_s": (round(mx[0] - t0_epoch, 3)
                                if mx[0] is not None else ""),
                    "delta_max": (mx[1] - pre
                                  if mx[1] is not None and pre is not None
                                  else None),
                    "error": err,
                })
                for t, v in pts:
                    long_rows.append([ev["event_id"], pv,
                                      round(t - t0_epoch, 6), v])
        plot_data[ev["event_id"]] = {pv: results[pv]
                                     for pv in results}

    with open(out / "quench_pv_selection_events.csv", "w", newline="",
              encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["event_id", "timestamp", "t0_offset_s", "selected_pvs"])
        for ev in events:
            w.writerow([ev["event_id"], ev["timestamp"], ev["t0_offset_s"],
                        ";".join(s["pv"] for s in ev["selected"])])
    with open(out / "quench_pv_selection_summary.csv", "w", newline="",
              encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(summary_rows[0]))
        w.writeheader()
        w.writerows(summary_rows)
    with open(out / "quench_pv_selection_long.csv", "w", newline="",
              encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["event_id", "pv", "t_rel_s", "value"])
        w.writerows(long_rows)

    # per-event panels
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    n = len(events)
    cols = 2
    rows_n = (n + cols - 1) // cols
    fig, axes = plt.subplots(rows_n, cols, figsize=(13, 3.2 * rows_n),
                             sharex=True)
    axes = axes.ravel()
    for ax, ev in zip(axes, events):
        eid = ev["event_id"]
        t0 = ev["t0_offset_s"]
        for pv, color in (("SR:RF:CRM1:BCL01", "tab:blue"),
                          ("SR:RF:CRM1:BCL03", "tab:red")):
            pts = plot_data[eid].get(pv, [])
            if pts:
                ax.plot([t - (datetime.strptime(ev["timestamp"],
                                                "%Y-%m-%dT%H:%M:%S")
                              .replace(tzinfo=KST).timestamp() + t0)
                         for t, _ in pts],
                        [v for _, v in pts], color=color, lw=1.2,
                        label=pv.split(":")[-1])
        ax.axvline(0, color="gray", ls="--", lw=1)
        ax.set_xlim(-args.pre, args.post)
        ax.set_title(f"{eid}  (t0={t0:+.1f}s)", fontsize=9)
        ax.grid(alpha=0.25)
        ax.legend(loc="upper right", fontsize=7)
        ax.set_ylabel("K")
    for ax in axes[n:]:
        ax.set_visible(False)
    fig.suptitle("PV-selection series for quench events (CM1 BCL01/BCL03)",
                 fontsize=12)
    fig.tight_layout()
    png = out / "quench_pv_selection_bcl.png"
    fig.savefig(png, dpi=140)
    plt.close(fig)

    print("\n== summary ==")
    for r in summary_rows:
        print(f"{r['event_id']} {r['pv'].split(':')[-1]:6s} n={r['n']:>3} "
              f"pre={r['pre_med']} max={r['max']} max_t={r['max_t_s']} "
              f"delta={r['delta_max']} err={r['error'] or '-'}")
    print(f"\nwrote {out / 'quench_pv_selection_events.csv'}")
    print(f"wrote {out / 'quench_pv_selection_summary.csv'}")
    print(f"wrote {out / 'quench_pv_selection_long.csv'}")
    print(f"wrote {png}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
