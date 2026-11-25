#!/usr/bin/env python3
"""Compare Archiver RF signals across quench events (and non-quench controls).

For every event the script resolves t0 from the beam-current 50% drop, pulls
the same RF/cryo PV set over [t0-60, t0+10] s, and computes:

    pre_med      median over [t0-60, t0-10]
    t0_val       nearest sample to t0
    post_med     median over [t0+1, t0+10]
    min/max      within [t0-2, t0+10]
    post_ratio   post_med / pre_med
    delta_t0     t0_val - pre_med

The commonality table counts how many events show each expected signature
(RF collapse, phase jump, detune jump, gradient-error spike, cavity heating).

Outputs (default data/archiver_rf):
    quench_rf_events.csv        event list, group, era, t0
    quench_rf_features.csv      one row per event x PV
    quench_rf_commonality.csv   signature prevalence for quench vs control

Examples:
    venv/bin/python scripts/compare_quench_rf.py
    venv/bin/python scripts/compare_quench_rf.py --controls 15 --no-controls
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

from src.alarm.event_link import (  # noqa: E402
    ARCHIVER_CUTOVER,
    KST,
    fetch_pv_range,
    resolve_t0,
)

# label -> PV.  CM1 is the quench unit in every waveform event; CM2/CM3 serve
# as controls that still see the RF trip but not the quench.
PVS = {
    "GMES_CM1": "SR:RF:LRF1:GMES_ARCH",
    "GCRDC_CM1": "SR:RF:LRF1:GCRDC_ARCH",
    "CRFP_CM1": "SR:RF:LRF1:CRFP_ARCH",
    "CRRP_CM1": "SR:RF:LRF1:CRRP_ARCH",
    "PMES_CM1": "SR:RF:LRF1:PMES_ARCH",
    "CFQE_CM1": "SR:RF:LRF1:CFQE_ARCH",
    "CFQEA_CM1": "SR:RF:LRF1:CFQEA_ARCH",
    "DETA_CM1": "SR:RF:LRF1:DETA_ARCH",
    "GLDE_CM1": "SR:RF:LRF1:GLDE_ARCH",
    "GMES_CM2": "SR:RF:LRF2:GMES_ARCH",
    "CFQE_CM2": "SR:RF:LRF2:CFQE_ARCH",
    "GMES_CM3": "SR:RF:LRF3:GMES_ARCH",
    "CFQE_CM3": "SR:RF:LRF3:CFQE_ARCH",
    "BCL03_CM1": "SR:RF:CRM1:BCL03",
    "BCL03_CM2": "SR:RF:CRM2:BCL03",
    "BCL03_CM3": "SR:RF:CRM3:BCL03",
    "BPR01_CM1": "SR:RF:CRM1:BPR01",
    "BEAM": "SR:G00:BEAMCURRENT_T",
}
FIELDS = ["event_id", "group", "era", "label", "pv", "n", "dt_med_s",
          "pre_med", "t0_val", "post_med", "min", "min_t_s", "max", "max_t_s",
          "post_ratio", "min_ratio", "max_ratio", "delta_t0", "max_abs_dev",
          "error"]


def _quench_unit(notes: str | None) -> str:
    """Cavity that quenched, from the operator note (default CM1)."""
    txt = (notes or "").upper()
    for n in (2, 3):
        if f"CM{n}" in txt or f"CM#{n}" in txt or f"SRF{n}" in txt:
            return f"CM{n}"
    return "CM1"


def _get_events(conn: sqlite3.Connection, controls: bool, n_controls: int):
    quench = conn.execute(
        """
        SELECT id, timestamp, notes FROM events
        WHERE lower(coalesce(notes,'')) LIKE '%quench%'
           OR notes LIKE '%켄치%' OR notes LIKE '%퀜치%'
           OR lower(coalesce(user_fault_type,'')) LIKE '%quench%'
        ORDER BY timestamp
        """
    ).fetchall()
    out = [{"id": r["id"], "timestamp": r["timestamp"], "group": "quench",
            "unit": _quench_unit(r["notes"])}
           for r in quench]
    if controls:
        rows = conn.execute(
            """
            SELECT id, timestamp FROM events
            WHERE lower(coalesce(notes,'')) NOT LIKE '%quench%'
              AND notes NOT LIKE '%켄치%' AND notes NOT LIKE '%퀜치%'
              AND lower(coalesce(user_fault_type,'')) NOT LIKE '%quench%'
              AND fault_type IN ('Case 3: RF_Interlock','Case 4: MIS',
                                 'Case 1: Beam_loss')
            ORDER BY timestamp DESC LIMIT ?
            """,
            (n_controls,),
        ).fetchall()
        out += [{"id": r["id"], "timestamp": r["timestamp"],
                 "group": "control", "unit": "CM1"} for r in rows]
    return out


def _median(pts, lo: float, hi: float):
    xs = [v for t, v in pts if lo <= t <= hi]
    return statistics.median(xs) if xs else None


def _nearest(pts, target: float, tol: float = 4.0):
    if not pts:
        return None
    t, v = min(pts, key=lambda p: abs(p[0] - target))
    return v if abs(t - target) <= tol else None


def _extreme(pts, lo: float, hi: float, want_max: bool):
    sel = [(t, v) for t, v in pts if lo <= t <= hi]
    if not sel:
        return None, None
    return max(sel, key=lambda p: p[1]) if want_max else min(sel, key=lambda p: p[1])


def _ratio(a, b):
    if a is None or b is None or abs(b) < 1e-12:
        return None
    return a / b


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--controls", type=int, default=10)
    ap.add_argument("--no-controls", action="store_true")
    ap.add_argument("--pre", type=float, default=60.0)
    ap.add_argument("--post", type=float, default=10.0)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--out", default=str(ROOT / "data" / "archiver_rf"))
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(ROOT / "db" / "events.db")
    conn.row_factory = sqlite3.Row
    events = _get_events(conn, not args.no_controls, args.controls)
    conn.close()

    feature_rows = []
    event_rows = []
    for idx, ev in enumerate(events, 1):
        eid = ev["id"]
        base = datetime.strptime(ev["timestamp"], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=KST)
        t0, _ = resolve_t0(base.isoformat())
        t0dt = base + timedelta(seconds=t0)
        t0_epoch = t0dt.timestamp()
        start = t0dt - timedelta(seconds=args.pre)
        end = t0dt + timedelta(seconds=args.post)
        era = "current" if base >= ARCHIVER_CUTOVER else "legacy"
        event_rows.append({"event_id": eid, "timestamp": ev["timestamp"],
                           "group": ev["group"], "era": era,
                           "quench_unit": ev["unit"], "t0_offset_s": t0})
        print(f"[{idx}/{len(events)}] {ev['group']:7s} {eid} t0={t0:+.3f}s",
              flush=True)

        def call(pv: str):
            try:
                return pv, fetch_pv_range(pv, start, end, ref=base), ""
            except Exception as exc:  # noqa: BLE001
                return pv, ([], {}), str(exc)

        results: dict[str, list[tuple[float, float]]] = {}
        errors: dict[str, str] = {}
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(call, pv): label
                       for label, pv in PVS.items()}
            for fut in as_completed(futures):
                label = futures[fut]
                pv, (pts, _meta), err = fut.result()
                results[label] = [(t, v) for t, v in pts]
                if err:
                    errors[label] = err

        for label, pv in PVS.items():
            pts = results[label]
            pre = _median(pts, t0_epoch - args.pre, t0_epoch - 10)
            post = _median(pts, t0_epoch + 1, t0_epoch + args.post)
            t0v = _nearest(pts, t0_epoch)
            tmin, vmin = _extreme(pts, t0_epoch - 2, t0_epoch + args.post, False)
            tmax, vmax = _extreme(pts, t0_epoch - 2, t0_epoch + args.post, True)
            vals = [v for _, v in pts]
            dts = sorted(b - a for a, b in zip([t for t, _ in pts],
                                               [t for t, _ in pts][1:]) if b > a)
            row = {
                "event_id": eid, "group": ev["group"], "era": era,
                "label": label, "pv": pv, "n": len(pts),
                "dt_med_s": round(statistics.median(dts), 3) if dts else "",
                "pre_med": pre, "t0_val": t0v, "post_med": post,
                "min": vmin, "min_t_s": (round(tmin - t0_epoch, 3)
                                         if tmin is not None else ""),
                "max": vmax, "max_t_s": (round(tmax - t0_epoch, 3)
                                         if tmax is not None else ""),
                "post_ratio": _ratio(post, pre),
                "min_ratio": _ratio(vmin, pre),
                "max_ratio": _ratio(vmax, pre),
                "delta_t0": (t0v - pre if t0v is not None and pre is not None
                             else None),
                "max_abs_dev": (max(abs(v - pre) for v in vals)
                                if vals and pre is not None else None),
                "error": errors.get(label, ""),
            }
            feature_rows.append(row)

    with open(out / "quench_rf_events.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(event_rows[0]))
        w.writeheader()
        w.writerows(event_rows)
    with open(out / "quench_rf_features.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(feature_rows)

    # ---- commonality -----------------------------------------------------
    def has_data(r):
        return int(r["n"]) > 0 and r["pre_med"] not in ("", None)

    def rule(r, kind):
        if kind == "collapse":
            x = r["post_ratio"]
            return x not in ("", None) and float(x) < 0.2
        if kind == "jump30":
            x = r["delta_t0"]
            return x not in ("", None) and abs(float(x)) > 30
        if kind == "jump100":
            x = r["delta_t0"]
            return x not in ("", None) and abs(float(x)) > 100
        if kind == "spike100":
            x = r["max"]
            return x not in ("", None) and float(x) > 100
        if kind == "heat5":
            if r["max"] in ("", None) or r["pre_med"] in ("", None):
                return False
            return float(r["max"]) - float(r["pre_med"]) > 5
        return False

    unit_by_event = {e["event_id"]: e["quench_unit"] for e in event_rows}
    rules = [
        ("RF collapse (post/pre < 0.2)", "GMES_CM1", "collapse"),
        ("RF collapse (post/pre < 0.2)", "CRFP_CM1", "collapse"),
        ("RF collapse (post/pre < 0.2)", "CRRP_CM1", "collapse"),
        ("RF collapse (post/pre < 0.2)", "GMES_CM2", "collapse"),
        ("RF collapse (post/pre < 0.2)", "GMES_CM3", "collapse"),
        ("Phase jump (|Δ| > 30)", "PMES_CM1", "jump30"),
        ("Detune jump (|Δ| > 30)", "DETA_CM1", "jump30"),
        ("Freq-error jump (|Δ| > 100)", "CFQE_CM1", "jump100"),
        ("Freq-error-angle jump (|Δ| > 30)", "CFQEA_CM1", "jump30"),
        ("Gradient-error spike (>100)", "GLDE_CM1", "spike100"),
        ("Quench-unit heating (max-pre > 5 K)", "BCL03_*", "heat5"),
    ]
    groups = ("quench-legacy", "quench-current", "control")
    common = []
    for title, label, kind in rules:
        for group in groups:
            if label == "BCL03_*":
                rows = [r for r in feature_rows
                        if r["group"] == group.split("-")[0] and has_data(r)
                        and r["label"] == f"BCL03_{unit_by_event[r['event_id']]}"
                        and (group == "control" or r["era"] == group.split("-")[1])]
            else:
                rows = [r for r in feature_rows
                        if r["label"] == label and has_data(r)
                        and (r["group"] == group or
                             (group.startswith("quench-") and
                              r["group"] == "quench" and
                              r["era"] == group.split("-")[1]))]
            hit = sum(1 for r in rows if rule(r, kind))
            pre_vals = [float(r["pre_med"]) for r in rows
                        if r["pre_med"] not in ("", None)]
            post_vals = [float(r["post_med"]) for r in rows
                         if r["post_med"] not in ("", None)]
            med_pre = statistics.median(pre_vals) if pre_vals else None
            med_post = statistics.median(post_vals) if post_vals else None
            actual = label if label != "BCL03_*" else "BCL03_<quench unit>"
            common.append({"signature": title, "label": actual,
                           "group": group, "available": len(rows),
                           "matched": hit,
                           "pct": round(100.0 * hit / len(rows), 1) if rows else "",
                           "median_pre": med_pre, "median_post": med_post})
    with open(out / "quench_rf_commonality.csv", "w", newline="",
              encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(common[0]))
        w.writeheader()
        w.writerows(common)

    print("\n== commonality ==")
    print(f"{'signature':38s} {'label':12s} {'group':15s} {'matched':>7s} "
          f"{'available':>9s} {'pct':>6s}")
    for c in common:
        print(f"{c['signature']:38s} {c['label']:12s} {c['group']:15s} "
              f"{c['matched']:>7} {c['available']:>9} {str(c['pct']):>6}")
    print(f"\nwrote {out / 'quench_rf_features.csv'}")
    print(f"wrote {out / 'quench_rf_commonality.csv'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
