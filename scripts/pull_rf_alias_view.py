#!/usr/bin/env python3
"""Call the supported RF PVs behind unsupported LLRF channel names.

Operators often refer to the oscilloscope/LLRF channel names ``CavV01``,
``FwdPow01``, ``CavP01``, ``RefV01``.  Those exact names are not in the
Archiver inventory (570 RF PVs) and the current API answers HTTP 500 for
them.  This script maps them to the supported LRF PVs, calls all of them over
the same t0 window, and writes a focused alias-labelled view.

Example:
    venv/bin/python scripts/pull_rf_alias_view.py --event 20251205_014253
"""

from __future__ import annotations

import argparse
import csv
import statistics
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.alarm.event_link import KST, fetch_pv_range, resolve_t0  # noqa: E402

# requested name -> (display, supported PV, official description)
ALIASES = {
    "CavV01": ("Cavity voltage", "SR:RF:LRF1:GMES_ARCH",
               "LLRF#1-GMES Probe Gradient"),
    "FwdPow01": ("Forward power", "SR:RF:LRF1:CRFP_ARCH",
                 "LLRF#1-CRFP Forward RF Power"),
    "RefV01": ("Reflect power", "SR:RF:LRF1:CRRP_ARCH",
               "LLRF#1-CRRP Reflect RF Power"),
    "CavP01": ("Cavity phase", "SR:RF:LRF1:PMES_ARCH",
               "LLRF#1-PMES Probe Phase"),
}
EXTRA = {
    "GCRDC_CM1": ("SR:RF:LRF1:GCRDC_ARCH", "LLRF#1-GCRDC Gradient CORDIC Read"),
}
OFFSETS = (-60, -10, -1, 0, 1, 5, 10, 60)


def _nearest(pts, target: float, tol: float = 4.0):
    if not pts:
        return None
    t, v = min(pts, key=lambda p: abs(p[0] - target))
    return v if abs(t - target) <= tol else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--event", default="20251205_014253")
    ap.add_argument("--pre", type=float, default=60.0)
    ap.add_argument("--post", type=float, default=60.0)
    ap.add_argument("--out", default=str(ROOT / "data" / "archiver_rf"))
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    event_id = args.event
    base = datetime.strptime(event_id, "%Y%m%d_%H%M%S").replace(tzinfo=KST)
    t0, _ = resolve_t0(base.isoformat())
    t0dt = base + timedelta(seconds=t0)
    t0_epoch = t0dt.timestamp()
    start = t0dt - timedelta(seconds=args.pre)
    end = t0dt + timedelta(seconds=args.post)
    print(f"event={event_id} t0_offset={t0:+.3f}s t0={t0dt.isoformat()}")

    jobs = {name: info[1] for name, info in ALIASES.items()}
    jobs.update({name: info[0] for name, info in EXTRA.items()})
    jobs["BEAM"] = "SR:G00:BEAMCURRENT_T"

    def call(pv: str):
        try:
            return pv, fetch_pv_range(pv, start, end, ref=base), ""
        except Exception as exc:  # noqa: BLE001
            return pv, ([], {}), str(exc)

    results: dict[str, list[tuple[float, float]]] = {}
    errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(call, pv): name for name, pv in jobs.items()}
        for fut in as_completed(futures):
            name = futures[fut]
            pv, (pts, _meta), err = fut.result()
            results[name] = pts
            if err:
                errors[name] = err
            print(f"  {name:10s} {pv:28s} n={len(pts):4d}"
                  + (f"  ERROR={err}" if err else ""))

    def meta(name: str) -> tuple[str, str]:
        if name in ALIASES:
            return ALIASES[name][0], ALIASES[name][2]
        if name in EXTRA:
            return name, EXTRA[name][1]
        return "Beam current", "Beam current"

    # long + summary
    with open(out / f"{event_id}_rf_alias_long.csv", "w", newline="",
              encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["event_id", "requested", "display", "actual_pv",
                    "description", "t_rel_s", "value"])
        for name, pts in results.items():
            display, desc = meta(name)
            pv = jobs[name]
            for t, v in pts:
                w.writerow([event_id, name, display, pv, desc,
                            round(t - t0_epoch, 6), v])

    with open(out / f"{event_id}_rf_alias_summary.csv", "w", newline="",
              encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["event_id", "requested", "display", "actual_pv",
                    "description", "n", "dt_med_s"]
                   + [f"v{o:+d}s" for o in OFFSETS]
                   + ["min", "t_min_s", "max", "t_max_s"])
        for name, pts in results.items():
            dts = sorted(b - a for a, b in zip([t for t, _ in pts],
                                               [t for t, _ in pts][1:]) if b > a)
            display, desc = meta(name)
            pv = jobs[name]
            row = [event_id, name, display, pv, desc, len(pts),
                   round(statistics.median(dts), 3) if dts else ""]
            row += ["" if (v := _nearest(pts, t0_epoch + o)) is None else v
                    for o in OFFSETS]
            if pts:
                tmin, vmin = min(pts, key=lambda p: p[1])
                tmax, vmax = max(pts, key=lambda p: p[1])
                row += [vmin, round(tmin - t0_epoch, 3),
                        vmax, round(tmax - t0_epoch, 3)]
            else:
                row += ["", "", "", ""]
            w.writerow(row)

    # focused alias view
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(4, 1, figsize=(12, 11), sharex=True)
    fig.suptitle(
        f"{event_id} — requested LLRF names mapped to supported Archiver PVs",
        fontsize=12,
    )
    beams = np.array(results.get("BEAM", []), dtype=float)
    for ax, (name, (display, pv, desc)) in zip(axes, ALIASES.items()):
        pts = results.get(name, [])
        if pts:
            arr = np.array(pts, dtype=float)
            ax.plot(arr[:, 0] - t0_epoch, arr[:, 1], lw=1.3,
                    label=f"{name} → {pv.split(':')[-1]}")
        ax.set_ylabel(display)
        ax.set_title(f"{name} ({display})  →  {pv}  [{desc}]", fontsize=10)
        ax.grid(alpha=0.25)
        ax.legend(loc="upper right", fontsize=8)
        ax.axvline(0, color="gray", ls="--", lw=1)
        ax.set_xlim(-args.pre, args.post)
        if beams.size and name == "CavV01":
            twin = ax.twinx()
            twin.plot(beams[:, 0] - t0_epoch, beams[:, 1], color="black",
                      lw=1.0, alpha=0.55, label="Beam current (mA)")
            twin.set_ylabel("Beam current (mA)")
            lines = [l for l in ax.get_lines() + twin.get_lines()
                     if not l.get_label().startswith("_")]
            ax.legend(lines, [l.get_label() for l in lines],
                      loc="upper right", fontsize=8)
    axes[-1].set_xlabel("time relative to t0 (s)")
    fig.tight_layout()
    png = out / f"{event_id}_rf_alias_view.png"
    fig.savefig(png, dpi=140)
    plt.close(fig)
    print(f"wrote {png}")

    with open(out / f"{event_id}_rf_alias_map.csv", "w", newline="",
              encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["requested", "display", "actual_pv", "description",
                    "status"])
        for name, (display, pv, desc) in ALIASES.items():
            w.writerow([name, display, pv, desc,
                        "ERROR " + errors[name] if name in errors else "ok"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
