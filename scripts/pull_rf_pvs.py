#!/usr/bin/env python3
"""Pull multiple RF Archiver PVs at once for a representative quench event.

The Archiver stores the LLRF RF PVs at roughly 1 Hz, so this script cannot
reproduce the 2 us oscilloscope waveform.  It is meant to show the macroscopic
RF sequence (gradient/forward/reflect collapse, phase jump, gradient-error
spike) around the beam-current drop, with all PVs fetched concurrently over
the same absolute window.

Notes
-----
* ``CavV01`` is not an Archiver PV.  The cavity-field proxies are
  ``SR:RF:LRF1:GMES_ARCH`` (probe gradient) and ``GCRDC_ARCH`` (CORDIC read);
  probe I/Q/phase are ``IMES/QMES/PMES``.  Asking the current API for a name
  that does not exist returns HTTP 500, not 404.
* ``BCL03``/``BPR01``/``BPO02`` are cryogenic/tuner PVs sampled every
  1.6-3.4 s; they are included only as slow context.
* The 2 us waveform itself lives in ``data/merged/event_<id>.parquet`` and is
  plotted separately (--waveform).

Outputs (default data/archiver_rf):
    <event>_rf_long.csv       raw points: event_id,pv,t_rel_s,value
    <event>_rf_wide_1s.csv    1 s grid, one column per PV (as-of forward fill)
    <event>_rf_summary.csv    n/dt and values at fixed offsets
    <event>_rf_archiver.png   multi-panel Archiver view
    <event>_rf_waveform.png   100 ms waveform zoom (if --waveform)

Examples:
    venv/bin/python scripts/pull_rf_pvs.py
    venv/bin/python scripts/pull_rf_pvs.py --events 20251205_014253 --pre 120 --post 120
"""

from __future__ import annotations

import argparse
import csv
import math
import statistics
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.alarm.event_link import KST, fetch_pv_range, resolve_t0  # noqa: E402

# label -> (PV, panel group)
PVS: dict[str, tuple[str, str]] = {
    "GMES_CM1": ("SR:RF:LRF1:GMES_ARCH", "rf"),
    "GMES_CM2": ("SR:RF:LRF2:GMES_ARCH", "rf"),
    "GMES_CM3": ("SR:RF:LRF3:GMES_ARCH", "rf"),
    "CRFP_CM1": ("SR:RF:LRF1:CRFP_ARCH", "rf"),
    "CRRP_CM1": ("SR:RF:LRF1:CRRP_ARCH", "rf"),
    "GCRDC_CM1": ("SR:RF:LRF1:GCRDC_ARCH", "rf"),
    "IMES_CM1": ("SR:RF:LRF1:IMES_ARCH", "rf"),
    "QMES_CM1": ("SR:RF:LRF1:QMES_ARCH", "rf"),
    "PMES_CM1": ("SR:RF:LRF1:PMES_ARCH", "rf"),
    "GLDE_CM1": ("SR:RF:LRF1:GLDE_ARCH", "rf"),
    "CFQE_CM1": ("SR:RF:LRF1:CFQE_ARCH", "tune"),
    "CFQEA_CM1": ("SR:RF:LRF1:CFQEA_ARCH", "tune"),
    "DETA_CM1": ("SR:RF:LRF1:DETA_ARCH", "tune"),
    "QL_CM1": ("SR:RF:LRF1:QL_ARCH", "tune"),
    "CFQE_CM2": ("SR:RF:LRF2:CFQE_ARCH", "tune"),
    "CFQE_CM3": ("SR:RF:LRF3:CFQE_ARCH", "tune"),
    "GSET_CM1": ("SR:RF:LRF1:GSET_ARCH", "state"),
    "RFON_CM1": ("SR:RF:LRF1:RFONr_ARCH", "state"),
    "BEAM": ("SR:G00:BEAMCURRENT_T", "beam"),
    "BCL03_CM1": ("SR:RF:CRM1:BCL03", "slow"),
    "BCL03_CM2": ("SR:RF:CRM2:BCL03", "slow"),
    "BCL03_CM3": ("SR:RF:CRM3:BCL03", "slow"),
    "BPR01_CM1": ("SR:RF:CRM1:BPR01", "slow"),
    "BPO02_CM1": ("SR:RF:CRM1:BPO02", "slow"),
}
OFFSETS = (-60, -10, -1, 0, 1, 5, 10, 60)


def _fetch_one(pv: str, start: datetime, end: datetime, ref: datetime):
    try:
        pts, meta = fetch_pv_range(pv, start, end, ref=ref)
        return pv, pts, meta, ""
    except Exception as exc:  # noqa: BLE001
        return pv, [], {}, str(exc)


def _asof(points: list[tuple[float, float]], target: float):
    """Last value at or before target (forward fill)."""
    best = None
    for t, v in points:
        if t <= target:
            best = v
        else:
            break
    return best


def _nearest(points: list[tuple[float, float]], target: float, tol: float = 4.0):
    if not points:
        return None
    t, v = min(points, key=lambda p: abs(p[0] - target))
    return v if abs(t - target) <= tol else None


def _plot_archiver(event_id: str, pre: float, post: float,
                    series: dict[str, list[tuple[float, float]]],
                    out: Path, t0_epoch: float) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(6, 1, figsize=(13, 15), sharex=True)
    fig.suptitle(f"{event_id} — Archiver RF PVs (t0 = beam-current drop)", fontsize=12)

    def get(label: str) -> tuple[np.ndarray, np.ndarray]:
        pts = series.get(label) or []
        if not pts:
            return np.array([]), np.array([])
        arr = np.array(pts, dtype=float)
        return arr[:, 0] - t0_epoch, arr[:, 1]

    # panel 1: beam + normalized gradients
    ax = axes[0]
    tb, vb = get("BEAM")
    if tb.size:
        ax.plot(tb, vb, color="black", lw=1.2, label="Beam current (mA)")
    ax.set_ylabel("Beam current (mA)")
    ax2 = ax.twinx()
    for label, color in (("GMES_CM1", "tab:red"), ("GMES_CM2", "tab:blue"),
                         ("GMES_CM3", "tab:green")):
        t, v = get(label)
        if not t.size:
            continue
        base = np.nanmedian(v[(t >= -pre) & (t <= -pre + 20)])
        if not np.isfinite(base) or abs(base) < 1e-9:
            base = 1.0
        ax2.plot(t, v / base, color=color, lw=1.2, label=label)
    ax2.set_ylabel("GMES / pre-event baseline")
    ax.set_title("Beam current and probe gradient (CM1/2/3)")
    lines = ax.get_lines() + ax2.get_lines()
    ax.legend(lines, [l.get_label() for l in lines], loc="upper right", fontsize=8)

    # panel 2: forward / reflected / CORDIC
    ax = axes[1]
    for label, color in (("CRFP_CM1", "tab:orange"), ("CRRP_CM1", "tab:purple"),
                         ("GCRDC_CM1", "tab:brown")):
        t, v = get(label)
        if t.size:
            ax.plot(t, v, color=color, lw=1.2, label=label)
    ax.set_ylabel("LLRF units")
    ax.set_title("CM1 forward / reflected / gradient CORDIC")
    ax.legend(loc="upper right", fontsize=8)

    # panel 3: phase + gradient error
    ax = axes[2]
    tp, vp = get("PMES_CM1")
    tg, vg = get("GLDE_CM1")
    if tp.size:
        ax.plot(tp, vp, color="tab:cyan", lw=1.2, label="PMES (probe phase)")
    ax.set_ylabel("Probe phase")
    ax3 = ax.twinx()
    if tg.size:
        ax3.plot(tg, vg, color="tab:red", lw=1.0, alpha=0.8,
                 label="GLDE (gradient error)")
        ax3.set_ylabel("Gradient error")
        ax.set_title("CM1 probe phase and gradient error")
    else:
        ax3.set_visible(False)
        ax.set_title("CM1 probe phase (GLDE not archived before 2025-09-18)")
    lines = ax.get_lines() + ax3.get_lines()
    ax.legend(lines, [l.get_label() for l in lines], loc="upper right", fontsize=8)

    # panel 4: cavity frequency error (RF_Tuner subsystem), symlog because
    # the three cavities span ~0 to ~10^4 in the same window.
    ax = axes[3]
    for label, color in (("CFQE_CM1", "tab:red"), ("CFQE_CM2", "tab:blue"),
                         ("CFQE_CM3", "tab:green")):
        t, v = get(label)
        if t.size:
            ax.plot(t, v, color=color, lw=1.2, label=label)
    ax.set_yscale("symlog", linthresh=100)
    ax.set_ylabel("CFQE (freq. error)")
    ax.set_title("CM1/2/3 cavity frequency error (symlog)")
    if ax.get_lines():
        ax.legend(loc="upper right", fontsize=8)
    else:
        ax.text(0.5, 0.5, "CFQE not archived before 2025-09-18",
                transform=ax.transAxes, ha="center", va="center",
                color="gray", fontsize=10)
        ax.set_yticks([])

    # panel 5: detune angle / frequency-error angle
    ax = axes[4]
    td, vd = get("DETA_CM1")
    tc, vc = get("CFQEA_CM1")
    if td.size:
        ax.plot(td, vd, color="tab:orange", lw=1.2, label="DETA_CM1")
    if tc.size:
        ax.plot(tc, vc, color="tab:purple", lw=1.0, alpha=0.85,
                label="CFQEA_CM1")
    ax.set_ylabel("angle (deg)")
    ax.set_title("CM1 detune angle and frequency-error angle")
    ax.legend(loc="upper right", fontsize=8)

    # panel 6: slow cryogenic context
    ax = axes[5]
    for label, color in (("BCL03_CM1", "tab:red"), ("BCL03_CM2", "tab:blue"),
                         ("BCL03_CM3", "tab:green")):
        t, v = get(label)
        if t.size:
            ax.plot(t, v, color=color, lw=1.2, marker=".", ms=2, label=label)
    ax.set_ylabel("BCL03 temperature (K)")
    ax.set_xlabel("time relative to t0 (s)")
    ax.set_title("Cavity top-equator temperature (slow, ~1.6-2 s)")
    ax.legend(loc="upper right", fontsize=8)

    for ax in axes:
        ax.axvline(0, color="gray", ls="--", lw=1)
        ax.grid(alpha=0.25)
        # The current API includes the last sample before `from` (which can be
        # tens of seconds early for slow PVs); keep the requested window.
        ax.set_xlim(-pre, post)
    fig.tight_layout()
    fig.savefig(out, dpi=140)
    plt.close(fig)


def _plot_waveform(event_id: str, out: Path) -> bool:
    parquet = ROOT / "data" / "merged" / f"event_{event_id}.parquet"
    if not parquet.is_file():
        return False
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import polars as pl

    df = pl.read_parquet(str(parquet))
    t = df["t_rel_s"].to_numpy() * 1000.0
    fig, axes = plt.subplots(3, 1, figsize=(12, 9), sharex=True)
    fig.suptitle(f"{event_id} — waveform zoom ($\\pm$50 ms)", fontsize=12)
    groups = [
        ("BeamCurrent", ["Cavity_SRF1", "Cavity_SRF2", "Cavity_SRF3"]),
        ("BeamCurrent", ["Forward_SRF1", "Forward_SRF2", "Forward_SRF3"]),
        ("BeamCurrent", ["Reflect_SRF1", "Reflect_SRF2", "Reflect_SRF3"]),
    ]
    titles = ["Cavity (SRF1/2/3)", "Forward (SRF1/2/3)", "Reflect (SRF1/2/3)"]
    for ax, (beam_col, cols), title in zip(axes, groups, titles):
        b = df[f"{beam_col}_v"].fill_null(np.nan).to_numpy()
        ax.plot(t, b, color="black", lw=0.8, alpha=0.55, label="BeamCurrent")
        for col in cols:
            v = df[f"{col}_v"].fill_null(np.nan).to_numpy()
            ax.plot(t, v, lw=1.0, label=col)
        ax.set_title(title)
        ax.set_ylabel("normalized")
        ax.grid(alpha=0.25)
        ax.legend(loc="upper right", fontsize=8, ncol=2)
    axes[-1].set_xlabel("time relative to trigger (ms)")
    for ax in axes:
        ax.axvline(0, color="gray", ls="--", lw=1)
    fig.tight_layout()
    fig.savefig(out, dpi=140)
    plt.close(fig)
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--events", default="20251205_014253,20240907_025313")
    ap.add_argument("--pre", type=float, default=60.0)
    ap.add_argument("--post", type=float, default=60.0)
    ap.add_argument("--out", default=str(ROOT / "data" / "archiver_rf"))
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--waveform", action="store_true", default=True)
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    for eid in [x.strip() for x in args.events.split(",") if x.strip()]:
        base = datetime.strptime(eid, "%Y%m%d_%H%M%S").replace(tzinfo=KST)
        t0, _ = resolve_t0(base.isoformat())
        t0dt = base + timedelta(seconds=t0)
        start = t0dt - timedelta(seconds=args.pre)
        end = t0dt + timedelta(seconds=args.post)
        t0_epoch = t0dt.timestamp()
        print(f"\n=== {eid} t0={t0:+.3f}s window=[-{args.pre:g},+{args.post:g}]s", flush=True)

        results: dict[str, list[tuple[float, float]]] = {}
        meta: dict[str, dict] = {}
        errors: dict[str, str] = {}
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = [pool.submit(_fetch_one, pv, start, end, base)
                       for pv, _ in PVS.values()]
            for fut in as_completed(futures):
                pv, pts, m, err = fut.result()
                label = next(k for k, (p, _) in PVS.items() if p == pv)
                results[label] = [(t, v) for t, v in pts]
                meta[label] = m
                if err:
                    errors[label] = err
                print(f"  {label:12s} {pv:26s} n={len(pts):4d}"
                      + (f"  ERROR={err}" if err else ""), flush=True)

        # raw long
        with open(out / f"{eid}_rf_long.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["event_id", "label", "pv", "t_rel_s", "value"])
            for label, pts in results.items():
                pv = PVS[label][0]
                for t, v in pts:
                    w.writerow([eid, label, pv, round(t - t0_epoch, 6), v])

        # 1 s wide grid (as-of forward fill)
        grid = [k for k in range(-int(args.pre), int(args.post) + 1)]
        with open(out / f"{eid}_rf_wide_1s.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["t_rel_s"] + list(PVS))
            for k in grid:
                target = t0_epoch + k
                row = [k]
                for label in PVS:
                    v = _asof(results.get(label, []), target)
                    row.append("" if v is None else v)
                w.writerow(row)

        # summary
        with open(out / f"{eid}_rf_summary.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["event_id", "label", "pv", "n", "dt_med_s"]
                       + [f"v{o:+d}s" for o in OFFSETS]
                       + ["min", "t_min_s", "max", "t_max_s"])
            for label, (pv, _) in PVS.items():
                pts = results.get(label, [])
                dts = sorted(b - a for a, b in zip(
                    [t for t, _ in pts], [t for t, _ in pts][1:]) if b > a)
                row = [eid, label, pv, len(pts),
                       round(statistics.median(dts), 3) if dts else ""]
                row += [("" if (v := _nearest(pts, t0_epoch + o)) is None else v)
                        for o in OFFSETS]
                if pts:
                    tmin, vmin = min(pts, key=lambda p: p[1])
                    tmax, vmax = max(pts, key=lambda p: p[1])
                    row += [vmin, round(tmin - t0_epoch, 3),
                            vmax, round(tmax - t0_epoch, 3)]
                else:
                    row += ["", "", "", ""]
                w.writerow(row)

        png = out / f"{eid}_rf_archiver.png"
        _plot_archiver(eid, args.pre, args.post, results, png, t0_epoch)
        print(f"  wrote {png}", flush=True)
        if args.waveform:
            wpng = out / f"{eid}_rf_waveform.png"
            if _plot_waveform(eid, wpng):
                print(f"  wrote {wpng}", flush=True)

    print(f"\nDONE {out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
