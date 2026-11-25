#!/usr/bin/env python3
"""List the Archiver-supported RF PVs and optionally test/pull them per event.

The Archiver inventory under SR/RF_LLRF, SR/RF_Tuner and SR/RF_CM1..3 holds
570 PVs.  Many are settings (gains, filters) or cryogenic sensors, so this
script categorises them and tests only the RF signal/state PVs for a given
event.  Availability is recorded because the pre-2025-09-18 legacy viewer does
not archive every LRF PV (GLDE/GCRDC/CFQE/CFQEA/QL are missing there).

Outputs (default data/archiver_rf):
    supported_rf_pvs.csv                 full 570-PV inventory + category
    <event>_supported_rf_pvs.csv         tested subset: n, dt, t0 values, error
    <event>_all_rf_long.csv              raw points of tested PVs with data
    <event>_all_rf_wide_1s.csv           1 s grid (as-of forward fill)

Examples:
    venv/bin/python scripts/list_supported_rf_pvs.py
    venv/bin/python scripts/list_supported_rf_pvs.py --event 20240907_025313
    venv/bin/python scripts/list_supported_rf_pvs.py --event 20251205_014253
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.alarm.event_link import KST, fetch_pv_range, resolve_t0  # noqa: E402

ARCHIVER = "https://plsarchiver.postech.ac.kr"
SUBS = ("RF_LLRF", "RF_Tuner", "RF_CM1", "RF_CM2", "RF_CM3")

# LLRF analog/status suffixes that carry a signal rather than a setting.
SIGNAL_TOKENS = {
    "AUXI", "AUXM", "AUXP", "AUXQ",
    "CRFPI", "CRFPP", "CRFPQ", "CRFP",
    "CRRPI", "CRRPP", "CRRPQ", "CRRP",
    "GASK", "GCRDC", "GLDE", "GMES", "IMES",
    "PASK", "PCRDC", "PLDE", "PMES",
    "QASK", "QMES", "GLOSr", "GDCLr",
    "ISET", "GSET", "PSET", "QSET",
    "POFFr", "TDOFFr", "PSEThr",
}
STATE_TOKENS = {"CNTL2MODE", "RFONr", "STAT1"}
TUNER_SIGNALS = {"CFQE", "CFQEA", "DETA", "QL"}
TEST_CATEGORIES = {"rf_signal", "rf_state"}


def _inventory() -> list[dict]:
    items: list[dict] = []
    for sub in SUBS:
        q = urllib.parse.urlencode({"class": "SR", "sub": sub})
        with urllib.request.urlopen(f"{ARCHIVER}/api/v1/db/pvs?{q}", timeout=30) as r:
            for p in json.loads(r.read().decode())["data"]["items"]:
                items.append({"sub": sub, "pv": p.get("pvKey", ""),
                              "description": p.get("description", "")})
    return items


def _category(sub: str, pv: str) -> str:
    if sub.startswith("RF_CM"):
        return "cryo_vacuum"
    token = pv.split(":")[-1]
    if token.endswith("_ARCH"):
        token = token[:-5]
    if sub == "RF_Tuner":
        if token in TUNER_SIGNALS:
            return "rf_signal"
        if token.startswith("TDETA") or token.startswith("THDRV"):
            return "tuner_state"
        return "tuner_other"
    if token in SIGNAL_TOKENS:
        return "rf_signal"
    if token in STATE_TOKENS:
        return "rf_state"
    if token.startswith(("IIRK", "IQ", "PLSG", "PLSO", "TMP")):
        return "rf_config"
    return "rf_other"


def _nearest(pts, target: float, tol: float = 4.0):
    if not pts:
        return None
    t, v = min(pts, key=lambda p: abs(p[0] - target))
    return v if abs(t - target) <= tol else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--event", default="")
    ap.add_argument("--pre", type=float, default=60.0)
    ap.add_argument("--post", type=float, default=10.0)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--out", default=str(ROOT / "data" / "archiver_rf"))
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    items = _inventory()
    for p in items:
        p["category"] = _category(p["sub"], p["pv"])
    with open(out / "supported_rf_pvs.csv", "w", newline="",
              encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["sub", "category", "pv", "description"])
        w.writeheader()
        w.writerows(items)

    by_cat: dict[str, int] = {}
    for p in items:
        by_cat[p["category"]] = by_cat.get(p["category"], 0) + 1
    print("inventory:", len(items), by_cat)
    print(f"wrote {out / 'supported_rf_pvs.csv'}")

    if not args.event:
        return 0

    base = datetime.strptime(args.event, "%Y%m%d_%H%M%S").replace(tzinfo=KST)
    t0, _ = resolve_t0(base.isoformat())
    t0dt = base + timedelta(seconds=t0)
    start = t0dt - timedelta(seconds=args.pre)
    end = t0dt + timedelta(seconds=args.post)
    t0_epoch = t0dt.timestamp()
    print(f"event={args.event} t0_offset={t0:+.3f}s t0={t0dt.isoformat()}")

    tested = [p for p in items if p["category"] in TEST_CATEGORIES]
    tested.append({"sub": "G00", "category": "beam",
                   "pv": "SR:G00:BEAMCURRENT_T", "description": "Beam current"})
    print(f"testing {len(tested)} signal/state PVs...", flush=True)

    def call(pv: str):
        try:
            return pv, fetch_pv_range(pv, start, end, ref=base), ""
        except Exception as exc:  # noqa: BLE001
            return pv, ([], {}), str(exc)

    results: dict[str, list[tuple[float, float]]] = {}
    errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(call, p["pv"]): p for p in tested}
        for fut in as_completed(futures):
            p = futures[fut]
            pv, (pts, _meta), err = fut.result()
            results[pv] = pts
            if err:
                errors[pv] = err

    rows = []
    for p in tested:
        pts = results.get(p["pv"], [])
        dts = sorted(b - a for a, b in zip([t for t, _ in pts],
                                           [t for t, _ in pts][1:]) if b > a)
        row = {
            "event_id": args.event, "sub": p["sub"], "category": p["category"],
            "pv": p["pv"], "description": p["description"], "n": len(pts),
            "dt_med_s": round(statistics.median(dts), 3) if dts else "",
            "error": errors.get(p["pv"], ""),
        }
        for o in (-60, -10, -1, 0, 1, 5, 10, 60):
            v = _nearest(pts, t0_epoch + o)
            row[f"v{o:+d}s"] = "" if v is None else v
        rows.append(row)
    fields = list(rows[0])
    check = out / f"{args.event}_supported_rf_pvs.csv"
    with open(check, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    available = [r for r in rows if r["n"] > 0]
    print(f"available={len(available)}/{len(rows)} errors={len(errors)}")
    missing = [r["pv"].split(":")[-1] for r in rows if r["n"] == 0]
    print("no_data:", ", ".join(missing))

    # raw long + wide 1 s for the PVs that returned data
    with open(out / f"{args.event}_all_rf_long.csv", "w", newline="",
              encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["event_id", "pv", "t_rel_s", "value"])
        for r in available:
            for t, v in results[r["pv"]]:
                w.writerow([args.event, r["pv"], round(t - t0_epoch, 6), v])

    grid = range(-int(args.pre), int(args.post) + 1)
    with open(out / f"{args.event}_all_rf_wide_1s.csv", "w", newline="",
              encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["t_rel_s"] + [r["pv"] for r in available])
        for k in grid:
            target = t0_epoch + k
            row = [k]
            for r in available:
                pts = results[r["pv"]]
                best = None
                for t, v in pts:
                    if t <= target:
                        best = v
                    else:
                        break
                row.append("" if best is None else best)
            w.writerow(row)
    print(f"wrote {check}")
    print(f"wrote {out / f'{args.event}_all_rf_long.csv'}")
    print(f"wrote {out / f'{args.event}_all_rf_wide_1s.csv'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
