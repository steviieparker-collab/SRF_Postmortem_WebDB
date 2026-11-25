#!/usr/bin/env python3
"""Check supported Archiver PVs, t0 mapping, and actual calls per event.

For each requested event this answers:

1. Is t0 sane?  ``resolve_t0`` is compared with an independent 50%
   beam-current drop.
2. Which key RF PVs exist in the Archiver inventory?
3. What does each key PV return over the t0 window (n / dt / values / error)?

It also probes common wrong names (``CavV01`` etc.) with ``--probe-bad`` so an
HTTP 500 can be attributed to the PV name instead of the event/t0.

Examples:
    venv/bin/python scripts/check_event_pvs.py --event 20251205_014253
    venv/bin/python scripts/check_event_pvs.py --all-quench
    venv/bin/python scripts/check_event_pvs.py --events 20251205_014253,20240907_025313
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
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

KEY_PVS = {
    "GMES_CM1": "SR:RF:LRF1:GMES_ARCH",
    "GCRDC_CM1": "SR:RF:LRF1:GCRDC_ARCH",
    "IMES_CM1": "SR:RF:LRF1:IMES_ARCH",
    "QMES_CM1": "SR:RF:LRF1:QMES_ARCH",
    "PMES_CM1": "SR:RF:LRF1:PMES_ARCH",
    "CRFP_CM1": "SR:RF:LRF1:CRFP_ARCH",
    "CRRP_CM1": "SR:RF:LRF1:CRRP_ARCH",
    "GLDE_CM1": "SR:RF:LRF1:GLDE_ARCH",
    "CFQE_CM1": "SR:RF:LRF1:CFQE_ARCH",
    "CFQEA_CM1": "SR:RF:LRF1:CFQEA_ARCH",
    "DETA_CM1": "SR:RF:LRF1:DETA_ARCH",
    "QL_CM1": "SR:RF:LRF1:QL_ARCH",
    "GMES_CM2": "SR:RF:LRF2:GMES_ARCH",
    "CFQE_CM2": "SR:RF:LRF2:CFQE_ARCH",
    "GMES_CM3": "SR:RF:LRF3:GMES_ARCH",
    "CFQE_CM3": "SR:RF:LRF3:CFQE_ARCH",
    "BEAM": "SR:G00:BEAMCURRENT_T",
}
BAD_CANDIDATES = (
    "CavV01",
    "FwdPow01",
    "CavP01",
    "RefV01",
    "SR:RF:LRF1:CavV01",
    "SR:RF:LRF1:CAVV01",
    "SR:RF:CRM1:CavV01",
    "SR:RF:LRF1:GMES",  # valid signal, but legacy spelling on the current API
)
SUBS = ("RF_LLRF", "RF_Tuner", "RF_CM1", "RF_CM2", "RF_CM3")
OFFSETS = (-60, -1, 0, 1, 5, 10, 60)
FIELDS = (["event_id", "label", "pv", "subsystem", "supported", "n",
           "dt_med_s", "unit", "t0_offset_s", "beam50_offset_s", "error"]
          + [f"v{o:+d}s" for o in OFFSETS])


def _inventory() -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for sub in SUBS:
        q = urllib.parse.urlencode({"class": "SR", "sub": sub})
        with urllib.request.urlopen(f"{ARCHIVER}/api/v1/db/pvs?{q}", timeout=30) as r:
            out[sub] = json.loads(r.read().decode())["data"]["items"]
    return out


def _quench_events() -> list[str]:
    con = sqlite3.connect(ROOT / "db" / "events.db")
    con.row_factory = sqlite3.Row
    rows = con.execute(
        """
        SELECT id FROM events
        WHERE lower(coalesce(notes,'')) LIKE '%quench%'
           OR notes LIKE '%켄치%' OR notes LIKE '%퀜치%'
           OR lower(coalesce(user_fault_type,'')) LIKE '%quench%'
        ORDER BY timestamp
        """
    ).fetchall()
    con.close()
    return [r["id"] for r in rows
            if (ROOT / "data" / "merged" / f"event_{r['id']}.parquet").is_file()]


def _nearest(pts, target: float, tol: float = 4.0):
    if not pts:
        return None
    t, v = min(pts, key=lambda p: abs(p[0] - target))
    return v if abs(t - target) <= tol else None


def _check_one(event_id: str, all_keys: dict[str, tuple[str, str]],
               args) -> tuple[list[dict], dict]:
    base = datetime.strptime(event_id, "%Y%m%d_%H%M%S").replace(tzinfo=KST)
    t0, _ = resolve_t0(base.isoformat())
    t0dt = base + timedelta(seconds=t0)
    t0_epoch = t0dt.timestamp()
    start = t0dt - timedelta(seconds=args.pre)
    end = t0dt + timedelta(seconds=args.post)

    # Independent t0 sanity check: 50% beam-current drop.
    beam50 = None
    try:
        bpts, _ = fetch_pv_range(
            "SR:G00:BEAMCURRENT_T",
            base - timedelta(seconds=60), base + timedelta(seconds=60), ref=base,
        )
        pre = [v for t, v in bpts if t < t0_epoch - 2]
        ref = statistics.median(pre) if pre else None
        if ref:
            hit = next((t for t, v in bpts
                        if t >= t0_epoch - 5 and v < 0.5 * ref), None)
            beam50 = round(hit - base.timestamp(), 3) if hit is not None else None
    except Exception:  # noqa: BLE001
        pass

    def call(pv: str):
        try:
            return pv, fetch_pv_range(pv, start, end, ref=base), ""
        except Exception as exc:  # noqa: BLE001
            return pv, ([], {}), str(exc)

    rows: list[dict] = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(call, pv): (label, pv)
                   for label, pv in KEY_PVS.items()}
        for fut in as_completed(futures):
            label, pv = futures[fut]
            _, (pts, meta), err = fut.result()
            sub = all_keys.get(pv, ("", ""))[0]
            dts = sorted(b - a for a, b in zip([t for t, _ in pts],
                                               [t for t, _ in pts][1:]) if b > a)
            row = {
                "event_id": event_id, "label": label, "pv": pv,
                "subsystem": sub, "supported": bool(sub) or label == "BEAM",
                "n": len(pts),
                "dt_med_s": round(statistics.median(dts), 3) if dts else "",
                "unit": (meta or {}).get("unit", ""),
                "t0_offset_s": t0, "beam50_offset_s": beam50,
                "error": err,
            }
            for o in OFFSETS:
                v = _nearest(pts, t0_epoch + o)
                row[f"v{o:+d}s"] = "" if v is None else v
            rows.append(row)
    rows.sort(key=lambda r: list(KEY_PVS).index(r["label"]))
    return rows, {"event_id": event_id, "t0": t0, "beam50": beam50}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--event", default="")
    ap.add_argument("--events", default="")
    ap.add_argument("--all-quench", action="store_true")
    ap.add_argument("--pre", type=float, default=60.0)
    ap.add_argument("--post", type=float, default=10.0)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--probe-bad", action="store_true", default=None)
    ap.add_argument("--out", default=str(ROOT / "data" / "archiver_rf"))
    args = ap.parse_args()

    if args.all_quench:
        events = _quench_events()
    else:
        raw = args.events or args.event or "20251205_014253"
        events = [x.strip() for x in raw.split(",") if x.strip()]
    if not events:
        raise SystemExit("no events selected")

    probe_bad = len(events) == 1 if args.probe_bad is None else args.probe_bad
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    inventory = _inventory()
    all_keys: dict[str, tuple[str, str]] = {}
    for sub, items in inventory.items():
        for p in items:
            all_keys[p.get("pvKey", "")] = (sub, p.get("description", ""))
    print("supported PV counts:",
          {sub: len(items) for sub, items in inventory.items()},
          "total", len(all_keys), flush=True)

    combined: list[dict] = []
    print(f"\n{'event':18s} {'t0':>10s} {'beam50':>10s} {'ok':>5s} {'nodata':>7s} {'err':>4s}")
    for event_id in events:
        rows, info = _check_one(event_id, all_keys, args)
        combined.extend(rows)
        per = out / f"{event_id}_pv_check.csv"
        with open(per, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS)
            w.writeheader()
            w.writerows(rows)
        errors = [r for r in rows if r["error"]]
        nodata = [r for r in rows if not r["error"] and r["n"] == 0]
        ok = len(rows) - len(errors) - len(nodata)
        beam_s = "-" if info["beam50"] is None else f"{info['beam50']:.3f}"
        print(f"{event_id:18s} {info['t0']:10.3f} {beam_s:>10s}", flush=True)
        print(f"    ok={ok}/{len(rows)} nodata={len(nodata)} errors={len(errors)}"
              + (f"  ERROR {errors[0]['label']}: {errors[0]['error']}"
                 if errors else ""), flush=True)

    combined_path = out / "all_events_pv_check.csv"
    with open(combined_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(combined)
    print(f"\nwrote {combined_path}", flush=True)

    if probe_bad:
        print("\nwrong-name probes (expect 500/no_data, not a t0 problem):")
        base = datetime.strptime(events[0], "%Y%m%d_%H%M%S").replace(tzinfo=KST)
        t0, _ = resolve_t0(base.isoformat())
        t0dt = base + timedelta(seconds=t0)
        for pv in BAD_CANDIDATES:
            try:
                pts, _ = fetch_pv_range(
                    pv, t0dt - timedelta(seconds=args.pre),
                    t0dt + timedelta(seconds=args.post), ref=base,
                )
                print(f"  {pv:26s} n={len(pts)}")
            except Exception as exc:  # noqa: BLE001
                print(f"  {pv:26s} ERROR {exc}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
