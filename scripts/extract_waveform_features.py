#!/usr/bin/env python3
"""Extract numeric waveform features from merged event parquet files.

This is the Phase-1 batch step of the MCP feature/vector plan.  It reads the
merged oscilloscope traces (``data/merged/event_*.parquet``) and writes one
JSON record per event plus a processing manifest and a validation report.
Nothing here talks to the Archiver or to a GPU: it is a pure, deterministic
CPU pass over the parquet corpus.

Time base
---------
Every parquet has ``t_rel_s`` in [-0.05, +0.05] around the scope trigger and
the trace is only 100 ms wide.  ``resolve_t0()`` in ``src/alarm/event_link.py``
finds the beam-current 50 % sustained drop from the 1 Hz Archiver, i.e. on a
seconds-scale window -- far too coarse to window a 100 ms trace.  We therefore
apply the *same rule* to the trace's own ``BeamCurrent``:

    t0 = first sample below 50 % of the trace maximum that is confirmed by the
         next sample.

``t0_rule`` records this, and ``t0_found`` says whether a confirmed drop was
seen.  When no drop exists, t0 falls back to 0.0 (the trigger) and the event is
flagged with a warning, not silently dropped.

Digital polarity
----------------
Interlocks are edge-triggered and the fault direction differs per channel.
The lists below mirror ``config.classification.digital_active_low/high`` in
``src/core/config.py`` (active_low: 1 -> 0 is a fault; active_high: 0 -> 1).
A channel in neither list is treated as active_high and flagged.

Outputs (default ``data/features``)
-----------------------------------
    waveform_features.jsonl        one JSON object per event
    waveform_feature_manifest.csv  status/error/size/hash per event
    waveform_feature_validation.json  aggregate pass/fail and checks

Examples
--------
    venv/bin/python scripts/extract_waveform_features.py --limit 3
    venv/bin/python scripts/extract_waveform_features.py \
        --manifest /path/to/parquet_manifest.csv --workers 8
    venv/bin/python scripts/extract_waveform_features.py --events 20240907_025313,20251205_014253
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

try:
    import numpy as np
    import polars as pl
except ImportError as exc:  # pragma: no cover - environment guard
    sys.stderr.write(
        "polars and numpy are required (use the project venv): %s\n" % exc
    )
    raise SystemExit(2)

ROOT = Path(__file__).resolve().parent.parent
SCHEMA_VERSION = 1

TIME_COL = "t_rel_s"
ANALOG_CHANNELS = [
    "BeamCurrent",
    "Cavity_SRF1", "Cavity_SRF2", "Cavity_SRF3",
    "Forward_SRF1", "Forward_SRF2", "Forward_SRF3",
    "Reflect_SRF1", "Reflect_SRF2", "Reflect_SRF3",
]
UNITS = {"CM1": 1, "CM2": 2, "CM3": 3}

# Mirrors src/core/config.py classification.digital_active_low.
ACTIVE_LOW = {
    "RDY_CM3_FC3_d", "RDY_CM2_FC2_d", "RDY_CM1_FC1_d",
    "HE-PR_CM3_FC3_d", "HE-PR_CM2_FC2_d", "HE-PR_CM1_FC1_d",
    "ARC_CM3_FC3_d", "ARC_CM2_FC2_d", "ARC_CM1_FC1_d",
    "QUEN_CM3_FC3_d", "QUEN_CM2_FC2_d", "QUEN_CM1_FC1_d",
    "VAC_CM3_FC3_d", "VAC_CM2_FC2_d", "VAC_CM1_FC1_d",
    "INT_IN_FC3_d", "INT_IC_FC2_d", "INT_IC_FC1_d",
}
# Mirrors src/core/config.py classification.digital_active_high.
ACTIVE_HIGH = {
    "INT_MIS1_IC_d", "INT_MIS2_IC_d",
    "INT_PSI2_IC_d", "INT_PSI1_IC_d",
    "RDY_KSU3_IC_d", "RDY_KSU2_IC_d", "RDY_KSU1_IC_d",
}

WINDOW_PRE_S = 0.050
WINDOW_POST_S = 0.050
PRE_RECENT_S = 0.001       # last 1 ms before t0 = precursor state
MAX_TRANSITIONS = 12       # cap per digital channel in the JSON record


# ---------------------------------------------------------------------------
# Numeric helpers
# ---------------------------------------------------------------------------

def _num(value: Any, digits: Optional[int] = 6) -> Optional[float]:
    """Return a JSON-safe float (None for NaN/inf) rounded to ``digits``."""
    if value is None:
        return None
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(x):
        return None
    if x == 0.0:  # normalize -0.0
        x = 0.0
    return round(x, digits) if digits is not None else x


def _finite(values: np.ndarray) -> np.ndarray:
    return values[np.isfinite(values)]


def _pct(values: np.ndarray, q: float) -> Optional[float]:
    vals = _finite(values)
    if vals.size == 0:
        return None
    return float(np.percentile(vals, q))


def _ffill(values: np.ndarray) -> np.ndarray:
    """Forward-fill NaNs; leading NaNs stay NaN."""
    out = values.astype(np.float64, copy=True)
    last = math.nan
    for i in range(out.size):
        if math.isfinite(out[i]):
            last = out[i]
        elif math.isfinite(last):
            out[i] = last
    return out


def _first_below(t: np.ndarray, v: np.ndarray, t0: float, level: float) -> Optional[float]:
    """First time at/after t0 where ``v < level``, relative to t0."""
    mask = (t >= t0) & np.isfinite(v) & (v < level)
    if not mask.any():
        return None
    return float(t[mask][0] - t0)


def _first_below_any(t: np.ndarray, v: np.ndarray, level: float) -> Optional[float]:
    """First time anywhere in the trace where ``v < level`` (absolute t)."""
    mask = np.isfinite(v) & (v < level)
    if not mask.any():
        return None
    return float(t[mask][0])


def _file_sha256(path: Path, chunk: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(chunk), b""):
            digest.update(block)
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# t0 resolution (trace-local equivalent of event_link.resolve_t0)
# ---------------------------------------------------------------------------

def resolve_t0_trace(t: np.ndarray, beam: np.ndarray) -> tuple[float, bool, str]:
    """Return (t0, found, rule) using the 50 % sustained-drop rule."""
    vals = _finite(beam)
    if vals.size == 0:
        return 0.0, False, "no_finite_beam_current"
    peak = float(np.max(vals))
    if peak <= 0:
        return 0.0, False, "beam_max_nonpositive"
    threshold = peak * 0.5
    below = np.isfinite(beam) & (beam < threshold)
    for i in range(below.size):
        if not below[i]:
            continue
        confirmed = (
            i + 1 >= below.size
            or (math.isfinite(float(beam[i + 1])) and beam[i + 1] < threshold)
        )
        if confirmed:
            return float(t[i]), True, "beam_50pct_sustained_drop"
    return 0.0, False, "no_50pct_sustained_drop"


# ---------------------------------------------------------------------------
# Feature blocks
# ---------------------------------------------------------------------------

def _missing_analog_block() -> dict[str, Any]:
    """Feature block for a channel absent from the parquet (never hidden)."""
    block = {
        "missing": True,
        "n_pre": 0, "n_post": 0,
        "pre_med": None, "pre_std": None, "pre_min": None, "pre_max": None,
        "pre_p05": None, "pre_p95": None, "pre_recent_med": None,
        "post_med": None, "post_min": None, "post_max": None,
        "ref": None, "min_ratio": None, "max_ratio": None,
        "recent_ratio": None, "min_t_s": None, "max_t_s": None,
    }
    return block


def analog_features(t: np.ndarray, v: np.ndarray, t0: float) -> dict[str, Any]:
    """Pre/post statistics plus min/max ratios for one analog channel."""
    finite = np.isfinite(v)
    pre = (t < t0) & finite
    post = (t >= t0) & finite
    recent = (t >= t0 - PRE_RECENT_S) & (t < t0) & finite

    out: dict[str, Any] = {
        "missing": False,
        "n_pre": int(pre.sum()),
        "n_post": int(post.sum()),
        "pre_med": None, "pre_std": None, "pre_min": None, "pre_max": None,
        "pre_p05": None, "pre_p95": None, "pre_recent_med": None,
        "post_med": None, "post_min": None, "post_max": None,
        "ref": None, "min_ratio": None, "max_ratio": None,
        "recent_ratio": None, "min_t_s": None, "max_t_s": None,
    }

    ref: Optional[float] = None
    if pre.any():
        pv = v[pre]
        out["pre_med"] = _num(_pct(pv, 50))
        out["pre_std"] = _num(np.nanstd(pv))
        out["pre_min"] = _num(np.nanmin(pv))
        out["pre_max"] = _num(np.nanmax(pv))
        out["pre_p05"] = _num(_pct(pv, 5))
        out["pre_p95"] = _num(_pct(pv, 95))
        # p95 is a more robust "healthy level" than the maximum.
        ref = _pct(pv, 95)
        if ref is None or abs(ref) < 1e-9:
            ref = _pct(pv, 50)
        out["ref"] = _num(ref)

    if recent.any():
        out["pre_recent_med"] = _num(_pct(v[recent], 50))

    if post.any():
        pv = v[post]
        out["post_med"] = _num(_pct(pv, 50))
        out["post_min"] = _num(np.nanmin(pv))
        out["post_max"] = _num(np.nanmax(pv))
        if ref is not None and abs(ref) > 1e-9:
            out["min_ratio"] = _num(np.nanmin(pv) / ref)
            out["max_ratio"] = _num(np.nanmax(pv) / ref)
            if out["pre_recent_med"] is not None:
                out["recent_ratio"] = _num(out["pre_recent_med"] / ref)
        # Time of post-window extremes, relative to t0.
        post_idx = np.flatnonzero(post)
        min_i = post_idx[int(np.nanargmin(v[post]))]
        max_i = post_idx[int(np.nanargmax(v[post]))]
        out["min_t_s"] = _num(t[min_i] - t0, 9)
        out["max_t_s"] = _num(t[max_i] - t0, 9)

    return out


def beam_features(
    t: np.ndarray, beam: np.ndarray, t0: float, channel: dict[str, Any]
) -> dict[str, Any]:
    """Beam fall timing: 99/90/50/10 % crossings and the 90->10 fall time.

    Crossings are searched over the whole trace, not just after t0: by
    definition t0 is already the 50 % point, so a post-t0-only search would
    report fall_90/fall_50 as 0.0 and hide the real edge.
    """
    ref = channel.get("ref")
    out = {"fall_99_s": None, "fall_90_s": None, "fall_50_s": None,
           "fall_10_s": None, "fall_90_10_s": None}
    if ref is None or abs(ref) < 1e-9:
        return out
    crossings: dict[str, Optional[float]] = {}
    for pct, key in ((0.99, "fall_99_s"), (0.90, "fall_90_s"),
                     (0.50, "fall_50_s"), (0.10, "fall_10_s")):
        hit = _first_below_any(t, beam, pct * ref)
        crossings[key] = hit
        out[key] = _num(None if hit is None else hit - t0, 9)
    if crossings["fall_90_s"] is not None and crossings["fall_10_s"] is not None:
        out["fall_90_10_s"] = _num(
            crossings["fall_10_s"] - crossings["fall_90_s"], 9
        )
    return out


def sequence_features(
    t: np.ndarray,
    columns: dict[str, np.ndarray],
    analog: dict[str, dict[str, Any]],
    beam_fall: dict[str, Any],
    t0: float,
) -> dict[str, Any]:
    """Cavity/beam crossing order and a coarse cause class.

    Times are seconds relative to the trace-local beam-current t0 (50 % drop).
    A negative cavity time means the cavity moved first.  ``delay_*_us`` is
    positive when the cavity precedes the beam.
    """
    out: dict[str, Any] = {
        "cavities": {},
        "n_cavities_collapsed": 0,
        "cavity_collapse_spread_us": None,
        "delay_cav50_to_beam50_us": None,
        "delay_cav90_to_beam99_us": None,
        "class": None,
        "class_rule": None,
    }
    collapsed_t50: list[float] = []
    collapsed_t90: list[float] = []
    collapsed_min: list[float] = []

    for unit, n in (("CM1", 1), ("CM2", 2), ("CM3", 3)):
        ch = f"Cavity_SRF{n}"
        block = analog.get(ch, {})
        v = columns.get(f"{ch}_v")
        ref = block.get("ref")
        rec: dict[str, Any] = {
            "pre_med": block.get("pre_med"),
            "min_ratio": block.get("min_ratio"),
            "t90_s": None, "t50_s": None, "t10_s": None,
            "tau_us": None, "collapsed": False,
        }
        if not block.get("missing") and v is not None and ref is not None \
                and abs(ref) > 1e-9 and (block.get("pre_med") or 0) > 0.05:
            for pct, key in ((0.90, "t90_s"), (0.50, "t50_s"), (0.10, "t10_s")):
                hit = _first_below_any(t, v, pct * ref)
                rec[key] = _num(None if hit is None else hit - t0, 9)
            if rec["t90_s"] is not None and rec["t10_s"] is not None:
                dt = rec["t10_s"] - rec["t90_s"]
                if dt > 0:
                    rec["tau_us"] = _num(dt / math.log(9) * 1e6, 2)
            min_ratio = block.get("min_ratio")
            rec["collapsed"] = bool(min_ratio is not None and min_ratio < 0.2)
            if rec["collapsed"]:
                collapsed_min.append(float(min_ratio))
                if rec["t50_s"] is not None:
                    collapsed_t50.append(float(rec["t50_s"]))
                if rec["t90_s"] is not None:
                    collapsed_t90.append(float(rec["t90_s"]))
        out["cavities"][unit] = rec

    out["n_cavities_collapsed"] = len(collapsed_min)
    if len(collapsed_t50) >= 2:
        out["cavity_collapse_spread_us"] = _num(
            (max(collapsed_t50) - min(collapsed_t50)) * 1e6, 1
        )
    if collapsed_t50:
        out["delay_cav50_to_beam50_us"] = _num(
            -float(np.median(collapsed_t50)) * 1e6, 1
        )
    if collapsed_t90 and beam_fall.get("fall_99_s") is not None:
        out["delay_cav90_to_beam99_us"] = _num(
            (beam_fall["fall_99_s"] - float(np.median(collapsed_t90))) * 1e6, 1
        )

    fwd_ratios = []
    for n in (1, 2, 3):
        fwd = analog.get(f"Forward_SRF{n}", {})
        if not fwd.get("missing") and fwd.get("min_ratio") is not None:
            fwd_ratios.append(float(fwd["min_ratio"]))
    fwd_med = float(np.median(fwd_ratios)) if fwd_ratios else None
    out["forward_median_ratio"] = _num(fwd_med)
    cav_min = float(np.median(collapsed_min)) if collapsed_min else None
    n_coll = out["n_cavities_collapsed"]
    delay = out["delay_cav50_to_beam50_us"]

    if n_coll == 0:
        cls, rule = "beam_only", "no cavity below 20%"
    elif delay is not None and delay <= 0:
        cls, rule = "beam_first_rf_trip", "cavity 50% after beam 50%"
    elif n_coll == 3 and cav_min is not None and cav_min < 0.1:
        cls, rule = "rf_trip", "3 cavities to <10%, cavity first"
    elif n_coll == 1 and cav_min is not None and cav_min < 0.1:
        cls, rule = "single_cavity_quench", "1 cavity to <10%, cavity first"
    else:
        # 2-cavity collapse is not labelled a dual quench: it has not been
        # observed at PLS-II and may be an RF station/common-point fault.
        cls, rule = "mixed", "cavity sag/collapse without a unique cause pattern"
    out["class"] = cls
    out["class_rule"] = rule
    return out


def unit_signature(
    unit: str,
    analog: dict[str, dict[str, Any]],
    t: np.ndarray,
    columns: dict[str, np.ndarray],
    t0: float,
) -> dict[str, Any]:
    """Cavity sag / reflect rise / forward trip for one CM unit."""
    n = UNITS[unit]
    cav = analog.get(f"Cavity_SRF{n}", {})
    fwd = analog.get(f"Forward_SRF{n}", {})
    ref = analog.get(f"Reflect_SRF{n}", {})
    unit_channels = [f"Cavity_SRF{n}", f"Forward_SRF{n}", f"Reflect_SRF{n}"]
    missing = [
        c for c in unit_channels
        if c not in analog or analog[c].get("missing")
    ]

    def depth(ch: dict[str, Any]) -> Optional[float]:
        r = ch.get("min_ratio")
        return None if r is None else _num(1.0 - r)

    fwd_v = columns.get(f"Forward_SRF{n}_v")
    fwd_ref = fwd.get("ref")
    trip_t = None
    if fwd_v is not None and fwd_ref is not None and abs(fwd_ref) > 1e-9:
        trip_t = _num(_first_below(t, fwd_v, t0, 0.5 * fwd_ref), 9)

    return {
        "unit_channels_missing": missing,
        "cavity_sag_depth": depth(cav),
        "cavity_min_ratio": cav.get("min_ratio"),
        "cavity_sag_time_s": cav.get("min_t_s"),
        "cavity_pre_state_ratio": cav.get("recent_ratio"),
        "reflect_rise_ratio": ref.get("max_ratio"),
        "reflect_peak_time_s": ref.get("max_t_s"),
        "reflect_pre_state_ratio": ref.get("recent_ratio"),
        "forward_trip_ratio": fwd.get("min_ratio"),
        "forward_trip_time_s": trip_t,
        "forward_pre_state_ratio": fwd.get("recent_ratio"),
    }


def digital_features(
    name: str, t: np.ndarray, values: np.ndarray, t0: float
) -> dict[str, Any]:
    """Transitions for one digital channel, with fault-edge polarity."""
    x = _ffill(values)
    finite = np.isfinite(x)
    bits = np.where(finite, (np.nan_to_num(x, nan=0.0) > 0.5).astype(np.int8), -1)

    if name in ACTIVE_LOW:
        polarity, fault_bit = "active_low", 0
    elif name in ACTIVE_HIGH:
        polarity, fault_bit = "active_high", 1
    else:
        # Do not guess a direction for an unmapped channel: name-based
        # grouping is known to be wrong (INT_IC_* is active_low despite
        # "INT").  The channel's transitions are still recorded.
        polarity, fault_bit = "unmapped", None

    state_before = None
    state_after = None
    before_idx = np.flatnonzero((t < t0) & (bits >= 0))
    after_idx = np.flatnonzero((t >= t0) & (bits >= 0))
    if before_idx.size:
        state_before = int(bits[before_idx[-1]])
    if after_idx.size:
        state_after = int(bits[after_idx[0]])

    transitions: list[dict[str, Any]] = []
    change_idx = np.flatnonzero(bits[1:] != bits[:-1]) + 1
    for i in change_idx:
        if bits[i - 1] < 0 or bits[i] < 0:
            continue
        if bits[i - 1] == bits[i]:
            continue
        transitions.append({
            "t_s": _num(t[i] - t0, 9),
            "from": int(bits[i - 1]),
            "to": int(bits[i]),
        })

    asserts = [
        tr for tr in transitions
        if fault_bit is not None and tr["to"] == fault_bit
    ]
    releases = [
        tr for tr in transitions
        if fault_bit is not None and tr["from"] == fault_bit
    ]
    first_assert = asserts[0]["t_s"] if asserts else None
    first_release = None
    if first_assert is not None:
        later = [tr["t_s"] for tr in releases if tr["t_s"] > first_assert]
        first_release = later[0] if later else None

    pulse_width = None
    if first_assert is not None and first_release is not None:
        pulse_width = _num(first_release - first_assert, 9)

    return {
        "polarity": polarity,
        "state_before": state_before,
        "state_after": state_after,
        "n_transitions": len(transitions),
        "n_asserts": len(asserts),
        "n_releases": len(releases),
        "asserted": bool(asserts),
        "first_assert_t_s": first_assert,
        "first_release_t_s": first_release,
        "pulse_width_s": pulse_width,
        "transitions": transitions[:MAX_TRANSITIONS],
        "transitions_truncated": len(transitions) > MAX_TRANSITIONS,
    }


def pick_quench_unit(digital: dict[str, dict[str, Any]]) -> tuple[str, str, list[str]]:
    """Return (unit, source, units_with_quen_assert)."""
    hits: list[tuple[str, float]] = []
    for unit, n in UNITS.items():
        ch = digital.get(f"QUEN_CM{n}_FC{n}_d")
        if ch and ch.get("asserted"):
            hits.append((unit, ch.get("first_assert_t_s") or 0.0))
    if hits:
        hits.sort(key=lambda x: x[1])
        return hits[0][0], "digital_quen", [u for u, _ in hits]
    # Every waveform event in this corpus is a CM1 quench; cavity collapse
    # alone cannot distinguish the unit (all three cavities collapse together).
    return "CM1", "default_cm1", []


# ---------------------------------------------------------------------------
# Per-event extraction
# ---------------------------------------------------------------------------

def extract_event(
    event_id: str,
    path: Path,
    meta: dict[str, str],
    compute_hash: bool = True,
) -> dict[str, Any]:
    started = time.time()
    warnings: list[str] = []

    df = pl.read_parquet(str(path)).sort(TIME_COL)
    if TIME_COL not in df.columns:
        raise ValueError(f"missing time column {TIME_COL!r}")

    t = df[TIME_COL].to_numpy().astype(np.float64)
    if t.size < 2:
        raise ValueError(f"trace too short: {t.size} samples")

    present = [c for c in ANALOG_CHANNELS if f"{c}_v" in df.columns]
    missing = [c for c in ANALOG_CHANNELS if c not in present]
    if not present:
        raise ValueError("no analog channels in parquet")
    if missing:
        warnings.append("missing analog channels: " + ",".join(missing))

    columns: dict[str, np.ndarray] = {}
    for ch in present:
        columns[f"{ch}_v"] = df[f"{ch}_v"].to_numpy().astype(np.float64)

    if "BeamCurrent_v" in columns:
        beam = columns["BeamCurrent_v"]
    else:  # t0 cannot be resolved without beam current; fall back to trigger
        beam = np.full(t.size, np.nan)
        warnings.append("BeamCurrent absent; t0 fallback to 0.0")
    t0, t0_found, t0_rule = resolve_t0_trace(t, beam)
    if not t0_found:
        warnings.append(f"t0 fallback to 0.0 ({t0_rule})")
    if t0 < float(t[0]) or t0 > float(t[-1]):
        warnings.append(f"t0 {t0:.6f}s outside trace; clamped")
        t0 = min(max(t0, float(t[0])), float(t[-1]))
    pre_duration = float(t0 - t[0])
    post_duration = float(t[-1] - t0)
    if abs(t0) > 1e-3:
        warnings.append(f"t0 {t0 * 1e3:.1f} ms from trigger")
    if pre_duration < 5e-3:
        warnings.append(f"short pre-window {pre_duration * 1e3:.1f} ms")

    dt = float(np.median(np.diff(t))) if t.size > 2 else None

    analog: dict[str, dict[str, Any]] = {}
    for ch in ANALOG_CHANNELS:
        if ch in present:
            analog[ch] = analog_features(t, columns[f"{ch}_v"], t0)
        else:
            analog[ch] = _missing_analog_block()
    beam_block = beam_features(t, beam, t0, analog["BeamCurrent"])

    digital: dict[str, dict[str, Any]] = {}
    for col in df.columns:
        if not col.endswith("_d"):
            continue
        digital[col] = digital_features(col, t, df[col].to_numpy().astype(np.float64), t0)

    unknown_polarity = [
        c for c, f in digital.items() if f["polarity"] == "unmapped"
    ]
    if unknown_polarity:
        warnings.append("unmapped polarity: " + ",".join(unknown_polarity))

    signatures = {
        unit: unit_signature(unit, analog, t, columns, t0) for unit in UNITS
    }
    quench_unit, quench_source, quench_units = pick_quench_unit(digital)

    event_ts = None
    if "event_timestamp" in df.columns and df.height:
        event_ts = str(df["event_timestamp"][0])

    record: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id,
        "source_file": path.name,
        "timestamp": meta.get("timestamp", ""),
        "fault_type": meta.get("fault_type", ""),
        "user_fault_type": meta.get("user_fault_type", ""),
        "event_timestamp_parquet": event_ts,
        "n_samples": int(t.size),
        "missing_channels": missing,
        "dt_s": _num(dt, 12),
        "duration_s": _num(float(t[-1] - t[0]), 9),
        "t0": {
            "value_s": _num(t0, 9),
            "found": t0_found,
            "rule": t0_rule,
            "method": "parquet_beam_current",
        },
        "windows": {
            "pre_start_s": _num(float(t[0]), 9),
            "post_end_s": _num(float(t[-1]), 9),
            "pre_duration_s": _num(pre_duration, 9),
            "post_duration_s": _num(post_duration, 9),
            "pre_recent_s": PRE_RECENT_S,
        },
        "analog": analog,
        "beam_fall": beam_block,
        "sequence": sequence_features(t, columns, analog, beam_block, t0),
        "signature": signatures,
        "digital": digital,
        "quench_unit": quench_unit,
        "quench_unit_source": quench_source,
        "quench_units_with_quen_assert": quench_units,
        "warnings": warnings,
        "source_sha256": _file_sha256(path) if compute_hash else None,
        "extract_seconds": round(time.time() - started, 3),
    }
    return record


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _walk_numbers(value: Any, path: str = "") -> Iterable[tuple[str, float]]:
    if isinstance(value, dict):
        for k, v in value.items():
            yield from _walk_numbers(v, f"{path}.{k}" if path else str(k))
    elif isinstance(value, list):
        for i, v in enumerate(value):
            yield from _walk_numbers(v, f"{path}[{i}]")
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        yield path, float(value)


def validate_record(rec: dict[str, Any]) -> list[str]:
    """Return a list of validation errors (empty means valid)."""
    errors: list[str] = []
    required = {
        "schema_version", "event_id", "n_samples", "t0", "analog",
        "beam_fall", "signature", "digital", "quench_unit",
    }
    missing = required - set(rec)
    if missing:
        errors.append("missing keys: " + ",".join(sorted(missing)))
        return errors
    if rec["schema_version"] != SCHEMA_VERSION:
        errors.append(f"schema_version {rec['schema_version']} != {SCHEMA_VERSION}")
    if rec["n_samples"] < 2:
        errors.append("n_samples < 2")
    if len(rec["analog"]) != len(ANALOG_CHANNELS):
        errors.append(f"analog count {len(rec['analog'])} != {len(ANALOG_CHANNELS)}")
    present = {
        c for c in ANALOG_CHANNELS
        if not rec["analog"].get(c, {}).get("missing")
    }
    missing = set(rec.get("missing_channels") or [])
    if present | missing != set(ANALOG_CHANNELS):
        errors.append("analog blocks and missing_channels are inconsistent")
    if not present:
        errors.append("no analog channels present")
    for ch in sorted(missing):
        if not rec["analog"].get(ch, {}).get("missing"):
            errors.append(f"{ch} listed in missing_channels but block is not marked")
    if len(rec["signature"]) != len(UNITS):
        errors.append(f"signature count {len(rec['signature'])} != {len(UNITS)}")
    if rec["quench_unit"] not in UNITS:
        errors.append(f"bad quench_unit {rec['quench_unit']!r}")
    if rec["t0"].get("value_s") is None:
        errors.append("t0.value_s is null")
    for path, x in _walk_numbers(rec):
        if not math.isfinite(x):
            errors.append(f"non-finite number at {path}")
            if len(errors) > 20:
                break
    try:
        json.dumps(rec, allow_nan=False)
    except (TypeError, ValueError) as exc:
        errors.append(f"not JSON-serializable: {exc}")
    return errors


# ---------------------------------------------------------------------------
# Input discovery
# ---------------------------------------------------------------------------

def load_manifest(path: Path) -> dict[str, dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    return {row["event_id"].strip(): row for row in rows if row.get("event_id")}


def discover_events(data_dir: Path, manifest: Optional[Path]) -> list[tuple[str, Path, dict[str, str]]]:
    if manifest is not None:
        meta = load_manifest(manifest)
        events = []
        for event_id in sorted(meta):
            p = data_dir / f"event_{event_id}.parquet"
            if p.is_file():
                events.append((event_id, p, meta[event_id]))
        missing = [eid for eid in meta if not (data_dir / f"event_{eid}.parquet").is_file()]
        if missing:
            sys.stderr.write(
                f"warning: {len(missing)} manifest events have no parquet: "
                f"{', '.join(missing[:5])}{' ...' if len(missing) > 5 else ''}\n"
            )
        return events
    return [
        (p.stem[len("event_"):], p, {})
        for p in sorted(data_dir.glob("event_*.parquet"))
    ]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", default=str(ROOT / "data" / "merged"),
                    help="directory containing event_*.parquet")
    ap.add_argument("--manifest", default=None,
                    help="optional parquet_manifest.csv (event_id column)")
    ap.add_argument("--out-dir", default=str(ROOT / "data" / "features"),
                    help="output directory")
    ap.add_argument("--events", default="",
                    help="comma-separated event IDs (default: all)")
    ap.add_argument("--limit", type=int, default=0, help="process at most N events")
    ap.add_argument("--workers", type=int, default=4, help="parallel readers")
    ap.add_argument("--no-hash", action="store_true",
                    help="skip sha256 of the parquet (faster, no input_hash)")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    out_dir = Path(args.out_dir)
    manifest = Path(args.manifest) if args.manifest else None

    events = discover_events(data_dir, manifest)
    only = {x.strip() for x in args.events.split(",") if x.strip()}
    if only:
        events = [e for e in events if e[0] in only]
    if args.limit:
        events = events[: args.limit]
    if not events:
        sys.stderr.write("no events to process\n")
        return 2

    out_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = out_dir / "waveform_features.jsonl"
    manifest_path = out_dir / "waveform_feature_manifest.csv"
    validation_path = out_dir / "waveform_feature_validation.json"

    started = time.time()
    results: dict[str, dict[str, Any]] = {}
    failures: dict[str, str] = {}
    validation_errors: dict[str, list[str]] = {}

    print(f"processing {len(events)} events with {args.workers} workers", flush=True)
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        future_map = {
            pool.submit(extract_event, eid, path, meta, not args.no_hash): eid
            for eid, path, meta in events
        }
        done = 0
        for future in as_completed(future_map):
            eid = future_map[future]
            done += 1
            try:
                rec = future.result()
            except Exception as exc:  # keep going; report the failure
                failures[eid] = f"{type(exc).__name__}: {exc}"
            else:
                errs = validate_record(rec)
                if errs:
                    validation_errors[eid] = errs
                results[eid] = rec
            if done % 25 == 0 or done == len(events):
                print(f"  {done}/{len(events)} done, "
                      f"{len(failures)} failed, {len(validation_errors)} invalid",
                      flush=True)

    # Write JSONL (sorted for reproducible diffs).
    with jsonl_path.open("w", encoding="utf-8") as fh:
        for eid in sorted(results):
            fh.write(json.dumps(results[eid], ensure_ascii=False,
                                allow_nan=False, sort_keys=True))
            fh.write("\n")

    # Manifest CSV.
    fieldnames = [
        "event_id", "status", "error", "n_samples", "dt_us", "t0_s", "t0_found",
        "t0_rule", "quench_unit", "quench_unit_source", "n_analog", "n_digital",
        "n_warnings", "json_bytes", "sha256", "extract_seconds",
    ]
    with manifest_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for eid, path, _meta in events:
            rec = results.get(eid)
            if rec is None:
                writer.writerow({
                    "event_id": eid, "status": "failed",
                    "error": failures.get(eid, "unknown"),
                })
                continue
            writer.writerow({
                "event_id": eid,
                "status": (
                    "invalid" if eid in validation_errors
                    else "partial" if rec["missing_channels"] else "ok"
                ),
                "error": "; ".join(validation_errors.get(eid, [])),
                "n_samples": rec["n_samples"],
                "dt_us": _num((rec["dt_s"] or 0) * 1e6, 3),
                "t0_s": rec["t0"]["value_s"],
                "t0_found": rec["t0"]["found"],
                "t0_rule": rec["t0"]["rule"],
                "quench_unit": rec["quench_unit"],
                "quench_unit_source": rec["quench_unit_source"],
                "n_analog": len(rec["analog"]),
                "n_digital": len(rec["digital"]),
                "n_warnings": len(rec["warnings"]),
                "json_bytes": len(json.dumps(rec, ensure_ascii=False)),
                "sha256": rec.get("source_sha256") or "",
                "extract_seconds": rec.get("extract_seconds"),
            })

    partial = sorted(
        eid for eid, r in results.items()
        if r["missing_channels"] and eid not in validation_errors
    )
    report = {
        "schema_version": SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "data_dir": str(data_dir),
        "out_dir": str(out_dir),
        "expected": len(events),
        "ok": len(results) - len(validation_errors) - len(partial),
        "partial": len(partial),
        "partial_events": partial,
        "invalid": len(validation_errors),
        "failed": len(failures),
        "t0_not_found": sorted(
            eid for eid, r in results.items() if not r["t0"]["found"]
        ),
        "quench_unit_sources": {
            src: sorted(eid for eid, r in results.items()
                        if r["quench_unit_source"] == src)
            for src in sorted({r["quench_unit_source"] for r in results.values()})
        },
        "validation_errors": validation_errors,
        "failures": failures,
        "elapsed_seconds": round(time.time() - started, 2),
        "outputs": {
            "jsonl": str(jsonl_path),
            "manifest": str(manifest_path),
        },
    }
    validation_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(json.dumps({k: report[k] for k in
                      ("expected", "ok", "partial", "invalid", "failed",
                       "elapsed_seconds")},
                     ensure_ascii=False))
    if failures or validation_errors:
        print(f"see {validation_path}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
