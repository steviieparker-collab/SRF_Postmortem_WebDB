"""Integrated quench analysis (Task 5).

Combines three evidence sources for one quench event:

  * waveform features from the static feature store (100 ms parquet)
  * Archiver PV series (BCL01/BCL03 cavity temperatures, beam current; ~1 Hz)
  * report text/image-caption evidence from the report index

The two clocks are different: waveform t0 is trace-local (50 % beam-current
drop inside the parquet), while the Archiver t0 is the beam-current drop found
on the 1 Hz series.  Both are reported explicitly; don't mix them.
"""

from __future__ import annotations

import math
import statistics
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Any, Optional

from ..alarm.event_link import KST, fetch_pv_range, resolve_t0
from ..rag.db import connect_events
from .physics import interpret_sequence, normalize_sequence
from .reports import search_reports
from .store import connect_feature_store, get_waveform_features

QUEN_CHANNELS = {
    "CM1": "QUEN_CM1_FC1_d",
    "CM2": "QUEN_CM2_FC2_d",
    "CM3": "QUEN_CM3_FC3_d",
}


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

def _event_meta(event_id: str) -> Optional[dict[str, Any]]:
    conn = connect_events()
    try:
        row = conn.execute(
            "SELECT id, timestamp, fault_type, user_fault_type, notes, beam_current "
            "FROM events WHERE id = ?",
            (event_id,),
        ).fetchone()
        if row is None:
            return None
        selection = [
            dict(r) for r in conn.execute(
                "SELECT pv, description FROM event_pv_selection "
                "WHERE event_id = ? ORDER BY position",
                (event_id,),
            )
        ]
    finally:
        conn.close()
    notes = row["notes"] or ""
    return {
        "event_id": row["id"],
        "timestamp": row["timestamp"],
        "fault_type": row["fault_type"] or "",
        "user_fault_type": row["user_fault_type"] or "",
        "notes_excerpt": " ".join(notes.split())[:500],
        "beam_current": row["beam_current"],
        "pv_selection": selection,
    }


def _unit_from_meta(meta: dict[str, Any]) -> str:
    text = f"{meta.get('notes_excerpt', '')} {meta.get('user_fault_type', '')}".upper()
    for n in (3, 2, 1):
        if (f"CM{n}" in text or f"CM#{n}" in text
                or f"SRF{n}" in text or f"SRF #{n}" in text
                or f"SRF# {n}" in text):
            return f"CM{n}"
    return "CM1"


def _report_titles(event_id: str) -> list[str]:
    try:
        conn = connect_feature_store()
        try:
            rows = conn.execute(
                "SELECT DISTINCT d.title FROM report_documents d "
                "JOIN report_document_events m ON m.doc_id = d.doc_id "
                "WHERE m.event_id = ? ORDER BY d.title",
                (event_id,),
            ).fetchall()
        finally:
            conn.close()
        return [r[0] for r in rows if r[0]]
    except Exception:  # noqa: BLE001 - report layer is optional
        return []


# ---------------------------------------------------------------------------
# Waveform
# ---------------------------------------------------------------------------

def _waveform_summary(record: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not record:
        return None
    features = record.get("features") or {}
    t0 = features.get("t0") or {}
    digital = features.get("digital") or {}
    quen = {}
    for unit, channel in QUEN_CHANNELS.items():
        block = digital.get(channel) or {}
        quen[unit] = {
            "asserted": bool(block.get("asserted")),
            "first_assert_t_s": block.get("first_assert_t_s"),
            "pulse_width_s": block.get("pulse_width_s"),
        }
    dt = features.get("dt_s")
    return {
        "available": True,
        "n_samples": features.get("n_samples"),
        "dt_us": round(dt * 1e6, 3) if dt is not None else None,
        "t0": t0,
        "quench_unit": record.get("quench_unit") or features.get("quench_unit"),
        "quench_unit_source": features.get("quench_unit_source"),
        "signature": features.get("signature") or {},
        "beam_fall": features.get("beam_fall") or {},
        "sequence": normalize_sequence(features.get("sequence")) or {},
        "physics": interpret_sequence(
            normalize_sequence(features.get("sequence")),
            features.get("beam_fall"),
        ),
        "quen": quen,
        "missing_channels": features.get("missing_channels") or [],
        "warnings": features.get("warnings") or [],
    }


# ---------------------------------------------------------------------------
# Archiver PVs
# ---------------------------------------------------------------------------

def _nearest(points: list[tuple[float, float]], target: float,
             tol: float = 5.0) -> Optional[float]:
    if not points:
        return None
    t, v = min(points, key=lambda p: abs(p[0] - target))
    return v if abs(t - target) <= tol else None


def _pv_summary(
    pv: str,
    t0_abs: datetime,
    base: datetime,
    pre: float,
    post: float,
) -> dict[str, Any]:
    t0 = t0_abs.timestamp()
    try:
        points, _meta = fetch_pv_range(
            pv,
            t0_abs - timedelta(seconds=pre),
            t0_abs + timedelta(seconds=post),
            ref=base,
        )
    except Exception as exc:  # noqa: BLE001 - report per PV
        return {"pv": pv, "quality": "error", "error": f"{type(exc).__name__}: {exc}"}

    points = sorted(
        (float(t), float(v)) for t, v in (points or []) if math.isfinite(float(v))
    )
    if not points:
        return {"pv": pv, "quality": "no_data", "n": 0}

    pre_vals = [v for t, v in points if t0 - pre <= t <= t0 - 10]
    post_pts = [(t, v) for t, v in points if t0 <= t <= t0 + post]
    pre_med = statistics.median(pre_vals) if pre_vals else None
    t0_val = _nearest(points, t0)
    post_min = min((v for _, v in post_pts), default=None)
    post_max = max((v for _, v in post_pts), default=None)
    post_max_t = None
    if post_pts:
        t_peak, _ = max(post_pts, key=lambda p: p[1])
        post_max_t = round(t_peak - t0, 1)

    delta = None
    delta_pct = None
    if pre_med is not None and post_max is not None:
        delta = post_max - pre_med
        if abs(pre_med) > 1e-9:
            delta_pct = 100.0 * delta / pre_med

    diffs = sorted(b - a for a, b in zip(
        [t for t, _ in points], [t for t, _ in points][1:]
    ) if b > a)
    resolution = round(statistics.median(diffs), 3) if diffs else None
    quality = "ok" if len(points) >= 10 and pre_vals and post_pts else "sparse"

    return {
        "pv": pv,
        "quality": quality,
        "n": len(points),
        "resolution_s": resolution,
        "pre_median": round(pre_med, 4) if pre_med is not None else None,
        "t0_value": round(t0_val, 4) if t0_val is not None else None,
        "post_min": round(post_min, 4) if post_min is not None else None,
        "post_max": round(post_max, 4) if post_max is not None else None,
        "post_max_t_s": post_max_t,
        "delta": round(delta, 4) if delta is not None else None,
        "delta_pct": round(delta_pct, 2) if delta_pct is not None else None,
    }


def _pv_set(meta: dict[str, Any], unit: str) -> list[tuple[str, str]]:
    n = unit[-1]
    selected = {s.get("pv") for s in meta.get("pv_selection") or []}
    out: list[tuple[str, str]] = []
    for pv in (f"SR:RF:CRM{n}:BCL03", f"SR:RF:CRM{n}:BCL01"):
        if pv in selected or not selected:
            out.append((pv.rsplit(":", 1)[-1], pv))
    if not out:
        out = [
            ("BCL03", f"SR:RF:CRM{n}:BCL03"),
            ("BCL01", f"SR:RF:CRM{n}:BCL01"),
        ]
    out.append(("BEAM", "SR:G00:BEAMCURRENT_T"))
    return out


def _fetch_pvs(
    meta: dict[str, Any],
    unit: str,
    t0_abs: datetime,
    base: datetime,
    pre: float,
    post: float,
) -> dict[str, Any]:
    specs = _pv_set(meta, unit)
    results: dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=min(4, len(specs))) as pool:
        futures = {
            pool.submit(_pv_summary, pv, t0_abs, base, pre, post): label
            for label, pv in specs
        }
        for future, label in futures.items():
            try:
                results[label] = future.result()
            except Exception as exc:  # noqa: BLE001
                results[label] = {
                    "quality": "error",
                    "error": f"{type(exc).__name__}: {exc}",
                }
    return results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def analyze_quench(
    event_id: str,
    include_pv: bool = True,
    include_reports: bool = True,
    pre: float = 60.0,
    post: float = 120.0,
) -> dict[str, Any]:
    event_id = str(event_id).strip()
    meta = _event_meta(event_id)
    if meta is None:
        return {
            "response_type": "quench_analysis",
            "available": False,
            "event_id": event_id,
            "error": "event not found",
        }

    try:
        waveform = _waveform_summary(get_waveform_features(event_id))
    except Exception as exc:  # noqa: BLE001
        waveform = None
        waveform_error = f"{type(exc).__name__}: {exc}"
    else:
        waveform_error = None

    unit = (
        (waveform or {}).get("quench_unit")
        or _unit_from_meta(meta)
    )

    pv_result: Optional[dict[str, Any]] = None
    t0_arch = None
    if include_pv:
        try:
            t0_arch, base = resolve_t0(meta["timestamp"])
        except Exception:
            t0_arch, base = 0.0, datetime.fromisoformat(
                meta["timestamp"]
            ).replace(tzinfo=KST)
        t0_abs = base + timedelta(seconds=t0_arch)
        pv_result = {
            "t0_arch_s": t0_arch,
            "cavity_unit": unit,
            "window": {"pre_s": pre, "post_s": post},
            "series": _fetch_pvs(meta, unit, t0_abs, base, pre, post),
        }

    report_result: Optional[dict[str, Any]] = None
    if include_reports:
        query = f"{unit} quench cavity temperature RF reflected"
        try:
            report_result = search_reports(query, event_id=event_id, top_k=3)
            if report_result.get("returned", 0) == 0:
                report_result = search_reports("quench", event_id=event_id, top_k=3)
        except Exception as exc:  # noqa: BLE001
            report_result = {
                "available": False,
                "error": f"{type(exc).__name__}: {exc}",
                "items": [],
            }

    highlights: list[str] = []
    if pv_result:
        bcl03 = (pv_result.get("series") or {}).get("BCL03") or {}
        if bcl03.get("delta") is not None:
            highlights.append(
                f"BCL03 {bcl03.get('pre_median')} → {bcl03.get('post_max')} K "
                f"(delta {bcl03.get('delta')}, peak at +{bcl03.get('post_max_t_s')}s)"
            )
    if waveform:
        sig = (waveform.get("signature") or {}).get(unit) or {}
        if sig.get("cavity_sag_depth") is not None:
            highlights.append(
                f"{unit} cavity sag depth {sig.get('cavity_sag_depth')} "
                f"(min ratio {sig.get('cavity_min_ratio')})"
            )
        if sig.get("cavity_pre_state_ratio") is not None:
            highlights.append(
                f"{unit} pre-t0 cavity state ratio "
                f"{sig.get('cavity_pre_state_ratio')}"
            )
        quen = (waveform.get("quen") or {}).get(unit) or {}
        if quen.get("asserted"):
            highlights.append(
                f"QUEN_{unit} asserted at "
                f"{quen.get('first_assert_t_s')} s (waveform t0)"
            )
    if report_result:
        highlights.append(
            f"{report_result.get('returned', 0)} report evidence items"
        )

    caveats = [
        "Archiver PVs are ~1 Hz (BCL03 often 1.6-4 s); the 100 ms waveform "
        "cannot be reconstructed from them.",
        "PV t0 is the Archiver beam-current 50% drop; waveform t0 is trace-local. "
        "They are different clocks and are reported separately.",
        "event_alarms is nearly empty, so alarm evidence is usually absent.",
    ]
    if waveform is None:
        caveats.append(
            "No parquet waveform for this event; only PV and report evidence."
            + (f" ({waveform_error})" if waveform_error else "")
        )
    if pv_result:
        if not any(
            (s or {}).get("quality") == "ok"
            for s in (pv_result.get("series") or {}).values()
        ):
            caveats.append(
                "PV series are sparse or unavailable; treat temperature numbers "
                "as indicative."
            )
    if report_result and report_result.get("available") is False:
        caveats.append("Report search unavailable; no report evidence included.")

    return {
        "response_type": "quench_analysis",
        "available": True,
        "event_id": event_id,
        "event": meta,
        "waveform": waveform,
        "pv": pv_result,
        "reports": report_result,
        "highlights": highlights,
        "citation": f"[E:{event_id}]",
        "url": f"/events/{event_id}",
        "caveats": caveats,
    }


def compare_quench_events(
    event_ids: list[str] | str,
    include_reports: bool = False,
) -> dict[str, Any]:
    if isinstance(event_ids, str):
        ids = [x.strip() for x in event_ids.split(",") if x.strip()]
    else:
        ids = [str(x).strip() for x in (event_ids or []) if str(x).strip()]
    ids = list(dict.fromkeys(ids))[:20]
    if not ids:
        return {
            "response_type": "quench_comparison",
            "available": False,
            "error": "event_ids is empty",
        }

    rows = []
    for event_id in ids:
        meta = _event_meta(event_id)
        if meta is None:
            rows.append({
                "event_id": event_id,
                "found": False,
                "error": "event not found",
            })
            continue
        try:
            record = get_waveform_features(event_id)
        except Exception:
            record = None
        summary = _waveform_summary(record)
        unit = (
            (summary or {}).get("quench_unit")
            or _unit_from_meta(meta)
        )
        signature = ((summary or {}).get("signature") or {}).get(unit) or {}
        quen = ((summary or {}).get("quen") or {}).get(unit) or {}
        titles = _report_titles(event_id)
        row = {
            "event_id": event_id,
            "found": True,
            "timestamp": meta["timestamp"],
            "fault_type": meta["fault_type"],
            "quench_unit": unit,
            "quench_unit_source": (summary or {}).get("quench_unit_source"),
            "has_waveform": bool(summary),
            "t0_wave_s": ((summary or {}).get("t0") or {}).get("value_s"),
            "cavity_sag_depth": signature.get("cavity_sag_depth"),
            "cavity_min_ratio": signature.get("cavity_min_ratio"),
            "cavity_pre_state_ratio": signature.get("cavity_pre_state_ratio"),
            "reflect_rise_ratio": signature.get("reflect_rise_ratio"),
            "forward_trip_ratio": signature.get("forward_trip_ratio"),
            "forward_trip_time_s": signature.get("forward_trip_time_s"),
            "beam_fall_90_10_s": ((summary or {}).get("beam_fall") or {}).get(
                "fall_90_10_s"
            ),
            "quen_asserted": bool(quen.get("asserted")),
            "quen_assert_t_s": quen.get("first_assert_t_s"),
            "report_titles": titles,
            "report_count": len(titles),
        }
        if include_reports:
            try:
                evidence = search_reports("quench", event_id=event_id, top_k=1)
                row["top_report_evidence"] = (
                    evidence.get("items") or [None]
                )[0]
            except Exception:
                row["top_report_evidence"] = None
        rows.append(row)

    rows.sort(key=lambda r: str(r.get("timestamp") or ""))
    with_waveform = sum(1 for r in rows if r.get("has_waveform"))
    with_reports = sum(1 for r in rows if r.get("report_count"))
    return {
        "response_type": "quench_comparison",
        "available": True,
        "requested": len(ids),
        "returned": len(rows),
        "with_waveform": with_waveform,
        "with_reports": with_reports,
        "rows": rows,
        "caveats": [
            "Feature columns come from the 100 ms waveform; report_titles come "
            "from report_document_events.",
            "Events without parquet have has_waveform=false and null feature "
            "columns, but may still have reports.",
            "This table does not fetch Archiver PVs; call postmortem_analyze_quench "
            "for temperature evidence.",
        ],
    }
