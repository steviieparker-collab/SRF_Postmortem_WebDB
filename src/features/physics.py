"""Textbook-grounded interpretation of a measured beam-dump sequence.

The fast-scope extraction measures *what happened* (cavity 90/50/10 %,
beam 99/50/10 %, delays, spread).  This module turns that measurement into a
short physics interpretation with explicit citations to
``config/physics_references.json``.

Nothing here calls an LLM.  The prose is a deterministic template filled with
numbers from the sequence block; the citations are curated entries.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Optional

_ROOT = Path(__file__).resolve().parent.parent.parent
_PATH = _ROOT / "config" / "physics_references.json"
_CACHE: dict[str, Any] = {"mtime": None, "data": None}


def load() -> dict[str, Any]:
    """Load the reference JSON, reloading when the file changes."""
    try:
        mtime = _PATH.stat().st_mtime
    except OSError:
        return {"available": False, "error": f"missing {_PATH}"}
    if _CACHE["mtime"] != mtime:
        try:
            _CACHE["data"] = json.loads(_PATH.read_text(encoding="utf-8"))
            _CACHE["mtime"] = mtime
        except (OSError, ValueError) as exc:
            return {"available": False, "error": f"{type(exc).__name__}: {exc}"}
    return _CACHE["data"]


def topics() -> dict[str, str]:
    doc = load()
    return {
        key: value.get("title", key)
        for key, value in (doc.get("topics") or {}).items()
    }


def reference_bundle(topic: Optional[str] = None) -> dict[str, Any]:
    """Return the curated reference library, optionally one topic."""
    doc = load()
    if not doc.get("available", True):
        return {"available": False, "error": doc.get("error")}
    refs = {r["id"]: r for r in doc.get("references", [])}
    if topic:
        block = (doc.get("topics") or {}).get(topic)
        if block is None:
            return {
                "available": False,
                "error": "unknown physics topic",
                "topics": topics(),
            }
        return {
            "available": True,
            "topic": topic,
            "title": block.get("title"),
            "summary_ko": block.get("summary_ko"),
            "equations": block.get("equations", []),
            "categories": block.get("categories", []),
            "mapping_ko": block.get("mapping_ko"),
            "citations": _resolve_citations(block.get("citations", []), refs),
            "caveats": block.get("caveats", []),
            "version": doc.get("version"),
            "updated": doc.get("updated"),
        }
    return {
        "available": True,
        "version": doc.get("version"),
        "updated": doc.get("updated"),
        "topics": {
            key: {
                "title": value.get("title"),
                "summary_ko": value.get("summary_ko"),
                "citations": [c.get("id") for c in value.get("citations", [])],
            }
            for key, value in (doc.get("topics") or {}).items()
        },
        "references": doc.get("references", []),
        "classes": doc.get("classes", {}),
        "pls2": doc.get("pls2", {}),
        "internal_test_notes": doc.get("internal_test_notes", []),
        "caveats": doc.get("caveats", []),
    }


def _resolve_citations(
    citations: list[dict[str, Any]],
    refs: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    out = []
    seen = set()
    for cite in citations:
        rid = cite.get("id")
        locator = cite.get("locator", "")
        key = (rid, locator)
        if key in seen:
            continue
        seen.add(key)
        ref = refs.get(rid, {})
        out.append({
            "id": rid,
            "title": ref.get("title", ""),
            "authors": ref.get("authors", ""),
            "year": ref.get("year"),
            "publisher": ref.get("publisher", ""),
            "doi": ref.get("doi"),
            "url": ref.get("url"),
            "locator": locator,
            "supports": cite.get("supports", ""),
        })
    return out


_LEGACY_SEQUENCE_CLASSES = {
    # Removed: PLS-II has not observed a dual-cavity quench.  Two-cavity
    # collapse is not enough evidence, so keep it as mixed.
    "dual_cavity_quench": "mixed",
}


def normalize_sequence(
    sequence: Optional[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    """Map removed/legacy sequence classes to the current taxonomy."""
    if not sequence:
        return sequence
    mapped = _LEGACY_SEQUENCE_CLASSES.get(sequence.get("class"))
    if not mapped:
        return sequence
    out = dict(sequence)
    out["class_original"] = sequence.get("class")
    out["class"] = mapped
    out["class_rule"] = (
        "2-cavity collapse is not a dual quench; "
        "check RF station/common-point and LLRF/HPRF logs"
    )
    return out


def _median(values: list[float]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    vals.sort()
    n = len(vals)
    return vals[n // 2] if n % 2 else 0.5 * (vals[n // 2 - 1] + vals[n // 2])


def _expected() -> dict[str, Any]:
    doc = load()
    p = doc.get("pls2") or {}
    f_rf = float(p.get("f_rf_mhz", 499.973)) * 1e6
    omega = 2.0 * math.pi * f_rf
    q0 = p.get("q0", [7.5e8, 1.1e9])
    qext = p.get("qext", [1.5e5, 1.7e5])
    v_total = p.get("v_total_mv", [4.5, 4.95])
    current = p.get("beam_current_ma", [300, 400])
    u0 = float(p.get("u0_mev_per_turn", 1.242))
    rq = float(p.get("r_over_q_ohm", 89))
    n_cav = int(p.get("cavities", 3))

    v_c = 0.5 * (v_total[0] + v_total[1]) / n_cav * 1e6
    i_b = 0.5 * (current[0] + current[1]) / 1000.0
    cosphi = u0 / (0.5 * (v_total[0] + v_total[1]))
    p_beam = v_c * i_b * cosphi
    u_store = v_c * v_c / (2.0 * omega * rq)
    tau_nat = [2.0 * q / omega for q in (qext[0], qext[1])]
    tau_beam = u_store / p_beam if p_beam > 0 else None
    beta = [q0[0] / qext[0] - 1.0, q0[1] / qext[1] - 1.0]
    return {
        "f_rf_mhz": f_rf / 1e6,
        "v_cavity_mv": v_c / 1e6,
        "beam_current_ma": i_b * 1000.0,
        "cosphi": round(cosphi, 4),
        "p_beam_kw": round(p_beam / 1000.0, 1),
        "u_store_j": round(u_store, 2),
        "tau_natural_us": [round(x * 1e6, 1) for x in tau_nat],
        "tau_beam_us": round(tau_beam * 1e6, 1) if tau_beam else None,
        "beta": [round(x, 0) for x in beta],
        "rf_acceptance_percent": p.get("rf_acceptance_percent"),
        "acceptance_note": p.get("acceptance_note"),
    }


def interpret_sequence(
    sequence: Optional[dict[str, Any]],
    beam_fall: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Return a textbook-grounded interpretation of one sequence block."""
    doc = load()
    if not doc.get("available", True):
        return {"available": False, "error": doc.get("error")}
    sequence = normalize_sequence(sequence)
    if not sequence or not sequence.get("class"):
        return {
            "available": False,
            "reason": "no sequence block (event has no fast-scope trace)",
            "caveats": [
                "1 Hz Archiver는 RF off와 beam off를 같은 샘플에 묶는 경우가 "
                "대부분이라 인과 순서를 줄 수 없다."
            ],
        }

    cls = sequence["class"]
    class_info = (doc.get("classes") or {}).get(cls, {})
    cavities = sequence.get("cavities") or {}
    collapsed = {
        unit: block for unit, block in cavities.items()
        if block.get("collapsed")
    }
    taus = [block.get("tau_us") for block in collapsed.values()]
    tau_med = _median(taus)
    spread = sequence.get("cavity_collapse_spread_us")
    delay50 = sequence.get("delay_cav50_to_beam50_us")
    delay90 = sequence.get("delay_cav90_to_beam99_us")

    cav90 = [block.get("t90_s") for block in collapsed.values()
             if block.get("t90_s") is not None]
    beam_t99 = None
    if cav90 and delay90 is not None:
        beam_t99 = _median(cav90) + delay90 * 1e-6
    beam_t10 = None
    if beam_fall:
        beam_t10 = beam_fall.get("fall_10_s")

    expected = _expected()
    tau_lo, tau_hi = expected["tau_natural_us"]
    tau_check_applicable = cls == "rf_trip"
    spans = [
        (block.get("t10_s") - block.get("t90_s"))
        for block in collapsed.values()
        if block.get("t10_s") is not None and block.get("t90_s") is not None
    ]
    tau_span_us = _median(spans) * 1e6 if spans else None
    # A 90-10 % span longer than 2 ms means a two-stage decay / slow tail,
    # not the single-exponential RF-off time constant.
    tau_reliable = tau_span_us is not None and tau_span_us <= 2000.0
    tau_consistent = (
        tau_med is not None
        and 0.3 * tau_lo <= tau_med <= 1.5 * tau_hi
    ) if (tau_check_applicable and tau_reliable) else None
    common_cause = spread is not None and spread <= 20.0
    cavity_first = delay50 is not None and delay50 > 0

    measured = []
    if tau_med is not None:
        if tau_reliable:
            label = (
                "cavity tau(90-10%)" if tau_check_applicable
                else "cavity tau(90-10%, quench)"
            )
            measured.append(f"{label} {tau_med:.0f} us")
        else:
            measured.append(
                f"cavity 90-10% span {tau_span_us:.0f} us "
                "(two-stage, tau not quoted)"
            )
    if spread is not None:
        measured.append(f"collapsed-cavity spread {spread:.1f} us")
    if delay50 is not None:
        measured.append(f"cavity50->beam50 {delay50:.0f} us")
    if delay90 is not None:
        measured.append(f"cavity90->beam99 {delay90:.0f} us")

    summary = class_info.get("summary_ko", "")
    if cls == "rf_trip" and spread is not None:
        if common_cause:
            summary = summary.rstrip() + (
                f" 3개 캐비티 50% 시각 차이는 {spread:.1f} us로 공통 원인을 시사한다."
            )
        else:
            summary = summary.rstrip() + (
                f" 3개 캐비티 50% 시각 차이는 {spread:.0f} us로 동시 붕괴는 아니다. "
                "순차 quench/전파를 확인해야 한다."
            )
    if measured:
        summary = summary.rstrip() + " 측정값: " + ", ".join(measured) + "."

    topic_ids = class_info.get("citations", [])
    refs = {r["id"]: r for r in doc.get("references", [])}
    citations = []
    for topic_id in topic_ids:
        block = (doc.get("topics") or {}).get(topic_id, {})
        citations.extend(_resolve_citations(block.get("citations", []), refs))

    steps: list[tuple[float, str]] = []
    for unit, block in collapsed.items():
        for key, label in (("t90_s", "90%"), ("t50_s", "50%"), ("t10_s", "10%")):
            if block.get(key) is not None:
                steps.append((
                    float(block[key]),
                    f"{unit} cavity {label} at {float(block[key]) * 1e6:+.0f} us",
                ))
    if beam_t99 is not None:
        steps.append((beam_t99, f"beam 99% at {beam_t99 * 1e6:+.0f} us"))
    steps.append((0.0, "beam 50% (t0)"))
    if beam_t10 is not None:
        steps.append((beam_t10, f"beam 10% at {float(beam_t10) * 1e6:+.0f} us"))
    steps.sort(key=lambda x: x[0])

    tau_txt = f"{tau_med:.0f} us" if (tau_med is not None and tau_reliable) else None
    delay_txt = f"{delay50:.0f} us" if delay50 is not None else "수백 us"
    if cls == "rf_trip":
        if tau_txt:
            decay_txt = f"Vgap이 tau≈{tau_txt}로 감쇠"
        else:
            decay_txt = (
                "Vgap이 빠르게 50%까지 붕괴한 뒤 느린 꼬리를 보임"
                f"(90-10% span {tau_span_us:.0f} us, 단일 tau 인용 안 함)"
                if tau_span_us is not None else "Vgap이 빠르게 붕괴"
            )
        chain = (
            "인터록/RF off → 3개 캐비티 "
            f"{decay_txt} → 빔이 매 회전 U0=1.242 MeV(≈1.32 GeV/ms)를 "
            f"잃음 → RF acceptance({expected['rf_acceptance_percent']}%)를 넘어 "
            "빔 손실. 캐비티가 빔보다 "
            f"{delay_txt} 먼저 움직였으므로 RF가 원인 쪽이다."
        )
    elif cls == "beam_first_rf_trip":
        chain = (
            "빔전류가 먼저 감소 → 빔부하가 줄어 캐비티 전압이 부분적으로 "
            "하강 → 뒤늦게 RF trip. Forward가 유지되면 RF 계통은 원인이 아니다. "
            "MPS, magnet, CONTROL/TFS/FOFB, MIS 로그를 함께 본다. "
            f"캐비티 50%가 빔 50%보다 {abs(delay50 or 0):.0f} us 늦다."
        )
    elif cls == "beam_only":
        chain = (
            "캐비티 전압이 20% 아래로 내려가지 않았다. RF는 원인이 아니며 "
            "MIS(진공/온도/냉각/방사선), CONTROL/TFS/FOFB, beam line, kicker, "
            "궤도/횡방향 손실을 봐야 한다."
        )
    elif cls == "single_cavity_quench":
        chain = (
            "1개 캐비티만 0으로 붕괴했고 캐비티가 빔보다 "
            f"{delay_txt} 먼저 움직였다. 해당 CM의 quench 또는 RF station fault를 "
            "의심하고 QUEN/forward/reflect와 LLRF/HPRF 로그를 확인한다."
        )
    else:
        chain = (
            "캐비티 변화는 있으나 순서가 명확하지 않다. 2개 캐비티만 붕괴한 경우 "
            "dual quench로 단정하지 말고 RF station/common point, LLRF/HPRF 로그와 "
            "cavity/forward/reflect, digital 전이를 함께 봐야 한다."
        )

    caveats = list(doc.get("caveats", []))
    caveats.append(
        "sequence 시간은 trace-local beam 50% t0 기준이다. 음수 캐비티 시간은 "
        "캐비티가 빔보다 먼저 움직였다는 뜻이다."
    )
    if cls == "rf_trip":
        caveats.append(
            "3개 캐비티가 20 us 안에 붕괴하면 공통 원인(인터록)이지 독립 quench가 아니다."
        )
    if not tau_reliable:
        caveats.append(
            "90-10% span이 2 ms를 넘어 단일 지수 감쇠가 아니다. tau 값은 인용하지 않고 "
            "cavity 50% 시각을 기준으로 해석한다."
        )
    if tau_consistent is False:
        caveats.append(
            "관측 tau가 PLS-II Qext 기반 밴드(약 29-162 us) 밖이다. LLRF ramp나 "
            "2단 감쇠 가능성을 확인해야 한다."
        )
    if cls in ("beam_first_rf_trip", "beam_only"):
        caveats.append(
            "캐비티 전압의 부분 하강은 빔부하 변화로 설명된다. Forward가 유지되면 "
            "RF 계통은 정상이다."
        )

    return {
        "available": True,
        "class": cls,
        "class_original": sequence.get("class_original"),
        "title_ko": class_info.get("title_ko", cls),
        "summary_ko": summary,
        "chain_ko": chain,
        "steps_ko": [text for _, text in steps],
        "class_rule": sequence.get("class_rule"),
        "timeline": {
            "cavities": cavities,
            "cavity_tau_median_us": tau_med,
            "cavity_collapse_spread_us": spread,
            "delay_cav50_to_beam50_us": delay50,
            "delay_cav90_to_beam99_us": delay90,
            "beam_t50_s": 0.0,
            "beam_t99_s": beam_t99,
            "beam_t10_s": beam_t10,
        },
        "checks": {
            "cavity_tau_observed_us": tau_med,
            "cavity_tau_span_us": tau_span_us,
            "tau_check_applicable": tau_check_applicable,
            "tau_reliable": tau_reliable,
            "cavity_tau_expected_natural_us": expected["tau_natural_us"],
            "cavity_tau_expected_beam_loaded_us": expected["tau_beam_us"],
            "cavity_tau_consistent": tau_consistent,
            "common_cause_3cavity": common_cause,
            "cavity_first": cavity_first,
            "pls2": expected,
        },
        "citations": citations,
        "caveats": caveats,
    }
