"""Deterministic event analysis bundles for MCP clients.

The MCP server exposes raw feature blocks today.  Fresh agents tend to mix
observation (`physics.class`) with root cause and to quote decay constants that
the feature extractor explicitly marks unreliable.  This module builds a
single, deterministic evidence bundle and a small set of cohort/example helpers
so an agent can quote evidence instead of inventing a causal story.

No LLM is called here.  Every number comes from the feature store or SQLite.
"""

from __future__ import annotations

import json
import math
import re
import statistics
from collections import Counter
from pathlib import Path
from typing import Any, Optional

from ..rag.db import connect_events
from ..rag.tools import get_event_context
from .physics import interpret_sequence, normalize_sequence
from .store import connect_feature_store

_ROOT = Path(__file__).resolve().parent.parent.parent
_GUIDE_PATH = _ROOT / "config" / "analysis_guide.json"
_EXAMPLES_PATH = _ROOT / "config" / "analysis_examples.json"

_GUIDE_CACHE: dict[str, Any] = {"mtime": None, "data": None}
_EXAMPLES_CACHE: dict[str, Any] = {"mtime": None, "data": None}

# Signals visible in the pre-rendered narrow-scope JPEGs.  The images are
# static panels, so an agent cannot infer this list from the JPEG alone.
_IMAGE_PANELS: dict[str, dict[str, Any]] = {
    "forward": {
        "analog": [
            "BeamCurrent", "Forward_SRF1", "Forward_SRF2", "Forward_SRF3",
        ],
        "digital": [
            "INT_MIS1_IC", "INT_MIS2_IC", "INT_IN_FC3",
            "INT_IC_FC1", "INT_IC_FC2",
            "RDY_KSU1_IC", "RDY_KSU2_IC", "RDY_KSU3_IC",
            "INT_PSI1_IC", "INT_PSI2_IC",
        ],
        "caption_ko": (
            "Forward 패널: BeamCurrent, Forward_SRF1/2/3와 KSU/MIS/PSI "
            "digital이 보인다. CM QUEN/ARC/VAC/HE-PR 신호는 이 패널에 없다."
        ),
    },
    "cavity": {
        "analog": ["BeamCurrent", "Cavity_SRF1", "Cavity_SRF2", "Cavity_SRF3"],
        "digital": [
            "RDY_CM1_FC1", "RDY_CM2_FC2", "RDY_CM3_FC3",
            "HE-PR_CM1_FC1", "HE-PR_CM2_FC2", "HE-PR_CM3_FC3",
            "ARC_CM1_FC1", "ARC_CM2_FC2", "ARC_CM3_FC3",
            "QUEN_CM1_FC1", "QUEN_CM2_FC2", "QUEN_CM3_FC3",
            "VAC_CM1_FC1", "VAC_CM2_FC2", "VAC_CM3_FC3",
        ],
        "caption_ko": (
            "Cavity 패널: Cavity_SRF1/2/3와 CM QUEN/ARC/VAC/HE-PR/RDY "
            "digital이 보인다. CM 내부 인터록의 first evidence는 여기에 있다."
        ),
    },
    "reflect": {
        "analog": ["BeamCurrent", "Reflect_SRF1", "Reflect_SRF2", "Reflect_SRF3"],
        "digital": [],
        "caption_ko": (
            "Reflect 패널: Reflect_SRF1/2/3와 BeamCurrent. 반사파 spike는 "
            "빔/ RF 위상 mismatch의 결과다."
        ),
    },
}

_FAMILY_LABELS = {
    "quench": "CM quench",
    "cm_arc": "CM ARC",
    "cm_vacuum": "CM vacuum",
    "cm_he": "CM He pressure",
    "ksu": "KSU/HPRF fault",
    "mis": "MIS trip",
    "mps": "MPS trip",
    "psi": "PSI trip",
    "kicker": "Kicker fault",
    "llrf": "LLRF/RF control fault",
    "magnet": "Magnet/MPS fault",
    "unknown": "unknown",
}


# ---------------------------------------------------------------------------
# Static guides
# ---------------------------------------------------------------------------

def _load_json_cached(path: Path, cache: dict[str, Any]) -> dict[str, Any]:
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return {"available": False, "error": f"missing {path}"}
    if cache["mtime"] != mtime:
        try:
            cache["data"] = json.loads(path.read_text(encoding="utf-8"))
            cache["mtime"] = mtime
        except (OSError, ValueError) as exc:
            return {"available": False, "error": f"{type(exc).__name__}: {exc}"}
    return cache["data"]


def analysis_guide() -> dict[str, Any]:
    """Analysis contract and good/bad examples for MCP clients."""
    return _load_json_cached(_GUIDE_PATH, _GUIDE_CACHE)


def analysis_examples() -> dict[str, Any]:
    """Curated example-event registry."""
    return _load_json_cached(_EXAMPLES_PATH, _EXAMPLES_CACHE)


# ---------------------------------------------------------------------------
# Feature helpers
# ---------------------------------------------------------------------------

def _first_digital(features: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Earliest asserted digital channel in the 100 ms trace."""
    items: list[tuple[float, str, dict[str, Any]]] = []
    for channel, block in (features.get("digital") or {}).items():
        t = block.get("first_assert_t_s")
        if t is None:
            continue
        items.append((float(t), str(channel), block))
    if not items:
        return None
    t, channel, block = min(items, key=lambda item: item[0])
    return {
        "channel": channel,
        "t_s": t,
        "t_us": round(t * 1e6, 1),
        "polarity": block.get("polarity"),
        "asserted": block.get("asserted"),
    }


def _digital_times(features: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for channel, block in (features.get("digital") or {}).items():
        t = block.get("first_assert_t_s")
        if t is not None:
            out[str(channel)] = float(t)
    return out


def _fault_family(
    note: str,
    first: Optional[dict[str, Any]],
    quench_units: Optional[list[str]] = None,
) -> str:
    text = (note or "").lower()
    first_ch = (first or {}).get("channel", "").upper()

    if "quench" in text or first_ch.startswith("QUEN") or quench_units:
        return "quench"
    if "arc" in text or first_ch.startswith("ARC"):
        return "cm_arc"
    if "vac" in text or "진공" in note or first_ch.startswith("VAC"):
        return "cm_vacuum"
    if "he-pr" in text or "he pressure" in text or first_ch.startswith("HE-PR"):
        return "cm_he"
    if (
        "ksu" in text or "kly" in text or "hvps" in text or "smps" in text
        or "water flow" in text or "유량" in note or "ion pump" in text
        or first_ch.startswith("RDY_KSU")
    ):
        return "ksu"
    if "mis" in text or first_ch.startswith("INT_MIS"):
        return "mis"
    if "mps" in text:
        return "mps"
    if "psi" in text or first_ch.startswith("INT_PSI"):
        return "psi"
    if "kicker" in text:
        return "kicker"
    if "llrf" in text or "rf" in text:
        return "llrf"
    if "magnet" in text:
        return "magnet"
    return "unknown"


def _root_cause(event: dict[str, Any], family: str) -> dict[str, Any]:
    note = (event.get("notes") or "").strip()
    m = re.search(r"원인\s*:\s*(.+)", note)
    if m:
        text = m.group(1).strip()
    elif note:
        text = note.splitlines()[0].strip()
    else:
        text = ""
    return {
        "status": "operator_note" if text else "unknown",
        "text": text,
        "source": "notes" if text else None,
        "fault_family": family,
        "label_ko": _FAMILY_LABELS.get(family, family),
    }


def _sequence_payload(
    sequence: Optional[dict[str, Any]],
    physics: dict[str, Any],
    features: dict[str, Any],
) -> dict[str, Any]:
    sequence = sequence or {}
    checks = physics.get("checks") or {}
    cavities = sequence.get("cavities") or {}
    collapsed = [
        (unit, block)
        for unit, block in cavities.items()
        if block.get("collapsed")
    ]
    first_cavity = None
    if collapsed:
        first_cavity = min(
            collapsed,
            key=lambda item: (
                item[1].get("t50_s")
                if item[1].get("t50_s") is not None
                else math.inf
            ),
        )[0]
    per_tau = {
        unit: block.get("tau_us")
        for unit, block in cavities.items()
        if block.get("tau_us") is not None
    }
    return {
        "class": sequence.get("class"),
        "class_is_observational": True,
        "class_rule": sequence.get("class_rule"),
        "n_cavities_collapsed": sequence.get("n_cavities_collapsed"),
        "cavities": cavities,
        "first_cavity": first_cavity,
        "cavity_collapse_spread_us": sequence.get(
            "cavity_collapse_spread_us"
        ),
        "delay_cav50_to_beam50_us": sequence.get(
            "delay_cav50_to_beam50_us"
        ),
        "delay_cav90_to_beam99_us": sequence.get(
            "delay_cav90_to_beam99_us"
        ),
        "beam_fall": features.get("beam_fall"),
        "beam_t0_s": 0.0,
        "tau_observed_us": checks.get("cavity_tau_observed_us"),
        "tau_span_us": checks.get("cavity_tau_span_us"),
        "tau_reliable": checks.get("tau_reliable"),
        "tau_expected_natural_us": checks.get(
            "cavity_tau_expected_natural_us"
        ),
        "tau_expected_beam_loaded_us": checks.get(
            "cavity_tau_expected_beam_loaded_us"
        ),
        "per_cavity_tau_us": per_tau,
        "pls2": checks.get("pls2"),
    }


def _late_ksu(
    digital: dict[str, float],
    first: Optional[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not first:
        return []
    first_t = float(first["t_s"])
    out = []
    for channel, t in digital.items():
        if channel.startswith("RDY_KSU") and t > first_t + 20e-6:
            out.append({
                "channel": channel,
                "t_us": round(t * 1e6, 1),
                "delay_us": round((t - first_t) * 1e6, 1),
            })
    return sorted(out, key=lambda item: item["t_us"])


def _warnings(
    event: dict[str, Any],
    classification: dict[str, Any],
    family: str,
    first: Optional[dict[str, Any]],
    quench_unit: Optional[str],
    quench_units: list[str],
    seq: dict[str, Any],
    digital: dict[str, float],
    features: dict[str, Any],
) -> list[str]:
    out: list[str] = []
    if classification.get("manual_overrides_auto"):
        out.append(
            "수동 분류와 자동 분류가 다르다: "
            f"manual={classification.get('manual')} / "
            f"auto={classification.get('auto')}."
        )
    if seq.get("class") == "single_cavity_quench" and not quench_units:
        out.append(
            "measurement class는 single_cavity_quench이지만 QUEN 채널은 "
            "트립하지 않았다. class는 모양 라벨이고 quench로 단정하면 안 된다."
        )
    if family == "quench" and not quench_units:
        out.append(
            "note/class는 quench를 가리키지만 QUEN 채널이 확인되지 않았다."
        )
    if seq.get("tau_reliable") is False:
        out.append(
            "tau_reliable=false: 90-10% span이 길어 단일 지수 tau를 인용하면 안 된다."
        )
    n = seq.get("n_cavities_collapsed") or 0
    spread = seq.get("cavity_collapse_spread_us")
    if n > 1:
        out.append(
            f"{n}개 캐비티가 붕괴했다. 동시 붕괴로 부르지 말고 per-cavity 시각을 쓴다."
        )
    if spread is not None and spread > 20:
        out.append(f"3-cavity 50% spread={spread:.1f} us: 동시가 아니다.")
    late = _late_ksu(digital, first)
    if late and first and not first["channel"].startswith("RDY_KSU"):
        names = ", ".join(item["channel"] for item in late[:4])
        out.append(
            f"{names}가 first interlock 이후에 나타난다. KSU trip을 root cause로 "
            "쓰지 말고 CM/QUEN/ARC 등 선행 신호를 먼저 본다."
        )
    if first and first["channel"].startswith("QUEN") and quench_unit:
        out.append(
            f"first interlock은 {first['channel']}이고 quench_unit={quench_unit}다."
        )
    forward_max = 0.0
    for name, block in (features.get("analog") or {}).items():
        if name.startswith("Forward_"):
            forward_max = max(forward_max, float(block.get("post_max") or 0.0))
    if forward_max > 1.0:
        out.append(
            f"forward 최대 {forward_max:.3f}은 LLRF demand가 아니라 "
            "과입력 방지기 제한 후 값일 수 있다."
        )
    if not features:
        out.append("waveform feature가 없어 timing/sequence를 해석할 수 없다.")
    return out


def _claim_checks(
    family: str,
    first: Optional[dict[str, Any]],
    quench_unit: Optional[str],
    quench_units: list[str],
    seq: dict[str, Any],
    digital: dict[str, float],
    features: dict[str, Any],
) -> list[dict[str, str]]:
    checks: list[dict[str, str]] = []

    def add(claim: str, status: str, reason: str) -> None:
        checks.append({"claim": claim, "status": status, "reason": reason})

    if seq.get("tau_reliable") is False:
        add(
            "tau=17 ms (또는 단일 tau)",
            "forbidden",
            "tau_reliable=false; 90-10% span이 길어 단일 지수 tau가 아니다.",
        )
        add(
            "quench로 QL이 급감해서 감쇠가 길어졌다",
            "forbidden",
            "quench는 QL을 낮춰 감쇠를 짧게 만든다. 긴 꼬리는 two-stage/slow tail이다.",
        )
    n = seq.get("n_cavities_collapsed") or 0
    spread = seq.get("cavity_collapse_spread_us")
    if n > 1 and (spread is None or spread > 20):
        add(
            "3개 캐비티가 동시에 붕괴했다",
            "forbidden",
            f"n={n}, spread={spread} us. per-cavity 시각이 다르다.",
        )
    late = _late_ksu(digital, first)
    if late and first and not first["channel"].startswith("RDY_KSU"):
        add(
            "RDY_KSU#가 root cause다",
            "forbidden",
            f"first interlock은 {first['channel']}이고 RDY_KSU는 뒤에 나타난다.",
        )
    if seq.get("class") == "single_cavity_quench" and not quench_units:
        add(
            "이 이벤트는 cavity quench다",
            "forbidden",
            "class는 single_cavity_quench이지만 QUEN 채널이 트립하지 않았다.",
        )
    add(
        "cavity probe가 회복했다",
        "caution",
        "dip 후 상승은 beam loading/phase-slip 진동일 수 있다.",
    )
    add(
        "forward 1.1~1.2는 LLRF demand다",
        "caution",
        "과입력 방지기 제한 후 값이다. demand는 더 클 수 있다.",
    )
    if family == "quench" and not quench_units:
        add(
            "quench unit을 특정했다",
            "caution",
            "QUEN 채널이 확인되지 않았다.",
        )
    return checks


def _required_disclosures(
    classification: dict[str, Any],
    seq: dict[str, Any],
    first: Optional[dict[str, Any]],
    late: list[dict[str, Any]],
) -> list[str]:
    out = ["physics.class는 관측 패턴이며 root cause가 아니다."]
    if classification.get("manual_overrides_auto"):
        out.append(
            f"manual={classification.get('manual')} / "
            f"auto={classification.get('auto')} 차이를 밝힌다."
        )
    if seq.get("tau_reliable") is False:
        out.append("tau_reliable=false이므로 단일 tau를 인용하지 않는다.")
    if late and first and not first["channel"].startswith("RDY_KSU"):
        out.append(
            f"{first['channel']}가 first interlock이며 RDY_KSU는 downstream이다."
        )
    if (seq.get("n_cavities_collapsed") or 0) > 1:
        out.append("캐비티별 50/10% 시각을 따로 제시한다.")
    return out


def _mechanism(
    event: dict[str, Any],
    family: str,
    first: Optional[dict[str, Any]],
    seq: dict[str, Any],
    root: dict[str, Any],
) -> list[dict[str, str]]:
    label = _FAMILY_LABELS.get(family, family)
    cm = seq.get("first_cavity") or "CM?"
    delay = seq.get("delay_cav50_to_beam50_us")
    first_ch = (first or {}).get("channel", "unknown")
    first_t = (first or {}).get("t_us")
    steps: list[dict[str, str]] = []

    if family in {"quench", "cm_arc", "cm_vacuum", "cm_he", "llrf"}:
        steps.append({
            "step": "1",
            "text": f"{cm} 내부 {label}가 먼저 발생한다.",
            "evidence": f"first interlock {first_ch} at {first_t} us; note={root.get('text') or '-'}",
        })
        steps.append({
            "step": "2",
            "text": f"{cm} station의 LLRF/KSU RF switch가 열려 RF 출력이 차단된다.",
            "evidence": "CM interlock hierarchy / Forward_SRF# drop",
        })
        steps.append({
            "step": "3",
            "text": f"{cm} cavity Vgap이 RF-off 시정수로 감쇠한다.",
            "evidence": "per-cavity sequence / tau_reliable",
        })
        steps.append({
            "step": "4",
            "text": "나머지 station의 LLRF가 보상하지만 과입력 방지기 약 1.2배 한계에 걸린다.",
            "evidence": "Forward_SRF# post-limit / over_input_protection",
        })
        steps.append({
            "step": "5",
            "text": f"빔이 에너지/위상을 잃고 {delay} us 뒤 빔전류가 50%로 떨어진다.",
            "evidence": "beam_fall / delay_cav50_to_beam50_us",
        })
    elif family == "ksu":
        steps.append({
            "step": "1",
            "text": "KSU/HPRF 내부 fault가 발생한다.",
            "evidence": f"first interlock {first_ch} at {first_t} us; note={root.get('text') or '-'}",
        })
        steps.append({
            "step": "2",
            "text": "RDY_KSU#가 뜨고 해당 KSU RF switch가 열린다.",
            "evidence": "RDY_KSU# / Forward_SRF# drop",
        })
        steps.append({
            "step": "3",
            "text": "해당 CM cavity가 RF-off로 감쇠하고 반사파가 spike한다.",
            "evidence": "per-cavity sequence / Reflect",
        })
        steps.append({
            "step": "4",
            "text": "나머지 station 보상은 약 1.2배로 제한된다.",
            "evidence": "over_input_protection",
        })
        steps.append({
            "step": "5",
            "text": f"빔 에너지 손실 후 {delay} us 뒤 빔덤프.",
            "evidence": "beam_fall / delay",
        })
    elif family in {"mis", "mps", "psi"}:
        steps.append({
            "step": "1",
            "text": f"{label}가 발생한다.",
            "evidence": f"first interlock {first_ch}; note={root.get('text') or '-'}",
        })
        steps.append({
            "step": "2",
            "text": "RF Ready/전원 계통이 끊기거나 빔 손실이 시작된다.",
            "evidence": "interlock hierarchy / classification",
        })
        steps.append({
            "step": "3",
            "text": "cavity가 RF off 또는 beam loading 변화로 붕괴/부분 sag한다.",
            "evidence": "per-cavity sequence",
        })
        steps.append({
            "step": "4",
            "text": f"빔이 {delay} us 규모로 손실된다.",
            "evidence": "beam_fall",
        })
    else:
        steps.append({
            "step": "1",
            "text": "첫 디지털 인터록이 발생한다.",
            "evidence": f"{first_ch} at {first_t} us",
        })
        steps.append({
            "step": "2",
            "text": "RF off 또는 빔 손실이 이어지고 cavity/beam이 반응한다.",
            "evidence": "sequence",
        })
        steps.append({
            "step": "3",
            "text": f"빔덤프 지연 {delay} us.",
            "evidence": "beam_fall",
        })
    return steps


def _image_metadata(event_id: str, first_channel: Optional[str] = None) -> list[dict[str, Any]]:
    base = _ROOT / "data" / "waveforms" / event_id
    out: list[dict[str, Any]] = []
    for scope, panel in _IMAGE_PANELS.items():
        path = base / f"{scope}_narrow.jpg"
        try:
            if not path.is_file() or path.stat().st_size <= 0:
                continue
        except OSError:
            continue
        caption = panel["caption_ko"]
        if first_channel:
            if scope == "forward" and not first_channel.startswith("RDY_KSU"):
                caption += (
                    f" 첫 인터록 {first_channel}은 이 패널에 없고 cavity 패널에 있다."
                )
            elif scope == "cavity" and (
                first_channel.startswith("QUEN")
                or first_channel.startswith("ARC")
                or first_channel.startswith("VAC")
                or first_channel.startswith("HE-PR")
            ):
                caption += f" 첫 인터록 {first_channel}이 이 패널에 있다."
        out.append({
            "event_id": event_id,
            "scope": scope,
            "mime_type": "image/jpeg",
            "source": str(path.relative_to(_ROOT)),
            "url": f"/api/waveforms/{event_id}/{scope}_narrow.jpg",
            "visible_channels": panel["analog"] + panel["digital"],
            "caption_ko": caption,
        })
    return out


def image_metadata(event_id: str, first_channel: Optional[str] = None) -> list[dict[str, Any]]:
    """Public helper used by the MCP image attachment layer."""
    return _image_metadata(event_id, first_channel)


# ---------------------------------------------------------------------------
# Per-event explanation
# ---------------------------------------------------------------------------

def build_event_analysis(event_id: str) -> dict[str, Any]:
    ctx = get_event_context(str(event_id))
    if ctx.get("error"):
        return {
            "response_type": "event_explanation",
            "available": False,
            "event_id": event_id,
            "error": ctx.get("error"),
        }
    event = ctx.get("event") or {}
    classification = ctx.get("classification") or {}
    record = None
    try:
        record = get_waveform_features(str(event_id))
    except Exception as exc:  # noqa: BLE001
        feature_error = f"{type(exc).__name__}: {exc}"
    else:
        feature_error = None

    if not record:
        return {
            "response_type": "event_explanation",
            "available": False,
            "event_id": event_id,
            "event": event,
            "classification": classification,
            "error": feature_error or "no waveform feature record for this event",
            "note": "No parquet waveform. Use postmortem_get_event for notes/alarms.",
            "images": _image_metadata(str(event_id)),
        }

    features = record.get("features") or {}
    sequence = normalize_sequence(features.get("sequence"))
    physics = interpret_sequence(sequence, features.get("beam_fall"))
    first = _first_digital(features)
    digital = _digital_times(features)
    quench_unit = record.get("quench_unit") or features.get("quench_unit")
    quench_units = features.get("quench_units_with_quen_assert") or []
    quench_source = features.get("quench_unit_source")
    if not quench_units and quench_source == "default_cm1":
        quench_unit = None
    family = _fault_family(event.get("notes") or "", first, quench_units)
    root = _root_cause(event, family)
    seq = _sequence_payload(sequence, physics, features)
    late = _late_ksu(digital, first)
    warnings = _warnings(
        event, classification, family, first, quench_unit, quench_units,
        seq, digital, features,
    )
    claims = _claim_checks(
        family, first, quench_unit, quench_units, seq, digital, features
    )
    disclosures = _required_disclosures(classification, seq, first, late)
    mechanism = _mechanism(event, family, first, seq, root)
    images = _image_metadata(
        str(event_id), (first or {}).get("channel")
    )

    summary = {
        "class": seq.get("class"),
        "class_is_observational": True,
        "primary_interlock": first,
        "quench_unit": quench_unit,
        "quench_unit_source": quench_source,
        "root_cause": root,
        "tau_reliable": seq.get("tau_reliable"),
        "n_cavities_collapsed": seq.get("n_cavities_collapsed"),
        "first_cavity": seq.get("first_cavity"),
        "beam_delay_us": seq.get("delay_cav50_to_beam50_us"),
        "classification_conflict": bool(
            classification.get("manual_overrides_auto")
        ),
        "late_ksu": late,
        "warnings": warnings,
    }
    return {
        "response_type": "event_explanation",
        "available": True,
        "event_id": str(event_id),
        "timestamp": event.get("timestamp"),
        "event": event,
        "classification": classification,
        "summary": summary,
        "primary_interlock": first,
        "quench": {
            "unit": quench_unit,
            "source": quench_source,
            "units_with_quen_assert": quench_units,
        },
        "sequence": seq,
        "root_cause": root,
        "mechanism": mechanism,
        "warnings": warnings,
        "claim_checks": claims,
        "required_disclosures": disclosures,
        "images": images,
        "citations": physics.get("citations", []),
        "caveats": physics.get("caveats", []),
        "url": event.get("url") or f"/events/{event_id}",
        "citation": f"[E:{event_id}]",
    }


# ---------------------------------------------------------------------------
# Cohort analysis
# ---------------------------------------------------------------------------

def analyze_cohort(
    *,
    class_name: Optional[str] = None,
    quench_unit: Optional[str] = None,
    first_interlock: Optional[str] = None,
    fault_family: Optional[str] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    tau_reliable: Optional[bool] = None,
    min_delay_us: Optional[float] = None,
    max_delay_us: Optional[float] = None,
    has_waveform: Optional[bool] = None,
    limit: int = 10,
) -> dict[str, Any]:
    """Filter the waveform feature store into a small cohort summary."""
    try:
        conn = connect_feature_store()
    except Exception as exc:  # noqa: BLE001
        return {
            "response_type": "cohort_analysis",
            "available": False,
            "error": f"feature store unavailable: {type(exc).__name__}: {exc}",
        }
    try:
        rows = conn.execute(
            "SELECT event_id, timestamp, quench_unit, has_waveform, features_json "
            "FROM event_features"
        ).fetchall()
    finally:
        conn.close()

    event_meta: dict[str, dict[str, Any]] = {}
    try:
        ev_conn = connect_events()
        try:
            for row in ev_conn.execute(
                "SELECT id, timestamp, COALESCE(NULLIF(user_fault_type,''), "
                "fault_type, '') AS effective, notes FROM events"
            ):
                event_meta[row["id"]] = dict(row)
        finally:
            ev_conn.close()
    except Exception:  # noqa: BLE001 - feature store can still work
        event_meta = {}

    items: list[dict[str, Any]] = []
    for row in rows:
        try:
            features = json.loads(row["features_json"])
        except (TypeError, ValueError):
            continue
        seq = normalize_sequence(features.get("sequence"))
        physics = interpret_sequence(seq, features.get("beam_fall"))
        first = _first_digital(features)
        meta = event_meta.get(row["event_id"], {})
        note = meta.get("notes") or ""
        family = _fault_family(
            note, first, features.get("quench_units_with_quen_assert") or []
        )
        seq_payload = _sequence_payload(seq, physics, features)
        item = {
            "event_id": row["event_id"],
            "timestamp": meta.get("timestamp") or row["timestamp"],
            "effective_fault_type": meta.get("effective") or "",
            "notes": note,
            "class": seq_payload.get("class"),
            "first_interlock": (first or {}).get("channel"),
            "first_interlock_us": (first or {}).get("t_us"),
            "quench_unit": row["quench_unit"] or features.get("quench_unit"),
            "fault_family": family,
            "tau_reliable": seq_payload.get("tau_reliable"),
            "n_cavities_collapsed": seq_payload.get("n_cavities_collapsed"),
            "beam_delay_us": seq_payload.get("delay_cav50_to_beam50_us"),
            "has_waveform": bool(row["has_waveform"]),
            "url": f"/events/{row['event_id']}",
        }
        items.append(item)

    def keep(item: dict[str, Any]) -> bool:
        if class_name and item.get("class") != class_name:
            return False
        if quench_unit and (item.get("quench_unit") or "").upper() != quench_unit.upper():
            return False
        if first_interlock and first_interlock.upper() not in (
            item.get("first_interlock") or ""
        ).upper():
            return False
        if fault_family and item.get("fault_family") != fault_family:
            return False
        if tau_reliable is not None and item.get("tau_reliable") is not tau_reliable:
            return False
        delay = item.get("beam_delay_us")
        if min_delay_us is not None and (delay is None or delay < min_delay_us):
            return False
        if max_delay_us is not None and (delay is None or delay > max_delay_us):
            return False
        if has_waveform is not None and bool(item.get("has_waveform")) is not has_waveform:
            return False
        year = (item.get("timestamp") or "")[:4]
        if year_from is not None and (not year.isdigit() or int(year) < year_from):
            return False
        if year_to is not None and (not year.isdigit() or int(year) > year_to):
            return False
        return True

    filtered = [item for item in items if keep(item)]
    delays = [
        item["beam_delay_us"] for item in filtered
        if item.get("beam_delay_us") is not None
    ]
    examples = sorted(
        filtered,
        key=lambda item: (
            item.get("beam_delay_us") is None,
            item.get("beam_delay_us") or 0,
        ),
    )[: max(1, min(int(limit or 10), 50))]
    return {
        "response_type": "cohort_analysis",
        "available": True,
        "filters": {
            "class": class_name,
            "quench_unit": quench_unit,
            "first_interlock": first_interlock,
            "fault_family": fault_family,
            "year_from": year_from,
            "year_to": year_to,
            "tau_reliable": tau_reliable,
            "min_delay_us": min_delay_us,
            "max_delay_us": max_delay_us,
            "has_waveform": has_waveform,
        },
        "total_waveform_events": len(items),
        "total_filtered": len(filtered),
        "class_counts": dict(Counter(item["class"] for item in filtered)),
        "first_interlock_counts": dict(
            Counter(item["first_interlock"] for item in filtered)
        ),
        "quench_unit_counts": dict(
            Counter(item["quench_unit"] for item in filtered)
        ),
        "fault_family_counts": dict(
            Counter(item["fault_family"] for item in filtered)
        ),
        "delay_us": {
            "count": len(delays),
            "median": round(statistics.median(delays), 1) if delays else None,
            "min": min(delays) if delays else None,
            "max": max(delays) if delays else None,
        },
        "examples": examples,
        "note": (
            "Cohort counts and timing are computed from the feature store; "
            "examples are sorted by beam delay. Use this to pick representative "
            "events, then call postmortem_explain_event for each one."
        ),
    }


# ---------------------------------------------------------------------------
# Curated examples
# ---------------------------------------------------------------------------

def find_examples(topic: Optional[str] = None) -> dict[str, Any]:
    doc = analysis_examples()
    if not doc.get("available", True):
        return {
            "response_type": "example_registry",
            "available": False,
            "error": doc.get("error"),
        }
    topics = doc.get("topics") or {}
    if not topic:
        return {
            "response_type": "example_registry",
            "available": True,
            "topics": {
                key: {
                    "description": value.get("description", ""),
                    "count": len(value.get("examples") or []),
                }
                for key, value in topics.items()
            },
            "usage": "Pass topic=<key> to get event summaries.",
        }
    block = topics.get(topic)
    if not block:
        return {
            "response_type": "example_registry",
            "available": False,
            "topic": topic,
            "error": "unknown topic",
            "topics": sorted(topics),
        }
    examples = []
    for entry in block.get("examples") or []:
        event_id = entry.get("event_id")
        if not event_id:
            continue
        analysis = build_event_analysis(str(event_id))
        examples.append({
            "event_id": event_id,
            "why_clean": entry.get("why_clean", ""),
            "caution": entry.get("caution", ""),
            "available": analysis.get("available"),
            "summary": analysis.get("summary"),
            "root_cause": analysis.get("root_cause"),
            "url": analysis.get("url"),
            "waveform_url": (
                f"/api/waveforms/{event_id}/forward_narrow.jpg"
                if analysis.get("images") else None
            ),
            "images": analysis.get("images") or [],
            "warnings": analysis.get("warnings") or [],
        })
    return {
        "response_type": "example_registry",
        "available": True,
        "topic": topic,
        "description": block.get("description", ""),
        "examples": examples,
        "note": (
            "These examples are curated for teaching, not a complete event list. "
            "Use postmortem_analyze_cohort to see the full cohort."
        ),
    }


# ---------------------------------------------------------------------------
# Notes retrieval (P2, optional embedding)
# ---------------------------------------------------------------------------

_NOTES_CACHE: dict[str, Any] = {"key": None, "rows": [], "vectors": None}


def _notes_rows() -> tuple[list[dict[str, Any]], tuple[Any, ...]]:
    conn = connect_events()
    try:
        rows = [
            dict(r)
            for r in conn.execute(
                "SELECT id, timestamp, "
                "COALESCE(NULLIF(user_fault_type,''), fault_type, '') AS fault_type, "
                "notes FROM events "
                "WHERE trim(coalesce(notes,'')) <> '' "
                "ORDER BY timestamp DESC"
            )
        ]
    finally:
        conn.close()
    cfg_path = None
    try:
        from ..rag.config import get_rag_config
        cfg_path = get_rag_config().db_path
    except Exception:  # noqa: BLE001
        pass
    key: tuple[Any, ...] = (len(rows),)
    if cfg_path is not None:
        try:
            stat = cfg_path.stat()
            key = (str(cfg_path), stat.st_mtime_ns, stat.st_size, len(rows))
        except OSError:
            pass
    return rows, key


def search_notes(query: str, top_k: int = 8) -> dict[str, Any]:
    """Hybrid lexical + dense retrieval over event notes.

    Dense retrieval uses the same CPU-only multilingual-e5-small model as
    report search.  If onnxruntime/model files are unavailable, it degrades
    to lexical scoring and says so.
    """
    query = (query or "").strip()
    if not query:
        return {
            "response_type": "note_search",
            "available": False,
            "error": "empty query",
        }
    rows, key = _notes_rows()
    if not rows:
        return {
            "response_type": "note_search",
            "available": True,
            "query": query,
            "items": [],
            "note": "No non-empty event notes in DB.",
        }
    dense_available = False
    scores = None
    try:
        import numpy as np
        from .embedding import E5Embedder

        if _NOTES_CACHE["key"] != key or _NOTES_CACHE["vectors"] is None:
            embedder = E5Embedder()
            texts = [
                "passage: " + (row.get("notes") or "")
                for row in rows
            ]
            _NOTES_CACHE["vectors"] = embedder.encode(texts, "")
            _NOTES_CACHE["rows"] = rows
            _NOTES_CACHE["key"] = key
            _NOTES_CACHE["embedder"] = embedder
        embedder = _NOTES_CACHE.get("embedder")
        qv = embedder.encode_one(query, "query: ")
        scores = _NOTES_CACHE["vectors"] @ qv
        dense_available = True
    except Exception:  # noqa: BLE001 - fall back to lexical
        scores = None

    q_tokens = [
        token.lower()
        for token in re.findall(r"[A-Za-z0-9_]+|[가-힣]{2,}", query)
        if len(token) >= 2
    ][:8]

    def lexical(row: dict[str, Any]) -> float:
        if not q_tokens:
            return 0.0
        text = (row.get("notes") or "").lower()
        return sum(1 for token in q_tokens if token in text) / len(q_tokens)

    ranked = []
    for i, row in enumerate(rows):
        lex = lexical(row)
        dense = float(scores[i]) if scores is not None else 0.0
        score = (0.8 * dense + 0.2 * lex) if scores is not None else lex
        if score <= 0:
            continue
        ranked.append({
            "event_id": row["id"],
            "timestamp": row["timestamp"],
            "fault_type": row["fault_type"],
            "notes": row["notes"],
            "score": round(score, 4),
            "dense_score": round(dense, 4) if scores is not None else None,
            "lexical_score": round(lex, 4),
            "citation": f"[E:{row['id']}]",
            "url": f"/events/{row['id']}",
        })
    ranked.sort(key=lambda item: item["score"], reverse=True)
    return {
        "response_type": "note_search",
        "available": True,
        "query": query,
        "mode": "hybrid_embedding" if dense_available else "lexical_fallback",
        "dense_available": dense_available,
        "returned": min(len(ranked), max(1, min(int(top_k or 8), 30))),
        "items": ranked[: max(1, min(int(top_k or 8), 30))],
        "note": (
            "Event-note retrieval is for finding similar operator wording, "
            "not for counts or causal proof. Pair with structured tools."
        ),
    }


# ---------------------------------------------------------------------------
# Question routing
# ---------------------------------------------------------------------------

def route_question(question: str) -> dict[str, Any]:
    """Rule-based routing hints for a fresh agent.  No LLM."""
    q = (question or "").lower()
    tools: list[str] = ["postmortem_get_capabilities"]
    topics: list[str] = []
    filters: dict[str, Any] = {}
    cautions: list[str] = []

    if any(k in q for k in ("몇 건", "건수", "분포", "추세", "통계", "count", "trend")):
        tools.append("postmortem_aggregate_events")
    if any(k in q for k in ("예시", "사례", "example", "교본")):
        tools.append("postmortem_find_examples")
    if re.search(r"\b(20\d{6}_\d{6})\b", question):
        tools.extend(["postmortem_get_event", "postmortem_explain_event"])
    if any(k in q for k in ("파형", "waveform", "quench", "인터록", "interlock")):
        tools.extend(["postmortem_explain_event", "postmortem_analyze_waveform"])
    if any(k in q for k in ("메커니즘", "어떻게", "원인", "설명", "왜", "fault", "고장")):
        tools.append("postmortem_explain_event")
    if any(k in q for k in ("전체", "cohort", "패턴", "비교", "집단")):
        tools.append("postmortem_analyze_cohort")
    if any(k in q for k in ("노트", "notes", "보고서", "report", "비슷", "유사")):
        tools.extend(["postmortem_search_notes", "postmortem_search_reports"])
    if any(k in q for k in ("용어", "약어", "뜻", "glossary", "배경")):
        tools.append("postmortem_get_glossary")

    if any(k in q for k in ("cm", "cavity", "캐비티", "quench", "arc")):
        topics.extend(["beam_loading_llrf", "cavity_decay_after_rf_off"])
    if "ksu" in q or "klystron" in q:
        topics.extend(["mis_machine_interlock", "beam_dump_causes"])
        filters["subsystem"] = "KSU"
    if "kicker" in q:
        topics.append("beam_dump_causes")
        filters["subsystem"] = "KICKER"
    if "mis" in q:
        topics.append("mis_machine_interlock")
    if "mps" in q:
        topics.append("beam_dump_causes")
    if any(k in q for k in ("cm1", "cm2", "cm3")):
        m = re.search(r"cm\s*#?\s*([123])", q)
        if m:
            filters["subsystem"] = f"CM{m.group(1)}"

    cautions.append("class는 관측 패턴이며 root cause가 아니다.")
    if "cm" in q and "quench" in q:
        cautions.append(
            "CM single-fault 예시는 ARC/flowmeter가 적절하다. quench는 multi-cavity/"
            "tau_unreliable 때문에 교본으로 쓰지 않는다."
        )
    return {
        "response_type": "question_route",
        "question": question,
        "recommended_tools": list(dict.fromkeys(tools)),
        "glossary_topics": list(dict.fromkeys(topics)),
        "suggested_filters": filters,
        "cautions": cautions,
        "note": (
            "This is a routing hint, not an answer. Call postmortem_explain_event "
            "or postmortem_analyze_cohort before writing a causal explanation."
        ),
    }
