#!/usr/bin/env python3
"""Golden-question retrieval/feature evaluation (Task 5).

Runs five questions from the plan directly against the feature store and the
quench/report modules.  This checks that the evidence needed for an answer is
retrievable and numerically present; it does not generate prose.

    venv-mcp/bin/python scripts/golden_eval.py
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.features.quench import analyze_quench, compare_quench_events  # noqa: E402
from src.features.reports import search_reports  # noqa: E402

SLOW = "20240907_025313"
SLOW2 = "20241002_171719"
FAST = "20251205_014253"
CM1_2024 = [
    "20240227_121438", "20240826_233522", SLOW, SLOW2, "20241111_044552",
]


def _rows(result: dict) -> dict[str, dict]:
    return {r["event_id"]: r for r in result.get("rows", []) if r.get("found")}


def q1_2024_common() -> dict:
    result = compare_quench_events(CM1_2024)
    rows = _rows(result)
    with_wf = [e for e in CM1_2024 if rows.get(e, {}).get("has_waveform")]
    cm1 = [e for e in CM1_2024 if rows.get(e, {}).get("quench_unit") == "CM1"]
    sag = [e for e in CM1_2024
           if (rows.get(e, {}).get("cavity_sag_depth") or 0) >= 0.5]
    pre = [e for e in CM1_2024
           if (rows.get(e, {}).get("cavity_pre_state_ratio") or 1) < 0.9]
    reports = [e for e in CM1_2024 if (rows.get(e, {}).get("report_count") or 0)]
    passed = (len(with_wf) >= 4 and len(cm1) >= 4 and len(sag) >= 4
              and len(pre) >= 3 and len(reports) >= 4)
    return {
        "id": "Q1",
        "question": "2024년 CM1 quench의 공통 파형 특징은?",
        "pass": passed,
        "evidence": {
            "events": CM1_2024,
            "with_waveform": len(with_wf),
            "quench_unit_cm1": len(cm1),
            "cavity_sag_ge_0.5": len(sag),
            "pre_state_lt_0.9": len(pre),
            "with_reports": len(reports),
            "sag_depths": {e: rows.get(e, {}).get("cavity_sag_depth")
                           for e in CM1_2024},
            "pre_state_ratios": {
                e: rows.get(e, {}).get("cavity_pre_state_ratio")
                for e in CM1_2024
            },
        },
    }


def q2_pair_difference() -> dict:
    result = compare_quench_events([SLOW, SLOW2])
    rows = _rows(result)
    a, b = rows.get(SLOW, {}), rows.get(SLOW2, {})
    deltas = {
        "cavity_sag_depth": abs((a.get("cavity_sag_depth") or 0)
                               - (b.get("cavity_sag_depth") or 0)),
        "cavity_pre_state_ratio": abs((a.get("cavity_pre_state_ratio") or 0)
                                      - (b.get("cavity_pre_state_ratio") or 0)),
        "reflect_rise_ratio": abs((a.get("reflect_rise_ratio") or 0)
                                  - (b.get("reflect_rise_ratio") or 0)),
    }
    passed = bool(a.get("has_waveform") and b.get("has_waveform")
                  and max(deltas.values()) > 0.02)
    return {
        "id": "Q2",
        "question": "20240907과 20241002의 파형 차이는?",
        "pass": passed,
        "evidence": {
            SLOW: {k: a.get(k) for k in (
                "cavity_sag_depth", "cavity_pre_state_ratio",
                "reflect_rise_ratio", "beam_fall_90_10_s")},
            SLOW2: {k: b.get(k) for k in (
                "cavity_sag_depth", "cavity_pre_state_ratio",
                "reflect_rise_ratio", "beam_fall_90_10_s")},
            "abs_deltas": {k: round(v, 6) for k, v in deltas.items()},
        },
    }


def q3_report_actions() -> dict:
    result = search_reports("quench 조치 내용", top_k=10)
    docs = {
        it["doc_id"] for it in result.get("items", [])
        if it.get("kind") == "chunk"
    }
    titles = sorted({
        it.get("title", "") for it in result.get("items", [])
        if it.get("kind") == "chunk"
    })
    passed = len(docs) >= 5
    return {
        "id": "Q3",
        "question": "quench 보고서에서 공통적으로 언급된 조치는?",
        "pass": passed,
        "evidence": {
            "returned": result.get("returned"),
            "distinct_documents": len(docs),
            "titles": titles[:8],
        },
    }


def q4_bcl03_vs_sag() -> dict:
    result = analyze_quench(SLOW, include_reports=False)
    series = ((result.get("pv") or {}).get("series") or {})
    bcl03 = series.get("BCL03") or {}
    signature = (((result.get("waveform") or {}).get("signature") or {})
                 .get("CM1") or {})
    delta = bcl03.get("delta")
    sag = signature.get("cavity_sag_depth")
    passed = delta is not None and delta > 5 and sag is not None and sag > 0.5
    return {
        "id": "Q4",
        "question": "BCL03 상승과 cavity sag의 시간 관계는?",
        "pass": passed,
        "evidence": {
            "bcl03_pre_median": bcl03.get("pre_median"),
            "bcl03_post_max": bcl03.get("post_max"),
            "bcl03_delta": delta,
            "bcl03_peak_t_s": bcl03.get("post_max_t_s"),
            "bcl03_quality": bcl03.get("quality"),
            "cavity_sag_depth": sag,
            "cavity_min_t_s": signature.get("cavity_sag_time_s"),
            "clock_note": "BCL03 peak is relative to Archiver t0; cavity min is "
                          "relative to waveform t0.",
        },
    }


def q5_fast_vs_slow() -> dict:
    result = compare_quench_events([SLOW, FAST])
    rows = _rows(result)
    slow, fast = rows.get(SLOW, {}), rows.get(FAST, {})
    pre_slow = slow.get("cavity_pre_state_ratio")
    pre_fast = fast.get("cavity_pre_state_ratio")
    pre_diff = (
        abs(pre_slow - pre_fast)
        if pre_slow is not None and pre_fast is not None else None
    )
    passed = (
        fast.get("quench_unit_source") == "digital_quen"
        and bool(fast.get("quen_asserted"))
        and slow.get("quench_unit_source") != "digital_quen"
        and pre_diff is not None and pre_diff > 0.2
    )
    return {
        "id": "Q5",
        "question": "20251205(fast)와 2024(slow) quench의 차이는?",
        "pass": passed,
        "evidence": {
            "slow": {
                "event_id": SLOW,
                "quench_unit_source": slow.get("quench_unit_source"),
                "quen_asserted": slow.get("quen_asserted"),
                "cavity_sag_depth": slow.get("cavity_sag_depth"),
                "cavity_pre_state_ratio": pre_slow,
                "beam_fall_90_10_s": slow.get("beam_fall_90_10_s"),
            },
            "fast": {
                "event_id": FAST,
                "quench_unit_source": fast.get("quench_unit_source"),
                "quen_asserted": fast.get("quen_asserted"),
                "quen_assert_t_s": fast.get("quen_assert_t_s"),
                "cavity_sag_depth": fast.get("cavity_sag_depth"),
                "cavity_pre_state_ratio": pre_fast,
                "beam_fall_90_10_s": fast.get("beam_fall_90_10_s"),
            },
            "pre_state_diff": round(pre_diff, 6) if pre_diff is not None else None,
        },
    }


def main() -> int:
    started = time.time()
    checks = [
        q1_2024_common(),
        q2_pair_difference(),
        q3_report_actions(),
        q4_bcl03_vs_sag(),
        q5_fast_vs_slow(),
    ]
    passed = sum(1 for c in checks if c["pass"])
    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "passed": passed,
        "total": len(checks),
        "checks": checks,
        "elapsed_seconds": round(time.time() - started, 2),
    }
    out = ROOT / "data" / "reports" / "golden_eval.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2),
                   encoding="utf-8")
    print(json.dumps({"passed": passed, "total": len(checks),
                      "elapsed_seconds": result["elapsed_seconds"]},
                     ensure_ascii=False))
    for c in checks:
        print(("PASS" if c["pass"] else "FAIL"), c["id"], c["question"])
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
