#!/usr/bin/env python3
"""Evaluate retrieval / answers against docs/rag_golden_set.json.

Retrieval-only (default) checks whether expected event IDs appear in the
retrieved evidence. ``--llm`` calls the configured LLM and checks event IDs
and expected terms in the answer.

Examples:
    venv/bin/python scripts/rag_eval.py
    venv/bin/python scripts/rag_eval.py --llm
    venv/bin/python scripts/rag_eval.py --llm --only q01,q02
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.rag.answer import answer_question  # noqa: E402
from src.rag.index import search  # noqa: E402
from src.rag.plan import plan_query  # noqa: E402
from src.rag.tools import search_events  # noqa: E402

GOLDEN = ROOT / "docs" / "rag_golden_set.json"


def _norm(value: str) -> str:
    """Normalize KSU#1/KSU1, CM#1/CM1, AI_LOW_FAULT40/AI LOW FAULT 40."""
    return re.sub(r"[^0-9a-z가-힣]", "", (value or "").lower())


def _retrieval_event_ids(question: str, top_k: int) -> set[str]:
    plan = plan_query(question)
    ids: set[str] = set()
    filters = {
        k: v for k, v in plan.filters.items()
        if k in {"doc_type", "event_id", "pv", "year", "year_from", "year_to",
                 "fault_type"}
    }
    for hit in search(question, filters=filters, top_k=top_k):
        eid = hit.get("event_id") or (hit.get("metadata") or {}).get("event_id")
        if eid:
            ids.add(eid)
    if plan.filters.get("event_id"):
        ids.add(str(plan.filters["event_id"]))
    if plan.filters.get("date"):
        for ev in search_events(
            {"date": plan.filters["date"], "subsystem": plan.filters.get("subsystem")},
            limit=10,
        ):
            ids.add(ev["id"])
    if plan.filters.get("text") or plan.filters.get("subsystem") or plan.filters.get("fault_type"):
        for ev in search_events(plan.filters, limit=top_k):
            ids.add(ev["id"])
    return ids


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--llm", action="store_true", help="run the full LLM answer")
    ap.add_argument("--top-k", type=int, default=10)
    ap.add_argument("--only", default="", help="comma-separated question ids")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    cases = json.loads(GOLDEN.read_text(encoding="utf-8"))
    only = {x.strip() for x in args.only.split(",") if x.strip()}
    if only:
        cases = [c for c in cases if c["id"] in only]

    results = []
    for case in cases:
        q = case["question"]
        expected = set(case.get("expected_events") or [])
        terms = [t for t in (case.get("expected_terms") or []) if t]
        should_refuse = bool(case.get("should_refuse"))
        row = {"id": case["id"], "question": q, "intent": case.get("intent")}
        if args.llm:
            ans = answer_question(q)
            text = ans.get("answer") or ""
            lower = text.lower()
            missing_events = sorted(e for e in expected if e not in text)
            normalized = _norm(text)
            missing_terms = [t for t in terms if _norm(t) not in normalized]
            refused = any(
                s in text for s in ("확인할 수 없", "근거가 없", "제공된 근거로는", "없습니다")
            )
            ok = not missing_events and not missing_terms and (not should_refuse or refused)
            row.update({
                "ok": ok,
                "missing_events": missing_events,
                "missing_terms": missing_terms,
                "should_refuse": should_refuse,
                "refused": refused,
                "llm_ok": ans.get("llm_ok"),
                "elapsed_ms": ans.get("elapsed_ms"),
            })
        else:
            found = _retrieval_event_ids(q, args.top_k)
            if expected:
                hit = expected & found
                recall = len(hit) / len(expected)
                row.update({
                    "ok": recall == 1.0,
                    "recall": round(recall, 3),
                    "found_expected": sorted(hit),
                    "missing_expected": sorted(expected - found),
                })
            else:
                row.update({"ok": None, "note": "no expected events (aggregate/status)"})
        results.append(row)

    passed = sum(1 for r in results if r.get("ok") is True)
    failed = sum(1 for r in results if r.get("ok") is False)
    skipped = sum(1 for r in results if r.get("ok") is None)
    if args.json:
        print(json.dumps({"results": results, "passed": passed, "failed": failed,
                          "skipped": skipped}, ensure_ascii=False, indent=1))
    else:
        for r in results:
            mark = "PASS" if r.get("ok") is True else "FAIL" if r.get("ok") is False else "SKIP"
            extra = ""
            if "recall" in r:
                extra = f" recall={r['recall']} missing={r.get('missing_expected')}"
            elif r.get("ok") is False:
                extra = f" missing_events={r.get('missing_events')} missing_terms={r.get('missing_terms')}"
            print(f"[{mark}] {r['id']} {r['intent']}{extra}")
        print(f"\npassed={passed} failed={failed} skipped={skipped} total={len(results)}")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
