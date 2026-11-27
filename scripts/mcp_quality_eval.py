#!/usr/bin/env python3
"""Golden checks for MCP answer-level behavior.

Run against a live JSON instance::

    python3 scripts/mcp_quality_eval.py --url http://141.223.105.232:8766/mcp

The checks are deliberately about evidence discipline, not prose similarity:
- class vs root cause
- first interlock vs downstream KSU
- tau_reliable handling
- image panel metadata
- curated example selection
- cohort/notes routing
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from typing import Any


def call(url: str, tool: str, args: dict[str, Any], req_id: int = 1) -> dict[str, Any]:
    body = json.dumps({
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "tools/call",
        "params": {"name": tool, "arguments": args},
    }).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        payload = json.load(resp)
    result = payload.get("result") or {}
    content = result.get("content") or []
    text = content[0].get("text") if content else "{}"
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"_raw": text}


def check(name: str, ok: bool, detail: str = "") -> bool:
    print(("PASS " if ok else "FAIL ") + name + (f" — {detail}" if detail else ""))
    return ok


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://141.223.105.232:8766/mcp")
    args = ap.parse_args()
    url = args.url
    failures = 0

    a = call(url, "postmortem_explain_event", {"event_id": "20261013_212628"})
    first = ((a.get("primary_interlock") or {}).get("channel") or "")
    failures += not check(
        "CM3 ARC is not called a quench",
        a.get("root_cause", {}).get("fault_family") == "cm_arc"
        and first.startswith("ARC_CM3"),
        f"family={a.get('root_cause', {}).get('fault_family')} first={first}",
    )
    claims = " ".join(c.get("claim", "") for c in a.get("claim_checks") or [])
    failures += not check(
        "CM3 ARC has no QUEN evidence",
        "quench" in claims.lower(),
        "expected a forbidden quench claim",
    )

    b = call(url, "postmortem_explain_event", {"event_id": "20251205_014253"})
    first_b = ((b.get("primary_interlock") or {}).get("channel") or "")
    seq = b.get("sequence") or {}
    failures += not check(
        "CM1 quench first digital is QUEN_CM1",
        first_b.startswith("QUEN_CM1"),
        first_b,
    )
    failures += not check(
        "CM1 quench tau is marked unreliable",
        seq.get("tau_reliable") is False,
        str(seq.get("tau_reliable")),
    )
    claims_b = " ".join(c.get("claim", "") for c in b.get("claim_checks") or [])
    failures += not check(
        "CM1 quench forbids a single tau and KSU root cause",
        ("tau" in claims_b.lower() and "ksu" in claims_b.lower()),
        claims_b[:200],
    )
    imgs = b.get("images") or []
    failures += not check(
        "image metadata carries event_id/visible_channels",
        bool(imgs) and all(
            i.get("event_id") == "20251205_014253"
            and i.get("visible_channels")
            and i.get("caption_ko")
            for i in imgs
        ),
        f"n={len(imgs)}",
    )

    c = call(url, "postmortem_find_examples", {"topic": "cm_single_fault"})
    ids = [e.get("event_id") for e in c.get("examples") or []]
    failures += not check(
        "cm_single_fault uses ARC/flowmeter examples",
        "20261013_212628" in ids and "20260623_110438" in ids,
        str(ids),
    )

    d = call(url, "postmortem_analyze_cohort",
             {"fault_family": "cm_arc", "limit": 5})
    failures += not check(
        "cohort returns cm_arc events",
        d.get("available") is True and (d.get("total_filtered") or 0) > 0,
        f"total={d.get('total_filtered')}",
    )

    e = call(url, "postmortem_route_question",
             {"question": "CM 하나 fault로 빔덤프 메커니즘을 설명해줘"})
    failures += not check(
        "route recommends explain_event",
        "postmortem_explain_event" in (e.get("recommended_tools") or []),
        str(e.get("recommended_tools")),
    )

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
