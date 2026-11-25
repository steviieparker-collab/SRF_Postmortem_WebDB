#!/usr/bin/env python3
"""Smoke test for the SRF Postmortem MCP server (stdio transport).

Run with the MCP venv:
    venv-mcp/bin/python scripts/mcp_smoke.py
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

ROOT = Path(__file__).resolve().parent.parent


def _payload(result):
    content = getattr(result, "content", None) or []
    if not content:
        return None
    text = getattr(content[0], "text", None)
    if text is None:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


async def main() -> int:
    params = StdioServerParameters(
        command=sys.executable,
        args=[str(ROOT / "scripts" / "run_mcp.py"), "--transport", "stdio"],
        cwd=str(ROOT),
    )
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            print("tools:", ", ".join(t.name for t in tools.tools))
            print("tool description lengths:",
                  [(t.name, len(t.description or "")) for t in tools.tools])
            resources = await session.list_resources()
            print("resources:", ", ".join(str(r.uri) for r in resources.resources))
            prompts = await session.list_prompts()
            print("prompts:", ", ".join(p.name for p in prompts.prompts))
            caps_res = await session.read_resource("postmortem://capabilities")
            if getattr(caps_res, "contents", None):
                print("resource capabilities bytes:", len(caps_res.contents[0].text))
            prompt = await session.get_prompt(
                "postmortem_event_analysis",
                {"event_id": "20251103_115812"},
            )
            print("prompt messages:", len(getattr(prompt, "messages", []) or []))

            caps = _payload(await session.call_tool("postmortem_get_capabilities", {}))
            print("capabilities:",
                  json.dumps(caps.get("data", {}), ensure_ascii=False) if isinstance(caps, dict) else caps)

            search = _payload(await session.call_tool(
                "postmortem_search_events",
                {"query": "2026년 KSU관련 빔덤프", "limit": 5},
            ))
            print("search returned:", search.get("returned") if isinstance(search, dict) else search)
            if isinstance(search, dict):
                for item in search.get("items", [])[:3]:
                    print("  ", item.get("event_id"), item.get("fault_type"))

            filtered = _payload(await session.call_tool(
                "postmortem_search_events",
                {
                    "query": "KSU",
                    "year_min": 2024,
                    "year_max": 2026,
                    "fault_types": ["Case 3: RF_Interlock"],
                    "has_waveform": True,
                    "limit": 3,
                },
            ))
            if isinstance(filtered, dict):
                print("filtered:", filtered.get("returned"),
                      [x.get("event_id") for x in filtered.get("items", [])])

            event = _payload(await session.call_tool(
                "postmortem_get_event", {"event_id": "20251103_115812"}
            ))
            if isinstance(event, dict):
                print("event:", event.get("event", {}).get("event_id"),
                      "alarms:", len(event.get("alarms", [])),
                      "pv_selection:", len(event.get("pv_selection", [])))
                notes = (event.get("event") or {}).get("notes") or ""
                print("utf8_ok:", ("유량" in notes or "출구" in notes))
                sims = event.get("similar") or []
                if sims:
                    print("get_event similar keys:", sorted(sims[0].keys()))

            agg = _payload(await session.call_tool(
                "postmortem_aggregate_events", {"group_by": "fault_type"}
            ))
            if isinstance(agg, dict):
                print("aggregate total:", agg.get("total"), "top:", (agg.get("rows") or [])[:2])

            agg2 = _payload(await session.call_tool(
                "postmortem_aggregate_events",
                {"group_by": "year,beam_time", "include_percent": True},
            ))
            if isinstance(agg2, dict):
                print("agg composite:", agg2.get("group_by"),
                      (agg2.get("rows") or [])[:2])

            status = _payload(await session.call_tool("postmortem_get_alarm_status", {}))
            if isinstance(status, dict):
                mon = status.get("monitor") or {}
                print("monitor:", mon.get("running"), mon.get("last_heartbeat"))

            catalog = _payload(await session.call_tool(
                "postmortem_search_alarm_catalog",
                {"query": "water flow", "subsystem": "KSU3", "limit": 3},
            ))
            if isinstance(catalog, dict):
                print("catalog:", [(x.get("pv"), x.get("graph_pv")) for x in catalog.get("items", [])])

            resolved = _payload(await session.call_tool(
                "postmortem_resolve_event",
                {"date": "2025-11-03", "subsystem": "KSU3"},
            ))
            if isinstance(resolved, dict):
                print("resolved:", [x.get("event_id") for x in resolved.get("items", [])])

            series = _payload(await session.call_tool(
                "postmortem_get_pv_series",
                {"event_id": "20241111_044552", "pv": "SR:RF:CRM1:BCL03"},
            ))
            if isinstance(series, dict):
                print("pv_series:", series.get("available"), series.get("quality"),
                      (series.get("summary") or {}).get("max_deviation_pct"))

            sim = _payload(await session.call_tool(
                "postmortem_similar_events",
                {"event_id": "20251103_115812", "limit": 3},
            ))
            if isinstance(sim, dict) and sim.get("items"):
                print("similar keys:", sorted(sim["items"][0].keys()))

            wave = _payload(await session.call_tool(
                "postmortem_analyze_waveform", {"event_id": "20240907_025313"}
            ))
            if isinstance(wave, dict):
                wf = wave.get("waveform") or {}
                print("waveform:", wave.get("available"),
                      (wf.get("t0") or {}).get("value_s"),
                      wf.get("quench_unit"),
                      (wf.get("beam_fall") or {}).get("fall_90_10_s"),
                      "digital:", len(wf.get("digital") or {}))

            missing = _payload(await session.call_tool(
                "postmortem_analyze_waveform", {"event_id": "20130709_050000"}
            ))
            print("waveform_no_parquet:", missing.get("available") if isinstance(missing, dict) else missing)

            wsim = _payload(await session.call_tool(
                "postmortem_find_similar_waveforms",
                {"event_id": "20251205_014253", "top_k": 3},
            ))
            if isinstance(wsim, dict):
                print("waveform_similar:", wsim.get("available"),
                      wsim.get("returned"),
                      [(i.get("event_id"), i.get("similarity"))
                       for i in (wsim.get("items") or [])])

            reports = _payload(await session.call_tool(
                "postmortem_search_reports",
                {"query": "켄치 발생 시 RF 반사파가 증가한 원인", "top_k": 3},
            ))
            if isinstance(reports, dict):
                print("reports:", reports.get("available"),
                      "dense=", reports.get("dense_available"),
                      "returned=", reports.get("returned"),
                      [(i.get("event_id"), (i.get("title") or "")[:24])
                       for i in (reports.get("items") or [])])

            nohit = _payload(await session.call_tool(
                "postmortem_search_reports",
                {"query": "존재하지않는키워드zzz", "top_k": 3},
            ))
            if isinstance(nohit, dict):
                print("reports_nohit:", nohit.get("returned"),
                      nohit.get("suppressed"))

            img = _payload(await session.call_tool(
                "postmortem_search_reports",
                {"query": "KLY Ion Pump 알람", "top_k": 4},
            ))
            if isinstance(img, dict):
                items = img.get("items") or []
                print("reports_image:", img.get("returned"),
                      "images_indexed=", img.get("images_indexed"),
                      "kinds=", [i.get("kind") for i in items],
                      "caption=", ((items[0].get("caption") or "")[:50]
                                   if items else ""))

            quench = _payload(await session.call_tool(
                "postmortem_analyze_quench", {"event_id": "20240907_025313"}
            ))
            if isinstance(quench, dict):
                series = (quench.get("pv") or {}).get("series") or {}
                print("quench:", quench.get("available"),
                      "bcl03_delta=", (series.get("BCL03") or {}).get("delta"),
                      "reports=", (quench.get("reports") or {}).get("returned"),
                      "highlights=", len(quench.get("highlights") or []))

            qcmp = _payload(await session.call_tool(
                "postmortem_compare_quench_events",
                {"event_ids": ["20240907_025313", "20241002_171719",
                               "20251205_014253"]},
            ))
            if isinstance(qcmp, dict):
                print("quench_compare:", qcmp.get("available"),
                      "with_waveform=", qcmp.get("with_waveform"),
                      "with_reports=", qcmp.get("with_reports"),
                      "rows=", len(qcmp.get("rows") or []))
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
