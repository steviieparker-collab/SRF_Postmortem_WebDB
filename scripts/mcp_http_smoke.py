#!/usr/bin/env python3
"""Smoke test for the SRF Postmortem MCP server over Streamable HTTP.

Run with the MCP venv after starting the server:
    MCP_BEARER_TOKEN=... venv-mcp/bin/python scripts/mcp_http_smoke.py \
        http://127.0.0.1:8765/mcp

If no token is provided, only the public endpoints are checked.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os

import httpx2
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamable_http_client


async def main(url: str, token: str) -> int:
    base = url.rsplit("/mcp", 1)[0]
    async with httpx2.AsyncClient() as public:
        for path in ("/health", "/docs", "/capabilities"):
            r = await public.get(base + path)
            print(path, r.status_code, r.headers.get("content-type", ""))
        r = await public.post(base + "/mcp", json={})
        print("/mcp without auth", r.status_code)

    if not token:
        print("no token: skipping authenticated MCP calls")
        return 0

    headers = {"Authorization": f"Bearer {token}"}
    async with httpx2.AsyncClient(headers=headers) as client:
        async with streamable_http_client(url, http_client=client) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools = await session.list_tools()
                print("tools:", len(tools.tools))
                caps = await session.call_tool("postmortem_get_capabilities", {})
                print("capabilities:", caps.content[0].text[:160].replace("\n", " "))
                search = await session.call_tool(
                    "postmortem_search_events",
                    {"query": "2026년 KSU관련 빔덤프", "limit": 3},
                )
                data = json.loads(search.content[0].text)
                print("search:", data.get("returned"),
                      [x.get("event_id") for x in data.get("items", [])])
                event = await session.call_tool(
                    "postmortem_get_event", {"event_id": "20251103_115812"}
                )
                ev = json.loads(event.content[0].text)
                print("event:", ev.get("event", {}).get("event_id"),
                      "alarms:", len(ev.get("alarms", [])),
                      "pv_selection:", len(ev.get("pv_selection", [])))
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("url", nargs="?", default="http://127.0.0.1:8765/mcp")
    ap.add_argument("--token", default=os.getenv("MCP_BEARER_TOKEN", ""))
    args = ap.parse_args()
    raise SystemExit(asyncio.run(main(args.url, args.token)))
