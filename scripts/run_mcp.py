#!/usr/bin/env python3
"""Run the SRF Postmortem MCP server.

Examples:
    venv-mcp/bin/python scripts/run_mcp.py --transport stdio
    venv-mcp/bin/python scripts/run_mcp.py --transport streamable-http \
        --host 127.0.0.1 --port 8765
    venv-mcp/bin/python scripts/run_mcp.py --print-capabilities
"""

from __future__ import annotations

import argparse
import base64
import hmac
import ipaddress
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.mcp_server.server import (  # noqa: E402
    TOOL_DESCRIPTIONS,
    postmortem_get_capabilities,
    server,
)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--transport",
        choices=["stdio", "streamable-http", "sse"],
        default="stdio",
    )
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--log-level", default="info")
    ap.add_argument("--allowed-host", action="append", default=[])
    ap.add_argument("--allowed-origin", action="append", default=[])
    ap.add_argument("--session-idle-timeout", type=float, default=1800.0)
    ap.add_argument("--max-sessions", type=int, default=10000)
    ap.add_argument("--json-response", action="store_true",
                    help="return JSON instead of SSE where supported")
    ap.add_argument(
        "--stateless",
        action="store_true",
        help=(
            "stateless Streamable HTTP: every POST is independent and no "
            "Mcp-Session-Id header is required (recommended for simple "
            "JSON-RPC/HTTP agents)"
        ),
    )
    ap.add_argument("--public-url", default="https://mcp.pls-srf.org/mcp")
    ap.add_argument("--tailscale-url", default="http://100.76.107.91:8765/mcp")
    ap.add_argument("--lan-url", default="http://141.223.105.232:8765/mcp")
    ap.add_argument("--print-capabilities", action="store_true")
    args = ap.parse_args()

    if args.print_capabilities:
        print(json.dumps(postmortem_get_capabilities(), ensure_ascii=False, indent=1))
        return 0

    if args.transport == "stdio":
        server.run(transport="stdio")
        return 0

    import html
    import uvicorn
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.responses import HTMLResponse, JSONResponse, Response

    from mcp.server.transport_security import TransportSecuritySettings

    allowed_hosts = args.allowed_host or [
        "127.0.0.1", f"127.0.0.1:{args.port}",
        "localhost", f"localhost:{args.port}",
        "mcp.pls-srf.org",
    ]
    allowed_origins = args.allowed_origin or [
        "http://127.0.0.1:8765", "http://localhost:8765",
        "https://mcp.pls-srf.org",
    ]
    security = TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=allowed_hosts,
        allowed_origins=allowed_origins,
    )
    if args.transport == "streamable-http":
        app = server.streamable_http_app(
            transport_security=security,
            session_idle_timeout=args.session_idle_timeout,
            max_sessions=args.max_sessions,
            json_response=args.json_response,
            stateless_http=args.stateless,
        )
    else:
        app = server.sse_app(transport_security=security)

    bearer = os.getenv("MCP_BEARER_TOKEN", "").strip()
    if bearer and any(ord(ch) > 127 for ch in bearer):
        raise SystemExit(
            "MCP_BEARER_TOKEN must be ASCII (HTTP headers are Latin-1). "
            "Check for Unicode ellipsis or smart quotes in the token."
        )
    access_password = os.getenv("MCP_ACCESS_PASSWORD", "").strip()
    if access_password and any(ord(ch) > 127 for ch in access_password):
        raise SystemExit(
            "MCP_ACCESS_PASSWORD must be ASCII (HTTP headers are Latin-1)."
        )
    allowed_cidrs = [
        x.strip()
        for x in os.getenv("MCP_ALLOWED_CIDRS", "").split(",")
        if x.strip()
    ]
    try:
        networks = [ipaddress.ip_network(x, strict=False) for x in allowed_cidrs]
    except ValueError as e:
        raise SystemExit(f"invalid MCP_ALLOWED_CIDRS: {e}")

    passwordless_cidrs = [
        x.strip()
        for x in os.getenv("MCP_PASSWORDLESS_CIDRS", "").split(",")
        if x.strip()
    ]
    try:
        passwordless_networks = [
            ipaddress.ip_network(x, strict=False) for x in passwordless_cidrs
        ]
    except ValueError as e:
        raise SystemExit(f"invalid MCP_PASSWORDLESS_CIDRS: {e}")

    def client_ip(request) -> str:
        return request.client.host if request.client else ""

    def ip_allowed(ip: str) -> bool:
        if not networks:
            return True
        try:
            addr = ipaddress.ip_address(ip)
        except ValueError:
            return False
        return any(addr in net for net in networks)

    # Traffic that arrives through Cloudflare Tunnel or a reverse proxy is
    # indistinguishable from localhost (TCP peer = 127.0.0.1), so it must always
    # authenticate. Never grant passwordless access to proxied requests.
    PROXY_HEADERS = (
        "cf-connecting-ip",
        "cf-ray",
        "x-forwarded-for",
        "x-real-ip",
        "true-client-ip",
        "forwarded",
    )

    def passwordless(request) -> bool:
        if not passwordless_networks:
            return False
        if any(request.headers.get(h) for h in PROXY_HEADERS):
            return False
        ip = client_ip(request)
        try:
            addr = ipaddress.ip_address(ip)
        except ValueError:
            return False
        return any(addr in net for net in passwordless_networks)

    def password_ok(request) -> bool:
        if not access_password:
            return False
        supplied = request.headers.get("x-mcp-password", "")
        if supplied and hmac.compare_digest(supplied, access_password):
            return True
        auth = request.headers.get("authorization", "")
        if auth.lower().startswith("basic "):
            try:
                decoded = base64.b64decode(
                    auth.split(" ", 1)[1], validate=True
                ).decode("utf-8")
                _, _, supplied_pw = decoded.partition(":")
                if hmac.compare_digest(supplied_pw, access_password):
                    return True
            except (ValueError, UnicodeDecodeError):
                pass
        return False

    public_paths = {"/health", "/docs", "/capabilities", "/connection"}

    class AccessControlMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            path = request.url.path
            ip = client_ip(request)
            # Localhost health checks always work (monitoring).
            if path == "/health" and ip in {"127.0.0.1", "::1"}:
                return await call_next(request)
            if not ip_allowed(ip):
                return JSONResponse(
                    {
                        "error": "forbidden",
                        "reason": "source_ip_not_allowed",
                        "allowed_cidrs": allowed_cidrs,
                    },
                    status_code=403,
                )
            if path in public_paths:
                return await call_next(request)
            # Stateless mode has no server-initiated messages, so a GET would
            # open an SSE stream that never ends and hang simple clients.
            if (
                args.stateless
                and path == "/mcp"
                and request.method in {"GET", "HEAD"}
            ):
                return JSONResponse(
                    {
                        "error": "method_not_allowed",
                        "reason": "stateless_mode_has_no_stream",
                        "hint": (
                            "This endpoint is stateless. POST JSON-RPC requests "
                            "directly; GET SSE streams and server-initiated "
                            "notifications are not supported."
                        ),
                        "allowed_methods": ["POST"],
                    },
                    status_code=405,
                    headers={"Allow": "POST"},
                )
            # Trusted internal networks: no credential needed for direct clients.
            if passwordless(request):
                return await call_next(request)
            if bearer and request.headers.get("authorization", "") == f"Bearer {bearer}":
                return await call_next(request)
            if password_ok(request):
                return await call_next(request)
            if not bearer and not access_password:
                return await call_next(request)
            return JSONResponse(
                {
                    "error": "unauthorized",
                    "reason": "missing_or_invalid_credentials",
                    "hint": (
                        "Use Authorization: Bearer <token> or "
                        "X-MCP-Password: <password>."
                    ),
                    "allowed_methods": ["bearer", "x-mcp-password", "basic"],
                },
                status_code=401,
                headers={"WWW-Authenticate": 'Bearer realm="srf-postmortem"'},
            )

    class UTF8Middleware(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            response = await call_next(request)
            content_type = response.headers.get("content-type", "")
            if (
                content_type.startswith("application/json")
                or content_type.startswith("text/event-stream")
                or content_type.startswith("text/html")
            ) and "charset" not in content_type:
                response.headers["content-type"] = content_type + "; charset=utf-8"
            return response

    class SessionErrorMiddleware(BaseHTTPMiddleware):
        """Turn opaque Streamable-HTTP session errors into actionable hints.

        The SDK reports a missing ``Mcp-Session-Id`` HTTP header as a
        JSON-RPC -32600 "Bad Request: Missing session ID". That message makes
        clients look for a ``sessionID`` field inside ``params``, which is not
        a thing in the MCP spec. Keep the status code and error code intact,
        but add an explicit hint in the body and in ``X-MCP-Hint``.
        """

        async def dispatch(self, request, call_next):
            response = await call_next(request)
            if request.url.path != "/mcp" or response.status_code not in (400, 404):
                return response

            body = b""
            async for chunk in response.body_iterator:
                body += (
                    bytes(chunk)
                    if isinstance(chunk, (bytes, bytearray, memoryview))
                    else chunk.encode()
                )
            text = body.decode("utf-8", errors="replace")
            if "Missing session ID" in text:
                hint = (
                    "HTTP transport header error, not a JSON-RPC params field: "
                    "this endpoint is stateful and expects the Mcp-Session-Id "
                    "header returned by initialize on every request. Either echo "
                    "that header, or use a stateless endpoint (:8766/mcp, or a "
                    "server started with --stateless)."
                )
                expired = False
            elif "Invalid or expired session ID" in text:
                hint = (
                    "The Mcp-Session-Id is unknown or expired. Call initialize "
                    "again and reuse the new Mcp-Session-Id header. Stateless "
                    "endpoints (:8766/mcp) have no sessions at all."
                )
                expired = True
            else:
                return Response(
                    content=body,
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    media_type=response.headers.get("content-type"),
                )

            payload = None
            try:
                payload = json.loads(text)
            except ValueError:
                pass
            if isinstance(payload, dict) and isinstance(payload.get("error"), dict):
                payload["error"]["hint"] = hint
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            elif not text:
                body = json.dumps(
                    {"error": {"code": -32600, "message": text or "Bad Request",
                               "hint": hint}},
                    ensure_ascii=False,
                ).encode("utf-8")

            headers = dict(response.headers)
            headers.pop("content-length", None)
            headers.pop("content-type", None)
            headers["X-MCP-Hint"] = hint
            if expired:
                headers["X-MCP-Session-Expired"] = "true"
            return Response(
                content=body,
                status_code=response.status_code,
                headers=headers,
                media_type="application/json",
            )

    app.add_middleware(AccessControlMiddleware)
    app.add_middleware(UTF8Middleware)
    app.add_middleware(SessionErrorMiddleware)

    async def health(request):
        return JSONResponse({"ok": True, "server": "srf-postmortem"})

    async def capabilities(request):
        return JSONResponse(postmortem_get_capabilities())

    async def connection(request):
        return JSONResponse(
            {
                "server": "srf-postmortem",
                "transport": "streamable-http",
                "json_response": bool(args.json_response),
                "urls": {
                    "public": args.public_url,
                    "tailscale": args.tailscale_url,
                    "lan": args.lan_url,
                    "local": f"http://127.0.0.1:{args.port}/mcp",
                },
                "network": {
                    "allowed_cidrs": allowed_cidrs,
                    "passwordless_cidrs": passwordless_cidrs,
                    "client_ip": client_ip(request),
                    "client_is_trusted_direct": passwordless(request),
                },
                "auth": {
                    "scheme": "passwordless on trusted networks; otherwise Bearer or X-MCP-Password",
                    "required": bool(
                        (bearer or access_password) and not passwordless(request)
                    ),
                    "passwordless": passwordless(request),
                    "methods": ["bearer", "x-mcp-password", "basic"],
                    "headers": {
                        "bearer": "Authorization: Bearer <token>",
                        "password": "X-MCP-Password: <password>",
                        "basic": "Authorization: Basic <base64(user:password)>",
                    },
                    "token_env": "MCP_BEARER_TOKEN",
                    "password_env": "MCP_ACCESS_PASSWORD",
                    "passwordless_env": "MCP_PASSWORDLESS_CIDRS",
                    "hint": (
                        "Direct clients inside MCP_PASSWORDLESS_CIDRS need no "
                        "credentials. All other clients must use an ASCII "
                        "Bearer token or the shared access password. Do not "
                        "copy Unicode ellipsis characters."
                    ),
                },
                "encoding": "utf-8",
                "session": {
                    "stateless": bool(args.stateless),
                    "requires_session_header": not args.stateless,
                    "session_header": None if args.stateless else "Mcp-Session-Id",
                    "idle_timeout_sec": (
                        None if args.stateless else args.session_idle_timeout
                    ),
                    "reinitialize_on_error": not args.stateless,
                    "error_header": "X-MCP-Session-Expired",
                    "note": (
                        "Stateless mode: every POST is independent; do not send "
                        "Mcp-Session-Id and do not look for a sessionID field "
                        "in params."
                        if args.stateless
                        else "Stateful mode: echo the Mcp-Session-Id response "
                        "header from initialize on every request. The session "
                        "ID is an HTTP header, not a JSON-RPC params field."
                    ),
                },
                "endpoints": {
                    "health": "/health",
                    "docs": "/docs",
                    "capabilities": "/capabilities",
                    "mcp": "/mcp",
                },
                "notes": [
                    "Use the MCP SDK or an SSE parser; do not call response.json() "
                    "on a text/event-stream response.",
                    "For clients that cannot parse SSE, restart with --json-response.",
                ],
            }
        )

    async def docs(request):
        rows = "".join(
            f"<tr><td><code>{html.escape(name)}</code></td>"
            f"<td>{html.escape(desc)}</td></tr>"
            for name, desc in TOOL_DESCRIPTIONS.items()
        )
        body = f"""<!doctype html>
<html lang="ko"><head><meta charset="utf-8">
<title>SRF Postmortem MCP</title>
<style>
body{{font-family:system-ui,sans-serif;max-width:1100px;margin:2rem auto;padding:0 1rem;background:#0d1117;color:#e6edf3}}
code{{color:#79c0ff}} a{{color:#58a6ff}} table{{border-collapse:collapse;width:100%}}
th,td{{border:1px solid #30363d;padding:.45rem;text-align:left;vertical-align:top}}
th{{background:#161b22}} .muted{{color:#8b949e}}
</style></head><body>
<h1>SRF Postmortem MCP</h1>
<p class="muted">MCP endpoint: <code>/mcp</code> (no auth on trusted internal
networks, otherwise auth required). Public:
<code>/health</code>, <code>/connection</code>, <code>/capabilities</code>, <code>/docs</code>.</p>
<h2>Connection</h2>
<ul>
<li>Tailscale: <code>{html.escape(args.tailscale_url)}</code></li>
<li>LAN: <code>{html.escape(args.lan_url)}</code></li>
<li>Public (planned): <code>{html.escape(args.public_url)}</code></li>
<li>Local: <code>http://127.0.0.1:{args.port}/mcp</code></li>
</ul>
<h2>Auth</h2>
<p>Direct clients inside the trusted internal networks (<code>MCP_PASSWORDLESS_CIDRS</code>:
<code>{html.escape(", ".join(passwordless_cidrs) or "(none)")}</code>) need
<strong>no password</strong>. Requests that arrive through Cloudflare Tunnel or
another reverse proxy always need credentials.</p>
<p>Elsewhere use <code>Authorization: Bearer &lt;ASCII token&gt;</code> or
<code>X-MCP-Password: &lt;password&gt;</code> (shared internal password).
Basic auth is also accepted. Never copy Unicode ellipsis characters; HTTP
headers are Latin-1 and will fail.</p>
<p>Allowed source CIDRs: <code>{html.escape(", ".join(allowed_cidrs) or "(all)")}</code></p>
<h2>Session / transport</h2>
<p>Mode: <strong>{'stateless' if args.stateless else 'stateful'}</strong>.
{'Each POST is independent; no <code>Mcp-Session-Id</code> header is needed.'
 if args.stateless else
 'Echo the <code>Mcp-Session-Id</code> response header returned by '
 '<code>initialize</code> on every subsequent request.'}
The session ID is an <strong>HTTP header</strong>, never a <code>sessionID</code>
field inside JSON-RPC <code>params</code>.</p>
<h2>Client notes</h2>
<ul>
<li>Use the MCP SDK or an SSE parser. Do not call <code>response.json()</code>
on a <code>text/event-stream</code> response.</li>
<li>Decode responses as UTF-8.</li>
<li>For simple clients that cannot parse SSE, use the <code>--json-response</code>
instance (port 8766).</li>
{('<li>Stateless mode: use <code>POST</code> only. <code>GET /mcp</code> returns '
  '405 because there is no server-initiated stream.</li>') if args.stateless else ''}
</ul>
<p>Tools use the <code>postmortem_</code> namespace. Citations:
<code>[E:event_id]</code>, <code>[A:event_id/PV]</code>,
<code>[PVS:event_id/PV]</code>, <code>[PV:PV]</code>, <code>[FT:fault_type]</code>.</p>
<h2>Tools</h2>
<table><thead><tr><th>Tool</th><th>Description</th></tr></thead>
<tbody>{rows}</tbody></table>
<h2>Resources</h2>
<ul>
<li><code>postmortem://capabilities</code></li>
<li><code>postmortem://schema</code></li>
<li><code>postmortem://fault-types</code></li>
<li><code>postmortem://glossary</code></li>
</ul>
<p class="muted">Responses are UTF-8 JSON. The MCP server is read-only.</p>
</body></html>"""
        return HTMLResponse(body, media_type="text/html; charset=utf-8")

    app.add_route("/health", health, methods=["GET"])
    app.add_route("/connection", connection, methods=["GET"])
    app.add_route("/capabilities", capabilities, methods=["GET"])
    app.add_route("/docs", docs, methods=["GET"])

    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)
    return 0


if __name__ == "__main__":
    sys.exit(main())
