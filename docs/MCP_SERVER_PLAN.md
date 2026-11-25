# SRF Postmortem MCP Server

작성일: 2026-11-18  
상태: **stdio / Streamable HTTP 동작 확인**, Cloudflare Tunnel 연결 전

---

## 1. 목적

빔덤프 Postmortem DB를 **MCP 서버 하나**로 제공한다.  
사용자는 각자 쓰는 MCP 지원 LLM(Claude Desktop/Code, Cursor 등)에 연결해
자기 모델로 질의한다. 따라서 서버 운영자는 **LLM 토큰 비용을 부담하지 않는다.**

- 단일 주소: **`mcp.pls-srf.org`**
- Tool prefix: `postmortem_*`
- 향후 다른 서비스(진공, RF 등)를 추가해도 같은 호스트에서 namespace로 확장 가능

---

## 2. 아키텍처

```
[Claude Desktop / Claude Code / 기타 MCP client]
        │  Streamable HTTP (Cloudflare Tunnel)
        ▼
[mcp.pls-srf.org]
        │
        ▼
[srf-hprf: MCP server (venv-mcp)]
  - MCPServer (mcp==2.3.0)
  - read-only SQLite
  - tools / resources / prompts
  - 내부 검색: SQL filter + FTS5 BM25
  - (선택) BGE-M3 embedding / reranker on GPU PC
        │
        ▼
[events.db / data/rag/rag.sqlite3 / Archiver]
```

- MCP 서버 자체는 GPU 불필요.
- dense embedding을 추가할 때만 query-time GPU/CPU 추론이 필요하다.
- 현재는 FTS5 + SQL만 사용.

---

## 3. Tools

| Tool | 역할 |
|---|---|
| `postmortem_get_capabilities` | 데이터 범위, 인덱스 신선도, 제약 |
| `postmortem_search_events` | 하이브리드 이벤트 검색 (query + filters) |
| `postmortem_get_event` | 이벤트 전체 컨텍스트 (alarms/PV/attachments/similar/waveform) |
| `postmortem_aggregate_events` | 연도/월/fault/beam time별 집계 |
| `postmortem_similar_events` | event_links 기반 유사 이벤트 |
| `postmortem_search_alarm_catalog` | PV 카탈로그 검색 |
| `postmortem_get_alarm_status` | 모니터 상태/heartbeat/last_state |
| `postmortem_resolve_event` | 날짜+시스템 → 이벤트 ID |
| `postmortem_list_fault_types` | fault taxonomy + 건수 |
| `postmortem_list_beam_times` | 빔타임 목록 + 건수 |
| `postmortem_get_pv_series` | Archiver 시계열 요약 (min/max/deviation) |

### 자연어 질문 해석 (2026-11-18 수정)

- **시점 표현**: `최근`·`요즘`·`최신`·`근래`는 검색어가 아니라 **최신순 정렬**
  의도로 처리한다(`sort="recent"`). 이 단어들을 텍스트 필터로 넣으면
  `notes`에 해당 단어가 없어 결과가 0건이 되고, 이후 FTS가 일반 단어
  (`빔덤프` 등)만으로 아무 이벤트나 반환하던 문제를 고쳤다.
  `postmortem_search_events`에 `sort` 파라미터를 직접 줄 수도 있다
  (`"relevance"` 기본, `"recent"`).
- **집계 질문**: `많이 발생한`, `가장 많이`, `월별`, `달별` 등을 집계 의도로
  인식한다. 검색 도구를 잘못 호출한 경우 응답에 `hint`가 붙어
  `postmortem_aggregate_events`를 쓰라고 안내한다.
- **월 단위 집계**: `group_by="month"`가 추가됐다(`YYYY-MM`).
  예: "2025년 RF 인터락이 가장 많은 달" → `group_by="month"`, `year=2025`.
- **한글 fault 명칭**: `RF 인터락`/`RF 인터록`, `빔로스`, `캐비티 퀜치` 등을
  DB의 `RF_Interlock`/`Beam_loss`/`Cavity_quench`로 매핑한다.
  이전에는 한글 명칭이 텍스트 AND 조건으로 남아 0건이 되곤 했다.

### 기본 필터: MS 제외 + 수동 분류 우선 (2026-11-24)

MCP의 목록/집계 도구는 다음 두 규칙을 기본 적용한다.

1. **MS(Machine Study) 빔타임 제외** — `user_beam_time`에 `MS`가 들어간
   빔타임(예: `2026-01MS`, `2026-LLRF-MS`)의 이벤트는 기본적으로 목록/집계에서
   빠진다. 웹 UI의 `hide_ms` 옵션과 같은 기준이다.
   - 대상: `postmortem_search_events`, `postmortem_aggregate_events`,
     `postmortem_list_fault_types`, `postmortem_list_beam_times`
   - 파라미터: `hide_ms` (기본 `true`). MS 기간까지 포함하려면 `hide_ms=false`.
   - `postmortem_get_event`/`postmortem_analyze_*`는 이벤트 ID를 직접 받으므로
     적용하지 않는다(MS 이벤트도 개별 조회 가능).
2. **수동 분류 우선** — `fault_type` 필터·집계는
   `COALESCE(NULLIF(user_fault_type,''), fault_type)`을 쓴다. 사용자가 웹에서
   지정한 case가 자동 분류를 덮어쓴다.
   - 이벤트 item에는 `fault_type`(자동)과 `effective_fault_type`(수동 우선)이
     함께 반환된다.
   - `fault_types` 포함 필터, `group_by="fault_type"`, `postmortem_list_fault_types`
     카운트가 모두 같은 식을 쓴다.

검증 예 (2026년):

| 조건 | 건수 | 상위 |
|---|---:|---|
| 기본 (`hide_ms=true`) | **38** | RF_Interlock 11 (28.9%), MPS/Control/Beam line 각 6 |
| `hide_ms=false` | **123** | MIS 52 (42.3%), RF_Interlock 18, RF_path_Forward 15 |

```bash
curl -sS http://141.223.105.232:8766/mcp \
  -H 'Content-Type: application/json' -H 'Accept: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"postmortem_aggregate_events","arguments":{"group_by":"fault_type","year":2026,"include_percent":true}}}'
```

`postmortem_get_capabilities`의 `query_defaults`에도 이 규칙이 노출된다.

### 인용 형식

```
[E:event_id]              이벤트
[A:event_id/PV]           event_alarms에 기록된 알람
[PVS:event_id/PV]         event_pv_selection에 수동 등록된 PV
[PV:PV]                   alarm_pv_catalog 매핑
[FT:fault_type]           fault type
```

---

## 4. Resources / Prompts

### Resources
```
postmortem://capabilities
postmortem://schema
postmortem://fault-types
postmortem://glossary
```

### Prompts
```
postmortem_event_analysis(event_id)
postmortem_trend_analysis(subsystem, year_from, year_to)
postmortem_compare_events(event_a, event_b)
postmortem_pv_investigation(event_id, pv)
```

---

## 5. 실행

### stdio (로컬 클라이언트)
```bash
venv-mcp/bin/python scripts/run_mcp.py --transport stdio
```

### Streamable HTTP (원격)
```bash
venv-mcp/bin/python scripts/run_mcp.py \
  --transport streamable-http \
  --host 0.0.0.0 --port 8765 \
  --allowed-host 127.0.0.1:8765 \
  --allowed-host localhost:8765 \
  --allowed-host 100.76.107.91:8765 \
  --allowed-host 141.223.105.232:8765 \
  --allowed-host mcp.pls-srf.org
```

현재 서버에서는 tmux `srf-mcp` 세션으로 `0.0.0.0:8765`에 떠 있다.
Tailscale `http://100.76.107.91:8765/mcp`, LAN `http://141.223.105.232:8765/mcp`로 접근한다.
`/mcp`는 기본적으로 `MCP_BEARER_TOKEN` 인증이 필요하고, `/health`, `/docs`,
`/capabilities`는 public이다. 단, 사내망 직접 접속은 `MCP_PASSWORDLESS_CIDRS`
설정 시 비밀번호 없이 허용된다(아래 "네트워크 제한" 절).
Cloudflare Tunnel은 `http://localhost:8765`로 연결하면 된다.

### Bearer token (선택)
```bash
MCP_BEARER_TOKEN=... venv-mcp/bin/python scripts/run_mcp.py ...
```
- `/health`, `/docs`, `/capabilities`는 인증 없이 200.
- `/mcp`는 `Authorization: Bearer ...` 필요.
- Cloudflare Access를 쓰면 앱 토큰은 생략 가능.

> **토큰은 `.env`(git-ignored)에만 둔다.** 문서/채팅/이슈에 복사하지 않는다.
> 테스트 토큰은 주기적으로 rotate하고, 운영에서는 Cloudflare Access/Service Token을 권장한다.

### 네트워크 제한 + 사내망 무비밀번호 (사내 전용)

`.env`:
```
MCP_ALLOWED_CIDRS=141.223.105.0/24,127.0.0.1/32,::1/128
MCP_PASSWORDLESS_CIDRS=141.223.105.0/24
MCP_ACCESS_PASSWORD=<shared password>
MCP_BEARER_TOKEN=<admin token>
```

- `MCP_ALLOWED_CIDRS`에 없는 IP는 **403 source_ip_not_allowed**.
- `MCP_PASSWORDLESS_CIDRS` 안에서 **직접** 접속한 클라이언트는 `/mcp`에
  **비밀번호 없이** 접근한다(사내망 기본값: `141.223.105.0/24`).
- 그 밖의 클라이언트는 아래 중 하나로 인증:
  - `Authorization: Bearer <MCP_BEARER_TOKEN>`
  - `X-MCP-Password: <MCP_ACCESS_PASSWORD>`
  - `Authorization: Basic base64(user:password)`
- **프록시 경유 요청은 절대 무비밀번호로 통과시키지 않는다.** Cloudflare Tunnel
  (`mcp.pls-srf.org`)은 서버 입장에서 peer가 `127.0.0.1`이므로, `CF-Connecting-IP`,
  `X-Forwarded-For`, `Forwarded` 등 전달 헤더가 있으면 무비밀번호 대상에서 제외한다.
  따라서 공개 도메인은 계속 토큰/비밀번호가 필요하다.
- `/health`는 localhost에서 항상 허용, `/docs`·`/capabilities`·`/connection`은
  **허용망 안에서만** public.
- 기본값은 **LAN `141.223.105.0/24`만 허용**한다. Tailscale `100.x`는 차단된다.
  필요하면 `MCP_ALLOWED_CIDRS`(그리고 원하면 `MCP_PASSWORDLESS_CIDRS`)에
  `100.76.107.0/24`(또는 `100.64.0.0/10`)를 추가한다.
- `MCP_PASSWORDLESS_CIDRS`를 비우면 모든 요청에 인증이 필요하다(기존 동작).
- 공유 비밀번호는 개별 폐기가 불가능하다. 사용자별 토큰이 필요하면
  `/token` 발급 페이지(향후) 또는 Cloudflare Access Service Token을 사용한다.
- LAN에서 HTTP를 쓰면 비밀번호가 암호화되지 않는다. 민감하면 HTTPS/Tailscale을 사용한다.
- 민감도가 낮은 읽기 전용 조회 서비스이므로 사내망 무비밀번호를 허용하되,
  서버를 인터넷에 직접 노출(`0.0.0.0` + 포트포워딩)하지 않는다.

### 연결 정보 endpoint
`GET /connection` (public)에서 사용 가능한 URL, 인증 방식, 인코딩,
SSE/JSON 응답 안내를 JSON으로 확인할 수 있다.

### 세션 없는(stateless) 모드 — 단순 HTTP 에이전트용
```bash
venv-mcp/bin/python scripts/run_mcp.py \
  --transport streamable-http --host 0.0.0.0 --port 8766 \
  --json-response --stateless ...
```
- `--stateless`면 **매 POST가 독립**이다. `initialize`를 먼저 보낼 필요도,
  `Mcp-Session-Id` 헤더를 되돌려 보낼 필요도 없다.
- `initialize` 뒤 `tools/list`를 그냥 POST하면 200이 온다.
- `GET /mcp`(서버→클라이언트 스트림)는 지원하지 않는다. 이 서버 도구들은
  모두 요청/응답형이라 문제 없다. stateless 모드에서 `GET`/`HEAD /mcp`는
  스트림이 열린 채 멈추지 않도록 **즉시 405**(`allowed_methods: ["POST"]`)로 응답한다.
- SDK 클라이언트, Claude Code, `mcp-remote` 모두 stateless에서 동작한다.

### 세션 오류 메시지에 대하여 (중요)
stateful 모드에서 세션 헤더 없이 요청하면 SDK가 다음을 반환한다.
```json
{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"Bad Request: Missing session ID"}}
```
- 이 메시지는 **JSON-RPC `params`에 `sessionID` 필드가 없다는 뜻이 아니다.**
  MCP 스펙에 그런 필드는 없다. **HTTP 헤더 `Mcp-Session-Id`가 없다**는 뜻이다.
- 서버는 이 응답에 `X-MCP-Hint` 헤더와 `error.hint` 필드를 덧붙여 원인을 명시한다.
- 해결: `initialize` 응답의 `Mcp-Session-Id` 헤더를 이후 모든 요청에 그대로 넣거나,
  `--stateless` 인스턴스(8766)를 사용한다.

현재 서버에는 두 인스턴스가 떠 있다.

| 세션 | 포트 | 응답 | 세션 | URL |
|---|---:|---|---|---|
| `srf-mcp` | 8765 | SSE (기본) | **stateless** (`--stateless`, 헤더 불필요) | `http://141.223.105.232:8765/mcp` |
| `srf-mcp-json` | 8766 | JSON (`--json-response`) | **stateless** (헤더 불필요) | `http://141.223.105.232:8766/mcp` |

> 이전 문서에 8765가 stateful로 적혀 있었으나, 현재 실행 명령은 두 인스턴스 모두
> `--stateless`다. `/connection`의 `session.stateless`가 실제 값을 보여준다.

### raw HTTP 호출 예 (stateless)

`tools/list`와 `tools/call`은 `initialize` 없이도 각 POST가 독립적으로 동작한다.
도구 이름을 JSON-RPC `method`로 쓰면 `-32601 Method not found`가 난다.
반드시 `method: "tools/call"` + `params.name`을 쓴다.

```bash
# 8766: JSON 응답. Accept: application/json 만으로 동작.
curl -sS http://141.223.105.232:8766/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"postmortem_get_capabilities","arguments":{}}}'

# 8765: SSE 응답. Accept에 두 타입이 모두 필요하다.
curl -sS http://141.223.105.232:8765/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"postmortem_get_capabilities","arguments":{}}}'
```

- `method`에 `postmortem_get_capabilities`를 직접 넣으면 `-32601`이다.
  올바른 형태: `{"method":"tools/call","params":{"name":"postmortem_get_capabilities","arguments":{}}}`.
- `initialize` 응답의 `capabilities.tools.listChanged: false`는 정상이다.
  이 값은 서버가 `notifications/tools/list_changed`를 보내는지 여부일 뿐,
  `tools/list`/`tools/call` 라우팅과 무관하다. 초기화 응답에 도구 정의가
  포함되지 않는 것도 정상이며, 목록은 `tools/list`로 받는다.
- 8765에서 `Accept: application/json`만 보내면 스펙상 `406 Not Acceptable`이다.
  JSON만 다루는 클라이언트는 8766을 사용한다.

두 인스턴스 모두 같은 Bearer 토큰을 사용한다. SSE 파싱이 어려운 클라이언트,
또는 세션 헤더 관리가 어려운 단순 에이전트는 **8766**을 쓰면 된다.

---

## 6. 클라이언트 연결

### Claude Code
최신 버전은 HTTP MCP를 지원한다.
```bash
claude mcp add --transport http srf-postmortem https://mcp.pls-srf.org/mcp
```
(정확한 옵션은 설치 버전의 `claude mcp add --help` 확인)

### Claude Desktop
버전에 따라 원격 HTTP MCP를 직접 지원하지 않을 수 있다.
이 경우 `mcp-remote` 브리지를 사용한다.

```json
{
  "mcpServers": {
    "srf-postmortem": {
      "command": "npx",
      "args": [
        "-y", "mcp-remote",
        "https://mcp.pls-srf.org/mcp",
        "--header", "Authorization: Bearer <token>"
      ]
    }
  }
}
```

- `mcp-remote`의 header 옵션 문법은 설치 버전 문서를 확인.
- Cloudflare Access를 쓰는 경우 Service Token header를 넘긴다.
- 최신 Claude Desktop이 Connectors/원격 MCP를 지원하면 그 경로를 사용한다.

---

## 7. Cloudflare Tunnel

1. Cloudflare Zero Trust → Tunnels → 기존 터널 선택
2. Public Hostname 추가:
   - `mcp.pls-srf.org`
   - Service: `http://localhost:8765`
3. Access policy:
   - Google/OTP 로그인, 또는
   - Service Token (`CF-Access-Client-Id`, `CF-Access-Client-Secret`)
4. MCP 클라이언트가 브라우저 로그인을 할 수 없으므로 **Service Token 또는 Bearer token**을 권장.
5. `mcp.pls-srf.org`를 `allowed_hosts`에 포함해야 한다 (기본값에 포함됨).
6. SSE를 쓸 경우 Cloudflare 버퍼링을 확인. Streamable HTTP 권장.

---

## 8. 보안

- DB는 **read-only**.
- 셸/파일 접근 도구 없음.
- `MCP_BEARER_TOKEN` 또는 Cloudflare Access로 인증.
- rate limit, max rows, timeout은 도구 파라미터/서버 설정으로 제한.
- 사용자의 LLM 제공자로 **도구 출력(내부 데이터)** 이 전송된다. 사용자에게 고지.
- 첨부/노트 텍스트는 데이터로만 취급 (prompt injection 방어).

---

## 9. 현재 제약

- HWP/PDF 보고서 **본문 미포함** (파일명만 제공)
- `report_md` 보일러플레이트 제외
- `event_alarms`는 2개 이벤트(3행)만 존재
- `event_pv_selection`은 27개 이벤트(45행)
- 도메인 문서(RF/가속기) 아직 미인덱싱
- dense embedding 미적용 (FTS5 + SQL만)
- 파형 값은 미포함 (presence/channel count만)
- `postmortem_get_pv_series`는 KSU/CRM/CRG/LRF/G00 계열 PV만 지원

---

## 10. 향후 확장

1. **도메인 문서** (`search_documents`, `get_subsystem_overview`)
2. **Hybrid retrieval**: BGE-M3 + FTS5 RRF + reranker (GPU PC)
3. **HWP/PDF 파싱**: 보고서 chunk + 페이지 인용
4. **파형 도구**: analog/digital 통계, 이벤트 비교
5. **추가 서비스 namespace**: `vacuum_*`, `rf_*`, `magnet_*`
6. **MCP gateway**: 서비스가 많아지면 path/gateway로 분리

---

## 11. 클라이언트 피드백 반영 (2026-11-18)

- **UTF-8**: 서버는 UTF-8 JSON을 반환한다. HTTP 응답에
  `Content-Type: application/json; charset=utf-8`을 명시한다.
  클라이언트는 UTF-8로 디코딩해야 한다(latin-1 디코딩 금지).
- **Event reference schema**: 모든 이벤트 참조는 `id`와 `event_id`를 모두 포함한다.
  `postmortem_get_event`, `postmortem_search_events`, `postmortem_similar_events`,
  `postmortem_resolve_event`에 동일하게 적용.
- **Session lifecycle**: Streamable HTTP 세션 idle timeout 기본 1800초.
  만료 후에는 재초기화(`initialize`)가 필요하다. `/capabilities`에 기본값을 노출.
- **Token 보안**: 테스트 토큰은 rotate했고 `.env`(git-ignored)에만 둔다.
  문서/채팅에 복사하지 않는다. 운영은 Cloudflare Access/Service Token 권장.
- **검색 필터**: `year_min`, `year_max`, `fault_types`(list), `has_waveform` 추가.
- **집계**: `group_by`에 `"year,beam_time"` 같은 복합 그룹화와
  `include_percent=true` 지원.
- **공개 문서**: `/docs`, `/capabilities`, `/health`, `/connection`은 public.
  `/mcp`만 Bearer 인증 필요.
- **Tool descriptions**: `tools/list`에 200~460자 수준의 전체 설명과
  `meta.response_shape`/`meta.examples`를 제공.
- **Response shape**: 모든 tool 응답에 `response_type`을 넣고,
  `/capabilities`와 `postmortem://schema`에 `response_shapes`를 문서화.
  - `event_list`, `event_detail`, `aggregate`, `pv_list`, `alarm_status`, `pv_series`
- **similar empty 의미**: `postmortem_get_event`가 `similar_count`,
  `similar_note`, `links_available`을 반환. 빈 `similar[]`는 API 오류가 아니라
  DB에 event_links가 없다는 뜻.
- **Token ASCII 검증**: 서버 시작 시 `MCP_BEARER_TOKEN`에 non-ASCII가 있으면
  명확한 메시지로 종료.
- **Session 오류 신호**: `/mcp`에서 세션 관련 400/404가 나면
  `X-MCP-Session-Expired: true`, `X-MCP-Hint: Re-initialize...` 헤더를 추가.

## 12. Troubleshooting

### Connection refused
- `127.0.0.1:8765`는 **서버 자신의 localhost**다.
- 원격에서는 `http://100.76.107.91:8765/mcp`(Tailscale) 또는
  `http://141.223.105.232:8765/mcp`(LAN)을 사용한다.
- `GET /connection`(public)에서 URL/인증/인코딩 정보를 확인할 수 있다.

### SSL handshake fail
- `https://100.76.107.91:8765`는 TLS가 없어 실패한다. **`http://`** 를 사용한다.
- HTTPS는 Cloudflare Tunnel로 `mcp.pls-srf.org`를 붙인 뒤에 사용한다.

### 401 Unauthorized
- `MCP_PASSWORDLESS_CIDRS` 밖이거나 프록시 경유 요청이면 아래 중 하나가 필요하다:
  - `Authorization: Bearer <MCP_BEARER_TOKEN>`
  - `X-MCP-Password: <MCP_ACCESS_PASSWORD>`
  - `Authorization: Basic base64(user:password)`
- 사내망에서 **직접** 접속했는데 401이면 `/connection`의 `network.client_ip`,
  `network.passwordless_cidrs`, `auth.passwordless`를 확인한다.
  전달 헤더(`CF-Connecting-IP`, `X-Forwarded-For`, `Forwarded` 등)가 붙은
  요청은 프록시 경유로 간주되어 무비밀번호 대상에서 제외된다.
- 값은 서버 `.env`에서 읽는다:
  ```bash
  ssh shpark@100.76.107.91 \
    'grep ^MCP_BEARER_TOKEN= ~/SRF_Postmortem_WebDB/.env | cut -d= -f2-'
  ssh shpark@100.76.107.91 \
    'grep ^MCP_ACCESS_PASSWORD= ~/SRF_Postmortem_WebDB/.env | cut -d= -f2-'
  ```
- 토큰/비밀번호는 **ASCII**여야 한다. `…`(U+2026)가 섞이면
  `UnicodeEncodeError: 'latin-1'`이 난다.
- rotate하면 이전 값은 401이 된다.

### t0 (빔덤프 시각) 매핑
- `resolve_t0()`는 빔전류 최대값의 **50% 아래로 지속 하강한 첫 샘플**을 t0로 쓴다.
- 과거 1% 기준은 덤프 전 작은 딥(예: 20241002의 3.5% 딥)에 반응해 t0가
  **1~2초 빨라졌다**. 2026-11-18에 50%+지속 조건으로 수정했고, 14개 quench
  이벤트 모두 독립적인 50% 교차 시각과 정확히 일치함을 확인했다.
- 이벤트 `timestamp`는 덤프 시각이 아니다(보통 11~53초 후). Archiver 창은
  반드시 `resolve_t0()` 결과를 기준으로 잡아야 한다.
- 기존 `data/alarm_series/*.json` sidecar의 `t0_offset_sec`는 수정 전
  규칙으로 계산된 값이라 1~2초 차이가 있을 수 있다.

### LLRF 채널 별칭 (지원 목록에 없는 이름)
운전/스코프 화면에서 쓰는 이름은 Archiver 인벤토리에 없다. 아래 표로 매핑한다.
CM2/CM3는 각각 `LRF2`, `LRF3`로 바꾼다.

| 요청 이름 | 실제 지원 PV | 의미 |
|---|---|---|
| `CavV01` | `SR:RF:LRF1:GMES_ARCH` | Probe Gradient (cavity field) |
| `FwdPow01` | `SR:RF:LRF1:CRFP_ARCH` | Forward RF Power |
| `RefV01` | `SR:RF:LRF1:CRRP_ARCH` | Reflect RF Power |
| `CavP01` | `SR:RF:LRF1:PMES_ARCH` | Probe Phase |
| (보조) `CavV01` | `SR:RF:LRF1:GCRDC_ARCH` | Gradient CORDIC Read |

- 이 이름들은 570개 RF PV 인벤토리에 없어 current API가 **500**을 반환한다.
  프로젝트 DB `alarm_pv_catalog`에도 없다(카탈로그는 KSU/CRM/CRG 인터록·cryo 위주).
- `scripts/pull_rf_alias_view.py --event <id>`가 매핑·호출·plot을 수행한다.

### 지원 RF PV 전체 목록과 가용성
`scripts/list_supported_rf_pvs.py`가 `RF_LLRF`/`RF_Tuner`/`RF_CM1..3`의
570개 PV를 분류해 `data/archiver_rf/supported_rf_pvs.csv`를 만든다.

| 분류 | 개수 |
|---|---:|
| `rf_signal` (GMES/CRFP/CRRP/GLDE/CFQE/DETA 등) | 105 |
| `rf_state` (CNTL2MODE/RFONr/STAT1) | 9 |
| `rf_config` (IIRK/IQ/PLSG/TMP 등 설정) | 45 |
| `tuner_state` / `tuner_other` | 27 / 3 |
| `cryo_vacuum` (RF_CM1..3) | 378 |

신호/상태 115개를 실제 호출한 결과:
- `20251205_014253` (current API): **115/115** 가용, 오류 0
- `20240907_025313` (레거시): **85/115** 가용, 오류 0
  - 레거시 누락: LRF당 `GCRDC`, `GDCLr`, `GLDE`, `GLOSr`, `PCRDC`, `PLDE`,
    `STAT1`; 튜너 `CFQE`, `CFQEA`, `QL`
- 결과: `<event>_supported_rf_pvs.csv`, `_all_rf_long.csv`,
  `_all_rf_wide_1s.csv` (1초 grid).

### Quench 공통 특징 (Archiver 기준, 2026-11-18)
`scripts/compare_quench_rf.py`로 quench 22건(legacy 18 + current 4)과
비-quench 빔덤프 10건을 같은 RF PV로 비교한 결과:

- `GMES`/`CRFP`/`CRRP` 붕괴(post/pre < 0.2), `PMES` 위상 점프, `DETA` 점프는
  **quench와 일반 빔덤프 모두 100%**에서 나타난다 → 덤프의 공통 서명이지
  quench 특이 신호가 아니다.
- `CFQE`/`CFQEA`/`GLDE` 급변도 대조군 10/10에서 나타난다(현재 quench는 1건만
  데이터 있어 특이성 판단 불가).
- **quench cavity의 `BCL03` 온도 상승(max-pre > 5 K)** 만 분리된다:
  legacy quench 15/18 (2023~2025-09 CM1은 **15/15**), 대조군 **0/8**.
  2021 CM2 quench 3건은 BCL03_CM2에서도 상승이 없고(센서/데이터 문제로 추정),
  current 4건은 BCL03 샘플이 4~18개뿐이라 급격한 가열을 놓쳤다.
- 결론: Archiver RF PV만으로는 quench 원인을 구분하기 어렵다. **해당 cavity의
  BCL03(가능하면 waveform의 pre-dump cavity sag/reflect rise)** 를 함께 봐야 한다.
  결과: `data/archiver_rf/quench_rf_{events,features,commonality}.csv`.

### Archiver PV 이름과 500 에러
- LLRF PV는 아카이브 시대에 따라 이름이 다르다.
  - 2025-09-18 이후(current API): `SR:RF:LRF1:GMES_ARCH`처럼 **`_ARCH` 접미사**
  - 그 이전(연도별 레거시 뷰어): `SR:RF:LRF1:GMES`처럼 **접미사 없음**
- 레거시 이름을 current API에 보내면 Archiver가 **HTTP 500**을 반환한다
  (PV가 없다는 404가 아니다). 이 경우 이벤트/도구 문제가 아니라 PV 이름 문제다.
- `fetch_pv_range()`가 두 표기를 자동 보정하므로 MCP 도구에는 어느 쪽을 넣어도 된다.
  보정 후에도 500이면 실제로 없는 PV 이름이다.
- `20250926_005735`, `20250926_015817`의 LRF PV는 아카이브 자체에 데이터가 없다
  (`quality: "no_data"`, 오류 아님). 빔전류·cryo PV는 조회된다.

### 403 Forbidden
- `MCP_ALLOWED_CIDRS`에 없는 IP에서 접속한 경우다.
- `/connection`의 `network.client_ip`와 `network.allowed_cidrs`를 확인한다.
- Tailscale에서 접속하려면 `MCP_ALLOWED_CIDRS`에 해당 대역을 추가해야 한다.

### JSONDecodeError on text/event-stream
- Streamable HTTP는 응답이 **SSE(`text/event-stream`)** 일 수 있다.
  `response.json()`을 직접 호출하면 실패한다.
- MCP SDK 클라이언트를 쓰거나 SSE `data:` 라인을 파싱한다.
- 간단한 클라이언트는 **JSON 인스턴스**(`http://100.76.107.91:8766/mcp`)를
  사용하면 `application/json` 응답을 받는다. (SDK 클라이언트와도 호환)

### 한글 깨짐
- 서버는 UTF-8 JSON을 반환하고 `charset=utf-8`을 명시한다.
- 클라이언트는 `response.content.decode("utf-8")` 또는
  `json.loads(response.content)`로 디코딩한다. latin-1 fallback을 쓰지 않는다.

### Tool 사용법
- `GET /docs`에서 tool 목록과 설명을 볼 수 있다.
- MCP resource `postmortem://quickstart`에 예시 호출이 있다.
- `postmortem_get_capabilities`의 `examples`/`tool_descriptions`를 참고한다.

## 13. 테스트

```bash
# stdio
venv-mcp/bin/python scripts/mcp_smoke.py

# HTTP (서버 실행 후)
venv-mcp/bin/python scripts/mcp_http_smoke.py http://127.0.0.1:8765/mcp
```
