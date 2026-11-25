# SRF Postmortem MCP 서버 구축 보고서 자료

작성 2026-11-25. 이 문서는 MCP 서버를 어떻게 구성했고, 무엇을 시도했으며,
어디서 실패했는지를 보고서에 그대로 옮길 수 있도록 정리한 자료다.
수치는 전부 코드/DB에서 확인한 값이다. LLM이 만든 숫자는 없다.

관련 문서:
- `docs/MCP_SERVER_PLAN.md` — 설계와 도구 정의 (25 KB)
- `docs/MCP_HANDOFF_TEST.md` — 다른 에이전트용 검증 시나리오 T1~T10
- `docs/SECURITY_ACCESS_PLAN.md` — 접근/방화벽 설계
- `docs/MCP_WAVEFORM_TOOLS.md`, `docs/MCP_FEATURE_VECTOR_PLAN.md` — 파형/벡터 도구
- `config/domain_glossary.json` — 도메인 배경지식 정본

---

## 1. 개요

| 항목 | 값 |
|---|---|
| 서버 이름 | `srf-postmortem` |
| 버전 | 0.1.0 |
| 구현 | Python MCP SDK + Starlette/Uvicorn |
| 도구 수 | **17개** |
| 데이터 | `db/events.db` (SQLite, 읽기 전용) + `data/feature_store.sqlite3` + `data/rag/rag.sqlite3` |
| 전송 | stdio / SSE(:8765) / Streamable HTTP JSON(:8766) |
| 실행 위치 | `shpark@141.223.105.232` (`~/SRF_Postmortem_WebDB`) |
| 실행 방식 | tmux 세션 `srf-mcp`(8765), `srf-mcp-json`(8766) |
| 웹앱 | 같은 서버의 `127.0.0.1:8050` (Cloudflare Tunnel 뒤) |

MCP 서버는 웹앱과 **같은 `events.db`** 를 읽는다. 별도로 웹 API를 긁어오지
않으므로 웹과 MCP의 스냅샷이 항상 같다. DB는 몇 달에 한 번 갱신되며,
그 기준일을 `data_as_of`로 함께 반환한다.

---

## 2. 아키텍처

```
MCP 클라이언트 (Claude Desktop / Claude Code / 임의 HTTP 에이전트)
        │  Streamable HTTP (JSON-RPC 2.0)
        ▼
scripts/run_mcp.py  ── 인증/allowlist/전송 계층
        │
        ▼
src/mcp_server/server.py  ── 17개 도구 정의, 응답 포맷, caveat, data_as_of
        │
        ├── src/rag/tools.py       ── 결정적 SQL 계층 (검색/집계/이벤트 상세)
        ├── src/features/*         ── 파형 특징 벡터, 보고서 인덱스, quench 분석
        └── src/rag/index.py 등    ── RAG 인덱스/보고서 검색
        │
        ▼
db/events.db (read-only)  +  data/feature_store.sqlite3  +  data/rag/rag.sqlite3
```

핵심 원칙:
- **수치 계산은 코드가 한다.** 도구가 SQL로 계산해 JSON에 담아 돌려준다.
  LLM은 응답에 있는 숫자만 인용한다.
- **DB는 읽기 전용.** MCP 도구는 쓰기 SQL을 실행하지 않는다.
- **원본 `events.db`는 역복사하지 않는다.** 서버 DB를 로컬로 되돌리지 않는다.

---

## 3. 도구 17개

| # | 도구 | 역할 |
|---:|---|---|
| 1 | `postmortem_get_capabilities` | 데이터 커버리지, `data_as_of`, 인덱스 상태, 도구 목록 |
| 2 | `postmortem_search_events` | 이벤트 검색 (기간/빔타임/PV/fault/case 필터) |
| 3 | `postmortem_get_event` | 이벤트 상세 + 분류(수동/자동) + notes |
| 4 | `postmortem_aggregate_events` | 기간별 집계 (fault_type/case_id/auto_case_id 등) |
| 5 | `postmortem_similar_events` | 유사 이벤트 |
| 6 | `postmortem_search_alarm_catalog` | 알람/PV 카탈로그 검색 |
| 7 | `postmortem_get_alarm_status` | 알람 현재 상태 |
| 8 | `postmortem_resolve_event` | 이벤트-보고서/문서 연결 |
| 9 | `postmortem_list_fault_types` | fault type 목록 |
| 10 | `postmortem_list_beam_times` | 빔타임 목록 |
| 11 | `postmortem_get_pv_series` | PV 시계열 |
| 12 | `postmortem_analyze_waveform` | 파형 분석 (t0, signature, beam_fall, sequence, physics, digital) |
| 13 | `postmortem_find_similar_waveforms` | 유사 파형 |
| 14 | `postmortem_search_reports` | 보고서/이미지 검색 (캡션/OCR 포함) |
| 15 | `postmortem_analyze_quench` | quench 통합 분석 (파형+PV+하이라이트) |
| 16 | `postmortem_compare_quench_events` | quench 이벤트 비교 |
| 17 | `postmortem_get_glossary` | 도메인 배경지식 + 물리 topic/citation (topic별) |

---

## 4. 데이터 커버리지 (2026-11-24 기준)

`postmortem_get_capabilities` 실측값:

| 항목 | 값 |
|---|---:|
| 이벤트 | 868 |
| notes | 679 |
| 첨부 | 96 |
| event_alarms | 3 (2개 이벤트) |
| event_pv_selection | 45 (27개 이벤트) |
| 알람 PV 카탈로그 | 524 (매핑 375) |
| event_links | 2,480 |
| 파형 이벤트 | 313 |
| 연도 범위 | 2013 ~ 2026 |

파생 인덱스:
- RAG 인덱스: 1,460 문서/청크, 3.85 MB, 마지막 ingest 2026-11-17
- feature store: 8.27 MB, 스냅샷 `snap_752d6c64258f666c` (2026-11-18),
  이벤트 특징 311 / 파형 벡터 311 / 보고서 문서 91
- 보고서 인덱스: 문서 91, 첨부 96, 이미지 407, 이미지 캡션/벡터 407

**data_as_of 문구**: `as of 2026-11-24, total 868 events in DB (675 non-MS)`

---

## 5. 전송 방식과 세션 모델

| 세션 | 포트 | 응답 | 세션 | URL |
|---|---:|---|---|---|
| `srf-mcp` | 8765 | SSE | stateless | `http://141.223.105.232:8765/mcp` |
| `srf-mcp-json` | 8766 | JSON | stateless | `http://141.223.105.232:8766/mcp` |

- 두 인스턴스 모두 `--stateless`다. 매 POST가 독립이며 `Mcp-Session-Id`
  헤더가 필요 없다. `initialize`를 먼저 보내지 않아도 된다.
- 8765(SSE)는 `Accept: application/json, text/event-stream` **둘 다** 필요.
  하나만 보내면 406.
- 8766은 `Accept: application/json`만으로 동작. 단순 HTTP 에이전트는 8766 권장.
- stateless 모드에서 `GET /mcp`는 405(`allowed_methods: ["POST"]`)로 즉시 응답한다.
- 보조 endpoint: `/health`, `/docs`, `/capabilities`, `/connection` (public).

---

## 6. 보안 / 네트워크

- IP allowlist: `MCP_ALLOWED_CIDRS=141.223.105.0/24,127.0.0.1/32,::1/128`
  → 목록 밖 요청은 `403 forbidden / source_ip_not_allowed`.
- `MCP_PASSWORDLESS_CIDRS=141.223.105.0/24` → 사내망 직접 접속은 인증 없이 허용.
- 그 외 클라이언트는 `Authorization: Bearer <token>` 또는 `X-MCP-Password`.
- Cloudflare Tunnel에는 MCP를 올리지 않는다 (사내망 전용).
- nftables: inbound default-drop.
  - `tailscale0`: 관리용 전체 허용
  - `eno1`: `141.223.105.0/24` → 8765/8766 허용, 관리 PC `.230` → SSH/8050/8765/8766
  - 8050/8090은 loopback 바인딩
- 비밀값은 `.env`(git-ignored)에만 둔다. `.env.example`에는 키 이름만 있다.

실측 확인:
- LAN `141.223.105.232:8766` → 200
- Tailscale `100.76.107.91:8766` → 403 (`source_ip_not_allowed`)

---

## 7. 배포 / 운영

### 최초 준비
```bash
cd ~/SRF_Postmortem_WebDB
python3 -m venv venv-mcp
venv-mcp/bin/pip install -r requirements-mcp.txt
```

### 실행 (현재 서버의 실제 명령)
```bash
# 8765 — SSE, stateless
tmux new-session -d -s srf-mcp "cd ~/SRF_Postmortem_WebDB && \
  venv-mcp/bin/python scripts/run_mcp.py --transport streamable-http \
  --host 0.0.0.0 --port 8765 \
  --allowed-host 127.0.0.1:8765 --allowed-host localhost:8765 \
  --allowed-host 100.76.107.91:8765 --allowed-host 141.223.105.232:8765 \
  --allowed-host mcp.pls-srf.org --stateless >> logs/mcp.log 2>&1"

# 8766 — JSON, stateless
tmux new-session -d -s srf-mcp-json "cd ~/SRF_Postmortem_WebDB && \
  venv-mcp/bin/python scripts/run_mcp.py --transport streamable-http \
  --host 0.0.0.0 --port 8766 --json-response --stateless \
  --allowed-host 127.0.0.1:8766 --allowed-host localhost:8766 \
  --allowed-host 100.76.107.91:8766 --allowed-host 141.223.105.232:8766 \
  --allowed-host mcp.pls-srf.org >> logs/mcp.log 2>&1"
```

### 상태 확인
```bash
ss -ltnp | grep -E ':(8765|8766)'
tmux ls
tail -n 20 ~/SRF_Postmortem_WebDB/logs/mcp.log
curl -s http://127.0.0.1:8766/health
```

### 재시작
```bash
tmux kill-session -t srf-mcp 2>/dev/null
tmux kill-session -t srf-mcp-json 2>/dev/null
# 위 실행 명령 재실행
```

### stdio (로컬 클라이언트)
```bash
venv-mcp/bin/python scripts/run_mcp.py --transport stdio
```

### 스모크 테스트
```bash
venv-mcp/bin/python scripts/mcp_http_smoke.py http://127.0.0.1:8766/mcp
venv-mcp/bin/python scripts/mcp_smoke.py
```

---

## 8. Claude Desktop 연결

`mcp-remote` 브리지 사용 (HTTP라 `--allow-http` 필요):

```json
{
  "mcpServers": {
    "srf-postmortem": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "http://141.223.105.232:8766/mcp", "--allow-http"]
    }
  }
}
```

Claude Desktop 설정 파일 위치(Windows): `%APPDATA%\Claude\claude_desktop_config.json`.
연결 후 도구 17개가 보이면 정상. 연결이 안 되면 8766 포트가 리스닝 중인지,
클라이언트 IP가 `141.223.105.0/24`인지 먼저 확인한다.

---

## 9. 시도한 것과 실패 원인 (보고서용)

| 시도 | 결과 | 원인 / 해결 |
|---|---|---|
| stdio 로컬 실행 | 성공 | 로컬 클라이언트 전용 |
| 8765 SSE | 성공 | `Accept` 두 타입 필요. 하나만 보내면 406 |
| 8766 JSON stateless | 성공 | 단순 에이전트용. 헤더 불필요 |
| stateful 모드 | 실패 다발 | `Mcp-Session-Id` 누락 → `-32600 Missing session ID`. stateless로 전환 |
| 도구 이름을 `method`에 넣음 | 실패 | `-32601 Method not found`. `method:"tools/call"` + `params.name` |
| Tailscale IP로 접속 | 403 | allowlist 밖. 사내망 IP 사용 |
| Cloudflare Tunnel 경유 | 보류 | MCP는 사내망 전용으로 결정 |
| 서버 중지 상태에서 접속 | Connection refused | 프로세스 미실행. tmux 세션 재기동 |
| Claude Desktop 직접 HTTP | 실패 | `mcp-remote --allow-http` 브리지 필요 |
| `pv` 필터 | 수정 전 무시됨 | `event_alarms`/`event_pv_selection` EXISTS 조건 추가 |
| `beam_time` 필터 | 신규 추가 | `user_beam_time` 부분 일치 |
| 수동/자동 분류 혼용 | 수정 | `user_fault_type` 우선, `auto_*`는 참고 |
| 하드코딩 건수 | 수정 | `data_as_of`로 기준일·총계를 DB에서 실시간 산출 |

---

## 10. 이번에 수정한 MCP 동작 (2026-11-25)

1. **`pv` 필터 무시 버그** — `search_events`/`aggregate_events`에서
   `event_alarms.pv`, `event_alarms.graph_pv`, `event_pv_selection.pv`를
   EXISTS로 조회. `pv="INT_MIS1_IC"` → 3건, `pv="BCL01"` → 15건.
2. **빔타임 필터** — `beam_time` 파라미터 추가. `beam_time="2026-11차"` → 3건.
   MS 라벨을 기본값으로 조회하면 `hide_ms=false` 힌트를 반환.
3. **수동 분류 우선** — `effective_fault_type`/`effective_case_id`는
   `user_fault_type` 우선. `auto_fault_type`/`auto_case_id`는 참고.
   `classification_note`와 `classification.display`로 표시.
   예: `"20261116_185917 이벤트는 Case 2: MPS입니다. (자동분류는 Case 1: Beam_loss로 판정)"`
4. **`data_as_of`** — capabilities/aggregate에 최신 이벤트 시각, 최신 ingest,
   기준일, 총계/비MS 총계, 문구를 포함. 하드코딩 제거.
5. **`waveform.sequence`** — `postmortem_analyze_waveform` 응답에 캐비티
   90/50/10% 도달 시각, 빔전류 99/50/10% 시각, 캐비티→빔 지연,
   3개 캐비티 붕괴 시각 차이, 원인 class(`rf_trip`, `beam_first_rf_trip`,
   `beam_only`, `single/dual_cavity_quench`)를 추가. fast scope 313건 기준.
   상세 계산과 PLS-II cavity 시정수 검증은 `beamdump/docs/SEQUENCE_PHYSICS_ANALYSIS.md`.
6. **`waveform.physics` + 교과서 인용** — `config/physics_references.json`에
   SRF/빔물리/BLM/PLS-II 문헌 20개와 7개 physics topic을 정리했다.
   `postmortem_analyze_waveform`/`postmortem_analyze_quench`는 class별
   `summary_ko`, `chain_ko`, `steps_ko`, timeline, PLS-II 파라미터 check,
   `citations`를 반환한다.
   `postmortem_get_glossary(topic=...)`로 topic별 equations/citations을
   조회할 수 있다. per-event 저장본은
   `data/features/event_physics_interpretations.jsonl` (313줄).
   사람이 읽는 정리는 `docs/PHYSICS_REFERENCES.md`.

---

## 11. 검증 시나리오 요약 (상세는 MCP_HANDOFF_TEST.md)

| ID | 호출 | 기대값 |
|---|---|---|
| T1 | capabilities | 도구 17, `data_as_of` 존재, `as of 2026-11-24, total 868 (675 non-MS)` |
| T2 | glossary | 섹션 10개, `trip_sequence.mis_beam_dump` |
| T3 | aggregate fault_type, 2026 | total 38, RF_Interlock 11 (28.95%) |
| T3-b | hide_ms=false | total 123, MIS 52 (42.28%) |
| T4 | case_id / auto_case_id | case_id 1위 3(11건), auto 1위 4(12건) |
| T5 | search Case 2: MPS | `effective_fault_type` 유지, `classification_note` 표시 |
| T6 | beam_time 2026-11차 | 3건 |
| T7 | pv BCL01 | 15건 |
| T8 | analyze_waveform | `waveform_analysis`, t0/signature/beam_fall/sequence/digital |
| T9 | search_reports | `kinds`에 image 포함 |
| T10 | analyze_quench | waveform/pv/highlights/caveats, BCL03 delta 양수 |

---

## 12. 데이터 해석 규칙 (보고서에 반드시 포함)

- MS(Machine Study)는 유저 서비스가 아니다. 기본 제외. "빔타임"이라고 부르지 않는다.
- 분류: `effective_fault_type` = 수동 `user_fault_type` 우선, 없으면 자동 `fault_type`.
- `case_id`/`effective_case_id`는 수동 우선. `auto_case_id`는 자동 참고값.
- 수동/자동이 다르면 수동을 본문에 쓰고 자동을 괄호로 표시.
- 총계를 말할 때 `data_as_of` 기준일을 함께 밝힌다.
- 인터록 fault는 "가장 먼저 트립한 인터록"(`time_groups.FIRST`).
- 아날로그 값은 정규화 값. 물리 단위로 해석하지 않는다.
- 릴레이 지연 400 µs (`classification.digital_digital_compensation`).
- 채널명 정본: `INT_IC_FC1`, `INT_IC_FC2`, **`INT_IN_FC3`** (문서의 `INT_IC_FC3`는 오타).

---

## 13. 파일 인벤토리

| 경로 | 설명 |
|---|---|
| `scripts/run_mcp.py` | MCP 실행 진입점, 인증/allowlist/전송 |
| `src/mcp_server/server.py` | 17개 도구 정의, 응답 포맷, caveat |
| `src/rag/tools.py` | 결정적 SQL 계층 (검색/집계/상세) |
| `src/rag/index.py` | RAG 인덱스 |
| `src/features/` | 파형 특징, 보고서, quench, 벡터 스토어 |
| `config/domain_glossary.json` | 도메인 배경지식 정본 |
| `config/physics_references.json` | 교과서/강의록/PLS-II 문헌 20개, physics topic 7개 |
| `src/features/physics.py` | 순차 class → 물리 해석·PLS-II check·citation |
| `docs/PHYSICS_REFERENCES.md` | 교과서 내용 사람이 읽는 정리 |
| `scripts/build_event_physics.py` | per-event physics JSONL 생성 |
| `requirements-mcp.txt` | MCP 의존성 |
| `.env.example` | 환경변수 템플릿 (비밀값 없음) |
| `scripts/mcp_smoke.py`, `scripts/mcp_http_smoke.py` | 스모크 테스트 |
| `docs/MCP_SERVER_PLAN.md` | 설계/도구 정의 |
| `docs/MCP_HANDOFF_TEST.md` | 검증 시나리오 |
| `docs/SECURITY_ACCESS_PLAN.md` | 접근/방화벽 |
| `db/feature_store.sql` | feature store 스키마 |
| `db/migrations/*.sql` | 케이스 분류 마이그레이션 |

---

## 14. 알려진 제약

- DB는 몇 달에 한 번 갱신된다. `data_as_of` 기준일을 항상 함께 말한다.
- `db/events.db`는 git에 커밋하지 않는다. 서버 원본을 로컬로 역복사하지 않는다.
- MS 이벤트를 유저 데이터로 세면 오답이다.
- 자동 분류만 쓰고 수동 분류를 무시하면 오답이다.
- `case_id`를 하위 분류("Case 3 / 0")로 해석하면 오답이다.
- 도구 응답에 없는 숫자를 LLM이 만들면 실패로 처리한다.
- VLM 캡션/OCR 수치는 원본 이미지로 확인해야 한다.
