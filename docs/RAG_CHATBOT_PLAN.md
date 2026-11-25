# SRF Postmortem RAG 챗봇 — 구현 계획 및 현황

작성일: 2026-11-17  
상태: **MVP 구현·배포 완료**, 평가 28/28 통과

---

## 1. 확정 사항

| 항목 | 결정 |
|---|---|
| LLM | OpenRouter **`deepseek/deepseek-v4.1-flash-1017`** (날짜 고정) |
| 인증 | `OPENROUTER_API_KEY`를 **git-ignored `.env`** 에서 로드 |
| HWP 파싱 | MVP에서 **제외** |
| 알람/PV | **포함** — event_alarms, event_pv_selection, alarm_pv_catalog, monitor 상태 |
| 검색 | SQLite **FTS5 trigram** + 한/영 동의어 + 구조화 SQL 도구 |
| Dense embedding | MVP에서는 미사용 (Python 3.14/PyTorch 의존성 회피). 추후 확장 |
| UI | 기존 FastAPI 앱의 `/chat` 페이지 |

---

## 2. 데이터 현황 (서버 실측)

| 데이터 | 수량 | RAG 문서 |
|---|---:|---:|
| events | 866 | event_card 866 |
| notes 보유 | 679 | event_card에 포함 |
| fault_types | 22 | fault_type 22 |
| event_alarms | 3 | event_alarm 3 |
| event_pv_selection | 45 | event_pv_selection 45 |
| alarm_pv_catalog | 524 (graph PV 375) | alarm_catalog 524 |
| **RAG index 합계** | | **1,460 documents / 1,460 chunks** |

- `report_md`는 자동 생성 보일러플레이트이므로 인덱싱하지 않는다.
- HWP/PDF 보고서는 Phase 2에서 추가한다.
- 파형(`analog_metrics`, `digital_pattern`, merged parquet)은 문서 검색 대상이 아니라 Phase 3의 도구 대상이다.

---

## 3. 아키텍처

```
[events.db / attachments]
        │
        ▼
[src/rag/documents.py]
  event_card / event_alarm / event_pv_selection /
  alarm_catalog / fault_type
        │
        ▼
[src/rag/index.py]  SQLite FTS5 trigram
        │
[src/rag/plan.py]   의도 + 필터 추출
        │
[src/rag/tools.py]  SQL 도구
  search_events / aggregate_events / get_event_context /
  similar_events / search_alarm_catalog / get_alarm_status
        │
[src/rag/answer.py] 근거 조립 + 인용 + fallback
        │
[src/rag/llm.py]    OpenRouter DeepSeek Flash
        │
[src/rag/web.py]    /chat, /api/chat, /api/rag/*
```

핵심 원칙:
- 이벤트/집계/알람/PV 질문은 **LLM이 SQL을 직접 고르지 않는다.** 백엔드가 결정적으로 도구를 실행한다.
- LLM은 **제공된 근거만** 사용하고 `[E:]`, `[A:]`, `[PVS:]`, `[PV:]`, `[FT:]` 인용을 붙인다.
- 이벤트별 기록 알람과 카탈로그 매핑을 명시적으로 구분한다.
- DB는 항상 **read-only**로 연다.

---

## 4. 문서 타입

### event_card
```
[E:20251103_115812] 2025-11-03T11:58:12 | Case 3: RF_Interlock | RDY_KSU3_IC_d
Case/Confidence, User fault, Beam time, Notes,
Attachments, Waveform, Similar
```

### event_alarm
```
[A:20260623_110438/SR:RF:KSU3:AI_LOW_FAULT40]
Alarm PV, description, subsystem, severity, graph PV, first_seen_rel
```

### event_pv_selection
```
[PVS:20251103_115812/SR:RF:KSU3:AI24]
Selected PV + description
```

### alarm_catalog
```
[PV:SR:RF:KSU3:AI_LOW_FAULT40]
description, subsystem, graph PV, severity, state mode
```

### fault_type
```
[FT:Case 3: RF_Interlock] description, severity
```

---

## 5. 도구

| 도구 | 역할 |
|---|---|
| `search_events(filters)` | 연도/날짜/fault/subsystem/text 조건 이벤트 조회 |
| `aggregate_events(group_by, filters)` | 연도/fault/빔타임별 집계 |
| `get_event_context(event_id)` | 이벤트 + 알람 + PV 선택 + 카탈로그 + 첨부 + 유사 이벤트 |
| `similar_events(event_id)` | event_links 기반 유사 이벤트 |
| `search_alarm_catalog(query, subsystem)` | PV 설명/카탈로그 검색 |
| `get_alarm_status()` | 모니터 running/heartbeat/last_state/최근 전이 |

---

## 6. API / UI

```
GET  /chat
POST /api/chat                 {message, filters?}
GET  /api/rag/status
GET  /api/chat/suggestions
GET  /api/rag/event/{id}/context
```

UI:
- `/chat` 페이지: 채팅, 필터(연도/subsystem/fault/event ID), 예시 질문, 출처 패널
- 인용 chip 클릭 → 이벤트 페이지/PV Explorer/알람 대시보드
- `base.html` 상단 Chat 메뉴 추가

---

## 7. 설정

`.env` (git-ignore, 절대 커밋 금지):
```
OPENROUTER_API_KEY=...
OPENROUTER_MODEL=deepseek/deepseek-v4.1-flash-1017
OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
RAG_INDEX_PATH=data/rag/rag.sqlite3
RAG_TOP_K=8
RAG_MAX_TOKENS=1500
RAG_TEMPERATURE=0.1
RAG_REASONING_EFFORT=low
```
`.env.example`만 저장소에 둔다.

---

## 8. 인덱스 구축

```
venv/bin/python scripts/rag_ingest.py --full
venv/bin/python scripts/rag_ingest.py --incremental
```

- 이벤트/PV 선택/알람이 추가되면 `--incremental`로 갱신한다.
- 인덱스는 `data/rag/rag.sqlite3` (git-ignore).
- 현재 서버 인덱스: **1,460 documents**, 2026-11-17 11:34 UTC.

---

## 9. 평가

`docs/rag_golden_set.json` — 28문항.

```
venv/bin/python scripts/rag_eval.py            # retrieval-only
venv/bin/python scripts/rag_eval.py --llm      # full answer
```

**2026-11-17 서버 결과**
- Retrieval-only: **25/25 통과** (3건은 aggregate/status/refusal로 skip)
- LLM answer: **28/28 통과**

평가 항목:
- 단일 이벤트 원인/조치
- 연도별 집계
- quench/알람/PV 질문
- 유사/목록 질문
- 현재 알람 모니터 상태
- 존재하지 않는 연도(2027) 거절

---

## 10. 남은 작업

1. **HWP/PDF 보고서 인덱싱** (Phase 2)
   - HWP 5.0 파싱 스파이크(pyhwp vs LibreOffice)
   - 페이지/섹션 인용
2. **Dense embedding** (선택)
   - Python 3.14 호환 모델/런타임 확보 후 BGE-M3 등 추가
3. **PV/Archiver 시계열 도구** (Phase 3)
   - graph PV의 min/max/deviation, t0 정렬 품질 플래그
   - 파형 통계(`analog_metrics`/`digital_pattern`)
4. **인증/운영**
   - `/chat` 접근 제어 정책
   - 질의/피드백 로그, 레이트리밋
5. **커밋/푸시**
   - 현재 RAG 작업은 서버/WSL 모두 uncommitted

---

## 11. 주의사항

- `event_alarms`는 2개 이벤트(3행)뿐이다. 대부분의 이벤트는 “기록된 알람 스냅샷 없음”이다.
- `event_pv_selection`은 27개 이벤트(45행)다. 수동 등록 PV와 기록된 알람을 혼동하지 않는다.
- 현재 알람 모니터는 running이지만 시작 이후 transition이 0일 수 있다. `last_heartbeat`와 `last_state_updated`를 함께 표시한다.
- OpenRouter로 SRF 내부 데이터가 전송된다. provider 데이터 정책(`data_collection: deny`)을 확인하고, 민감정보/키를 프롬프트에 넣지 않는다.
- 검색 근거 안의 지시문은 데이터로만 취급한다(prompt injection 방어).
