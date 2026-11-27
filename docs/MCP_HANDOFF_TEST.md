# MCP 테스트 인수인계 (2026-11-25)

다른 에이전트가 SRF Postmortem MCP를 검증할 때 쓰는 문서.
서버 재시작: 2026-11-25 11:30 KST. 도구 17개.

## 1. 접속

| 항목 | 값 |
|---|---|
| LAN (JSON 응답) | `http://141.223.105.232:8766/mcp` |
| LAN (SSE) | `http://141.223.105.232:8765/mcp` |
| 인증 | 141.223.105.0/24 에서 passwordless |
| Tailscale `100.76.107.91` | 403 (allowed CIDR 밖) |
| 공개 `mcp.pls-srf.org` | DNS 미해결 |

Claude Desktop은 `mcp-remote` 브리지 사용:

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

## 2. 호출 형식 — 여기서 많이 틀린다

올바른 JSON-RPC:

```json
{"jsonrpc":"2.0","id":1,"method":"tools/call",
 "params":{"name":"postmortem_aggregate_events","arguments":{...}}}
```

- 도구 이름을 `method`에 넣으면 `-32601 Method not found`가 돌아온다. 정상 동작이다.
- 8765(SSE)는 `Accept: application/json, text/event-stream` **둘 다** 필요. 하나만 보내면 406.
- 8766은 `Accept: application/json`만으로 동작.
- `initialize` 응답의 `listChanged:false`는 정상. 서버가 도구 목록 변경 알림을 보내지 않는다는 뜻일 뿐이다.

## 3. 검증 시나리오와 기대값

수치는 전부 코드가 계산한다. LLM이 만들어낸 숫자는 실패로 처리할 것.

### T1. capabilities
`postmortem_get_capabilities` → 도구 **17개**, `domain_glossary` 블록, `query_defaults.hide_ms=true`.
`data_as_of`에 `latest_event_timestamp`, `total_events`, `total_events_non_ms`, `statement`가 있어야 한다.
현재 값: `as of 2026-11-24, total 868 events in DB (675 non-MS)`.

### T2. 배경지식
`postmortem_get_glossary` (topic 없이) → `response_type="glossary"`, 섹션 10개
(`scope, authority, system_overview, interlock_hierarchy, trip_sequence, signals, naming, data_rules, answer_style, caveats`).

`postmortem_get_glossary(topic="trip_sequence")` → `content.mis_beam_dump` 존재.

### T3. MS 제외 기본값 (fault_type)
`postmortem_aggregate_events(group_by="fault_type", year=2026)` → **total 38**

**정답 행 (그대로 대조할 것)**

| key | count | percent |
|---|---:|---:|
| Case 3: RF_Interlock | 11 | 28.95 |
| Case 2: MPS | 6 | 15.79 |
| Case 17: Control | 6 | 15.79 |
| Case 16: Beam line | 6 | 15.79 |
| Case 19: Magnet | 3 | 7.89 |
| Case 21: MechVac | 2 | 5.26 |
| Case 14: Utility | 2 | 5.26 |
| Case 1: Beam_loss | 1 | 2.63 |
| Case 12: Cavity_detune | 1 | 2.63 |

`sum_of_counts`가 38이어야 한다. 응답의 `data_as_of.statement`도 함께 확인한다.
응답의 `note`는 수치를 그대로 옮기라고 지시한다.

**흔한 실패 (2026-11-24 실제 발생)**

어떤 에이전트가 total 38만 맞추고 행을 이렇게 지어냈다.

> RF_Interlock 15, Beam line 7, Control 5, MPS 4, Unknown 3, Utility 2, MechVac/Others 1

이 분포는 DB 어디에도 없다. raw `fault_type`, `effective`, `case_id`, `user_fault_type`,
`case_description`, 11-18 스냅샷, 2024/2025 어느 것으로도 재현되지 않는다.
**total만 맞고 행을 지어낸 전형적 환각**이다. 행 단위로 대조하지 않으면 걸러지지 않는다.

### T3-b. MS 포함 (fault_type)
같은 호출에 `hide_ms=false` → **total 123**

| key | count | percent |
|---|---:|---:|
| Case 4: MIS | 52 | 42.28 |
| Case 3: RF_Interlock | 18 | 14.63 |
| Case 10: RF_path_Forward | 15 | 12.20 |
| Case 1: Beam_loss | 8 | 6.50 |
| Case 2: MPS | 6 | 4.88 |
| Case 17: Control | 6 | 4.88 |
| Case 16: Beam line | 6 | 4.88 |
| Case 19: Magnet | 3 | 2.44 |
| Case 0: Unknown | 2 | 1.63 |
| Case 14: Utility | 2 | 1.63 |
| Case 21: MechVac | 2 | 1.63 |
| Case 12: Cavity_detune | 1 | 0.81 |
| Case 11: RF_path_Cavity | 1 | 0.81 |
| Case 13: RF_source_fault | 1 | 0.81 |

### T4. case_id = 수동 분류 우선
`postmortem_aggregate_events(group_by="case_id", year=2026)` → **total 38**, 1위 `case_id=3, count=11`.
`postmortem_aggregate_events(group_by="auto_case_id", year=2026)` → **total 38**, 1위 `case_id=4, count=12`.
즉 `case_id`는 수동 분류를 따르고, `auto_case_id`는 자동 분류 참고값이다.
`case_id`를 "Case 3 / 0" 같은 하위 분류로 해석하면 실패다.

### T5. 수동 분류 우선 (이벤트)
`postmortem_search_events(fault_types=["Case 2: MPS"], year=2026)` →
모든 item의 `effective_fault_type`이 `Case 2: MPS`, `effective_case_id=2`, `case_id=1`(자동).
`classification_note`는 `수동분류 Case 2: MPS (자동분류 Case 1: Beam_loss)` 형태.

`postmortem_get_event("20261116_185917")` →
`classification.manual="Case 2: MPS"`, `classification.auto="Case 1: Beam_loss"`,
`classification.manual_overrides_auto=true`,
`classification.display="20261116_185917 이벤트는 Case 2: MPS입니다. (자동분류는 Case 1: Beam_loss로 판정)"`.

### T6. 빔타임 필터
`postmortem_search_events(beam_time="2026-11차")` → **returned 3**, `filters.beam_time` 유지.
`postmortem_aggregate_events(group_by="fault_type", beam_time="2026-11차")` → 해당 차수만 집계.

MS 라벨을 기본값으로 조회하면 오류 힌트가 온다:
`postmortem_search_events(beam_time="2026-01MS")` → `error` + `hint` ("Pass hide_ms=false").
MS 기간을 정말 볼 때만 `hide_ms=false`를 함께 넘긴다.

### T7. PV 필터
`postmortem_search_events(pv="BCL01")` → **returned 15**.
`pv`는 `event_alarms`/`event_pv_selection`의 PV 이름과 부분 일치한다.
필터를 무시하고 전체 이벤트를 돌려주면 실패다.

### T8. 파형
`postmortem_analyze_waveform("20240907_025313")` → `response_type="waveform_analysis"`,
`t0`, `signature.CM1`, `beam_fall`, `sequence`, `physics`, `digital` 포함.
`physics.class`가 `rf_trip`/`single_cavity_quench` 등이고,
`physics.citations`에 `padamsee2008`, `caspers2024` 같은 id가 있어야 한다.

### T9. 보고서 검색
`postmortem_search_reports("KLY Ion Pump 알람", top_k=5)` → `kinds`에 `image` 포함.
캡션/OCR 기반 이미지 검색이 동작하는지 확인.

### T10. Quench 통합
`postmortem_analyze_quench("20240907_025313")` → `waveform`, `pv`, `highlights`, `caveats`.
`waveform.physics.class`와 `citations`가 포함되는지 확인.
`pv`의 BCL03 delta가 양수(온도 상승)인지 확인.

### T11. 물리 glossary/citation
`postmortem_get_glossary(topic="cavity_decay_after_rf_off")` →
`summary_ko`, `equations`(3개), `citations`(padamsee2008, caspers2024, pls2_srf2013) 포함.
`postmortem_get_glossary()` → `physics` 블록에 7개 topic, reference 27개,
`internal_test_notes` 1건(2020-04-05 수직시험) 포함.
`postmortem_get_glossary(topic="list")` → `addressable_topic_count=16`
(domain 9 + physics 7).
`postmortem_get_glossary(topic="beam_dump_causes")` → `categories` 12개
(RF single/common, MPS, MIS, CONTROL, BEAMLINE, MECHVAC, KICKER, MAGNET,
PSI, LINAC, external)와 `mapping_ko` 포함.

## 4. 해석 규칙 — 틀리면 답이 틀어진다

| 규칙 | 내용 |
|---|---|
| MS | Machine Study 기간. 유저 서비스가 아니므로 기본 제외. "beam time"이라고 부르지 않는다. |
| 분류 | `effective_fault_type` = 수동 `user_fault_type` 우선, 없으면 자동 `fault_type`. |
| case_id | `case_id`/`effective_case_id` = 수동 분류 우선. `auto_case_id`/`auto_fault_type` = 자동 참고. |
| 수동/자동 불일치 | 수동 분류를 본문에 쓰고, 자동 분류는 괄호로 참고 표시. 예: `Case 3: RF_Interlock입니다. (자동분류는 Case 4: MIS로 판정)` |
| 데이터 기준일 | DB는 몇 달에 한 번 갱신된다. 총계를 말할 때 `data_as_of`의 날짜를 함께 밝힌다. |
| 인터록 fault | "가장 먼저 트립한 인터록"이 fault. `time_groups.FIRST`와 `case_fault`가 일치한다. |
| MIS | `INT_MIS1_IC`/`INT_MIS2_IC` 이중화. 둘 다 같은 트립을 나타낸다. |
| 릴레이 지연 | 분류기 값 **400 µs** (`classification.digital_digital_compensation`). |
| "300 µs" | 빔전류가 늦게 떨어지는 시간. 실측 약 200 µs. 릴레이 지연과 다른 값이다. |
| 아날로그 | 정규화된 값. 물리 단위가 아니다. 비율로만 해석한다. |
| 채널명 | 실제 채널명은 `INT_IC_FC1`, `INT_IC_FC2`, **`INT_IN_FC3`**. 문서의 `INT_IC_FC3`는 오타다. |

## 5. 답변 형식 (집계 질문)

나쁜 예: "RF 인터록이 가장 많습니다."

좋은 예:

> 2026년(MS 제외, 2026-11-24 기준) 가장 많은 fault는 RF interlock으로 총 11건(28.95%) 발생했습니다.
> 이 중 '과입력방지' 관련 notes가 6건으로 가장 많았습니다. 보고서에 따르면 ...

즉 **Case 집계 → notes 인용 → 보고서 근거** 순서로 쓰고, MS 제외 여부와 데이터 기준일을 명시한다.

## 6. 실패로 처리할 항목

- MS 이벤트를 유저 데이터로 세는 답변
- 자동 `fault_type`만 쓰고 수동 분류를 무시한 답변
- `case_id`를 하위 분류로 오해한 답변 (예: "Case 3 / 0", "Case 3 / 3")
- LLM이 만든 수치 (도구 응답에 없는 숫자)
- MS를 "빔타임"으로 부르는 답변
- 총계를 말하면서 `data_as_of` 기준일을 빠뜨린 답변
- `pv`/`beam_time` 필터를 무시하고 전체를 돌려주는 답변
