# Quench 통합 분석 (`analyze_quench`, `compare_quench_events`)

Task 5 산출물. 파형 feature, Archiver PV(BCL01/BCL03·beam current), 보고서
텍스트/이미지 캡션 근거를 한 이벤트에 묶어서 반환한다.

## 도구

### `postmortem_analyze_quench(event_id, include_pv=True, include_reports=True, pre=60, post=120)`

`response_type="quench_analysis"`:

- `event` — timestamp, fault_type, user_fault_type, notes 발췌, 등록된 PV selection
- `waveform` — 100 ms parquet feature (없으면 `null`)
  - `t0`, `quench_unit`, `quench_unit_source`
  - `signature.CM1/CM2/CM3` — cavity sag, pre-state, reflect rise, forward trip
  - `beam_fall`, `quen.CM1/2/3` (QUEN assert 시각/pulse)
  - `missing_channels`, `warnings`
- `pv` — Archiver t0와 BCL01/BCL03/BEAM 요약
  - `pre_median` [-60,-10]s, `t0_value`, `post_max`, `post_max_t_s`, `delta`, `delta_pct`
  - `quality` — `ok` / `sparse` / `no_data` / `error`
- `reports` — `postmortem_search_reports` 결과 (event_id 필터, top 3)
- `highlights` — 코드가 만든 사실 문장 (LLM 추측 아님)
- `caveats`

### `postmortem_compare_quench_events(event_ids, include_reports=False)`

`response_type="quench_comparison"`. Archiver를 호출하지 않아 빠르다.

행 필드: `timestamp`, `quench_unit`, `quench_unit_source`, `has_waveform`,
`t0_wave_s`, `cavity_sag_depth`, `cavity_min_ratio`, `cavity_pre_state_ratio`,
`reflect_rise_ratio`, `forward_trip_ratio`, `forward_trip_time_s`,
`beam_fall_90_10_s`, `quen_asserted`, `quen_assert_t_s`,
`report_titles`, `report_count`.

## 두 개의 t0 — 섞으면 안 됨

| t0 | 정의 | 용도 |
|---|---|---|
| waveform t0 | parquet 100 ms 안의 50% 빔전류 지속 하강 | 파형 feature |
| Archiver t0 (`resolve_t0`) | 1 Hz 빔전류에서 찾은 50% 하강 (−수십 초 가능) | PV 시계열 window |

`analyze_quench`는 둘을 별도 필드로 반환한다. BCL03 peak 시각은 Archiver t0
기준, cavity sag 시각은 waveform t0 기준이므로 직접 빼면 안 된다.

## 실제 결과 (2026-11-20)

```text
20240907_025313 (slow)
  BCL03 10.17 → 30.61 K (delta +20.43, peak +0.0s)
  CM1 cavity sag depth 0.690 (min ratio 0.310)
  CM1 pre-t0 ratio 0.517
  reports 3

20251205_014253 (fast)
  BCL03 10.06 → 10.08 K (delta +0.02, sparse sampling)
  CM1 cavity sag depth 0.992
  QUEN_CM1 asserted at -0.000426 s (waveform t0)
  reports 0

20250306_195900 (no waveform)
  BCL03 10.13 → 31.88 K (delta +21.75)
  reports 3
```

`20251205`는 BCL03 샘플이 1.6~4초 간격이라 빠른 상승을 놓칠 수 있다.
파형의 QUEN assert와 cavity sag가 더 신뢰할 수 있는 fast-quench 신호다.

## Golden question 평가

`scripts/golden_eval.py` — 도구를 직접 호출해 근거 검색 가능성과 수치 존재를
검사한다. 프로즈 생성은 하지 않는다.

| ID | 질문 | 결과 |
|---|---|---|
| Q1 | 2024 CM1 quench 공통 파형 특징 | PASS (5/5 events, sag ≥0.5, pre-state <0.9, reports) |
| Q2 | 20240907 vs 20241002 파형 차이 | PASS (reflect 2.295 vs 2.034, pre 0.517 vs 0.558) |
| Q3 | quench 보고서 공통 조치 | PASS (10 hits, 8 distinct documents) |
| Q4 | BCL03 상승과 cavity sag | PASS (BCL03 +20.43K, sag 0.69) |
| Q5 | 20251205 fast vs 2024 slow | PASS (digital_quen vs default; pre-state diff 0.308) |

실행:

```bash
cd ~/SRF_Postmortem_WebDB
venv-mcp/bin/python scripts/golden_eval.py
# {"passed": 5, "total": 5, "elapsed_seconds": 0.6}
```

결과 파일: `data/reports/golden_eval.json`.

## 검증

- MCP stdio smoke: 16개 도구.
  - `quench: True bcl03_delta=20.4317 reports=3 highlights=4`
  - `quench_compare: True with_waveform=3 with_reports=2 rows=3`
- 20240907/20251205/20250306 직접 호출 정상.
- `compare_quench_events`는 Archiver 없이 0.6초 내 5건 평가.

## 한계

- Archiver PV는 ~1 Hz, BCL03는 1.6~4초. 100 ms 파형을 재구성할 수 없다.
- `event_alarms`가 거의 비어 있어 알람 근거는 대부분 없다.
- 보고서가 없는 quench 이벤트(예: 20251205)는 `reports.returned=0`이다.
- VLM 캡션/OCR 수치는 원본 이미지로 확인해야 한다.
- 실행 중 MCP(`srf-mcp` 8765, `srf-mcp-json` 8766)는 2026-11-24 재시작 완료.
  16개 도구가 라이브이며, LAN(141.223.105.0/24)에서는 passwordless다.
