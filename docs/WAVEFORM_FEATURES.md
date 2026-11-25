# 파형 feature 추출물 (`extract_waveform_features.py`)

Task 1 산출물. `data/merged/event_*.parquet` 311건에서 수치 feature를 뽑아
`data/features/` 에 쓴다. GPU·Archiver·임베딩 API를 쓰지 않는 순수 CPU 배치다.

## 실행

```bash
cd ~/SRF_Postmortem_WebDB
venv/bin/python scripts/extract_waveform_features.py \
    --data-dir data/merged \
    --manifest /path/to/parquet_manifest.csv \
    --out-dir data/features \
    --workers 8
```

- `--manifest` 는 선택. 주면 `timestamp`/`fault_type`/`user_fault_type` 메타가
  JSON에 함께 들어간다. 없으면 `data/merged/event_*.parquet` 전체를 처리한다.
- `--events A,B`, `--limit N` 으로 부분 실행.
- `--no-hash` 는 `source_sha256` 생략(속도).
- 종료 코드: 실패/검증 오류가 있으면 1.

## 출력

| 파일 | 내용 |
|---|---|
| `waveform_features.jsonl` | 이벤트 1건 = 1줄 JSON |
| `waveform_feature_manifest.csv` | 상태/오류/t0/quench unit/해시 |
| `waveform_feature_validation.json` | 건수·부분처리·실패·경고 요약 |

2026-11-18 실행 결과: **311 처리 / 309 완전 / 2 부분 / 0 검증오류 / 0 실패**
(61초, 8 workers).

## t0 규칙 — `resolve_t0()` 와의 관계

`resolve_t0()` 는 Archiver의 1 Hz 빔전류에서 **10분 전 구간**을 보고 50% 지속
하강을 찾는다. 반면 parquet는 **±50 ms** 만 담고 있어, 초 단위 t0로는 window를
잡을 수 없다. 그래서 **같은 규칙(50% 지속 하강)** 을 파형 자체의
`BeamCurrent` 에 적용한다:

```
t0 = trace 최대값의 50% 아래로 내려간 첫 샘플 (다음 샘플도 아래일 때)
```

- `t0.value_s`: `t_rel_s` 기준, 초 단위
- `t0.method`: `"parquet_beam_current"`
- `t0.rule`: `"beam_50pct_sustained_drop"`
- `t0.found=false` 이면 0.0으로 fallback 하고 `warnings` 에 기록.

**Archiver t0(`resolve_t0`)는 여기서 쓰지 않는다.** 필요하면 Task 2에서
`event_features.t0_offset_s` 에 별도로 채운다.

## JSON 구조 (schema_version 1)

```jsonc
{
  "schema_version": 1,
  "event_id": "20251205_014253",
  "source_file": "event_20251205_014253.parquet",
  "timestamp": "2025-12-05T01:42:53",        // manifest
  "fault_type": "...", "user_fault_type": "...",
  "event_timestamp_parquet": "2025-12-05 01:42:53.402+09:00",
  "n_samples": 50000, "dt_s": 2e-06, "duration_s": 0.099998,
  "missing_channels": [],
  "t0": {"value_s": -2e-06, "found": true, "rule": "...", "method": "..."},
  "windows": {"pre_start_s": -0.05, "post_end_s": 0.049998,
              "pre_duration_s": 0.049998, "post_duration_s": 0.05,
              "pre_recent_s": 0.001},
  "analog": { "BeamCurrent": {...}, "Cavity_SRF1": {...}, ... },
  "beam_fall": {"fall_90_s": ..., "fall_50_s": ..., "fall_10_s": ...,
                "fall_90_10_s": ...},
  "signature": {"CM1": {...}, "CM2": {...}, "CM3": {...}},
  "digital": {"QUEN_CM1_FC1_d": {...}, ...},
  "quench_unit": "CM1",
  "quench_unit_source": "digital_quen" | "default_cm1",
  "quench_units_with_quen_assert": ["CM1"],
  "warnings": [],
  "source_sha256": "...",
  "extract_seconds": 0.4
}
```

### analog[name]

| 필드 | 의미 |
|---|---|
| `missing` | parquet에 채널이 없음 |
| `pre_med/std/min/max/p05/p95` | t0 이전 전체 pre-window 통계 |
| `pre_recent_med` | t0 직전 1 ms 중앙값 (slow precursor) |
| `post_med/min/max` | t0 이후 post-window 통계 |
| `ref` | 기준 레벨 = pre p95 (0에 가까우면 median) |
| `min_ratio` / `max_ratio` | post min/max ÷ ref |
| `recent_ratio` | pre_recent_med ÷ ref |
| `min_t_s` / `max_t_s` | post 극값 시각, **t0 기준** |

### beam_fall

`fall_90/50/10_s` 는 trace 전체에서 90/50/10% 레벨을 처음 지나는 시각(t0 기준).
`fall_50_s` 는 정의상 t0(=0)다. `fall_90_10_s` 는 실제 하강 시간.

### signature[CM1|CM2|CM3]

| 필드 | 의미 |
|---|---|
| `cavity_sag_depth` | `1 - Cavity_SRFn.min_ratio` (음수 = post가 pre보다 높음) |
| `cavity_sag_time_s` | cavity 최저점 시각 |
| `cavity_pre_state_ratio` | t0 직전 cavity 레벨 ÷ ref (1 미만이면 이미 sag 중) |
| `reflect_rise_ratio` / `reflect_peak_time_s` | 반사 상승 |
| `forward_trip_ratio` / `forward_trip_time_s` | forward가 ref의 50% 아래로 |
| `*_pre_state_ratio` | t0 직전 상태 |
| `unit_channels_missing` | 해당 unit에서 빠진 채널 |

### digital[name]

`state_before/after` 는 t0 직전/직후 값. `n_transitions`, `transitions`
(t0 기준 시각, 최대 12개), `first_assert_t_s`, `first_release_t_s`,
`pulse_width_s`.

극성은 `src/core/config.py` 의 `digital_active_low/high` 를 그대로 복제했다.

- `active_low`: 1→0 이 fault (`QUEN/ARC/RDY/VAC/HE-PR/INT_IC/INT_IN`)
- `active_high`: 0→1 이 fault (`INT_MIS/INT_PSI/RDY_KSU`)
- `unmapped`: 위 목록에 없는 채널. **방향을 추정하지 않고** transition만 기록.

## 알려진 데이터 공백 (2026-11-18)

- **부분 처리 2건** — parquet에 채널 자체가 없음. 실패가 아니라 `partial`.
  - `20251215_100348`: Cavity_SRF1/2/3 없음
  - `20251215_100401`: Reflect_SRF1/2/3 없음
- **t0 fallback 2건** (`found=false`) — 100 ms 창 안에 50% 하강이 없음.
  - `20240311_215329`: 빔전류 평탄(~0.59)
  - `20250427_141356`: 완만한 감소(0.57→0.42), 50% 미달
- **unmapped digital 26건** — `Quench_low_active_d`(24), `QUENCH-Low_fault_d`(2).
  config의 극성 목록에 없다. 샘플에서는 상수라 transition이 없었지만,
  분류기 극성 목록에 추가할지 검토 필요.
- **t0가 trigger에서 1 ms 이상 떨어진 이벤트 11건** (최대 −27.6 ms).
  파형이 하강 중간에서 시작하는 slow beam loss. `warnings` 에 기록됨.
- 2023년 일부 parquet는 19.7 µs 간격(10,000점), 이후는 2 µs(50,000점).
  `dt_s` 로 구분한다.

다음 단계: Task 2 — `schema/feature_store.sql` 에 맞춰
`feature_store.sqlite3` writer 작성. `features_json` 은 이 JSON을 그대로 넣고,
`input_hash` 는 `source_sha256` 을 사용한다. → **완료**: `docs/FEATURE_STORE.md` 참고.
