# MCP 파형 도구 (`analyze_waveform`, `find_similar_waveforms`)

Task 3 산출물. `src/features/store.py` 가 `data/feature_store.sqlite3` 를 읽고,
`src/mcp_server/server.py` 가 두 도구로 노출한다. 런타임은 CPU·SQLite만 쓰며
GPU, Archiver, 임베딩 API에 의존하지 않는다.

## 도구

### `postmortem_analyze_waveform(event_id)`

한 이벤트의 파형 수치 feature를 반환한다.

- `response_type="waveform_analysis"`
- `available=false` 이면 311개 parquet 대상이 아니다(2022 이전, parquet 없음).
- `waveform`:
  - `t0` — trace-local 50% 빔전류 지속 하강
  - `beam_fall` — 90/50/10% 교차, 90→10 하강 시간
  - `signature` — CM1/2/3 cavity sag, pre-state, reflect rise, forward trip
  - `analog` — 10채널 pre/post 통계
  - `digital` — transition/assert가 있는 채널만 요약
  - `missing_channels`, `warnings`
- `caveats` — 100 ms 창, t0 정의, 결측, 2023 샘플링, quench unit 기본값.

### `postmortem_find_similar_waveforms(event_id, top_k=5)`

`waveform_manual_v1` 78차원 벡터의 cosine top-k.

- `response_type="waveform_similarity"`
- `target` — 대상 이벤트 signature
- `items[]` — `event_id`, `similarity`, `timestamp`, `fault_type`,
  `signature`, `delta_vs_target`, `n_imputed`, `missing_channels`, citation
- `top_k` 는 1~20으로 clamp.
- DB `event_links` 그래프와 다른 지표다.

## 실제 응답 (2026-11-18)

```text
analyze 20240907_025313  available=True  t0=-4e-06  quench=CM1/default_cm1
                           beam_fall_90_10=0.000174 s  digital_channels=4
analyze 20251205_014253  available=True  t0=-2e-06  quench=CM1/digital_quen
analyze 20130709_050000  available=False  no waveform feature record
```

```text
similar 20240907_025313 → 20240826_233522 0.9981
                          20241002_171719 0.9417
                          20241028_035836 0.9414
similar 20251205_014253 → 20251023_234454 0.9908
                          20251012_233123 0.9831
                          20250926_005735 0.9515
```

`20251205` 의 이웃은 모두 digital QUEN 이벤트다. `20240907` 의 2순위는
기획서의 비교 대상인 `20241002` 다. slow vs fast cosine은 0.372.

## smoke test

```bash
cd ~/SRF_Postmortem_WebDB
venv-mcp/bin/python scripts/mcp_smoke.py
```

`scripts/mcp_smoke.py` 에 두 도구 호출을 추가했다. stdio로 서버를 띄워
도구 목록, `analyze_waveform`, parquet 없는 이벤트, `find_similar_waveforms` 를
확인한다. 2026-11-18 실행에서 13개 도구와 신규 응답 모두 정상.

## 배포 주의

- 코드는 `src/mcp_server/server.py`, `src/features/{__init__,store}.py` 에 있다.
- **실행 중인 tmux `srf-mcp`(8765) / `srf-mcp-json`(8766) 는 재시작해야 새 도구가
  노출된다.** 재시작 전에는 기존 stdio smoke만 검증된 상태다.
- `FEATURE_STORE_PATH` 환경변수로 DB 경로를 바꿀 수 있다. 기본값은
  `<repo>/data/feature_store.sqlite3`.
- feature store를 재빌드하면 mtime이 바뀌고, 다음 호출에서 벡터 캐시가 자동
  무효화된다(서버 재시작 불필요).
- 도구가 `feature store unavailable: ...` 을 반환하면 DB가 없거나 스키마가 오래된
  것이다. `scripts/build_feature_store.py` 로 재빌드한다.

## 알려진 한계

- 866개 중 311개만 파형 feature가 있다(2023+). 나머지는 `available=false`.
- 부분 2건은 결측 feature를 median impute한 벡터다. `n_imputed`,
  `missing_channels` 로 표시된다.
- 수작업 벡터는 학습 임베딩이 아니다. 유사도는 feature-space cosine이며
  물리적 동일성을 의미하지 않는다.
- `quench_unit` 은 QUEN assertion이 없으면 CM1로 기본값 처리된다(caveat에 표시).
