# Feature store (`feature_store.sqlite3`)

Task 2 산출물. Task 1의 `waveform_features.jsonl`과 2026-11-18 `events.db`
스냅샷을 합쳐 **읽기 전용 단일 SQLite 파일**을 만든다. MCP 런타임은 이 파일만
읽으면 되고, GPU·Archiver·임베딩 API에 의존하지 않는다.

## 빌드

```bash
cd ~/SRF_Postmortem_WebDB
venv/bin/python scripts/build_feature_store.py \
    --features data/features/waveform_features.jsonl \
    --events-db /tmp/events_snapshot_20261118.db \
    --schema db/feature_store.sql \
    --out data/feature_store.sqlite3 \
    --force
```

- `--events-db` 는 **스냅샷 사본**을 연다(`mode=ro`). 운영 `db/events.db` 를
  덮어쓰지 않는다.
- `--snapshot-id` 기본값은 `snap_<db sha256 앞 16자>` (내용 기반, 재현 가능).
- `--force` 없이 기존 출력이 있으면 실패한다.
- 빌드 후 `data/feature_store_validation.json` 을 쓴다.

## 결과 (2026-11-18)

| 항목 | 값 |
|---|---:|
| `event_features` | 311 |
| `waveform_vectors` | 311 |
| vector dim | 78 |
| DB 크기 | 4.78 MB |
| snapshot | `snap_752d6c64258f666c` |
| events / parquet / reports | 866 / 311 / 96 |
| 빌드 시간 | 0.13 s |

DB `integrity_check=ok`, `foreign_key_check` 0건, 벡터 노름 1.0, NaN 0,
309건으로 scaler fit(부분 2건 제외).

## 테이블

`db/feature_store.sql` 그대로. Task 2에서 추가한 것은
`feature_vector_specs` 하나다.

| 테이블 | Task 2 내용 |
|---|---|
| `snapshots` | 1행 — snapshot id / db sha256 / 866·311·96 |
| `model_versions` | `waveform_manual_v1`, dim 78, normalize 1 |
| `feature_vector_specs` | feature 이름 78개, impute/scale/bounds, fit 309 |
| `event_features` | 311행 — `features_json` = Task 1 레코드 원문 |
| `waveform_vectors` | 311행 — float32 LE BLOB + `meta_json` |
| `report_*` | Task 4에서 91 docs / 157 chunks / 407 images / 157+407 vectors 적재 |
| `feature_vector_specs` | `waveform_manual_v1` + `text_e5small_v1` 스펙 |

`event_features.t0_offset_s` 는 **파형 t0**(`t0.value_s`)다. Archiver 기반
`resolve_t0()` 의 초 단위 offset이 아니다. Archiver t0가 필요하면 별도 컬럼/행으로
추가해야 한다.

## 수작업 벡터 `waveform_manual_v1`

학습 임베딩이 아니라 Task 1 feature에서 결정적으로 만든 78차원 벡터다.
Task 3의 `find_similar_waveforms` 가 CPU cosine으로 바로 쓸 수 있다.

| 그룹 | 차원 | 내용 |
|---|---:|---|
| analog ratio | 30 | 10채널 × (`min_ratio`, `max_ratio`, `recent_ratio`) |
| beam fall | 2 | `fall_90_ms`, `fall_90_10_ms` |
| signature | 21 | CM1/2/3 × (sag depth, pre-state, reflect rise/pre, forward trip) + 2 timing |
| quench | 3 | `QUEN_CM1/2/3` asserted |
| interlock | 22 | QUEN 제외 22개 인터록 asserted |

전처리(모두 `feature_vector_specs` 에 저장):

1. raw clip — ratio `[-10, 20]`, time `[0, 100] ms`, binary `[0, 1]`
2. 결측/N-A → fit median으로 impute
3. robust z-score — `(v - median) / (1.4826 * MAD)`, MAD=0이면 scale=1
4. z clip `[-5, 5]`
5. L2 normalize

`meta_json` 에는 `imputed_features`, `n_imputed`, `missing_channels` 를 남긴다.
`imputed_features` 는 채널 결측뿐 아니라 **N/A**(예: forward trip 없음)도 포함하므로,
채널 단위 공백은 `missing_channels` 로 구분한다.

### 재현성 검증

저장된 spec만으로 3개 이벤트 벡터를 다시 계산해 blob과 비교:
최대 오차 **7.75e-9**. spec이 벡터를 완전히 결정한다.

## 검색 스모크 테스트

```python
import sqlite3, numpy as np
c = sqlite3.connect("file:data/feature_store.sqlite3?mode=ro", uri=True)
rows = list(c.execute("SELECT event_id, vector FROM waveform_vectors"))
ids = [r[0] for r in rows]
V = np.stack([np.frombuffer(r[1], dtype="<f4") for r in rows])
sim = V @ V[ids.index("20240907_025313")]
for k in np.argsort(-sim)[1:6]:
    print(ids[k], round(float(sim[k]), 3))
```

관찰된 이웃:

- `20240907_025313` (slow quench) → `20240826_233522` 0.998,
  `20241002_171719` 0.942 — 기획서의 비교 대상이 실제로 가깝다.
- `20251205_014253` (fast, QUEN pulse) → `20251023_234454` 0.991,
  `20251012_233123` 0.983, `20250926_005735` 0.951 — 모두 digital QUEN 이벤트.
- `20240907` vs `20251205` cosine 0.372 — slow/fast가 분리된다.

## 알려진 한계

- **부분 2건**은 벡터의 18~30개 feature가 median impute 상태다. 검색에는 쓰되
  `meta_json.missing_channels` 를 caveat로 노출해야 한다.
- `reflect_rise_ratio` 처럼 대부분 이벤트에서 1 미만인 feature는 스케일 후에도
  분산이 작다. 필요하면 Phase 5에서 group weighting을 검토한다.
- 311건 규모라 SQLite + numpy 전수 cosine으로 충분하다. 수만 건이 되면
  FAISS/Qdrant로 옮긴다.
- Task 1 feature가 바뀌면 snapshot id가 같아도 **재빌드**해야 한다
  (`features_input.sha256` 으로 추적).

다음 단계: Task 3 — MCP 도구 `postmortem_analyze_waveform`,
`postmortem_find_similar_waveforms` 를 이 DB에 연결.
