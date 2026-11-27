# 빔덤프 물리 참고문헌 정리

작성 2026-11-25, 갱신 2026-11-27. MCP의 `config/physics_references.json`과
`src/features/physics.py`가 인용하는 교과서/강의록/논문/PLS-II 자료를 사람이
읽기 쉽게 정리한 문서다. MCP 응답의 `physics.citations`는 이 표의 `id`와
`locator`를 그대로 사용한다.

---

## 1. SRF cavity

| id | 문헌 | 이 분석에서 쓰는 부분 |
|---|---|---|
| `padamsee2008` | Padamsee, Knobloch, Hays, *RF Superconductivity for Accelerators*, 2nd ed., Wiley-VCH 2008, ISBN 978-3-527-40842-9 | 표준 교과서. Part I cavity fundamentals(Q0, R/Q, geometry factor, surface resistance), Part II quench/thermal breakdown/field emission, Part III couplers/tuners |
| `padamsee2009` | Padamsee, *RF Superconductivity: Science, Technology and Applications*, Wiley-VCH 2009, ISBN 978-3-527-40572-5 | cavity geometry, critical field, fabrication의 보조 교재 |
| `padamsee2023` | Padamsee, *Superconducting Radiofrequency Technology for Accelerators*, Wiley-VCH 2023, ISBN 978-3-527-41409-3 | N-doping, flux expulsion, high-field Q-slope, Nb3Sn. 최신 동향 |
| `caspers2024` | Caspers & Calatroni, "II.5 — Superconducting RF cavities", CERN Yellow Reports 2024, DOI 10.23730/CYRSP-2024-003.1085 | CC-BY 오픈액세스. surface impedance, R/Q, geometry factor, 측정법 |
| `schmuser2003` | Schmüser, "Basic Principles of RF Superconductivity and Superconducting Cavities", SRF2003 MOT01 / CERN-2006-002, DOI 10.5170/CERN-2006-002.183 | pillbox cavity, two-fluid model, BCS surface resistance의 간결한 유도 |
| `geng2015` | Geng, "Superconducting RF Cavity Basics", USPAS 2015, JLab | Q0=G/Rs, pillbox G=257 Ω, R/Q=196 Ω, fabrication/processing 실무 |

### Cavity 계산에 쓴 식

```
1/QL = 1/Q0 + 1/Qext = (1+beta)/Q0,   beta = Q0/Qext
U    = V^2 / (2*omega*(R/Q))
Pc   = V^2 / (2*(R/Q)*Q0)
Pbeam= V*I*cos(phi)
beta_opt = 1 + Pbeam/Pc
tau_Q = 2*QL/omega            (전원 off, 빔 없음)
tau_beam ~ U/(Pc+Pbeam)       (초기 빔부하)
```

PLS-II 운전점(Qext 1.5–1.7e5, Q0 7.5e8–1.1e9, R/Q=89 Ω, V_c 1.5 MV,
I 300–400 mA)을 넣으면:

| 양 | 값 |
|---|---:|
| U | 4.0 J |
| Pc | 11–17 W |
| Pbeam | 124–166 kW |
| beta | 4,400–7,300 |
| tau_Q | 95–108 µs |
| tau_beam | ≈28 µs (24–32 µs) |
| 관측 tau (90→10%) | **55–76 µs, 중앙값 66 µs** |

관측값은 자연 감쇠와 빔부하 감쇠 사이에 있고, 빔전류가 줄면서 빔부하가
사라지는 과도과정과 일치한다.

---

## 2. 빔물리

| id | 문헌 | 이 분석에서 쓰는 부분 |
|---|---|---|
| `wiedemann2015` | Wiedemann, *Particle Accelerator Physics*, 4th ed., Springer 2015, DOI 10.1007/978-3-319-18317-6 (CC-BY) | longitudinal dynamics, synchrotron motion, beam parameters/emittance. 무료 PDF |
| `edwards1993` | Edwards & Syphers, *An Introduction to the Physics of High Energy Accelerators*, Wiley 1993 | betatron motion, phase stability, coherent instabilities |
| `lee2011` | Lee, *Accelerator Physics*, 3rd ed., World Scientific 2011, DOI 10.1142/8335 | synchrotron motion, beam-beam, action-angle |
| `chao2023` | Chao et al. (eds.), *Handbook of Accelerator Physics and Engineering*, 3rd ed., World Scientific 2023 | 공식·수치 사전 |
| `peggs2017` | Peggs & Satogata, *Introduction to Accelerator Dynamics*, Cambridge 2017 | 학부 수준 transverse/longitudinal dynamics |

### 빔 에너지 손실

```
dE/dt = -U0/T_rev
U0 = 1.242 MeV/turn, T_rev = 0.940 us  ->  1.32 GeV/ms
1%  (30 MeV)  : 22.7 us
2.8% (84 MeV) : 63.6 us   <- PLS-II RF acceptance (4.5 MV)
```

PLS-II 설계는 injection/lifetime을 위해 ΔE/E > 2.7%를 요구한다. RF acceptance
2.8%는 4.5 MV 기준 상한값이고, lattice momentum acceptance는 별도 공개값이
없어 이보다 작을 수 있다.

위 값은 RF 감쇠를 무시한 상수 손실률 기준이다. 실제 Vgap은 지수 감쇠하므로
손실률이 점점 커진다. 지수 감쇠(τ≈66 us)를 적분하면 2.8%까지 약 120 us가
걸린다. PLS-II의 lattice momentum acceptance는 RF acceptance(2.8%)보다 작고
단일 측정값이 공개되어 있지 않으므로, 실제 손실 시작은 120 us보다 이르다.

```
dE(t) = (U0/T_rev) * [t - tau*(1 - exp(-t/tau))]
```

관측 beam current 99% 시각 ~222 µs는 DCCT/scope 응답 지연을 포함한 값으로 본다.
PLS(2.5 GeV)의 1.35% 값은 PLS-II와 다른 머신이므로 사용하지 않는다.

---

## 3. MIS / MPS / BLM

PLS-II의 기본 장치보호 시스템은 **MIS(Machine Interlock System)**다. MIS는
진공도, 빔튜브/빔라인 온도, 냉각수 유량·온도, 방사선량(BLM), 게이트밸브 상태
등을 감시하고, 임계값을 넘으면 빔 permit을 끊어 RF를 OFF시켜 빔을 덤프한다.
이때 MPS 전원을 끄지는 않는다. **BLM은 모니터링 계측기**이며, 방사선량이
한도를 넘으면 MIS 입력이 된다.
**MPS는 마그넷 전원장치 보호 시스템**으로 MIS와 별개다. MPS 고장은 마그넷
전류를 끊어 빔 손실을 만들고, RF는 빔부하 감소로 뒤늦게 반응한다.

| id | 문헌 | 이 분석에서 쓰는 부분 |
|---|---|---|
| `pls2_mis2020` | Lee et al., "Fast Interlock System for Machine Protection in PLS-II", JKPS 76, 301 (2020), DOI 10.3938/jkps.76.301 | PLS-II fast interlock, beam shut-off time |
| `pls2_mis_hmi2006` | Park et al., "Development of Machine Interlock System HMI for PLS", EPAC2006 THPCH121 | MIS PLC 3계층 구조, local/central unit |
| `pls2_vacuum2015` | Ha & Park, "Design and Operation Experience of PLS-II Vacuum System", KEK seminar 2015 | thermocouple/gauge controller MIS 트립 사례 |
| `pls2_operation2019` | Kim, "Several Significant Fault Recoveries and the Operation Status of the PLS-II", ARW2019 | 고장 통계, fast orbit interlock 200 ms -> 10 ms |
| `forck_juas` | Forck, *Beam Instrumentation and Diagnostics*, GSI/JUAS | §8.3 BLM 기반 모니터링 |
| `zhukov2010` | Zhukov, "Beam Loss Monitors", BIW2010 THTNB01 | BLM 종류와 신호 |
| `holzer2016` | Holzer, "Beam Loss Measurement for Protection and Diagnostics", CAS 2016 | BLM 동적범위/시간응답 |
| `dehning2014` | Dehning, "Beam Instrumentation used for Machine Protection", JAS14 | BLM 시스템 |

MIS 트립을 곧 root cause로 기록하면 안 된다. MIS는 보호/트리거이고, 원인은
RF station, MPS, CONTROL, 빔라인, 진공 등에 따로 있을 수 있다.

---

## 4. PLS-II 운전 파라미터

| id | 문헌 | 값 |
|---|---|---|
| `pls2_status2016` | NAPAC2016 THPOA60, Status of PLS-II Operation | 3 GeV, 400 mA, 499.973 MHz, 3 SRF cavities, gap voltage 4.5 MV |
| `pls2_srf2012` | IPAC2012 WEPPC024, Preliminary Test of SRF Cavities for PLS-II | 4.5 MV, RF acceptance 2.8%, U0 1242 keV/turn |
| `pls2_srf2013` | IPAC2013 WEPWO041 | Q0 7.5e8–1.1e9 @ 2 MV, Qext 1.5–1.7e5, Vacc 2.35–2.40 MV/cavity (2013년 2대 운전) |
| `pls2_llrf2019` | arXiv:1910.01861 | 1.5 MV/cavity 운전(4.5 MV), 1.65 MV/cavity는 top-up 시험, forward 140–190 kW |
| `pls2_srf2015` | IPAC2015 WEPMN042 | 400 mA, optimal coupling, reflected power |
| `pls2_tfs2021` | Lee et al., J. Synchrotron Rad. 28, 1417 (2021), DOI 10.1107/S1600577521005889 | PLS-II TFS, bunch-by-bunch transverse feedback |
| `pls2_fofb2016` | Kim et al., IPAC2016 WEPOR004, DOI 10.18429/JACoW-IPAC2016-WEPOR004 | FOFB 96 BPM, 48 corrector/plane, 813 Hz |
| `pls2_kicker2012` | Hwang et al., IPAC2012 MOPPP056 | 4 kicker magnets, single thyratron, 6.8 us pulse |

### 4.1 내부 수직시험 기록 (문헌 인용 아님)

`2020-04-05` 500 MHz CESR-B type cavity vertical test (원본 xlsx: `20261127-022809-2204-1-vertical test-20200405.xlsx`). SN과 Date 셀은 비어 있다.

| 항목 | 값 |
|---|---:|
| R/Q | 89 Ω |
| Epk/Eacc | 2.5 |
| L | 0.3 m |
| Q0 (저전계) | 1.845e9 |
| Q0 @ 6.67 MV/m | ≈1.30e9 |
| Q0 @ 9.33 MV/m | ≈9.0e8 |
| 최고 Eacc | 11.06 MV/m @ Q0≈6.3e8 |
| X-ray 최대 | 14,800 mR/hr |
| tau (power) | 0.139–0.216 s |
| beta (저전계) | 1.769 |

인수 기준 `Q0 ≥ 1e9 at 6.67 MV/m (2.0 MV)`, `Q0 ≥ 5e8 at 9.33 MV/m (2.8 MV)`를 모두 통과했다. 단, 수직시험은 빔이 없고 β≈1.77의 약한 결합이므로, PLS-II 운전점(β≈5,000, Qext 1.5–1.7e5, 전압 시정수 ≈100 µs)의 감쇠 해석에 직접 쓰지 않는다. 상승/하강 스윕 Q0가 다르고(7.3 MV/m에서 9.3e8 vs 1.28e9), 내장 스크린샷 일부 파워 값은 시트 Step3 셀과 일치하지 않는다.

---

## 5. 측정 패턴과 원인 분류

### 5.1 Fast-scope 측정 패턴

| class | 의미 | 인용 topic |
|---|---|---|
| `rf_trip` | 3개 캐비티 동시 붕괴, cavity가 beam보다 ~300 µs 먼저 | `cavity_decay_after_rf_off`, `beam_energy_loss_after_rf_trip`, `mis_machine_interlock` |
| `beam_first_rf_trip` | beam이 먼저, cavity는 뒤에 붕괴. forward 유지 | `cavity_figures_of_merit`, `transverse_instability`, `beam_dump_causes` |
| `beam_only` | cavity 무변화. RF 무관 | `transverse_instability`, `mis_machine_interlock`, `beam_dump_causes` |
| `single_cavity_quench` | 1개 캐비티만 붕괴, cavity가 먼저 | `cavity_decay_after_rf_off`, `padamsee2008`, `mis_machine_interlock` |
| `mixed` | 순서 불명확 또는 2개 캐비티 붕괴(dual quench로 단정하지 않음) | `beam_dump_causes`, `mis_machine_interlock` |

`dual_cavity_quench`는 PLS-II에서 관측된 적이 없어 제거했다. 2개 캐비티만
붕괴한 12건은 `mixed`로 두고 RF station/common point 로그를 확인하도록 한다.

### 5.2 원인 분류 (경험적, 교과서 아님)

| key | 의미 | 관측 단서 |
|---|---|---|
| `rf_single_station` | RF station(LLRF-HPRF-CM) 1개 고장 | 한 station만 급격 붕괴, 나머지 sag, 이후 MIS/진공/He 압력 |
| `rf_common` | RF 공통점 고장(전 station 동시 off) | 3개 cavity 동시 붕괴, BPR01/CM 인터록 가능 |
| `mps` | 대용량 마그넷 전원(MPS) 고장 | beam current가 RF보다 먼저, 수백 ms |
| `mis` | MIS 트립(진공/온도/냉각/방사선) | RF off 전 진공/온도/방사선/flow 상승 |
| `control` | TFS/FOFB/feedback 불안정 | beam current 먼저, RF 유지/부분 sag |
| `beamline` | 빔라인 온도/진공/냉각/방사선 | MIS 트립, RF 정상 |
| `mechvac` | CM 진공/He 압력/냉각 | CM 인터록 + beam loss 동시 가능 |
| `kicker` | Kicker 펄스전원(thyratron) 고장 | 전체 빔 급격 손실, RF 정상 |
| `magnet` | 마그넷 temp/water flow | MPS와 같은 패턴 |
| `psi` | 인명/방사선 안전 인터록 | PSI 로그, RF/MPS 정상 |
| `linac` | Linac/입사 계통 | 입사 중단, 저장빔 RF 정상 |
| `external` | power sag, earthquake, unknown | 외부 이벤트 로그 |

측정 class와 원인은 1:1이 아니다. fast scope만으로 root cause를 확정하지 말고
MIS/MPS/LLRF/CG 로그를 함께 본다.

---

## 6. MCP에서의 위치

- `postmortem_get_glossary(topic="cavity_decay_after_rf_off")` 등
  7개 physics topic이 equations/citations/caveats를 반환한다.
  `beam_dump_causes`는 `categories[]`와 `mapping_ko`를 함께 반환한다.
- `postmortem_get_glossary()`는 도메인 glossary와 함께 `physics` 라이브러리를
  반환한다. `physics`에는 `internal_test_notes`(2020-04-05 수직시험 1건)가
  포함되며, 이는 문헌 citation이 아니라 내부 시험 기록이다.
- `postmortem_get_glossary(topic="list")`는 domain 9 + physics 7 = 16개
  addressable topic index를 반환한다.
- `postmortem_analyze_waveform(event_id)`는 `waveform.physics`에
  class, summary_ko, chain_ko, steps_ko, timeline, checks, citations, caveats를 넣는다.
- `postmortem_analyze_quench(event_id)`도 `waveform.physics`를 포함한다.

Per-event 저장본:

```
data/features/event_physics_interpretations.jsonl
```

313줄. 각 줄은 `event_id`, `fault_type`, `class`, `physics`를 가진다.
`event_physics_summary.csv`는 같은 내용을 한 줄 요약(class, tau, delay,
citation ids)으로 담은 313행 표다.
재생성:

```bash
venv/bin/python scripts/build_event_physics.py \
  --features data/features/waveform_features.jsonl \
  --out data/features/event_physics_interpretations.jsonl
```

---

## 7. 주의

- 수치는 fast scope 313건에서 코드로 계산한 값이다. LLM이 만든 숫자가 아니다.
- Q0, Qext, R/Q는 문헌/측정값이며 이벤트별 실측값이 아니다.
- 1 Hz 아카이브는 RF off와 beam off를 같은 샘플에 묶는 경우가 대부분이라
  인과 순서를 줄 수 없다. 313건 중 116/118 검증 이벤트가 same-sample이었다.
- Class는 측정 패턴이며 root cause 라벨과 1:1 대응하지 않는다.
- MIS는 보호/트리거이고, MPS와 혼동하지 않는다. BLM은 모니터링이다.
- turn-by-turn BPM이 없으므로 ms급 횡방향 불안정 성장은 확정할 수 없다.
- PLS(2.5 GeV)와 PLS-II(3 GeV)는 다른 머신이다. PLS 수치를 PLS-II에 쓰지 않는다.
