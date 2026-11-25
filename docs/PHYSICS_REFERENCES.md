# 빔덤프 물리 참고문헌 정리

작성 2026-11-25. MCP의 `config/physics_references.json`과
`src/features/physics.py`가 인용하는 교과서/강의록/논문을 사람이 읽기 쉽게
정리한 문서다. MCP 응답의 `physics.citations`는 이 표의 `id`와 `locator`를
그대로 사용한다.

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

PLS-II 값(Qext 1.5–1.7e5, Q0 7.5e8–1.1e9, V_c 1.45–1.65 MV, I 300–400 mA)을
넣으면:

| 양 | 값 |
|---|---:|
| U | 3.3–4.3 J |
| Pc | 12–16 W |
| Pbeam | 110–124 kW |
| beta | 4,400–7,300 |
| tau_Q | 95–108 µs |
| tau_beam | 30–35 µs |
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
5%  (150 MeV) : 113 us
10% (300 MeV) : 227 us
```

캐비티 전압이 tau로 지수 감쇠한다고 보고 적분하면:

```
dE(t) = (U0/T_rev) * [t - tau*(1 - exp(-t/tau))]
```

PLS momentum acceptance 1.35%(2.5 GeV 기준)를 적용하면 물리적 빔 손실은
cavity 90% 후 약 75 µs에 시작한다. 관측 beam current 99% 시각 ~222 µs는
DCCT/scope 응답 지연을 포함한 값으로 본다.

---

## 3. 기계보호 / BLM

| id | 문헌 | 이 분석에서 쓰는 부분 |
|---|---|---|
| `forck_juas` | Forck, *Beam Instrumentation and Diagnostics*, GSI/JUAS | §8.3 Machine protection using beam loss detectors |
| `zhukov2010` | Zhukov, "Beam Loss Monitors: Physics, Simulations and Applications", BIW2010 THTNB01 | BLM 종류, 시뮬레이션, MPS 인터페이스 |
| `holzer2016` | Holzer, "Beam Loss Measurement for Protection and Diagnostics", CAS 2016 | BLM 동적범위, LHC BLM 100–200 µs 반응 |
| `dehning2014` | Dehning, "Beam Instrumentation used for Machine Protection", JAS14 | LHC BLM 시스템 |

핵심: BLM/MPS는 µs–ms 규모다. 1 Hz 아카이브는 원인과 결과를 같은 샘플에
묶어버리므로, 순서를 알려면 fast scope나 turn-by-turn 계측이 필요하다.

---

## 4. PLS-II 운전 파라미터

| id | 문헌 | 값 |
|---|---|---|
| `pls2_status2016` | NAPAC2016 THPOA60, Status of PLS-II Operation | 3 GeV, 400 mA, 499.973 MHz, 3 SRF cavities, 4.5 MV |
| `pls2_srf2013` | IPAC2013 WEPWO041 | Q0 7.5e8–1.1e9, Qext 1.5–1.7e5, Vacc 2.35–2.40 MV |
| `pls2_llrf2019` | arXiv:1910.01861 | 1.5–1.65 MV/cavity, forward 140–190 kW, reflected 1–5 kW |
| `pls2_srf2015` | IPAC2015 WEPMN042 | 400 mA, optimal coupling, reflected power |
| `pls2_momentum2001` | APAC2001 WEP021 | momentum acceptance 1.35% at 2.5 GeV |

---

## 5. 순차 유형과 인용

| class | 의미 | 인용 topic |
|---|---|---|
| `rf_trip` | 3개 캐비티 동시 붕괴, cavity가 beam보다 ~300 µs 먼저 | `cavity_decay_after_rf_off`, `beam_energy_loss_after_rf_trip`, `machine_protection_timescales` |
| `beam_first_rf_trip` | beam이 먼저, cavity는 뒤에 붕괴. forward 유지 | `cavity_figures_of_merit`, `transverse_instability`, `beam_dump_sequence_classes` |
| `beam_only` | cavity 무변화. RF 무관 | `transverse_instability`, `machine_protection_timescales` |
| `single_cavity_quench` | 1개 캐비티만 붕괴, cavity가 먼저 | `cavity_decay_after_rf_off`, `padamsee2008` |
| `dual_cavity_quench` | 2개 캐비티 붕괴, cavity가 먼저 | `cavity_decay_after_rf_off`, `padamsee2008` |
| `mixed` | 순서 불명확 | `beam_dump_sequence_classes` |

---

## 6. MCP에서의 위치

- `postmortem_get_glossary(topic="cavity_decay_after_rf_off")` 등
  7개 physics topic이 equations/citations/caveats를 반환한다.
- `postmortem_get_glossary()`는 도메인 glossary와 함께 `physics` 라이브러리를
  반환한다.
- `postmortem_analyze_waveform(event_id)`는 `waveform.physics`에
  class, summary_ko, timeline, checks, citations, caveats를 넣는다.
- `postmortem_analyze_quench(event_id)`도 `waveform.physics`를 포함한다.

Per-event 저장본:

```
data/features/event_physics_interpretations.jsonl
```

313줄. 각 줄은 `event_id`, `fault_type`, `class`, `physics`를 가진다.
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
- Class는 서술적 분류이며 root cause 라벨과 1:1 대응하지 않는다.
- turn-by-turn BPM이 없으므로 ms급 횡방향 불안정 성장은 확정할 수 없다.
