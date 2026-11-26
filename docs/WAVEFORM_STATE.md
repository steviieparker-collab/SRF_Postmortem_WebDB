# Waveform 페이지 상태 (2026-11-26)

Waveform Viewer(`/waveforms`)의 이미지 생성 방식과 현재 데이터 범위를 기록한다.
코드는 `origin/main`에 있고, **이미지·DB·parquet은 gitignored라 repo에 없다.**

## 1. 범위와 수량

- 대상: **merged parquet이 있는 이벤트 전체** (기존 2025-05-29 컷오프 제거)
- 최종 **312건 / 936장** (forward / cavity / reflect 각 1장)
- 연도별: 2023 **20**, 2024 **84**, 2025 **88**, 2026 **120**
- 최신: `20261124_010107`, 최초: `20231122_134127`
- API: `GET /api/waveforms` → `total: 312`, `year_counts` 위와 동일

## 2. 제외 5건과 사유

| 이벤트 | 사유 |
|---|---|
| `20260525_111100` | merged parquet 없음. NAS CSV는 있으나 `tek1026.csv`의 mtime이 이벤트 시각과 7일 불일치 |
| `20250907_114807` | parquet 손상: `Reflect_SRF3_v`에 inf 16개 (상세 API 500 이력) |
| `20261003_000000` | 수기 이벤트, parquet/CSV/이미지 없음 |
| `20260603_083621` | 수기 이벤트, parquet/CSV/이미지 없음 |
| `20251130_000350` | 수기 이벤트, parquet/CSV/이미지 없음 |

대전제: **확실한 파형만** waveform 페이지에 넣는다. 위 5건은 데이터가 없거나
시각 정합성이 깨져 제외했다. `20250907_114807`은 parquet이 정상화되면 재포함 가능.

## 3. 렌더러 규격

파일: `src/pipeline/waveform_images.py` → `_render_merged()`

- **아날로그**: 4개 트레이스를 **하나의 y축에 겹침**
  - baseline = narrow ±1ms 창 앞 10%의 중앙값
  - `norm = v / baseline` → 시작값 **1.0** (0 기준선 공통)
  - `ax_a.plot(..., lw=4.0)` (굵기 2배 적용)
  - y축: `min(lo,0)-0.05` ~ `max(peak,1.15)+0.05`
    - **상한 캡 없음(동적)** — reflect spike가 잘리지 않음
    - 저 baseline 채널은 spike가 20배 이상으로 보일 수 있음
- **디지털**: 아날로그 **상단** 패널, 원본 scope CSV와 동일한 세트
  - Forward: `INT_*`/`RDY_KSU*` **10개**
  - Cavity: `RDY_CM`/`HE-PR_CM`/`ARC_CM`/`QUEN_CM`/`VAC_CM` **15개**
  - Reflect: **없음**
  - `gap=1.0`, `amp=0.6`, 라벨 12pt, bbox pad 0.1 (박스가 거의 맞닿음)
- **Figure**: `19.5 × 9.3 in`, 150 dpi → `~2411–2424 × 1371` (16:9)
- **전처리**: `nan/±inf → 0`
- x축 ±1.0ms, t=0 세로 점선, 1.0/0.0 가로 점선
- 2023~2024 parquet은 5,000~25,000행(10~20µs)이라 좁은 창에서 계단형으로 보인다.

## 4. 데이터 위치

| 위치 | 내용 |
|---|---|
| repo (`origin/main`) | 코드·템플릿·스크립트·문서. 이미지/DB/config 없음 |
| 서버 `~/SRF_Postmortem_WebDB` | `db/events.db`, `data/merged/*.parquet`(313), `data/waveforms/*`(936), `config/config.yaml`, attachments, logs |
| NAS `/mnt/{forward,cavity,reflect}` | 원본 CSV/PNG, 과거 `*_range2.jpg` |

- 매니페스트: `data/waveforms/backfill_manifest.json` (312건, excluded 5건)
- 서버 HEAD: `dec280f`, `origin/main`과 동기화 완료

## 5. 재생성 방법

단일 이벤트:

```python
import sys; sys.path.insert(0, "src")
from pathlib import Path
from pipeline.waveform_images import generate_image_set

generate_image_set("20261124_010107",
                   Path("data/merged/event_20261124_010107.parquet"),
                   force=True)
```

전체 parquet 이벤트:

```python
import glob, os, sys
from pathlib import Path
sys.path.insert(0, "src")
from pipeline.waveform_images import generate_image_set

EXCLUDE = {"20260525_111100", "20250907_114807",
           "20261003_000000", "20260603_083621", "20251130_000350"}
for p in sorted(glob.glob("data/merged/event_*.parquet")):
    eid = os.path.basename(p)[6:-8]
    if eid in EXCLUDE:
        continue
    generate_image_set(eid, Path(p), force=True)
```

## 6. 운영 상태

- `src/web/server.py` startup에서 `start_monitor()` 자동 실행
  - `PIPELINE_AUTOSTART=0`으로 비활성화
  - 이미 실행 중이면 중복 시작하지 않음
- 갤러리 날짜 컷오프(`_WFG_MIN_TS`) 제거 → 2023/2024도 노출
- 뷰어: 휠 줌(1~8x), 드래그 팬, 모바일 핀치 줌, 더블클릭/더블탭 초기화

## 7. 알려진 후속 작업

**신규 이벤트는 아직 통합 스타일이 아니다.**
`src/web/pipeline_manager.py`의 `_full_worker()`가 grouper 이후
`ensure_image_set(..., sources=scope_sources)`로 `data/graph_only`의
raw CSV 1µs 이미지를 복사한다. 따라서 새 이벤트는 기존 절대값 스케일
(raw-CSV) 스타일로 저장되어, parquet 정규화 스타일로 통일된 312건과 섞인다.

통일하려면 해당 호출을 아래처럼 바꿔 parquet 렌더러를 쓰게 한다:

```python
# before
ensure_image_set(event_id, merged_path, sources=scope_sources)
# after
ensure_image_set(event_id, merged_path, force=True)
```

`scope_sources`/`SCOPE_INDEX`는 더 이상 필요 없고, `data/graph_only`는
이메일 첨부용으로만 남긴다. 변경 후 `srf-web` 재시작 필요.

## 8. SSH 메모

WSL의 `~/.ssh/id_ed25519`가 passphrase 보호 + ssh-agent 미실행 상태였다.
이번 배포는 Windows OpenSSH(`/mnt/c/Windows/System32/OpenSSH/ssh.exe`)로 수행.
다음 세션에서 WSL ssh를 쓰려면 `ssh-add` 또는 암호 구문 입력이 필요하다.
