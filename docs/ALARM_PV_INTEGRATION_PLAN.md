# SRF 알람 대시보드 + 빔덤프 이벤트 PV 통합 계획 (v3, 확정)

작성일: 2026-11-11
상태: **계획 확정 / 구현 전**

v3 변경:
- Archiver PV 인벤토리 캐시/일일 검증 **삭제** → pv-explorer와 동일한 **드롭다운 선택만** 사용
- 대시보드는 postmortem과 같은 앱/같은 도메인의 서브 페이지
- CSV를 초기 기준으로 대시보드 구성, 이후 수정은 대시보드에서 (비밀번호 필요)
- 복구(recovery) 메시지 **표시하지 않음** — 알람 발생(onset) 기록만 중요
- 알람 모니터는 **subprocess**, postmortem 종료 시 함께 종료
- 과거 이벤트는 전부 "알람 데이터 없음", 신규 이벤트부터 적용

---

## 1. 목표

### 1.1 알람 대시보드 (신규 서브 페이지)

- SRF 전체 인터록 알람 524개의 **현재 상태**를 한 화면에서 본다.
- 각 알람에 대응하는 **PLS Archiver PV(그래프용)** 를 지정/수정한다.
- 이 매핑이 postmortem 이벤트 페이지의 그래프 설정이 된다.
- 모니터 실행 상태, 마지막 수신 시각, 알람/정상/미매핑 개수를 보여준다.
- postmortem과 대시보드는 같은 앱의 서로 다른 페이지이며, 양쪽 상단에 상호 링크 버튼을 둔다.

### 1.2 빔덤프 이벤트 페이지 (기존 확장)

- 덤프 전후(`t0-60s ~ t0+10s`)에 **발생한** 알람 메시지를 표시한다.
- 각 알람에 매칭된 Archiver PV 그래프를 표시한다.
- 사용자가 임의의 PV를 추가해 같은 시간축에서 선후 관계를 비교한다.
- CSV 파형(ms)과 Archiver(초)의 해상도 차이를 UI에 명시한다.

---

## 2. 확정 결정

| # | 결정 |
|---|---|
| 1 | **알람 극성**: OPI `<rules>`의 red/green 조건을 기준으로 한다. |
| 2 | **t0**: 빔전류 최대값 이후 99% 아래로 처음 떨어지는 시각. |
| 3 | **알람 범위**: t0 직전/직후 모두 표시. t0 전부터 활성이면 `active_at_start` 배지. |
| 4 | **복구 메시지**: 표시하지 않는다. 이벤트에는 "이 알람이 발생했다"만 기록. |
| 5 | **과거 이벤트**: 재연결/소급 복원 없음. 전부 "알람 데이터 없음". |
| 6 | **Start/Stop Monitor**: postmortem 설정의 버튼이 CSV 파이프라인 + 알람 모니터를 함께 제어. |
| 7 | **알람 모니터**: 별도 subprocess. postmortem 종료 시 함께 종료. |
| 8 | **매핑 편집**: 대시보드에서만. postmortem은 읽기 전용. |
| 9 | **PV 선택 방식**: pv-explorer와 동일한 **Archiver hierarchy 드롭다운**만 사용. 직접 입력/일일 존재 검증 없음. |
| 10 | **대시보드 위치**: postmortem과 같은 FastAPI 앱, 같은 도메인의 `/alarm-dashboard`. |
| 11 | **인증**: 기존 비밀번호. 대시보드 편집/CSV 업로드에 비밀번호 필요. 추후 보안 고도화. |
| 12 | **비교 차트 최대 PV**: 8개. |

---

## 3. 전체 아키텍처

```
┌──────────────────────────────┐        ┌──────────────────────────────┐
│  Postmortem (FastAPI)        │        │  Alarm Dashboard (같은 앱)    │
│  /events/{id}                │◄──────►│  /alarm-dashboard            │
│  /settings                   │  링크   │  - 현재 알람 상태             │
│  Start/Stop Monitor ─────────┼──┐     │  - PV 매핑 편집 (드롭다운)     │
└──────────────┬───────────────┘  │     └──────────────┬───────────────┘
               │ spawn/stop       │                    │ read/write
               ▼                  ▼                    ▼
      ┌─────────────────────────────────────────────────────────┐
      │  alarm-monitor (subprocess, pyepics, srf-hprf)           │
      │  - 524 PV camonitor                                     │
      │  - 초기 상태 1회 + 전이 기록                             │
      │  - heartbeat / status 기록                              │
      │  - postmortem 종료 시 함께 종료 (PDEATHSIG + shutdown)   │
      └───────────────────────────┬─────────────────────────────┘
                                  │
                                  ▼
      ┌─────────────────────────────────────────────────────────┐
      │  events.db (SQLite WAL)                                 │
      │  alarm_pv_catalog / alarm_transitions / alarm_last_state│
      │  alarm_monitor_status / event_alarms / mapping_history  │
      └───────────────────────────┬─────────────────────────────┘
                                  │
             ┌────────────────────┴────────────────────┐
             ▼                                         ▼
   ┌──────────────────┐                     ┌──────────────────────┐
   │ PLS Archiver      │                     │ CSV pipeline          │
   │ 그래프 PV 조회     │                     │ 이벤트 생성/DB import  │
   └──────────────────┘                     └──────────────────────┘
```

핵심:
- 알람 상태 PV 524개는 Archiver에 대부분 없으므로 CA 모니터가 유일한 소스다.
- 그래프 PV는 Archiver에서 조회한다.
- **매핑 정본은 DB**이고, CSV는 초기 seed + import/export 형식이다.
- 대시보드와 postmortem은 같은 DB를 공유한다.

---

## 4. 데이터 모델

### 4.1 `alarm_pv_catalog` (정본, 대시보드에서 편집)

```sql
CREATE TABLE IF NOT EXISTS alarm_pv_catalog (
    pv                TEXT PRIMARY KEY,
    description       TEXT NOT NULL,
    subsystem         TEXT NOT NULL,        -- KSU1/2/3, CRM1/2/3, CRG
    source_file       TEXT,
    graph_pv          TEXT,                 -- ';' 구분, 없으면 NULL
    graph_description TEXT,
    match_type        TEXT,
    confidence        TEXT,
    alarm_state       TEXT DEFAULT '',      -- '0', '1', 'complex'
    normal_state      TEXT DEFAULT '',
    enabled           INTEGER NOT NULL DEFAULT 1,
    severity          TEXT DEFAULT 'medium',
    updated_by        TEXT DEFAULT '',
    updated_at        TEXT NOT NULL DEFAULT (datetime('now'))
);
```

- 초기값은 `CSS/alarm_archiver_map.csv`를 seed로 사용한다.
- `graph_pv`가 `;`로 여러 개면 모두 그래프에 표시한다 (예: `ST750_2 → BCL01;BCL03`).
- `alarm_state`/`normal_state`는 OPI rules에서 자동 추출. `complex`는 수동 검토.

### 4.2 `alarm_transitions`

```sql
CREATE TABLE IF NOT EXISTS alarm_transitions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    pv          TEXT NOT NULL,
    ts_utc      TEXT NOT NULL,
    value       TEXT,
    prev_value  TEXT,
    state       TEXT NOT NULL,       -- alarm / normal / unknown
    source      TEXT NOT NULL DEFAULT 'ca',
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_alarm_tr_ts    ON alarm_transitions(ts_utc);
CREATE INDEX IF NOT EXISTS idx_alarm_tr_pv_ts ON alarm_transitions(pv, ts_utc);
```

- 모니터 시작 시 524개 초기 상태를 1회 기록.
- 이후 값이 바뀔 때만 기록 (0.3초 디바운스).
- normal 전이도 상태 추적을 위해 기록하지만, 이벤트 페이지에는 표시하지 않는다.

### 4.3 `alarm_last_state` (대시보드 현재 상태)

```sql
CREATE TABLE IF NOT EXISTS alarm_last_state (
    pv          TEXT PRIMARY KEY,
    value       TEXT,
    state       TEXT,                -- alarm / normal / unknown / complex
    ts_utc      TEXT,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
```

### 4.4 `alarm_monitor_status` (헬스체크)

```sql
CREATE TABLE IF NOT EXISTS alarm_monitor_status (
    id                INTEGER PRIMARY KEY CHECK (id = 1),
    running           INTEGER NOT NULL DEFAULT 0,
    pid               INTEGER,
    started_at        TEXT,
    last_heartbeat    TEXT,
    pv_count          INTEGER DEFAULT 0,
    connected_count   INTEGER DEFAULT 0,
    transition_count  INTEGER DEFAULT 0,
    error             TEXT DEFAULT ''
);
```

### 4.5 `event_alarms` (이벤트 스냅샷, onset만)

```sql
CREATE TABLE IF NOT EXISTS event_alarms (
    event_id          TEXT NOT NULL,
    pv                TEXT NOT NULL,
    description       TEXT,
    subsystem         TEXT,
    graph_pv          TEXT,          -- 이벤트 생성 시점의 매핑 스냅샷
    graph_description TEXT,
    first_seen_utc    TEXT,
    first_seen_rel    REAL,          -- t0 기준 초
    active_at_start   INTEGER DEFAULT 0,
    severity          TEXT,
    PRIMARY KEY (event_id, pv)
);
```

- 복구 시각/최종 상태는 저장하지 않는다 (결정 4).
- 매핑은 이벤트 생성 시점에 스냅샷. 이후 대시보드 편집은 신규 이벤트에만 적용.

### 4.6 `alarm_mapping_history` (감사 로그, 선택)

```sql
CREATE TABLE IF NOT EXISTS alarm_mapping_history (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    pv            TEXT NOT NULL,
    old_graph_pv  TEXT,
    new_graph_pv  TEXT,
    old_enabled   INTEGER,
    new_enabled   INTEGER,
    changed_by    TEXT,
    changed_at    TEXT NOT NULL DEFAULT (datetime('now'))
);
```

---

## 5. 알람 대시보드

### 5.1 위치/인증

- `GET /alarm-dashboard` — postmortem과 같은 FastAPI 앱, 같은 도메인.
- postmortem 상단 nav에 **Alarm Dashboard** 링크.
- 대시보드 헤더에 **Postmortem** 링크.
- 기존 비밀번호(`_check_password`) 사용. 편집/CSV 업로드 시 비밀번호 필요.

### 5.2 화면 구성

```
┌─ SRF Alarm Dashboard ─────────────────────────── [Postmortem ↗] ─┐
│ Monitor: ● Running  last heartbeat 3s  PV 524  connected 524     │
│ Summary: 총 524 · Alarm 3 · Normal 521 · 미매핑 149              │
├──────────────────────────────────────────────────────────────────┤
│ [Subsystem ▼] [State ▼] [Mapping ▼] [검색________] [CSV ↓/↑]     │
├──────────────────────────────────────────────────────────────────┤
│ subsystem│ alarm PV │ description │ state │ graph PV │ mapped │ … │
│ KSU3     │ ...      │ Filament…   │ ALARM │ AI17     │ ✅     │   │
│ CRM1     │ ...      │ Ion pump…   │ normal│ AIP01p   │ ✅     │   │
│ CRG      │ ...      │ Compressor… │ normal│ —        │ ⚠      │   │
└──────────────────────────────────────────────────────────────────┘
│ Recent transitions (last 50)                                      │
└──────────────────────────────────────────────────────────────────┘
```

- 자동 새로고침 3~5초. 모니터 정지 시 `stale` 배지 + 마지막 heartbeat.
- 필터: subsystem, state, mapping(mapped/unmapped), enabled, 검색(PV/description).
- 요약: total / alarm / normal / unknown / unmapped / complex.
- 최근 전이 50건.

### 5.3 매핑 편집 (드롭다운만)

- 행의 `Edit` → 모달:
  ```
  Alarm: SR:RF:KSU3:AI_LOW_FAULT40
  Description: KSU Water Flow too low
  Current state: normal (0)

  Graph PV:
    [Class ▼] [Sub ▼] [PV ▼ / 검색]
    선택: SR:RF:KSU3:AI24 (KSU#3-HVPS WatFl)
    [+ Add PV]  (최대 4개, ';' 저장)

  [Test] 최근 1시간 데이터: 720 points, last 16:02 (OK)
  [Cancel] [Save]
  ```
- **PV는 pv-explorer와 동일한 Archiver hierarchy 드롭다운에서만 선택**한다.
  - `/api/pv-hierarchy` → class 목록
  - `/api/pv-list?class=SR&sub=RF_KSU` → PV + description 목록
  - 선택 시 description 자동 채움
- 직접 입력 필드 없음. 일일 존재 검증/인벤토리 캐시 없음.
- `Test`는 선택한 PV로 `/api/pv-data`를 호출해 최근 1시간 데이터 존재/최신성만 확인.
- 저장: `alarm_pv_catalog` update + `alarm_mapping_history` insert.

### 5.4 CSV 초기값 / import / export

- **초기 seed**: `CSS/alarm_archiver_map.csv`를 `data/alarm/alarm_pv_catalog.csv`로 복사하고, DB가 비어 있으면 1회 적재한다.
- 이후 편집은 DB에 저장한다. CSV는 **내보내기/가져오기 형식**으로만 사용한다.
  - 내보내기: 현재 DB 카탈로그를 CSV로 다운로드.
  - 가져오기: CSV 업로드 → diff 확인 → 일괄 반영 (비밀번호 필요).
- CSV는 정본이 아니다. DB가 정본이다.
- (사용자 확인 필요) CSV 파일 자체를 계속 덮어쓰는 방식이 필요하면 별도로 논의.

### 5.5 복구 메시지

- 표시하지 않는다 (결정 4).
- 대시보드/이벤트 페이지 모두 알람 발생(onset)만 보여준다.
- `alarm_transitions`에는 normal 전이도 남지만 UI에는 노출하지 않는다.

---

## 6. 알람 모니터

### 6.1 실행 구조

- `src/alarm/monitor.py` — CA 구독 + 전이 기록.
- postmortem 웹 프로세스가 **subprocess**로 spawn:
  ```
  python -m src.alarm.monitor --config config/config.yaml
  ```
- `src/alarm/manager.py` — start/stop/status, PID 파일, heartbeat 확인.
- Linux `PR_SET_PDEATHSIG(SIGTERM)` + FastAPI shutdown 훅으로 postmortem 종료 시 함께 종료.
- 로그: `logs/alarm_monitor.log`.
- 웹 시작 시 남은 PID 파일을 확인해 고아 프로세스를 정리한다.

### 6.2 Start/Stop Monitor 연동

기존 `src/web/pipeline_manager.py`의 `start_monitor()` / `stop_monitor()`에 연결:

```
start_monitor():
    existing CSV pipeline start
    alarm_manager.start()      # 실패해도 CSV pipeline은 계속
    status["alarm_monitor"] = ...

stop_monitor():
    alarm_manager.stop()       # SIGTERM → 10s 대기 → SIGKILL
    existing CSV pipeline stop
```

- 설정 페이지에 알람 모니터 상태 표시:
  - `Alarm Monitor: Running (pid 1234) · 524 PV · connected 524 · heartbeat 3s`
  - 실패 시: `Alarm Monitor: Error — pyepics 없음 / CA 연결 실패`
- Start Monitor 하나로 둘 다 시작, Stop Monitor 하나로 둘 다 중지.

### 6.3 모니터 동작

1. 카탈로그에서 `enabled=1` PV를 읽는다.
2. `caget`로 초기 상태를 읽고 `alarm_transitions` + `alarm_last_state`에 기록.
3. `epics.camonitor`로 구독. 콜백:
   - 이전 값과 같으면 무시
   - `alarm_state`/`normal_state`와 비교해 state 결정
   - 0.3초 디바운스
   - 큐 → writer 스레드가 배치 insert/upsert
4. 5초마다 `alarm_monitor_status` heartbeat 갱신.
5. CA 연결 수/끊김을 집계해 status에 반영.

### 6.4 CA 환경

```
EPICS_CA_ADDR_LIST="192.168.1.6 192.168.1.49 141.223.105.2 141.223.105.126 141.223.105.142 141.223.105.145 141.223.105.149 141.223.105.213"
EPICS_CA_AUTO_ADDR_LIST=NO
```

- `requirements.txt`에 `pyepics>=3.5` 추가.
- 운영 `srf-hprf`의 `venv`에 설치.
- CA는 내부망에서만 가능하므로 모니터는 `srf-hprf`에서만 실행.

### 6.5 폴라리티

- OPI `<rules>`에서 red/green 조건을 파싱해 `alarm_state`/`normal_state`를 채운다.
- 예: `pv0>=-1 && pv0<0` → 0=alarm, `pv0>=1 && pv0<2` → 1=normal.
- **LED 위젯 주의**: LED는 `background_color`가 아니라 rule의 대상인 `off_color`가
  실제 기본 상태다. CRM Readychain LED(47×3=141개)는 기본 `off_color`가 Major red이고,
  `1`=green, `-1`=red(120개) 또는 dark green(21개)이다. dark green `(0,100,0)`도
  normal로 판정한다. 이 처리를 하지 않으면 CRM 설비의 실제 빨강 상태가 unknown(회색)이 된다.
- `complex`는 `:ACTUAL_STATE` 요약 PV 3개뿐이며 `enabled=0`으로 제외한다.
  (`KSUn_Status.opi`의 7개 LED 중 하나를 고르는 상태 선택자)
- 대시보드 요약은 Unknown/Complex 카드를 쓰지 않는다. 내부 unknown은 NaN/무효값용
  안전장치로만 남긴다.
- 폴라리티 CSV를 바꾼 뒤에는 `scripts/refresh_alarm_polarity.py`로 기존 행을 갱신하고
  알람 모니터를 재시작한다 (모니터는 시작 시 규칙을 메모리에 로드한다).

---

## 7. 이벤트 연결

### 7.1 신규 이벤트

`import_job.py`의 `create_event()` 직후:

```
after_event_created(event_id, timestamp):
    t0 = resolve_t0(event_id, timestamp)      # 빔전류 하강 시작
    window = [t0 - 60, t0 + 10]
    rows = query_alarm_onsets(conn, window)   # alarm 전이만
    save_event_alarms(event_id, rows)         # 매핑 스냅샷 포함
    schedule_alarm_series_fetch(event_id, rows)  # 백그라운드
```

- `resolve_t0`은 현재 `server.py`의 `_event_t0` 로직을 `src/alarm/timebase.py`로 옮겨 공용 사용.
- Archiver fetch는 이벤트 생성 응답을 막지 않도록 백그라운드.
- `event_alarms`에는 그 시점의 `graph_pv`/`graph_description`을 저장.

### 7.2 윈도우와 active-at-start

- `[t0-60, t0+10]`.
- 윈도우 시작 시점 상태는 "윈도우 이전 마지막 전이"로 결정.
- 윈도우 전부터 alarm이면 `active_at_start=1`로 표시하고, 첫 전이는 윈도우 밖일 수 있음을 UI에 표시.
- 복구 전이는 무시.

### 7.3 과거 이벤트

- 모니터 시작 전 이벤트는 `alarm_transitions`가 없으므로 "알람 데이터 없음".
- 실운영 데이터를 소급 재구성하지 않는다 (결정 5).
- 예외: 데모/검증용으로 특정 과거 이벤트에 확인된 알람을 붙이는 백필 스크립트가 있다.
  `event_alarms` 행과 사이드카를 라이브 경로와 동일한 형식으로 쓰고, t0와 그래프 PV
  시계열은 PLS Archiver에서 가져온다. 알람 발생 시각은 그래프 PV의 최대 편차 지점으로
  추정하며 사이드카에 `"source": "backfill"`로 표시된다.

  ```bash
  venv/bin/python scripts/backfill_event_alarm.py 20260623_110438 \
      --alarm-pv SR:RF:KSU3:AI_LOW_FAULT40
  ```

  예: 2026-06-23 KSU3 HVPS water-flow 인터록 → `SR:RF:KSU3:AI24` 유량이 t0에서
  38.5 → 4.0 L/min으로 떨어지는 실제 Archiver 데이터가 표시된다.

---

## 8. API 설계

### 8.1 대시보드

```http
GET /api/alarm/state?subsystem=KSU3&state=alarm&mapping=unmapped&q=filament
```
```json
{
  "monitor": {"running": true, "last_heartbeat": "...", "pv_count": 524, "connected_count": 524},
  "summary": {"total": 524, "alarm": 3, "normal": 521, "unknown": 0, "unmapped": 149},
  "items": [
    {
      "pv": "SR:RF:KSU3:AI_LOW_FAULT33",
      "description": "KLY Filament voltage too low",
      "subsystem": "KSU3",
      "value": "0",
      "state": "alarm",
      "ts_utc": "...",
      "graph_pv": ["SR:RF:KSU3:AI17"],
      "graph_description": ["KSU#3-FIL.V"],
      "enabled": true
    }
  ]
}
```

```http
POST /api/alarm/catalog/{pv}
{"graph_pv": "SR:RF:KSU3:AI24", "enabled": true, "severity": "high", "alarm_state": "0"}

GET  /api/alarm/catalog/export        → CSV
POST /api/alarm/catalog/import        → CSV 업로드 (diff 후 적용)
GET  /api/alarm/transitions?since=... → 최근 전이
GET  /api/alarm/monitor/status        → 모니터 상태
```

- PV 선택은 기존 `/api/pv-hierarchy`, `/api/pv-list`를 그대로 사용한다.
- 별도 인벤토리 검색 API는 없다.

### 8.2 이벤트 페이지

```http
GET /api/events/{event_id}/alarms
GET /api/events/{event_id}/alarm-series?pvs=...
GET /api/pv-data?event_id=...&pv=...&pre=60&post=10
GET /api/alarm-catalog?q=...
```

### 8.3 기존 API 재사용

- `/api/pv-hierarchy`, `/api/pv-list` — PV Explorer/대시보드 드롭다운.
- `/api/events/{id}/pv-notes` — note 저장.
- `/api/pipeline/monitor/start|stop` — 알람 모니터 start/stop을 내부에서 함께 호출.

---

## 9. UI 설계

### 9.1 대시보드 (`/alarm-dashboard`)

- 상단: 제목, 모니터 상태, 요약 카드, **PV 연결 수정** 버튼, Postmortem 링크.
- **Alarm Lamps 탭 (기본)**:
  - subsystem별 램프 벽 (524개). normal=초록, alarm=빨강(점멸), unknown=회색, complex=노랑.
  - 램프 hover 시 PV / description / value / time 표시.
  - **Active Alarms** 표: 빨간 램프만 발생 시각(KST), subsystem, description, PV, graph PV로 표시.
- **PV Mapping 탭** (`PV 연결 수정` 버튼으로 진입):
  - 필터/검색 + 524행 표 + Edit 버튼.
  - Edit 모달: **Archiver 드롭다운만**, 설명 자동 채움, 최대 4개, 비밀번호 저장.
- 자동 새로고침 5초. 모니터 정지 시 상태는 `unknown`(회색)으로 표시.
- CSV는 초기 seed만 사용하며, 대시보드에서 import/export하지 않는다.

### 9.2 이벤트 페이지

기존 카드 순서:
```
Event Sequence / User Beam Time / Notes / Fault
Report
PV Evidence (Reference)
Waveform  ← ms
Attachments
```

추가: **Waveform 아래, Attachments 위**에 `Alarm Messages & PV` 카드.

```
┌─ Alarm Messages & PV  (덤프 -60s ~ +10s) ───────────────────────────┐
│ [7 alarms] [archiver ~5s] [Open in PV Explorer]                    │
│ ┌───────────────────────────────┬──────────────────────────────────┐│
│ │ time  subsystem message  PV   │ Plotly: PV별 세로 스택            ││
│ │ -12.3 KSU3  Filament…   AI…   │ [KSU3:AI17]                      ││
│ │ -11.8 CRM3  Ion pump…   AI…   │ [CRM3:AIP01p]                    ││
│ │ ...                           │ [+ Add PV]                       ││
│ └───────────────────────────────┴──────────────────────────────────┘│
│ 0초 = 빔전류 하강 시작 (이벤트 기준 -10.98s)                         │
│ ⚠ Archiver는 초 단위, CSV 파형은 ms 단위. 선후 관계는 ±해상도.      │
└─────────────────────────────────────────────────────────────────────┘
```

- 표 행 클릭 → 해당 graph PV 표시/하이라이트.
- 복구 메시지 토글 없음.
- `Add PV` 모달: **Archiver 드롭다운만**, 최대 8개.
- `Open in PV Explorer` → `/events/{id}/pv-explorer?pvs=...`.

### 9.3 PV 비교

- 기본: PV별 세로 스택 (각자 y축, 공통 x축).
- 선택: normalized overlay (z-score/0-1) — 선후 관계 비교.
- 각 PV의 unit, `resolution_s` 표시.
- 2초 초과 해상도는 `low resolution` 배지.
- `t=0` 세로 점선, 알람 발생 마커.

---

## 10. 시간축/해상도

| 소스 | 해상도 | 용도 |
|---|---|---|
| CSV 파형 | 수십 ms | 기존 Waveform |
| PLS Archiver | 보통 1~5초 | 알람 PV, 사용자 PV 비교 |
| 기존 `data/pv_series` (데모) | 1초 | PV Evidence (기존) |

- t0 = 빔전류 하강 시작 (`_event_t0`).
- 알람/PV 패널은 `t0-60 ~ t0+10`.
- CSV 파형과 Archiver를 같은 축에 겹치지 않는다.
- "선후 관계"는 Archiver 해상도 한계 내에서만 해석하도록 고정 안내.

---

## 11. 단계별 계획

### Phase 0 — 스키마/카탈로그/대시보드 shell (2일)

- [ ] 알람 테이블 6개 (`alarm_pv_catalog`, `alarm_transitions`, `alarm_last_state`, `alarm_monitor_status`, `event_alarms`, `alarm_mapping_history`)
- [ ] `CSS/alarm_archiver_map.csv` → `data/alarm/alarm_pv_catalog.csv` seed, DB 1회 적재
- [ ] OPI rules에서 극성 추출 (`scripts/extract_alarm_polarity.py`) + `complex` 목록
- [ ] `/alarm-dashboard` shell (현재 상태/매핑 읽기 전용)
- [ ] 상호 링크 버튼

### Phase 1 — 모니터 + Start/Stop + 대시보드 live (3~5일)

- [ ] `src/alarm/monitor.py`, `manager.py`, `store.py`
- [ ] `pyepics` 설치, CA 환경, srf-hprf 기동
- [ ] `start_monitor`/`stop_monitor`에 알람 모니터 연결
- [ ] heartbeat/status, `alarm_last_state` upsert
- [ ] 대시보드 현재 상태/요약/최근 전이/자동 새로고침
- [ ] 설정 페이지에 알람 모니터 상태 표시

### Phase 2 — 대시보드 매핑 편집 (2~3일)

- [ ] 매핑 편집 모달 (Archiver 드롭다운, 설명 자동 채움, 다중 PV, Test)
- [ ] 저장 + 감사 로그
- [ ] CSV 내보내기/가져오기 (diff 확인)
- [ ] 필터/정렬/요약 카드

### Phase 3 — 이벤트 페이지 통합 (3~4일)

- [ ] `GET /api/events/{id}/alarms`, `/alarm-series`
- [ ] 이벤트 생성 후처리 훅 + 매핑 스냅샷
- [ ] Archiver fetch/캐시 (`data/alarm_series`)
- [ ] 이벤트 페이지 알람 표/그래프
- [ ] active-at-start
- [ ] "Open in PV Explorer" `?pvs=`

### Phase 4 — PV 추가/비교/운영 (2~3일)

- [ ] `pv_panel.js` 공용화, Archiver 드롭다운, 최대 8개
- [ ] normalized overlay
- [ ] 캐시/TTL/동시 fetch 제한
- [ ] 보존/정리 정책, 로그/모니터링, 배포 문서

---

## 12. 변경 파일 목록

| 파일 | 변경 |
|---|---|
| `src/db/schema.py` | 알람 테이블 6개 + 마이그레이션 |
| `src/db/repository.py` | 카탈로그/전이/last_state/status CRUD |
| `src/alarm/__init__.py` | 신규 패키지 |
| `src/alarm/catalog.py` | 카탈로그 로드/seed |
| `src/alarm/monitor.py` | CA 모니터 |
| `src/alarm/manager.py` | subprocess start/stop/status |
| `src/alarm/store.py` | SQLite writer |
| `src/alarm/timebase.py` | `_event_t0` 공용화 |
| `src/alarm/series.py` | Archiver fetch/cache |
| `src/import_job.py` | 이벤트 생성 후 알람 첨부 |
| `src/web/pipeline_manager.py` | Start/Stop에 알람 모니터 연결 |
| `src/web/server.py` | 대시보드/이벤트 알람 API, CSV import/export |
| `src/web/templates/alarm_dashboard.html` | 신규 대시보드 |
| `src/web/templates/event_detail.html` | 알람 카드/표/그래프 |
| `src/web/templates/base.html` | Alarm Dashboard 링크 |
| `src/web/templates/settings.html` | 알람 모니터 상태 |
| `src/web/templates/pv_explorer.html` | 공용 컴포넌트, `?pvs=` |
| `src/web/static/js/pv_panel.js` | 신규 공용 PV 패널 |
| `src/web/static/js/alarm_dashboard.js` | 신규 대시보드 JS |
| `config/config.yaml` | CA 주소, window, max PV, cache TTL |
| `requirements.txt` | `pyepics>=3.5` |
| `scripts/extract_alarm_polarity.py` | OPI rules 파싱 |
| `scripts/seed_alarm_catalog.py` | CSV → DB seed |
| `data/alarm/` | 카탈로그 seed/export |
| `data/event_alarms/`, `data/alarm_series/` | 사이드카 |

---

## 13. 리스크

| 리스크 | 대응 |
|---|---|
| CA는 내부망에서만 접근 가능 | 모니터는 `srf-hprf`에서만. 웹과 분리 |
| 알람 PV 524개는 Archiver에 없음 | 과거 이벤트는 알람 데이터 없음. 신규부터 |
| 매핑 정본이 CSV와 DB로 이원화 | DB 정본, CSV는 seed/import/export |
| 대시보드에서 매핑 변경 중 이벤트 발생 | 이벤트 생성 시 매핑 스냅샷 |
| Archiver 해상도 초 단위 | UI 명시. ms 비교는 CSV |
| Archiver 응답 지연/큰 payload | 좁은 window, 캐시, 동시 fetch 제한 |
| SQLite 쓰기 경합 | WAL, busy_timeout, 단일 writer |
| 고아 모니터 프로세스 | PID 파일 + PDEATHSIG + shutdown 훅 + 웹 시작 시 정리 |
| pyepics 미설치/CA 실패 | Start 시 경고, CSV pipeline은 계속 |
| 대시보드 무단 매핑 변경 | 기존 비밀번호 + 감사 로그 |
| 미커밋 워킹 트리 | 배포 전 백업/커밋 정리 |
| PV 드롭다운이 Archiver API에 의존 | API 실패 시 명확한 에러 표시, 재시도 |

---

## 14. 남은 확인 사항

1. **CSV 운용 방식**: DB를 정본으로 하고 CSV는 seed/export로 두는 방식이 맞는지. (권장)
   - CSV 파일 자체를 계속 덮어써서 파일이 항상 최신이어야 하면 별도 구현 필요.
2. **대시보드 페이지 자체의 공개 범위**: 페이지 조회도 비밀번호를 요구할지, 편집만 비밀번호를 요구할지.
3. **매핑 편집 시 다중 PV 최대 개수**: 4개(권장) vs 다른 값.
4. **모니터 heartbeat 주기**: 5초(권장).

---

## 15. 참고

- 현재 데모: `http://localhost:8051/events/20251103_115812/pv-explorer?demo=1`
- 기존 PV Evidence: `data/pv_evidence/{event_id}.json`
- 기존 PV Series: `data/pv_series/{event_id}.json`
- Archiver: `https://plsarchiver.postech.ac.kr`
- 알람 매핑 초안: `CSS/alarm_archiver_map.csv` (375/524 mapped)
