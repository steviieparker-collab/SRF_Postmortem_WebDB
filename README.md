# SRF Postmortem WebDB

**포항가속기 SRF Beam Dump 사후분석(Port-mortem) Event Viewer & History Database**

3대의 오실로스코프에서 수집된 CSV 데이터를 자동 수집·분석·분류하여 웹에서 조회·관리하는 통합 시스템입니다.

---

## Features

### 📊 Event Viewer
- **Event List** — 페이징, 연도·Beam Time·Fault Type·메모 필터, MS(정비) 기간 제외
- **Event Detail** — Interactive Plotly 파형 차트 (Analog + Digital 오버레이), 확대/축소
- **필터 유지 Prev/Next** — 필터 컨텍스트 유지하며 이전/다음 이벤트 이동
- **분류 결과 표시** — Rule-based 분류기 v4.0 (13개 Fault Case) 결과를 상세 페이지에 표시
- **Similar Events** — 동일 디지털 패턴 이벤트 링크 (유사도 기반)

### 📝 User Annotations
- **Beam Time** — 사용자 할당 빔 운전 기간 (예: "2026-2nd")
- **Fault Type** — 분류기 결과 override 가능
- **Notes** — 이벤트별 자유 텍스트 메모 (목록에 40자 미리보기)
- REST API로 자동 저장

### 📎 File Attachments
- 이미지, PDF, 문서 업로드
- 원본 파일명 보존 다운로드
- MIME-aware 파일 아이콘
- 비밀번호 인증 삭제
- 목록에서 첨부파일 개수 배지 표시

### 📈 Statistics
- Case/Fault Type 분포 히스토그램
- **Fault Type Over Time** — 운전 기간별 Breakdown
- 디지털 채널 동시발생 분석
- MS(정비) 기간 필터링

### 💾 Backup & Restore
- Full backup: DB + merged parquet + attachments → `.tar.gz`
- Settings 페이지에서 원클릭 복원
- 비밀번호 인증 작업
- 복원 시 DB, merged 파일, attachment 경로 자동 업데이트

### 🔧 Append Data Pipeline
- **Settings 페이지** — 3개 Scope CSV 입력 폴더를 지정하고 전체 파이프라인 실행
- 전처리(CSV → Parquet) → Grouper(타임스탬프 정렬) → Classifier → Visualizer → Reporter → Email → DB Import
- 동일 event_id 자동 감지 및 교체
- 백그라운드 실행 + 실시간 상태 로그

### 🌐 Email Notification
- 이벤트 발생 시 자동 이메일 발송
- Classifier 결과 + 그래프 이미지 첨부
- **Cloudflare 도메인 링크** 포함 (`config.web.url_base` 설정)
- SMTP TLS 지원, 재시도 로직 내장

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Web Server** | FastAPI + Uvicorn |
| **Template** | Jinja2 + Bootstrap 5 Dark Theme |
| **Charts** | Plotly.js |
| **Database** | SQLite (WAL mode) |
| **Data Processing** | Polars + Pandas/NumPy/SciPy |
| **Classification** | Custom rule-based engine (v4.0, 13 cases) |
| **Email** | smtplib (TLS, retry, multi-attachment) |
| **Graphics** | Matplotlib |

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        SRF Postmortem WebDB                             │
│                                                                         │
│  ┌──────────────┐     ┌──────────────────┐     ┌───────────────────┐   │
│  │  3x Scope    │────▶│   Pipeline       │────▶│   Web Viewer     │   │
│  │  CSV Files   │     │   Engine (5단계)  │     │   (FastAPI:8050)  │   │
│  └──────────────┘     └──────────────────┘     └───────────────────┘   │
│                               │                         │               │
│                               ▼                         ▼               │
│                        ┌──────────────┐        ┌──────────────┐        │
│                        │  Email Sender│        │  SQLite DB   │        │
│                        │  (SMTP+TLS)  │        │  (WAL mode)  │        │
│                        └──────────────┘        └──────────────┘        │
└─────────────────────────────────────────────────────────────────────────┘
```

### Pipeline 상세 흐름

```
                    ┌─────────────────────────┐
                    │  3x Oscilloscope CSV     │
                    │  (Scope1, Scope2, Scope3)│
                    └───────────┬─────────────┘
                                │
          ┌─────────────────────┼─────────────────────┐
          ▼                     ▼                     ▼
    ┌──────────┐         ┌──────────┐          ┌──────────┐
    │ Scope 1  │         │ Scope 2  │          │ Scope 3  │
    │Preprocess│         │Preprocess│          │Preprocess│
    │ CSV→Parq │         │ CSV→Parq │          │ CSV→Parq │
    └────┬─────┘         └────┬─────┘          └────┬─────┘
         │                   │                     │
         └───────────────────┼─────────────────────┘
                             ▼
                    ┌────────────────┐
                    │    Grouper     │
                    │  Merge by      │
                    │  timestamp     │
                    │  (window: 180s)│
                    └───────┬────────┘
                            │ event_YYYYMMDD_HHMMSS.parquet
                            ▼
                    ┌────────────────┐
                    │  Classifier    │
                    │  Rule-based    │
                    │  v4.0 (13 case)│
                    └───────┬────────┘
                            ▼
                    ┌────────────────┐
                    │  Visualizer    │
                    │  (Matplotlib)  │
                    │  wide/narrow   │
                    └───────┬────────┘
                            ▼
                    ┌────────────────┐
                    │   Reporter     │
                    │  (Markdown)    │
                    └───────┬────────┘
                            ▼
                    ┌────────────────┐
                    │  Email Sender  │
                    │  (Cloudflare   │
                    │   링크 포함)    │
                    └───────┬────────┘
                            ▼
                    ┌────────────────┐
                    │  DB Importer   │
                    │  → SQLite      │
                    └───────┬────────┘
                            │
                            ▼
                    ┌────────────────────—┐
                    │   Web Server        │
                    │  FastAPI:8050       │
                    │  /events/{id}       │
                    │  /api/events        │
                    │  /stats             │
                    │  /settings          │
                    └────────────────────—┘
```

---

## Project Structure

```
SRF_postmortem/
├── config/
│   ├── config.yaml.example     ← 설정 템플릿 (민감정보 제외)
│   └── config.yaml             ← 실제 설정 (Git 미추적)
├── src/
│   ├── main.py                 ← CLI 진입점
│   ├── orchestrator.py         ← 파이프라인 + DB 통합 오케스트레이터
│   ├── import_job.py           ← DB Import 로직
│   │
│   ├── core/                   ← 공통 유틸리티
│   │   ├── config.py           ← 통합 설정 (Pydantic + YAML)
│   │   ├── logger.py           ← 로깅
│   │   ├── utils.py            ← 공통 함수
│   │   ├── channel_utils.py    ← 채널 분류
│   │   └── exceptions.py       ← 커스텀 예외
│   │
│   ├── pipeline/               ← 모니터링 파이프라인
│   │   ├── preprocessor.py     ← CSV → Parquet 전처리
│   │   ├── grouper.py          ← 3개 Scope 동기화/정렬
│   │   ├── classifier.py       ← Rule-based 분류
│   │   ├── visualizer.py       ← 그래프 생성 (Matplotlib)
│   │   ├── reporter.py         ← 보고서 생성 (Markdown)
│   │   ├── email_sender.py     ← SMTP 이메일 발송
│   │   ├── rule_engine.py      ← 분류 규칙 엔진
│   │   ├── datatypes.py        ← 파이프라인 데이터 타입
│   │   └── append_merge.py     ← 수동 CSV 추가/병합
│   │
│   ├── classifier/             ← DB 분류
│   │   ├── classifier.py       ← DB import 전 분류
│   │   └── datatypes.py        ← 분류 데이터 타입
│   │
│   ├── db/                     ← 데이터베이스
│   │   ├── schema.py           ← 테이블 정의
│   │   ├── models.py           ← 데이터 모델
│   │   ├── repository.py       ← CRUD
│   │   └── similarity.py       ← 유사 이벤트 링크
│   │
│   ├── templates/report/       ← 이메일/보고서 템플릿 (Jinja2)
│   │   ├── email_body.md.j2
│   │   ├── report.html.j2
│   │   ├── summary.txt.j2
│   │   └── batch_summary.md.j2
│   │
│   └── web/                    ← 웹 서버
│       ├── server.py           ← FastAPI 앱 (라우트 전체)
│       ├── pipeline_manager.py ← 웹에서 파이프라인 실행 관리
│       ├── static/css/style.css
│       ├── static/js/charts.js
│       └── templates/          ← Jinja2 페이지 템플릿
│           ├── base.html
│           ├── index.html
│           ├── event_detail.html
│           ├── settings.html
│           └── stats.html
│
├── data/                       ← 런타임 데이터 (Git 미추적)
│   ├── processed/              ← 전처리된 parquet
│   ├── merged/                 ← 병합된 parquet
│   ├── results/                ← 분류 결과
│   ├── reports/                ← 보고서
│   ├── graphs/                 ← 그래프 이미지
│   └── attachments/            ← 파일 첨부
├── db/                         ← SQLite DB 파일 (Git 미추적)
├── logs/                       ← 로그 (Git 미추적)
├── requirements.txt
├── pyproject.toml
├── seed_db.py                  ← 테스트 DB 시드 스크립트
└── README.md
```

---

## Quick Start

### 1. 의존성 설치

```bash
pip install -r requirements.txt
```

### 2. 설정

```bash
cp config/config.yaml.example config/config.yaml
# config.yaml 수정: SMTP 정보, DB 경로, url_base 등
```

### 3. 실행

```bash
# 웹 서버 모드
python3 -m src.main --mode web

# 배치 모드 (기존 CSV 모두 처리)
python3 -m src.main --mode batch

# 전체 파이프라인 (모니터링 + 웹)
python3 -m src.main --mode full
```

웹 서버는 기본적으로 `http://0.0.0.0:8050`에서 실행됩니다.

### 4. 테스트용 시드 데이터

```bash
python3 seed_db.py
```

---

## Config 주요 항목

| 항목 | 설명 |
|------|------|
| `web.url_base` | Cloudflare 등 외부 도메인 (이메일 링크에 사용) |
| `web.port` | 웹 서버 포트 (기본 8050) |
| `access.password` | 웹 로그인 비밀번호 |
| `email.*` | SMTP 발신 설정 |
| `db.path` | SQLite DB 파일 경로 |

---

## Operating Modes

| Mode | 설명 |
|------|------|
| `web` | 웹 서버만 실행 (기존 DB 조회) |
| `batch` | CSV를 한 번에 읽어 파이프라인 실행 후 DB 저장 |
| `monitor` | Watch 폴더 감시 + 실시간 처리 + 이메일 발송 |
| `full` | 모니터링 + 웹 서버 동시 실행 |

---

## License

MIT
