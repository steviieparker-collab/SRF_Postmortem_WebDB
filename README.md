# SRF Postmortem WebDB

**Superconducting RF Beam Dump Event Viewer & History Database**

A web-based system for viewing, analyzing, and managing SRF beam dump postmortem events. Replaces existing monitoring system Event DB with an independent implementation.

---

## Features

### 📊 Event Viewer
- **Event List** — Paginated list with filters (year, beam time, fault type, notes search, MS exclusion)
- **Event Detail** — Interactive Plotly waveform charts (analog + digital overlays), zoom controls
- **Filter-Aware Navigation** — Prev/Next preserves current filter context

### 🔍 Classification Engine
- Rule-based v4.0 classifier with **13 fault cases**
- Analog signal analysis (threshold-based: lowlow/low/high/highhigh)
- Digital interlock pattern matching (MIS, PSI, INT_FC, RDY_KSU, CM groups)
- Noise filtering with delay compensation

### 📝 User Annotations
- **Beam Time** — User-assignable beam time periods (e.g., "2026-2nd")
- **Fault Type** — User override for classifier results
- **Notes** — Free-text notes per event (preview on main page)
- Auto-save via REST API

### 📎 File Attachments
- Upload files to any event (images, PDFs, documents)
- Download with original filename preserved
- MIME-type aware file icons
- Password-protected deletion

### 📈 Statistics
- Case distribution & fault type histogram
- **Fault Type Over Time** — Period-by-period breakdown
- Digital channel co-occurrence analysis
- MS period filtering

### 💾 Backup & Restore
- Full backup: DB + merged parquet + attachments → single `.tar.gz`
- One-click restore from Settings page
- Password-protected operations

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Web Server** | FastAPI + Uvicorn |
| **Database** | SQLite (WAL mode) |
| **Frontend** | Jinja2 + Bootstrap 5 Dark Theme |
| **Charts** | Plotly.js |
| **Data Processing** | Polars + Pandas/NumPy |
| **Classification** | Custom rule-based engine (v4.0) |

---

## Quick Start

### Prerequisites

```bash
# Python 3.11+ required
pip install -r requirements.txt
```

### Configuration

```bash
# Copy example config and edit
cp config/config.yaml.example config/config.yaml
# Set your password and SMTP credentials in config.yaml
```

### Run the Server

```bash
python -m src.web.server
# → http://localhost:50510
```

### Import Events

```bash
# 1. Preprocess and merge scope CSV files
python3 data/append/append-merge.py \
    -i data/append/scope1 data/append/scope2 data/append/scope3 \
    -o data/append/merged

# 2. Copy merged parquet to data/merged/
cp data/append/merged/*.parquet data/merged/

# 3. Import via Settings page or API
curl -X POST http://localhost:50510/api/import
```

### Backup & Restore

```bash
# Backup (via API)
curl -X POST "http://localhost:50510/api/db/backup?password=YOUR_PW"

# Or use Settings page → Backup DB / Get Backup Files → Restore
```

---

## Project Structure

```
SRF_postmortem/
├── config/
│   ├── config.yaml              # Local config (gitignored)
│   └── config.yaml.example      # Config template with secrets removed
├── db/
│   ├── events.db                # SQLite database
│   └── backups/                 # Generated backup files
├── data/
│   ├── merged/                  # Merged parquet waveforms
│   ├── attachments/             # Uploaded files per event
│   ├── append/                  # CSV preprocessing utility
│   ├── processed/               # Preprocessed parquet files
│   ├── graphs/                  # Generated waveform images
│   ├── results/                 # Classification results (JSON)
│   ├── reports/                 # Generated markdown reports
│   └── watch/                   # Raw CSV monitor folders
├── src/
│   ├── web/                     # FastAPI server + Jinja2 templates
│   │   ├── server.py            # Web routes & API endpoints
│   │   ├── templates/           # HTML templates (Bootstrap 5)
│   │   └── static/              # CSS, JS (charts)
│   ├── db/                      # Database layer
│   │   ├── schema.py            # SQLite DDL + connection
│   │   ├── models.py            # Pydantic models
│   │   ├── repository.py        # CRUD operations
│   │   └── similarity.py        # Event similarity engine
│   ├── classifier/              # Rule-based classification
│   ├── pipeline/                # Preprocessor / Grouper / Report
│   └── orchestrator.py          # Pipeline orchestration
├── logs/                        # Application logs
└── pyproject.toml
```

---

## API Endpoints

### Events
| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/events` | Paginated event list |
| `GET` | `/api/events/{id}` | Event detail |
| `GET` | `/api/events/{id}/waveforms` | Waveform data (parquet) |
| `GET` | `/api/events/{id}/similar` | Similar events |

### Attachments
| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/events/{id}/attachments` | Upload files |
| `GET` | `/api/events/{id}/attachments` | List attachments |
| `GET` | `/api/attachments/{id}/download` | Download file |
| `DELETE` | `/api/attachments/{id}` | Delete attachment |

### Annotations
| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/events/{id}/notes` | Update notes |
| `POST` | `/api/events/{id}/user-beam-time` | Set beam time |
| `POST` | `/api/events/{id}/user-fault-type` | Override fault type |

### System
| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/db/backup` | Create full backup |
| `GET` | `/api/db/backups` | List available backups |
| `POST` | `/api/db/restore` | Restore from backup |
| `POST` | `/api/import` | Import parquet → DB |
| `GET` | `/api/stats/cases` | Case statistics |
| `GET` | `/api/stats/histogram` | Fault type over time |

---

## Classification Cases

| Case | Type | Description |
|------|------|-------------|
| 0 | Unknown | Unrecognized pattern |
| 1-2 | Beam Loss | Beam loss detected |
| 3 | RF Interlock | First digital interlock |
| 4 | MIS | INT_MIS_IC fault |
| 5 | PSI | INT_PSI_IC fault |
| 6 | Multi (same group) | Same-group interlocks |
| 7 | Multi (different group) | Cross-group interlocks |
| 8 | Cavity Blip | Single cavity blip |
| 9 | Cavity Quench | Quench detected |
| 10-11 | RF Path | RF station path fault |
| 12 | Cavity Detune | Cavity detuning |
| 13 | RF Source | Common RF source fault |

---

## License

Internal use — POSTECH SRF Lab
