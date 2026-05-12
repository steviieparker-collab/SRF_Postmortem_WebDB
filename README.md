# SRF Postmortem WebDB

**Superconducting RF Beam Dump Event Viewer & History Database**

A web-based system for viewing, analyzing, and managing SRF beam dump postmortem events.

---

## Features

### 📊 Event Viewer
- **Event List** — Paginated list with filters (year, beam time, fault type, notes search, MS exclusion)
- **Event Detail** — Interactive Plotly waveform charts (analog + digital overlays), zoom controls
- **Filter-Aware Navigation** — Prev/Next preserves current filter context
<img width="1119" height="927" alt="image" src="https://github.com/user-attachments/assets/a72437ae-4dd2-4192-8466-154a08d725fc" />
<img width="1129" height="473" alt="image" src="https://github.com/user-attachments/assets/17d82fb1-2e3f-4065-9c7b-2c2119d9f6d4" />
<img width="1128" height="854" alt="image" src="https://github.com/user-attachments/assets/6cafcde6-fed0-431f-a458-585711f34cc8" />

### 🔍 Classification Engine
- Rule-based v4.0 classifier with **13 fault cases**
- Analog signal analysis (threshold-based: lowlow/low/high/highhigh)
- Digital interlock pattern matching (MIS, PSI, INT_FC, RDY_KSU, CM groups)
- Noise filtering with delay compensation

### 📝 User Annotations
- **Beam Time** — User-assignable beam time periods (e.g., "2026-2nd")
- **Fault Type** — User override for classifier results
- **Notes** — Free-text notes per event (preview on main page, first 40 chars)
- Auto-save via REST API

### 📎 File Attachments
- Upload files to any event (images, PDFs, documents)
- Download with original filename preserved
- MIME-type aware file icons
- Password-protected deletion
- Attachment count badge on event list

### 📈 Statistics
- Case distribution & fault type histogram
- **Fault Type Over Time** — Period-by-period breakdown
- Digital channel co-occurrence analysis
- MS period filtering
<img width="1117" height="929" alt="image" src="https://github.com/user-attachments/assets/3463629f-0e4a-40c6-a340-993916659dc2" />
<img width="1101" height="468" alt="image" src="https://github.com/user-attachments/assets/5b015d4e-2f49-40be-b474-27b9382ce336" />

### 💾 Backup & Restore
- Full backup: DB + merged parquet + attachments → single `.tar.gz`
- One-click restore from Settings page
- Password-protected operations
- Restore replaces DB, merged parquet files, and attachments
<img width="1111" height="800" alt="image" src="https://github.com/user-attachments/assets/6f585c02-fd04-4907-bc21-545e54268944" />

### 🔧 Append Data Pipeline
- **Settings page** — Input 3 scope CSV directories and run the full pipeline
- Preprocess CSVs → Merge by timestamp → Classify → Import to DB — all in one click
- Automatically detects and replaces existing events with the same ID
- Background execution with real-time status logging

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

## System Architecture & Data Flow

### High-Level Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SRF Postmortem WebDB                         │
│                                                                     │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌─────────────────┐  │
│  │  Source  │──▶│ Pipeline │──▶│   Web    │──▶│   User (Admin)  │  │
│  │  Data    │   │  Engine  │   │  Server  │   │   / Researcher   │  │
│  └──────────┘   └──────────┘   └──────────┘   └─────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### End-to-End Data Flow

```
                    ┌─────────────────────────┐
                    │  3x Oscilloscope CSV     │
                    │  (Scope1, Scope2, Scope3)│
                    └───────────┬─────────────┘
                                │
          ┌─────────────────────┼─────────────────────┐
          │                     │                     │
          ▼                     ▼                     ▼
    ┌──────────┐         ┌──────────┐          ┌──────────┐
    │ Scope 1  │         │ Scope 2  │          │ Scope 3  │
    │Preprocess│         │Preprocess│          │Preprocess│
    │ CSV→Parq │         │ CSV→Parq │          │ CSV→Parq │
    └────┬─────┘         └────┬─────┘          └────┬─────┘
         │                   │                     │
         └───────────────────┼─────────────────────┘
                             │
                             ▼
                    ┌────────────────┐
                    │    Grouper     │
                    │  Merge by      │
                    │  timestamp     │
                    │  (window: 180s)│
                    └───────┬────────┘
                            │
                            ▼ event_YYYYMMDD_HHMMSS.parquet
                    ┌────────────────┐
                    │  Classifier    │
                    │  Rule-based    │
                    │  v4.0 (13 case)│
                    └───────┬────────┘
                            │
                    ┌────────────────┐
                    │  Visualizer    │
                    │  (Plotly JSON) │
                    └───────┬────────┘
                            │
                    ┌────────────────┐
                    │  Importer      │
                    │  → SQLite DB   │
                    └───────┬────────┘
                            │
                            ▼
                    ┌────────────────┐
                    │   Web Server   │
                    │  FastAPI:50510 │
                    └───────┬────────┘
                            │
              ┌─────────────┼──────────────┐
              ▼             ▼              ▼
        ┌──────────┐ ┌──────────┐ ┌──────────────┐
        │Event List│ │  Event   │ │  Statistics  │
        │  (Filter)│ │  Detail  │ │  (Dashboard) │
        └──────────┘ │ + Charts │ └──────────────┘
                     │+ Notes   │
                     │+ Files   │
                     └──────────┘
```

### Pipeline Processing Modes

```
┌──────────────────────────────────────────────────────────────────────┐
│                         PIPELINE MODES                               │
├───────────────────┬──────────────────────┬───────────────────────────┤
│   Append Mode     │    Monitor Mode      │    Batch Mode             │
│   (Settings UI)   │   (Folder Watch)     │   (Full Pipeline)         │
├───────────────────┼──────────────────────┼───────────────────────────┤
│ User specifies    │ Watches 3 folders    │ Manual one-shot           │
│ 3 scope dirs      │ for NEW CSV files    │ includes email sending    │
│ Process all CSVs  │ Auto-detects all-3   │                           │
│ Overwrites dupes  │ Waits for timeout    │                           │
│ Classify + DB     │ Pipeline + Email     │                           │
└───────────────────┴──────────────────────┴───────────────────────────┘
```

### Database Schema

```
┌───────────────────┐       ┌───────────────────────┐
│      events       │       │    fault_types         │
├───────────────────┤       ├───────────────────────┤
│ id (PK)           │       │ name (PK)              │
│ timestamp         │       │ description            │
│ fault_type (FK)   │──────▶│ severity               │
│ fault_confidence  │       │ event_count            │
│ beam_voltage      │       └───────────────────────┘
│ beam_current      │
│ analog_metrics    │       ┌───────────────────────┐
│ digital_pattern   │       │   event_links          │
│ time_groups       │       ├───────────────────────┤
│ case_id           │       │ event_id (FK)          │
│ user_beam_time    │       │ related_event_id (FK)  │
│ notes             │       │ similarity_score       │
│ user_fault_type   │       └───────────────────────┘
│ merged_file       │
└───────────────────┘       ┌───────────────────────┐
                            │  event_attachments    │
                            ├───────────────────────┤
                            │ event_id (FK)          │
                            │ original_name          │
                            │ stored_name            │
                            │ mime_type              │
                            │ file_size              │
                            └───────────────────────┘
```

### Classification Pipeline (Detailed)

```
Merged Parquet
      │
      ▼
┌──────────────────────────────────────┐
│  Analog Metrics Computation          │
│  - Beam current (baseline mean)      │
│  - RF forward/reflect/cavity voltages│
│  - Threshold checks (low/high/highhigh)│
└──────────────────┬───────────────────┘
                   │
┌──────────────────▼───────────────────┐
│  Digital Pattern Extraction          │
│  - 25 digital channels (MIS, PSI,   │
│    INT_FC, RDY_KSU, CM groups)      │
│  - 0.4ms delay compensation         │
│  - 0.01ms persistence filter        │
│  - 0→1 transitions only (valid)     │
└──────────────────┬───────────────────┘
                   │
┌──────────────────▼───────────────────┐
│  Rule-Based Classifier v4.0         │
│  Strict priority order (Case 1→13)  │
│                                     │
│  Case  1-2: Beam loss               │
│  Case    3: RF Interlock            │
│  Case  4-5: MIS / PSI               │
│  Case  6-7: Multi interlock         │
│  Case    8: Cavity blip             │
│  Case    9: Cavity quench           │
│  Case 10-11: RF path               │
│  Case   12: Cavity detune           │
│  Case   13: RF source fault         │
│  Case    0: Unknown                 │
└──────────────────┬───────────────────┘
                   │
                   ▼
          Stored in events DB
```


## Quick Start

### Prerequisites

```bash
# Python 3.10+ required

# Option A: Using requirements.txt
pip install -r requirements.txt

# Option B: Using pyproject.toml (editable install)
pip install -e .
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

---

## Usage Guide

### Append Data (CSV → DB)

The **Append** pipeline processes raw CSV files from 3 oscilloscope channels, merges them by timestamp, runs classification, and stores results in the database.

**Via Settings Page:**

1. Navigate to `http://localhost:50510/settings` (enter admin password)
2. Scroll to **Append Data** section
3. Enter the 3 scope CSV directories (default: `data/append/scope1`, `scope2`, `scope3`)
4. Click **Append (CSV Preprocess → Merge → Classify → Import)**
5. Monitor progress in the status log panel

```
Scope 1:  data/append/scope1
Scope 2:  data/append/scope2
Scope 3:  data/append/scope3
                          │
                    ┌─────┴─────┐
                    │ Append    │
                    └─────┬─────┘
                          │
            ┌─────────────┼─────────────┐
            ▼             ▼             ▼
      Preprocessor   Preprocessor   Preprocessor
      (CSV→Parquet)  (CSV→Parquet)  (CSV→Parquet)
            │             │             │
            └─────────────┼─────────────┘
                          ▼
                     Grouper
                 (Merge by time)
                          │
                          ▼
              ┌─────────────────────┐
              │  Check existing     │
              │  events in merged/  │
              │  and DB → overwrite │
              └──────────┬──────────┘
                         ▼
                    Import to DB
```

**Pipeline replaces existing events:**
- If a merged parquet with the same event ID already exists → old merged file + DB record are removed before importing the new one
- This ensures clean updates when re-processing the same CSV data

### Import Events (Parquet → DB)

If you already have merged parquet files in `data/merged/`:

```bash
# Via Settings page: click "Import to DB"
# Via API:
curl -X POST http://localhost:50510/api/import
```

### Backup & Restore

```bash
# Backup (via API)
curl -X POST "http://localhost:50510/api/db/backup?password=YOUR_PW"

# Or use Settings page → Backup DB / Get Backup Files → Restore
```

### File Attachments

- Navigate to an **Event Detail** page
- Scroll to the **Attachments** section below the waveform
- Click **Upload** to add files (images, PDFs, documents)
- Click the download link to retrieve files with original filename
- Click the delete icon (trash) to remove (requires password)

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
│   ├── append/                  # CSV preprocessing utility (data dirs)
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
│   ├── pipeline/                # Preprocessor, Grouper, AppendMerge
│   │   ├── preprocessor.py      # CSV → Parquet
│   │   ├── grouper.py           # Merge by timestamp
│   │   ├── append_merge.py      # Integrated append pipeline
│   │   ├── classifier.py        # Event classifier
│   │   └── ...
│   └── orchestrator.py          # Pipeline orchestration
├── logs/                        # Application logs
├── requirements.txt             # Python dependencies
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

### Pipeline
| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/pipeline/append` | Run append: CSV → Merge → Classify → DB |
| `POST` | `/api/pipeline/batch` | Run full monitoring pipeline |
| `POST` | `/api/pipeline/import` | Import merged parquet → DB only |
| `POST` | `/api/pipeline/monitor/start` | Start folder monitoring |
| `POST` | `/api/pipeline/monitor/stop` | Stop folder monitoring |
| `POST` | `/api/pipeline/stop` | Stop all running pipelines |
| `GET` | `/api/pipeline/status` | Get current pipeline status |

### System
| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/db/backup` | Create full backup |
| `GET` | `/api/db/backups` | List available backups |
| `POST` | `/api/db/restore` | Restore from backup |
| `POST` | `/api/import` | Import parquet → DB |
| `GET` | `/api/config/append-dirs` | Get configured append directories |
| `GET` | `/api/stats/cases` | Case statistics |
| `GET` | `/api/stats/histogram` | Fault type over time |

---

## Configuration

**`config/config.yaml`** key sections:

```yaml
paths:
  append_dirs:              # Default scope dirs for Append pipeline
    - ./data/append/scope1
    - ./data/append/scope2
    - ./data/append/scope3

access:
  password: 'your_password'  # Web UI admin password

web:
  host: 0.0.0.0
  port: 50510
```

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

Internal use — POHANG ACCELERATOR LABORATORY SRF GROUP
