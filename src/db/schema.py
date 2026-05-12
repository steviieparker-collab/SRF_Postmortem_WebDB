"""
Database schema and connection management for SRF Event DB.

Uses SQLite via aiosqlite for async support in web context,
and sqlite3 for sync operations in monitor context.
"""

import sqlite3
import aiosqlite
from pathlib import Path
from typing import Optional

from ..core.config import get_config


# ── Schema DDL ──────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS fault_types (
    name            TEXT PRIMARY KEY,
    description     TEXT NOT NULL DEFAULT '',
    typical_pattern TEXT,           -- JSON: representative pattern
    severity        TEXT NOT NULL DEFAULT 'medium'
                    CHECK (severity IN ('low', 'medium', 'high')),
    event_count     INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS events (
    id              TEXT PRIMARY KEY,   -- YYYYMMDD_HHMMSS_{scope}
    timestamp       TEXT NOT NULL,       -- ISO8601 event time
    scope1_file     TEXT,
    scope2_file     TEXT,
    scope3_file     TEXT,
    merged_file     TEXT,
    fault_type      TEXT REFERENCES fault_types(name),
    fault_confidence REAL DEFAULT 0.0,
    beam_voltage    REAL,
    beam_current    REAL,
    analog_metrics  TEXT,               -- JSON
    digital_pattern TEXT,               -- JSON
    time_groups     TEXT,               -- JSON
    graphs_path     TEXT,
    report_path     TEXT,
    report_md       TEXT,
    case_id         INTEGER DEFAULT 0,  -- Rule-based case number (1-13)
    case_description TEXT DEFAULT '',    -- Rule match description
    case_fault      TEXT DEFAULT '',     -- Rule match fault message
    user_beam_time  TEXT DEFAULT '',     -- e.g., "2026-2nd"
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
CREATE INDEX IF NOT EXISTS idx_events_fault_type ON events(fault_type);

CREATE TABLE IF NOT EXISTS event_links (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT NOT NULL REFERENCES events(id),
    related_event_id TEXT NOT NULL REFERENCES events(id),
    similarity_score REAL NOT NULL DEFAULT 0.0,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(event_id, related_event_id)
);

CREATE INDEX IF NOT EXISTS idx_links_event_id ON event_links(event_id);

CREATE TABLE IF NOT EXISTS event_attachments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT NOT NULL REFERENCES events(id),
    original_name   TEXT NOT NULL,
    stored_name     TEXT NOT NULL,
    mime_type       TEXT,
    file_size       INTEGER,
    uploaded_at     TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_attachments_event_id ON event_attachments(event_id);
"""


# ── Sync Connection ─────────────────────────────────────────

def get_db_path() -> Path:
    cfg = get_config()
    path = cfg.db.path
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def get_sync_connection() -> sqlite3.Connection:
    """Get a synchronous SQLite connection (for monitor process)."""
    db_path = get_db_path()
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db_sync() -> sqlite3.Connection:
    """Initialize database schema (sync version)."""
    conn = get_sync_connection()
    conn.executescript(SCHEMA_SQL)
    # Migrate: add new columns if missing
    try:
        conn.execute("ALTER TABLE events ADD COLUMN notes TEXT DEFAULT ''")
    except sqlite3.OperationalError:
        pass  # already exists
    try:
        conn.execute("ALTER TABLE events ADD COLUMN user_fault_type TEXT DEFAULT ''")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    return conn


# ── Async Connection ────────────────────────────────────────

async def get_async_connection() -> aiosqlite.Connection:
    """Get an async SQLite connection (for web server)."""
    db_path = get_db_path()
    conn = await aiosqlite.connect(str(db_path))
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA foreign_keys=ON")
    return conn


async def init_db_async() -> aiosqlite.Connection:
    """Initialize database schema (async version)."""
    conn = await get_async_connection()
    await conn.executescript(SCHEMA_SQL)
    # Migrate: add new columns if missing
    for col in [("notes", "TEXT DEFAULT ''"), ("user_fault_type", "TEXT DEFAULT ''")]:
        try:
            await conn.execute(f"ALTER TABLE events ADD COLUMN {col[0]} {col[1]}")
        except sqlite3.OperationalError:
            pass
    await conn.commit()
    return conn


# ── Seed Default Fault Types ────────────────────────────────

CASE_FAULT_TYPES = [
    ("Case 1: Beam_loss", "Beam loss, check other systems like MPS, feedback system."),
    ("Case 2: Beam_loss", "Beam loss, check other systems like MPS, feedback system."),
    ("Case 3: RF_Interlock", '"the first digital interlock" is the fault'),
    ("Case 4: MIS", "INT_MIS_IC is the fault."),
    ("Case 5: PSI", "INT_PSI_IC is the fault."),
    ("Case 6: Multi_interlock(same group)", '"same group" came together. Check common source of "same group" and check MIS interlock.'),
    ("Case 7: Multi_interlock(different group)", 'Several different interlocks came together. Severe noise seems like a fault source. Check common fault of "digital interlocks".'),
    ("Case 8: Cavity_blip", "Cavity# blip. Check Cavity#."),
    ("Case 9: Cavity_quench", "old_Quench_CM#"),
    ("Case 10: RF_path_Forward", "RF station# moved first. Check RF station# path"),
    ("Case 11: RF_path_Cavity", "RF station# moved first. Check RF station# path"),
    ("Case 12: Cavity_detune", "The Cavity# was detuned. Check RF path of Cavity#."),
    ("Case 13: RF_source_fault", "All RF station moved together. Check common RF source like master oscillator,..."),
]


def seed_fault_types(conn: sqlite3.Connection) -> None:
    """Insert case-based fault types if not exist."""
    for name, desc in CASE_FAULT_TYPES:
        conn.execute(
            "INSERT OR IGNORE INTO fault_types (name, description) VALUES (?, ?)",
            (name, desc),
        )
    conn.commit()
