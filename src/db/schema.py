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

CREATE TABLE IF NOT EXISTS page_views (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    date    TEXT NOT NULL,               -- '2026-06-21'
    path    TEXT NOT NULL,               -- '/'  '/events/xxx'
    count   INTEGER NOT NULL DEFAULT 0,
    UNIQUE(date, path)
);

-- ── Alarm dashboard / monitor ───────────────────────────────
CREATE TABLE IF NOT EXISTS alarm_pv_catalog (
    pv                TEXT PRIMARY KEY,
    description       TEXT NOT NULL DEFAULT '',
    subsystem         TEXT NOT NULL DEFAULT '',
    source_file       TEXT DEFAULT '',
    graph_pv          TEXT DEFAULT '',      -- ';' separated
    graph_description TEXT DEFAULT '',
    match_type        TEXT DEFAULT '',
    confidence        TEXT DEFAULT '',
    alarm_state       TEXT DEFAULT '',      -- '0', '1', 'complex' (legacy)
    normal_state      TEXT DEFAULT '',
    alarm_expr        TEXT DEFAULT '',      -- JSON list of red(expr) conditions
    normal_expr       TEXT DEFAULT '',      -- JSON list of green(expr) conditions
    default_state     TEXT DEFAULT '',      -- normal / alarm / unknown
    state_mode        TEXT DEFAULT '',      -- expr / complex / default
    enabled           INTEGER NOT NULL DEFAULT 1,
    severity          TEXT NOT NULL DEFAULT 'medium',
    updated_by        TEXT DEFAULT '',
    updated_at        TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_alarm_catalog_subsystem ON alarm_pv_catalog(subsystem);

CREATE TABLE IF NOT EXISTS alarm_transitions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    pv          TEXT NOT NULL,
    ts_utc      TEXT NOT NULL,
    value       TEXT,
    prev_value  TEXT,
    state       TEXT NOT NULL,              -- alarm / normal / unknown / complex
    source      TEXT NOT NULL DEFAULT 'ca',
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_alarm_tr_ts    ON alarm_transitions(ts_utc);
CREATE INDEX IF NOT EXISTS idx_alarm_tr_pv_ts ON alarm_transitions(pv, ts_utc);

CREATE TABLE IF NOT EXISTS alarm_last_state (
    pv          TEXT PRIMARY KEY,
    value       TEXT,
    state       TEXT,
    ts_utc      TEXT,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

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

CREATE TABLE IF NOT EXISTS event_alarms (
    event_id          TEXT NOT NULL,
    pv                TEXT NOT NULL,
    description       TEXT,
    subsystem         TEXT,
    graph_pv          TEXT,
    graph_description TEXT,
    first_seen_utc    TEXT,
    first_seen_rel    REAL,
    active_at_start   INTEGER DEFAULT 0,
    severity          TEXT,
    PRIMARY KEY (event_id, pv)
);
CREATE INDEX IF NOT EXISTS idx_event_alarms_event ON event_alarms(event_id);

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

CREATE TABLE IF NOT EXISTS event_pv_selection (
    event_id          TEXT NOT NULL,
    pv                TEXT NOT NULL,
    description       TEXT NOT NULL DEFAULT '',
    position          INTEGER NOT NULL DEFAULT 0,
    updated_at        TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (event_id, pv)
);
CREATE INDEX IF NOT EXISTS idx_event_pv_selection_event
    ON event_pv_selection(event_id, position);
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
    for col, typ in [
        ("alarm_expr", "TEXT DEFAULT ''"),
        ("normal_expr", "TEXT DEFAULT ''"),
        ("default_state", "TEXT DEFAULT ''"),
        ("state_mode", "TEXT DEFAULT ''"),
    ]:
        try:
            conn.execute(f"ALTER TABLE alarm_pv_catalog ADD COLUMN {col} {typ}")
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
    for col, typ in [
        ("alarm_expr", "TEXT DEFAULT ''"),
        ("normal_expr", "TEXT DEFAULT ''"),
        ("default_state", "TEXT DEFAULT ''"),
        ("state_mode", "TEXT DEFAULT ''"),
    ]:
        try:
            await conn.execute(f"ALTER TABLE alarm_pv_catalog ADD COLUMN {col} {typ}")
        except sqlite3.OperationalError:
            pass
    await conn.commit()
    return conn


# ── Seed Default Fault Types ────────────────────────────────

CASE_FAULT_TYPES = [
    ("Case 0: Unknown", "Unclassified by classifier."),
    ("Case 1: Beam_loss", "Beam loss, check other systems like MPS, feedback system."),
    ("Case 2: MPS", "MPS trip (manually assigned)."),
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
    ("Case 14: Utility", "Utility event: vacuum leak, cooling water, power supply, manual entry, etc."),
    ("Case 15: Others", "Other miscellaneous events."),
    ("Case 16: Beam line", "Beam line related event: beam dump, shutter, valve, etc."),
    ("Case 17: Control", "Control system related event: IOC/EPICS, network, timing, etc."),
    ("Case 18: Kicker", "Kicker magnet / pulsed power related event."),
    ("Case 19: Magnet", "Magnet / magnet power supply related event."),
    ("Case 20: Operator", "Operator action or manual intervention."),
    ("Case 21: MechVac", "Mechanical vacuum related event."),
]


def seed_fault_types(conn: sqlite3.Connection) -> None:
    """Insert case-based fault types if not exist."""
    for name, desc in CASE_FAULT_TYPES:
        conn.execute(
            "INSERT OR IGNORE INTO fault_types (name, description) VALUES (?, ?)",
            (name, desc),
        )
    conn.commit()
