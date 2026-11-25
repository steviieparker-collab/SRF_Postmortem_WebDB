"""SQLite helpers for the RAG assistant.

The live events DB is always opened read-only.  The RAG index is a separate
SQLite file so a rebuild can never touch production data.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional

from .config import get_rag_config


def connect_events(readonly: bool = True) -> sqlite3.Connection:
    cfg = get_rag_config()
    if readonly:
        uri = f"file:{cfg.db_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=10)
    else:
        conn = sqlite3.connect(str(cfg.db_path), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def connect_index(readonly: bool = True) -> sqlite3.Connection:
    cfg = get_rag_config()
    cfg.index_path.parent.mkdir(parents=True, exist_ok=True)
    if readonly:
        uri = f"file:{cfg.index_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=10)
    else:
        conn = sqlite3.connect(str(cfg.index_path), timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


def index_exists(path: Optional[Path] = None) -> bool:
    p = path or get_rag_config().index_path
    return p.exists() and p.stat().st_size > 0
