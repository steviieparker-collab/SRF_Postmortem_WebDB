"""SQLite writer for the alarm monitor.

CA callbacks enqueue events; a single writer thread batches them so the
monitor never blocks on disk I/O and SQLite sees one writer at a time.
"""

from __future__ import annotations

import queue
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path


class AlarmStore:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self.q: "queue.Queue[tuple]" = queue.Queue(maxsize=200_000)
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None

    # ── lifecycle ────────────────────────────────────────────
    def start(self):
        self.thread = threading.Thread(target=self._run, daemon=True, name="alarm-store")
        self.thread.start()

    def stop(self, timeout: float = 5.0):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=timeout)

    # ── producer API ─────────────────────────────────────────
    def transition(self, pv: str, value, prev_value, state: str, ts_utc: str, source: str = "ca"):
        self._put(("transition", (
            pv, ts_utc,
            "" if value is None else str(value),
            "" if prev_value is None else str(prev_value),
            state, source,
        )))

    def last_state(self, pv: str, value, state: str, ts_utc: str):
        self._put(("last", (
            pv,
            "" if value is None else str(value),
            state, ts_utc, ts_utc,
        )))

    def status(self, running: bool, pid: int, started_at: str, pv_count: int,
               connected_count: int, transition_count: int, error: str = ""):
        ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self._put(("status", (
            1 if running else 0, pid, started_at, ts,
            pv_count, connected_count, transition_count, error,
        )))

    def _put(self, item):
        try:
            self.q.put_nowait(item)
        except queue.Full:
            # Never block a CA callback; drop and let the next state update win.
            pass

    # ── writer ───────────────────────────────────────────────
    def _run(self):
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            while not self.stop_event.is_set() or not self.q.empty():
                batch = []
                deadline = time.time() + 0.5
                while len(batch) < 200 and time.time() < deadline:
                    try:
                        batch.append(self.q.get(timeout=0.1))
                        self.q.task_done()
                    except queue.Empty:
                        break
                if batch:
                    self._write_batch(conn, batch)
        finally:
            conn.close()

    def _write_batch(self, conn: sqlite3.Connection, batch: list[tuple]):
        transitions = []
        lasts = []
        statuses = []
        for kind, payload in batch:
            if kind == "transition":
                transitions.append(payload)
            elif kind == "last":
                lasts.append(payload)
            elif kind == "status":
                statuses.append(payload)

        try:
            if transitions:
                conn.executemany(
                    """
                    INSERT INTO alarm_transitions
                        (pv, ts_utc, value, prev_value, state, source)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    transitions,
                )
            if lasts:
                conn.executemany(
                    """
                    INSERT INTO alarm_last_state (pv, value, state, ts_utc, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(pv) DO UPDATE SET
                        value = excluded.value,
                        state = excluded.state,
                        ts_utc = excluded.ts_utc,
                        updated_at = excluded.updated_at
                    """,
                    lasts,
                )
            if statuses:
                conn.executemany(
                    """
                    INSERT INTO alarm_monitor_status
                        (id, running, pid, started_at, last_heartbeat,
                         pv_count, connected_count, transition_count, error)
                    VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        running = excluded.running,
                        pid = excluded.pid,
                        started_at = excluded.started_at,
                        last_heartbeat = excluded.last_heartbeat,
                        pv_count = excluded.pv_count,
                        connected_count = excluded.connected_count,
                        transition_count = excluded.transition_count,
                        error = excluded.error
                    """,
                    statuses,
                )
            conn.commit()
        except sqlite3.Error:
            # Keep the monitor alive; a transient DB lock should not kill CA.
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
