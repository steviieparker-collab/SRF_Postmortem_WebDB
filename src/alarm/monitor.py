"""EPICS CA alarm monitor.

Subscribes to the 524 interlock/status PVs, classifies each value with the
OPI-derived polarity rules, and records state transitions and the latest
state in SQLite.  Started/stopped by the web app (Start/Stop Monitor).

Run manually:
    python -m src.alarm.monitor --config config/config.yaml
    python -m src.alarm.monitor --config config/config.yaml --once
"""

from __future__ import annotations

import argparse
import ctypes
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from multiprocessing import Pool
from pathlib import Path

from ..core.config import load_config
from ..db.schema import get_sync_connection, init_db_sync
from .catalog import monitor_catalog
from .polarity import classify
from .store import AlarmStore


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _set_pdeathsig():
    """Ask the kernel to SIGTERM us when the web process dies (Linux only).

    Only enabled when the web app's manager sets ALARM_MONITOR_PDEATHSIG=1;
    manual/ops starts (scripts/alarm_monitor_ctl.py) intentionally skip it.
    """
    if os.environ.get("ALARM_MONITOR_PDEATHSIG") != "1":
        return
    if not sys.platform.startswith("linux"):
        return
    try:
        libc = ctypes.CDLL("libc.so.6")
        libc.prctl(1, signal.SIGTERM)  # PR_SET_PDEATHSIG
    except Exception:
        pass


def _log(msg: str):
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}] {msg}", flush=True)


def _set_ca_env(config):
    alarm = getattr(config, "alarm", None)
    if alarm is None:
        return
    if alarm.ca_addr_list:
        os.environ["EPICS_CA_ADDR_LIST"] = alarm.ca_addr_list
    if alarm.ca_auto_addr_list:
        os.environ["EPICS_CA_AUTO_ADDR_LIST"] = alarm.ca_auto_addr_list


def _load_rows(db_path: str):
    conn = init_db_sync()
    try:
        return monitor_catalog(conn)
    finally:
        conn.close()


def _get_one(row):
    """Worker for snapshot mode. Each process gets its own CA context."""
    import epics
    try:
        return row, epics.caget(row["pv"], timeout=2.0, connection_timeout=2.0)
    except Exception:
        return row, None


def run_once(epics, db_path: str, rows: list[dict]) -> int:
    """Read one snapshot, write last_state + a baseline transition, exit."""
    _log(f"snapshot mode: {len(rows)} PVs")

    with Pool(12) as pool:
        results = pool.map(_get_one, rows, chunksize=2)

    ts = _now()
    conn = get_sync_connection()
    try:
        last_rows = []
        trans_rows = []
        counts = {"alarm": 0, "normal": 0, "complex": 0, "unknown": 0}
        connected = 0
        for row, value in results:
            if value is not None:
                connected += 1
            state = classify(row, value)
            counts[state] = counts.get(state, 0) + 1
            last_rows.append((row["pv"], "" if value is None else str(value), state, ts, ts))
            if state == "alarm":
                trans_rows.append((row["pv"], ts, "" if value is None else str(value),
                                   "", state, "snapshot"))
        conn.executemany(
            """
            INSERT INTO alarm_last_state (pv, value, state, ts_utc, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(pv) DO UPDATE SET
                value = excluded.value, state = excluded.state,
                ts_utc = excluded.ts_utc, updated_at = excluded.updated_at
            """,
            last_rows,
        )
        conn.executemany(
            """
            INSERT INTO alarm_transitions (pv, ts_utc, value, prev_value, state, source)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            trans_rows,
        )
        conn.execute(
            """
            INSERT INTO alarm_monitor_status
                (id, running, pid, started_at, last_heartbeat,
                 pv_count, connected_count, transition_count, error)
            VALUES (1, 0, ?, ?, ?, ?, ?, ?, '')
            ON CONFLICT(id) DO UPDATE SET
                running = 0, pid = excluded.pid, started_at = excluded.started_at,
                last_heartbeat = excluded.last_heartbeat,
                pv_count = excluded.pv_count, connected_count = excluded.connected_count,
                transition_count = excluded.transition_count, error = ''
            """,
            (os.getpid(), ts, ts, len(rows), connected, len(trans_rows)),
        )
        conn.commit()
    finally:
        conn.close()

    _log(f"snapshot done: connected {connected}/{len(rows)} states {counts}")
    return 0


def run_forever(epics, db_path: str, rows: list[dict], config, duration: float = 0) -> int:
    rows_by_pv = {r["pv"]: r for r in rows}
    store = AlarmStore(db_path)
    store.start()

    stop_event = threading.Event()
    started_at = _now()
    state_lock = threading.Lock()
    last: dict[str, dict] = {}
    transition_count = 0
    pvs = {}
    connected_count = 0

    alarm_cfg = getattr(config, "alarm", None)
    heartbeat_s = getattr(alarm_cfg, "heartbeat_s", 5.0)

    def on_value(pv_name: str, value):
        nonlocal transition_count
        row = rows_by_pv.get(pv_name)
        if row is None:
            return
        state = classify(row, value)
        ts = _now()
        with state_lock:
            prev = last.get(pv_name)
            if prev is None:
                last[pv_name] = {"value": value, "state": state}
                # Baseline: keep the current state, but only write a
                # transition when the alarm is already active at startup.
                if state == "alarm":
                    store.transition(pv_name, value, None, state, ts, "init")
                    transition_count += 1
                store.last_state(pv_name, value, state, ts)
                return

            # Only state changes are persisted.  Normal value changes are not
            # written; the dashboard only needs the current alarm state.
            if state != prev["state"]:
                store.transition(pv_name, value, prev["value"], state, ts, "ca")
                transition_count += 1
                prev["state"] = state
                prev["value"] = value
                store.last_state(pv_name, value, state, ts)

    def make_cb(name):
        def cb(**kw):
            try:
                on_value(name, kw.get("value"))
            except Exception as e:
                _log(f"callback error {name}: {e}")
        return cb

    _log(f"starting subscriptions for {len(rows)} PVs")
    for row in rows:
        name = row["pv"]
        try:
            pvs[name] = epics.PV(
                name, callback=make_cb(name), auto_monitor=True,
                form="time", connection_timeout=2.0,
            )
        except Exception as e:
            _log(f"PV create failed {name}: {e}")

    def heartbeat():
        nonlocal connected_count
        while not stop_event.is_set():
            try:
                connected_count = sum(1 for p in pvs.values() if p.connected)
                store.status(True, os.getpid(), started_at, len(pvs),
                             connected_count, transition_count)
            except Exception as e:
                _log(f"heartbeat error: {e}")
            stop_event.wait(heartbeat_s)

    hb = threading.Thread(target=heartbeat, daemon=True, name="alarm-heartbeat")
    hb.start()
    store.status(True, os.getpid(), started_at, len(pvs), 0, transition_count)
    _log(f"monitor running: pid={os.getpid()} pv={len(pvs)}")

    def _stop(signum, frame):
        _log(f"signal {signum}, stopping")
        stop_event.set()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    if duration and duration > 0:
        stop_event.wait(duration)
        stop_event.set()
    else:
        stop_event.wait()

    _log("stopping subscriptions")
    for pv in pvs.values():
        try:
            pv.clear_callbacks()
        except Exception:
            pass
    try:
        connected_count = sum(1 for p in pvs.values() if p.connected)
        store.status(False, os.getpid(), started_at, len(pvs),
                     connected_count, transition_count)
    except Exception:
        pass
    store.stop()
    _log(f"monitor stopped: transitions={transition_count}")
    return 0


def main():
    _set_pdeathsig()
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--once", action="store_true", help="one-shot snapshot and exit")
    ap.add_argument("--duration", type=float, default=0, help="run for N seconds (testing)")
    args = ap.parse_args()

    config = load_config(args.config)
    db_path = str(config.db.path)
    _set_ca_env(config)

    try:
        import epics
    except Exception as e:
        _log(f"pyepics import failed: {e}")
        try:
            conn = init_db_sync()
            conn.execute(
                """
                INSERT INTO alarm_monitor_status
                    (id, running, pid, started_at, last_heartbeat, pv_count,
                     connected_count, transition_count, error)
                VALUES (1, 0, ?, ?, ?, 0, 0, 0, ?)
                ON CONFLICT(id) DO UPDATE SET running = 0, error = excluded.error,
                    last_heartbeat = excluded.last_heartbeat
                """,
                (os.getpid(), _now(), _now(), f"pyepics import failed: {e}"),
            )
            conn.commit()
            conn.close()
        except Exception:
            pass
        return 1

    rows = _load_rows(db_path)
    if not rows:
        _log("no enabled alarm PVs in catalog")
        return 1

    if args.once:
        return run_once(epics, db_path, rows)
    return run_forever(epics, db_path, rows, config, args.duration)


if __name__ == "__main__":
    sys.exit(main())
