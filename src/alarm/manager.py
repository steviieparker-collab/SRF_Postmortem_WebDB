"""Lifecycle manager for the alarm-monitor subprocess.

The web app starts/stops the monitor together with the CSV pipeline monitor.
The monitor writes its own heartbeat; this module only spawns/signals it and
reports the combined status.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.config import load_config
from ..db.schema import get_sync_connection
from .catalog import get_monitor_status

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PID_FILE = PROJECT_ROOT / "data" / "alarm" / "monitor.pid"
LOG_FILE = PROJECT_ROOT / "logs" / "alarm_monitor.log"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _read_pid() -> dict[str, Any] | None:
    try:
        return json.loads(PID_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except (OSError, ValueError):
        return False


def _write_status(running: bool, pid: int | None = None, error: str = ""):
    try:
        conn = get_sync_connection()
        try:
            cur = conn.execute(
                "UPDATE alarm_monitor_status SET running = ?, error = ? WHERE id = 1",
                (1 if running else 0, error),
            )
            if cur.rowcount == 0:
                conn.execute(
                    "INSERT INTO alarm_monitor_status (id, running, pid, error) VALUES (1, ?, ?, ?)",
                    (1 if running else 0, pid, error),
                )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass


def start_alarm_monitor(config_path: str = "config/config.yaml", pdeathsig: bool = True) -> dict[str, Any]:
    info = _read_pid()
    if info and _alive(info.get("pid")):
        return {"ok": True, "already": True, "pid": info.get("pid")}

    # Clean a stale PID file.
    if info:
        PID_FILE.unlink(missing_ok=True)

    try:
        import epics  # noqa: F401
    except Exception as e:
        msg = f"pyepics is not installed in this Python environment: {e}"
        _write_status(False, error=msg)
        return {"ok": False, "error": msg}

    try:
        config = load_config(config_path)
    except Exception as e:
        return {"ok": False, "error": f"config load failed: {e}"}

    env = os.environ.copy()
    if pdeathsig:
        env["ALARM_MONITOR_PDEATHSIG"] = "1"
    else:
        env.pop("ALARM_MONITOR_PDEATHSIG", None)
    alarm = getattr(config, "alarm", None)
    if alarm is not None:
        if alarm.ca_addr_list:
            env["EPICS_CA_ADDR_LIST"] = alarm.ca_addr_list
        if alarm.ca_auto_addr_list:
            env["EPICS_CA_AUTO_ADDR_LIST"] = alarm.ca_auto_addr_list

    PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    cmd = [sys.executable, "-m", "src.alarm.monitor", "--config", str(config_path)]
    log = open(LOG_FILE, "a", encoding="utf-8", buffering=1)
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except Exception as e:
        log.close()
        _write_status(False, error=f"spawn failed: {e}")
        return {"ok": False, "error": f"spawn failed: {e}"}
    finally:
        try:
            log.close()
        except Exception:
            pass

    PID_FILE.write_text(
        json.dumps({"pid": proc.pid, "started_at": _now()}, ensure_ascii=False),
        encoding="utf-8",
    )
    _write_status(True, proc.pid)
    return {"ok": True, "pid": proc.pid, "message": "Alarm monitor started"}


def stop_alarm_monitor() -> dict[str, Any]:
    info = _read_pid()
    if not info:
        _write_status(False)
        return {"ok": True, "message": "Alarm monitor not running"}

    pid = info.get("pid")
    if _alive(pid):
        try:
            os.kill(int(pid), signal.SIGTERM)
        except OSError:
            pass
        for _ in range(50):  # up to 10 seconds
            if not _alive(pid):
                break
            time.sleep(0.2)
        if _alive(pid):
            try:
                os.kill(int(pid), signal.SIGKILL)
            except OSError:
                pass

    PID_FILE.unlink(missing_ok=True)
    _write_status(False, pid)
    return {"ok": True, "pid": pid, "message": "Alarm monitor stopped"}


def alarm_monitor_status() -> dict[str, Any]:
    conn = get_sync_connection()
    try:
        status = get_monitor_status(conn)
    finally:
        conn.close()

    info = _read_pid()
    pid = info.get("pid") if info else status.get("pid")
    alive = _alive(pid)
    if status.get("running") and not alive:
        _write_status(False, pid)
        status["running"] = False
    status["pid"] = pid
    status["pid_alive"] = alive
    return status


def ensure_clean() -> None:
    """Remove a stale PID file left by a crashed/restarted web process."""
    info = _read_pid()
    if info and not _alive(info.get("pid")):
        PID_FILE.unlink(missing_ok=True)
        _write_status(False)
