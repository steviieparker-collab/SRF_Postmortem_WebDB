"""Ops helper for the alarm monitor.

Examples:
    python scripts/alarm_monitor_ctl.py start
    python scripts/alarm_monitor_ctl.py stop
    python scripts/alarm_monitor_ctl.py status
    python scripts/alarm_monitor_ctl.py once

`start` detaches the monitor from the calling shell (no PDEATHSIG), so it is
suitable for bringing the monitor up before the web app's Start Monitor is
pressed.  The web app's Stop Monitor and the separate API can still stop it.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.alarm.manager import (  # noqa: E402
    alarm_monitor_status,
    start_alarm_monitor,
    stop_alarm_monitor,
)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["start", "stop", "status", "once"])
    ap.add_argument("--config", default=str(ROOT / "config" / "config.yaml"))
    args = ap.parse_args()

    if args.action == "start":
        result = start_alarm_monitor(args.config, pdeathsig=False)
    elif args.action == "stop":
        result = stop_alarm_monitor()
    elif args.action == "status":
        result = alarm_monitor_status()
    else:
        result = subprocess.run(
            [sys.executable, "-m", "src.alarm.monitor", "--config", args.config, "--once"],
            cwd=str(ROOT),
        )
        return result.returncode

    print(json.dumps(result, ensure_ascii=False, indent=1, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
