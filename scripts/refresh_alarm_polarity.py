#!/usr/bin/env python3
"""Re-apply data/alarm/alarm_polarity.csv to the existing catalog rows.

Use after regenerating the polarity CSV with scripts/extract_alarm_polarity.py.
Only the polarity columns (alarm_expr, normal_expr, default_state, state_mode)
are rewritten; graph-PV mappings and user edits to them are preserved.
The alarm monitor reads rules at startup, so restart it afterwards.

    python scripts/refresh_alarm_polarity.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from src.alarm.catalog import apply_alarm_polarity  # noqa: E402
from src.db.schema import init_db_sync  # noqa: E402


def main() -> int:
    conn = init_db_sync()
    try:
        changed = apply_alarm_polarity(conn, refresh=True)
        rows = conn.execute(
            """SELECT state_mode, default_state, COUNT(*) AS n
                 FROM alarm_pv_catalog
                GROUP BY state_mode, default_state
                ORDER BY state_mode, default_state"""
        ).fetchall()
    finally:
        conn.close()
    print(f"polarity rows updated: {changed}")
    for r in rows:
        print(f"  {r['state_mode']:8} {r['default_state']:8} {r['n']}")
    print("restart the alarm monitor so it reloads the rules")
    return 0


if __name__ == "__main__":
    sys.exit(main())
