"""Alarm PV catalog: seed from CSV, query current state, update mapping.

The CSV under data/alarm/ is a one-time seed.  After the first import the
SQLite table `alarm_pv_catalog` is the source of truth; the dashboard edits it.
"""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SEED_PATH = PROJECT_ROOT / "data" / "alarm" / "alarm_pv_catalog_seed.csv"
POLARITY_PATH = PROJECT_ROOT / "data" / "alarm" / "alarm_polarity.csv"

SOURCE_BY_SUBSYSTEM = {
    "KSU1": "KSU/KSU1_Status.opi",
    "KSU2": "KSU/KSU2_Status.opi",
    "KSU3": "KSU/KSU3_Status.opi",
    "CRM1": "cryo_module/Readychain1.opi",
    "CRM2": "cryo_module/Readychain2.opi",
    "CRM3": "cryo_module/Readychain3.opi",
    "CRG": "cryogenic/CRG_Status.opi",
}


def _severity(confidence: str) -> str:
    return {"high": "high", "medium": "medium", "low": "low"}.get(
        (confidence or "").lower(), "medium"
    )


def load_polarity() -> dict[str, dict[str, str]]:
    """Load data/alarm/alarm_polarity.csv (generated from the OPI rules)."""
    if not POLARITY_PATH.exists():
        return {}
    out: dict[str, dict[str, str]] = {}
    with open(POLARITY_PATH, encoding="utf-8-sig", newline="") as fh:
        for r in csv.DictReader(fh):
            pv = (r.get("pv") or "").strip()
            if pv:
                out[pv] = r
    return out


def apply_alarm_polarity(conn, refresh: bool = False) -> int:
    """Fill polarity columns from the OPI rules.

    Existing user edits are preserved by default: only rows with an empty
    state_mode are updated, and `enabled` is only forced to 0 for multi-state
    summary PVs.  ``refresh=True`` re-applies the rules to every row (used by
    scripts/refresh_alarm_polarity.py after the extractor changes); graph-PV
    mappings and updated_by are never touched.
    """
    polarity = load_polarity()
    if not polarity:
        return 0
    where = "pv = ?" if refresh else "pv = ? AND (state_mode IS NULL OR state_mode = '')"
    updated = 0
    for pv, p in polarity.items():
        cur = conn.execute(
            f"""
            UPDATE alarm_pv_catalog
               SET alarm_expr = ?, normal_expr = ?, default_state = ?, state_mode = ?,
                   enabled = CASE WHEN ? = 0 THEN 0 ELSE enabled END
             WHERE {where}
            """,
            (
                p.get("alarm_expr", ""),
                p.get("normal_expr", ""),
                p.get("default_state", ""),
                p.get("state_mode", ""),
                int(p.get("enabled", 1)),
                pv,
            ),
        )
        updated += cur.rowcount
    conn.commit()
    return updated


def monitor_catalog(conn) -> list[dict[str, Any]]:
    """Rows the CA monitor should subscribe to."""
    rows = conn.execute(
        """
        SELECT pv, description, subsystem, alarm_expr, normal_expr,
               default_state, state_mode
          FROM alarm_pv_catalog
         WHERE enabled = 1
         ORDER BY subsystem, pv
        """
    ).fetchall()
    return [dict(r) for r in rows]


def seed_alarm_catalog(conn) -> int:
    """Import the seed CSV once, when the catalog table is empty.

    Returns the number of inserted rows (0 if already seeded / no seed file).
    """
    count = conn.execute("SELECT COUNT(*) FROM alarm_pv_catalog").fetchone()[0]
    if count:
        return 0
    if not SEED_PATH.exists():
        return 0

    polarity = load_polarity()
    rows: list[tuple] = []
    with open(SEED_PATH, encoding="utf-8-sig", newline="") as fh:
        for r in csv.DictReader(fh):
            pv = (r.get("alarm_pv") or "").strip()
            if not pv:
                continue
            subsystem = (r.get("alarm_file") or "").strip()
            pol = polarity.get(pv, {})
            rows.append((
                pv,
                (r.get("description") or "").strip(),
                subsystem,
                SOURCE_BY_SUBSYSTEM.get(subsystem, ""),
                (r.get("graph_pv") or "").strip(),
                (r.get("graph_description") or "").strip(),
                (r.get("match_type") or "").strip(),
                (r.get("confidence") or "").strip(),
                "0",                      # legacy polarity defaults
                "1",
                pol.get("alarm_expr", ""),
                pol.get("normal_expr", ""),
                pol.get("default_state", ""),
                pol.get("state_mode", ""),
                int(pol.get("enabled", 1)),
                _severity(r.get("confidence") or ""),
                "seed",
            ))
    if not rows:
        return 0
    conn.executemany(
        """
        INSERT OR IGNORE INTO alarm_pv_catalog
            (pv, description, subsystem, source_file, graph_pv, graph_description,
             match_type, confidence, alarm_state, normal_state,
             alarm_expr, normal_expr, default_state, state_mode,
             enabled, severity, updated_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def _split_multi(value: Optional[str]) -> list[str]:
    if not value:
        return []
    return [x.strip() for x in value.split(";") if x.strip()]


def _row_to_item(row) -> dict[str, Any]:
    d = dict(row)
    d["graph_pv_list"] = _split_multi(d.get("graph_pv"))
    d["graph_description_list"] = _split_multi(d.get("graph_description"))
    d["live_state"] = d.get("live_state") or "unknown"
    d["mapped"] = bool(d.get("graph_pv_list"))
    return d


def list_alarm_states(
    conn,
    subsystem: Optional[str] = None,
    state: Optional[str] = None,
    mapping: Optional[str] = None,
    enabled: Optional[str] = None,
    q: Optional[str] = None,
) -> list[dict[str, Any]]:
    sql = """
        SELECT c.pv, c.description, c.subsystem, c.source_file,
               c.graph_pv, c.graph_description, c.match_type, c.confidence,
               c.alarm_state, c.normal_state, c.enabled, c.severity,
               c.updated_by, c.updated_at,
               s.value AS live_value, s.state AS live_state, s.ts_utc AS live_ts
        FROM alarm_pv_catalog c
        LEFT JOIN alarm_last_state s ON s.pv = c.pv
        WHERE 1=1
    """
    params: list[Any] = []
    if subsystem:
        sql += " AND c.subsystem = ?"
        params.append(subsystem)
    if state:
        if state == "unknown":
            sql += " AND (s.state IS NULL OR s.state = 'unknown')"
        else:
            sql += " AND s.state = ?"
            params.append(state)
    if mapping == "mapped":
        sql += " AND TRIM(COALESCE(c.graph_pv, '')) <> ''"
    elif mapping == "unmapped":
        sql += " AND TRIM(COALESCE(c.graph_pv, '')) = ''"
    if enabled in ("0", "1"):
        sql += " AND c.enabled = ?"
        params.append(int(enabled))
    if q:
        sql += " AND (c.pv LIKE ? OR c.description LIKE ?)"
        like = f"%{q}%"
        params.extend([like, like])
    sql += " ORDER BY c.subsystem, c.pv"
    return [_row_to_item(r) for r in conn.execute(sql, params).fetchall()]


def alarm_summary(conn) -> dict[str, int]:
    """Counts for the dashboard.

    State counts are over enabled alarms only; disabled rows (multi-state
    summaries such as ACTUAL_STATE) are reported separately.
    """
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            COALESCE(SUM(CASE WHEN c.enabled = 1 THEN 1 ELSE 0 END), 0) AS enabled,
            COALESCE(SUM(CASE WHEN c.enabled = 1 AND s.state = 'alarm' THEN 1 ELSE 0 END), 0) AS alarm,
            COALESCE(SUM(CASE WHEN c.enabled = 1 AND s.state = 'normal' THEN 1 ELSE 0 END), 0) AS normal,
            COALESCE(SUM(CASE WHEN c.enabled = 1 AND s.state = 'complex' THEN 1 ELSE 0 END), 0) AS complex,
            COALESCE(SUM(CASE WHEN c.enabled = 1
                               AND (s.state IS NULL OR s.state = 'unknown') THEN 1 ELSE 0 END), 0) AS unknown,
            COALESCE(SUM(CASE WHEN c.enabled = 1
                               AND TRIM(COALESCE(c.graph_pv, '')) = '' THEN 1 ELSE 0 END), 0) AS unmapped,
            COALESCE(SUM(CASE WHEN c.enabled = 0 THEN 1 ELSE 0 END), 0) AS disabled
        FROM alarm_pv_catalog c
        LEFT JOIN alarm_last_state s ON s.pv = c.pv
        """
    ).fetchone()
    return {k: int(row[k] or 0) for k in row.keys()}


def live_states(conn) -> dict[str, dict[str, Any]]:
    """Compact current-state map used by the 1-second dashboard poll."""
    rows = conn.execute(
        "SELECT pv, value, state, ts_utc FROM alarm_last_state"
    ).fetchall()
    return {
        r["pv"]: {
            "state": r["state"] or "unknown",
            "value": r["value"],
            "ts": r["ts_utc"],
        }
        for r in rows
    }


def get_monitor_status(conn) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM alarm_monitor_status WHERE id = 1").fetchone()
    if not row:
        return {
            "running": False, "pid": None, "started_at": None,
            "last_heartbeat": None, "pv_count": 0, "connected_count": 0,
            "transition_count": 0, "error": "",
        }
    d = dict(row)
    d["running"] = bool(d.get("running"))
    return d


def update_alarm_catalog(conn, pv: str, data: dict[str, Any], changed_by: str = "web"):
    """Update one catalog row and append a mapping-history entry."""
    old = conn.execute(
        "SELECT graph_pv, enabled FROM alarm_pv_catalog WHERE pv = ?", (pv,)
    ).fetchone()
    if not old:
        return None

    fields: list[str] = []
    params: list[Any] = []

    if "graph_pv" in data:
        gp = data.get("graph_pv")
        if isinstance(gp, (list, tuple)):
            gp = ";".join(str(x).strip() for x in gp if str(x).strip())
        else:
            gp = ";".join(_split_multi(str(gp or "")))
        fields.append("graph_pv = ?")
        params.append(gp)

    if "graph_description" in data:
        gd = data.get("graph_description")
        if isinstance(gd, (list, tuple)):
            gd = ";".join(str(x).strip() for x in gd if str(x).strip())
        else:
            gd = ";".join(x.strip() for x in str(gd or "").split(";") if x.strip())
        fields.append("graph_description = ?")
        params.append(gd)

    if "enabled" in data:
        fields.append("enabled = ?")
        params.append(1 if int(data.get("enabled") or 0) else 0)

    if "severity" in data:
        sev = str(data.get("severity") or "medium")
        if sev not in ("low", "medium", "high"):
            sev = "medium"
        fields.append("severity = ?")
        params.append(sev)

    if "alarm_state" in data:
        fields.append("alarm_state = ?")
        params.append(str(data.get("alarm_state") or ""))
    if "normal_state" in data:
        fields.append("normal_state = ?")
        params.append(str(data.get("normal_state") or ""))

    if not fields:
        return None

    fields.append("updated_by = ?")
    params.append(changed_by)
    fields.append("updated_at = ?")
    params.append(datetime.now(timezone.utc).isoformat(timespec="seconds"))
    params.append(pv)

    conn.execute(f"UPDATE alarm_pv_catalog SET {', '.join(fields)} WHERE pv = ?", params)

    new_graph = data.get("graph_pv", old["graph_pv"])
    if isinstance(new_graph, (list, tuple)):
        new_graph = ";".join(str(x).strip() for x in new_graph if str(x).strip())
    new_enabled = data.get("enabled", old["enabled"])
    conn.execute(
        """
        INSERT INTO alarm_mapping_history
            (pv, old_graph_pv, new_graph_pv, old_enabled, new_enabled, changed_by)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (pv, old["graph_pv"], new_graph, old["enabled"],
         1 if int(new_enabled or 0) else 0, changed_by),
    )
    conn.commit()
    return conn.execute(
        "SELECT * FROM alarm_pv_catalog WHERE pv = ?", (pv,)
    ).fetchone()
