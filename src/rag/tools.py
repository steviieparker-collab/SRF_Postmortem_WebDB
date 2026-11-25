"""Deterministic structured tools over the read-only events DB."""

from __future__ import annotations

import json
import re
import sqlite3
from typing import Any, Optional

from .db import connect_events
from .index import expand_query, search as index_search


# Manual case designation (user_fault_type) always overrides the automatic
# classifier. Keep this expression in sync with src/db/repository.py and the
# web app so MCP answers match the UI.
EFFECTIVE_FAULT_SQL = "COALESCE(NULLIF(user_fault_type,''), NULLIF(fault_type,''), '')"

# case_id is the numeric part of the fault-type label ("Case 3: RF_Interlock"
# -> 3). The automatic case_id column is reference only; the effective case id
# follows the manual label when the operator reclassified the event.
EFFECTIVE_CASE_ID_SQL = (
    "CASE WHEN trim(coalesce(user_fault_type,'')) <> '' "
    "AND instr(user_fault_type, ':') > 6 "
    "THEN CAST(substr(user_fault_type, 6, instr(user_fault_type, ':') - 6) AS INTEGER) "
    "ELSE case_id END"
)

# "MS" beam times are Machine Study periods (e.g. 2026-LLRF-MS, 2026-01MS).
# MCP answers default to excluding them, matching the web UI's hide_ms option.
MS_EXCLUDED_SQL = (
    "(user_beam_time IS NULL OR user_beam_time = '' "
    "OR user_beam_time NOT LIKE '%MS%')"
)

_CASE_ID_RE = re.compile(r"Case\s+(\d+)")


def _case_id_from_label(label: str | None) -> int | None:
    """Extract the numeric case id from a 'Case N: Name' label."""
    if not label:
        return None
    match = _CASE_ID_RE.search(label)
    return int(match.group(1)) if match else None


def _json_count(value: str | None) -> int:
    if not value:
        return 0
    try:
        obj = json.loads(value)
    except (TypeError, ValueError):
        return 0
    return len(obj) if hasattr(obj, "__len__") else 0


def _event_where(filters: Optional[dict[str, Any]]) -> tuple[str, list[Any]]:
    filters = filters or {}
    clauses: list[str] = []
    params: list[Any] = []
    if filters.get("event_id"):
        clauses.append("id = ?")
        params.append(filters["event_id"])
    if filters.get("date"):
        clauses.append("substr(timestamp,1,10) = ?")
        params.append(str(filters["date"]))
    if filters.get("year"):
        clauses.append("substr(timestamp,1,4) = ?")
        params.append(str(filters["year"]))
    if filters.get("year_from"):
        clauses.append("substr(timestamp,1,4) >= ?")
        params.append(str(filters["year_from"]))
    if filters.get("year_to"):
        clauses.append("substr(timestamp,1,4) <= ?")
        params.append(str(filters["year_to"]))
    if filters.get("fault_type"):
        clauses.append(f"{EFFECTIVE_FAULT_SQL} LIKE ?")
        params.append(f"%{filters['fault_type']}%")
    if filters.get("fault_types"):
        types = filters["fault_types"]
        if isinstance(types, str):
            types = [types]
        types = [str(t) for t in types if t]
        if types:
            clauses.append(
                f"{EFFECTIVE_FAULT_SQL} IN ("
                + ",".join("?" for _ in types) + ")"
            )
            params.extend(types)
    if filters.get("has_waveform") is not None:
        if filters["has_waveform"]:
            clauses.append("trim(coalesce(merged_file,'')) <> ''")
        else:
            clauses.append("trim(coalesce(merged_file,'')) = ''")
    if filters.get("beam_time"):
        clauses.append("user_beam_time LIKE ?")
        params.append(f"%{filters['beam_time']}%")
    if filters.get("pv"):
        # Match a PV name against the event-level alarm and manually selected
        # PV tables. The catalog is global, so it is not an event association.
        pv = str(filters["pv"])
        clauses.append(
            "(EXISTS (SELECT 1 FROM event_alarms a "
            "WHERE a.event_id = events.id AND (a.pv LIKE ? OR a.graph_pv LIKE ?)) "
            "OR EXISTS (SELECT 1 FROM event_pv_selection s "
            "WHERE s.event_id = events.id AND s.pv LIKE ?))"
        )
        params.extend([f"%{pv}%"] * 3)
    if filters.get("subsystem"):
        sub = str(filters["subsystem"]).replace("#", "").upper()
        if sub == "KSU":
            variants = ["KSU"]
        elif sub in {"CRM", "CM"}:
            variants = [
                "CM1", "CM2", "CM3", "CM#1", "CM#2", "CM#3",
                "CRM1", "CRM2", "CRM3", "SRF1", "SRF2", "SRF3",
            ]
        elif sub == "CRG":
            variants = ["CRG"]
        elif sub.startswith("CRM") and len(sub) == 4:
            n = sub[3:]
            variants = [
                f"CRM{n}", f"CRM#{n}", f"CM{n}", f"CM#{n}",
                f"SRF{n}", f"SRF#{n}", f"SRF #{n}",
            ]
        elif sub.startswith("KSU") and len(sub) == 4:
            n = sub[3:]
            variants = [f"KSU{n}", f"KSU#{n}"]
        else:
            variants = [sub]
        sub_clauses = []
        for variant in variants:
            sub_clauses.append(
                "(notes LIKE ? OR case_description LIKE ? OR case_fault LIKE ?)"
            )
            params.extend([f"%{variant}%"] * 3)
        clauses.append("(" + " OR ".join(sub_clauses) + ")")
    if filters.get("text"):
        # AND the meaningful tokens so "ion pump current high" matches
        # "Ion Pump 2 current too high" without requiring exact phrasing.
        tokens = [t for t in str(filters["text"]).split() if len(t) >= 2][:6]
        for token in tokens:
            clauses.append(
                "(notes LIKE ? OR case_description LIKE ? OR case_fault LIKE ?)"
            )
            params.extend([f"%{token}%"] * 3)
    if filters.get("hide_ms"):
        clauses.append(MS_EXCLUDED_SQL)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


def search_events(filters: Optional[dict[str, Any]] = None,
                  limit: int = 20) -> list[dict[str, Any]]:
    where, params = _event_where(filters)
    conn = connect_events()
    try:
        rows = conn.execute(
            """
            SELECT id, timestamp, fault_type, case_id, case_description, case_fault,
                   user_fault_type, user_beam_time, notes, merged_file,
                   (SELECT count(*) FROM event_alarms a WHERE a.event_id = events.id)
                       AS alarm_count,
                   (SELECT count(*) FROM event_pv_selection s WHERE s.event_id = events.id)
                       AS pv_selection_count
              FROM events
            """ + where + " ORDER BY timestamp DESC LIMIT ?",
            [*params, limit],
        ).fetchall()
        items = []
        for r in rows:
            effective = r["user_fault_type"] or r["fault_type"] or ""
            effective_case_id = _case_id_from_label(effective)
            if effective_case_id is None:
                effective_case_id = r["case_id"]
            manual = r["user_fault_type"] or ""
            auto = r["fault_type"] or ""
            note = None
            if manual and auto and manual != auto:
                note = f"수동분류 {manual} (자동분류 {auto})"
            items.append({
                "id": r["id"],
                "timestamp": r["timestamp"],
                "fault_type": r["fault_type"],
                "auto_fault_type": auto,
                "effective_fault_type": effective,
                "case_id": r["case_id"],
                "effective_case_id": effective_case_id,
                "case_description": r["case_description"],
                "case_fault": r["case_fault"],
                "user_fault_type": r["user_fault_type"],
                "classification_note": note,
                "user_beam_time": r["user_beam_time"],
                "notes": r["notes"],
                "has_waveform": bool(r["merged_file"]),
                "alarm_count": r["alarm_count"],
                "pv_selection_count": r["pv_selection_count"],
                "url": f"/events/{r['id']}",
            })
        return items
    finally:
        conn.close()


GROUP_EXPRS = {
    "year": "substr(timestamp,1,4)",
    "month": "substr(timestamp,1,7)",
    "fault_type": (
        "COALESCE(NULLIF(user_fault_type,''), NULLIF(fault_type,''), "
        "'(unclassified)')"
    ),
    # Manual classification takes priority; the automatic case_id is reference.
    "case_id": EFFECTIVE_CASE_ID_SQL,
    "auto_case_id": "case_id",
    "auto_fault_type": "COALESCE(NULLIF(fault_type,''),'(unclassified)')",
    "user_fault_type": "COALESCE(NULLIF(user_fault_type,''),'(none)')",
    "beam_time": "COALESCE(NULLIF(user_beam_time,''),'(none)')",
}


def aggregate_events(group_by: str | list[str] = "year",
                     filters: Optional[dict[str, Any]] = None,
                     include_percent: bool = False) -> dict[str, Any]:
    """Aggregate events by one or more fields.

    ``group_by`` accepts a single field or a comma-separated / list of fields:
    year, month, fault_type, case_id, auto_case_id, auto_fault_type,
    user_fault_type, beam_time. ``case_id`` follows the manual classification
    first; ``auto_case_id`` is the automatic reference.
    """
    if isinstance(group_by, str):
        groups = [g.strip() for g in group_by.split(",") if g.strip()]
    else:
        groups = [str(g).strip() for g in group_by if str(g).strip()]
    if not groups:
        groups = ["year"]
    exprs: list[str] = []
    for group in groups:
        expr = GROUP_EXPRS.get(group)
        if not expr:
            return {
                "error": f"unsupported group_by: {group}",
                "supported": sorted(GROUP_EXPRS),
            }
        exprs.append(expr)
    select = ", ".join(f"{expr} AS key{i}" for i, expr in enumerate(exprs))
    group_clause = ", ".join(f"key{i}" for i in range(len(exprs)))
    where, params = _event_where(filters)
    conn = connect_events()
    try:
        rows = conn.execute(
            f"SELECT {select}, count(*) AS n FROM events"
            + where
            + f" GROUP BY {group_clause} ORDER BY n DESC",
            params,
        ).fetchall()
        total = conn.execute(
            "SELECT count(*) FROM events" + where, params
        ).fetchone()[0]
        as_of = conn.execute(
            "SELECT max(timestamp) AS latest, max(created_at) AS latest_created, "
            "count(*) AS db_total FROM events"
        ).fetchone()
        out: list[dict[str, Any]] = []
        for r in rows:
            keys = {groups[i]: r[f"key{i}"] for i in range(len(groups))}
            key = (
                keys[groups[0]]
                if len(groups) == 1
                else " / ".join(str(keys[g]) for g in groups)
            )
            item: dict[str, Any] = {"key": key, "keys": keys, "count": r["n"]}
            if include_percent:
                item["percent"] = (
                    round(100.0 * r["n"] / total, 2) if total else 0.0
                )
            out.append(item)
        latest = as_of["latest"] or ""
        latest_date = latest[:10] if latest else ""
        return {
            "group_by": groups,
            "total": total,
            "sum_of_counts": sum(item["count"] for item in out),
            "rows": out,
            "data_as_of": {
                "latest_event_timestamp": latest,
                "latest_created_at": as_of["latest_created"],
                "db_total_events": as_of["db_total"],
                "statement": (
                    f"as of {latest_date}, total {as_of['db_total']} events in DB"
                    if latest_date else "DB is empty"
                ),
            },
            "note": (
                "Counts and percentages are computed by SQL. Copy key and count "
                "values verbatim into the answer; do not estimate, merge, or "
                "recompute them. sum_of_counts must equal total. When stating a "
                "total, include the data_as_of date so the reader knows the "
                "snapshot the counts come from."
            ),
        }
    finally:
        conn.close()


def get_event_context(event_id: str) -> dict[str, Any]:
    conn = connect_events()
    try:
        r = conn.execute(
            """
            SELECT id, timestamp, fault_type, fault_confidence, case_id,
                   case_description, case_fault, user_fault_type, user_beam_time,
                   notes, merged_file, analog_metrics, digital_pattern,
                   beam_voltage, beam_current
              FROM events WHERE id = ?
            """,
            (event_id,),
        ).fetchone()
        if not r:
            return {"error": "event not found", "event_id": event_id}
        attachments = [
            dict(x)
            for x in conn.execute(
                """
                SELECT original_name, mime_type, file_size
                  FROM event_attachments WHERE event_id = ? ORDER BY id
                """,
                (event_id,),
            )
        ]
        similar = [
            dict(x)
            for x in conn.execute(
                """
                SELECT l.related_event_id AS id, l.similarity_score,
                       e.timestamp, e.fault_type, e.case_description, e.notes
                  FROM event_links l
                  JOIN events e ON e.id = l.related_event_id
                 WHERE l.event_id = ?
                 ORDER BY l.similarity_score DESC LIMIT 10
                """,
                (event_id,),
            )
        ]
        alarms = [
            dict(x)
            for x in conn.execute(
                """
                SELECT pv, description, subsystem, graph_pv, graph_description,
                       first_seen_utc, first_seen_rel, active_at_start, severity
                  FROM event_alarms WHERE event_id = ? ORDER BY first_seen_rel, pv
                """,
                (event_id,),
            )
        ]
        selection = [
            dict(x)
            for x in conn.execute(
                """
                SELECT pv, description, position
                  FROM event_pv_selection WHERE event_id = ?
                 ORDER BY position, pv
                """,
                (event_id,),
            )
        ]
        pv_set = {a["pv"] for a in alarms} | {s["pv"] for s in selection}
        for a in alarms:
            for p in (a.get("graph_pv") or "").split(";"):
                if p.strip():
                    pv_set.add(p.strip())
        catalog = []
        for c in conn.execute(
            """
            SELECT pv, description, subsystem, graph_pv, graph_description,
                   severity, state_mode
              FROM alarm_pv_catalog ORDER BY subsystem, pv
            """
        ):
            graph_pvs = {p.strip() for p in (c["graph_pv"] or "").split(";") if p.strip()}
            if c["pv"] in pv_set or (graph_pvs & pv_set):
                catalog.append(dict(c))
        manual = r["user_fault_type"] or ""
        auto = r["fault_type"] or ""
        effective = manual or auto
        effective_case_id = _case_id_from_label(effective)
        if effective_case_id is None:
            effective_case_id = r["case_id"]
        if manual and auto and manual != auto:
            display = (
                f"{r['id']} 이벤트는 {manual}입니다. "
                f"(자동분류는 {auto}로 판정)"
            )
        elif effective:
            display = f"{r['id']} 이벤트는 {effective}입니다."
        else:
            display = f"{r['id']} 이벤트는 분류되지 않았습니다."
        return {
            "event": {
                "id": r["id"],
                "timestamp": r["timestamp"],
                "fault_type": r["fault_type"],
                "auto_fault_type": auto,
                "effective_fault_type": effective,
                "fault_confidence": r["fault_confidence"],
                "case_id": r["case_id"],
                "effective_case_id": effective_case_id,
                "case_description": r["case_description"],
                "case_fault": r["case_fault"],
                "user_fault_type": r["user_fault_type"],
                "user_beam_time": r["user_beam_time"],
                "notes": r["notes"],
                "beam_voltage": r["beam_voltage"],
                "beam_current": r["beam_current"],
                "url": f"/events/{event_id}",
            },
            "classification": {
                "effective": effective,
                "manual": manual or None,
                "auto": auto or None,
                "effective_case_id": effective_case_id,
                "auto_case_id": r["case_id"],
                "manual_overrides_auto": bool(manual and auto and manual != auto),
                "display": display,
            },
            "waveform": {
                "available": bool(r["merged_file"]),
                "analog_channels": _json_count(r["analog_metrics"]),
                "digital_channels": _json_count(r["digital_pattern"]),
            },
            "attachments": attachments,
            "similar": similar,
            "alarms": alarms,
            "pv_selection": selection,
            "catalog": catalog,
        }
    finally:
        conn.close()


def similar_events(event_id: str, limit: int = 10) -> list[dict[str, Any]]:
    conn = connect_events()
    try:
        rows = conn.execute(
            """
            SELECT l.related_event_id AS id, l.similarity_score,
                   e.timestamp, e.fault_type, e.case_description, e.notes
              FROM event_links l
              JOIN events e ON e.id = l.related_event_id
             WHERE l.event_id = ?
             ORDER BY l.similarity_score DESC LIMIT ?
            """,
            (event_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def search_alarm_catalog(query: str, subsystem: Optional[str] = None,
                         limit: int = 20) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    seen: set[str] = set()
    expanded = expand_query(query)
    cleaned = re.sub(
        r"(알람|pv|그래프|인터록|카탈로그|매핑|graph|alarm|interlock)",
        " ", query, flags=re.IGNORECASE,
    ).strip()
    if cleaned:
        # Semantic/FTS hits from the RAG index first.
        for hit in index_search(
            expanded,
            filters={"doc_type": "alarm_catalog", "subsystem": subsystem} if subsystem
            else {"doc_type": "alarm_catalog"},
            top_k=limit,
        ):
            meta = hit.get("metadata") or {}
            pv = meta.get("pv") or hit.get("pv")
            if pv and pv not in seen:
                seen.add(pv)
                results.append(meta)
    # Exact LIKE fallback for PV names and short terms.
    conn = connect_events()
    try:
        clauses: list[str] = []
        params: list[Any] = []
        if cleaned:
            clauses.append("(pv LIKE ? OR description LIKE ? OR graph_description LIKE ?)")
            params.extend([f"%{cleaned}%"] * 3)
        if subsystem:
            clauses.append("subsystem = ?")
            params.append(subsystem)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = conn.execute(
            "SELECT pv, description, subsystem, graph_pv, graph_description, "
            "severity, state_mode FROM alarm_pv_catalog"
            + where
            + " ORDER BY subsystem, pv LIMIT ?",
            [*params, limit],
        ).fetchall()
        for r in rows:
            if r["pv"] not in seen:
                seen.add(r["pv"])
                results.append(dict(r))
        return results[:limit]
    finally:
        conn.close()


def get_alarm_status() -> dict[str, Any]:
    conn = connect_events()
    try:
        status = conn.execute(
            "SELECT * FROM alarm_monitor_status WHERE id = 1"
        ).fetchone()
        state_counts = {
            r["state"]: r["n"]
            for r in conn.execute(
                "SELECT state, count(*) AS n FROM alarm_last_state GROUP BY state"
            )
        }
        last_state_span = conn.execute(
            "SELECT min(updated_at) AS first, max(updated_at) AS last "
            "FROM alarm_last_state"
        ).fetchone()
        transitions = [
            dict(r)
            for r in conn.execute(
                "SELECT pv, ts_utc, value, prev_value, state FROM alarm_transitions "
                "ORDER BY ts_utc DESC LIMIT 10"
            )
        ]
        catalog = conn.execute(
            "SELECT count(*) AS n, "
            "sum(CASE WHEN trim(coalesce(graph_pv,'')) <> '' THEN 1 ELSE 0 END) AS mapped "
            "FROM alarm_pv_catalog"
        ).fetchone()
        return {
            "monitor": dict(status) if status else None,
            "last_state_counts": state_counts,
            "last_state_updated": dict(last_state_span) if last_state_span else None,
            "recent_transitions": transitions,
            "catalog": dict(catalog) if catalog else None,
            "note": (
                "alarm_last_state is the monitor's last known state; "
                "check last_heartbeat for freshness. Historical events have "
                "very few recorded transitions."
            ),
        }
    finally:
        conn.close()
