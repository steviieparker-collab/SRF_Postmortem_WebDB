"""
CRUD operations for SRF Event DB.

All sync functions accept an explicit sqlite3.Connection parameter
so callers control transaction boundaries.
"""

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from ..db.models import (
    Event,
    EventAttachment,
    EventCreate,
    EventLink,
    EventListResponse,
    EventSummary,
    FaultType,
    SimilarEvent,
)
from ..db.schema import get_sync_connection


# ── Helpers ──────────────────────────────────────────────────

def _row_to_event(row: sqlite3.Row) -> Event:
    return Event(
        id=row["id"],
        timestamp=row["timestamp"],
        scope1_file=row["scope1_file"],
        scope2_file=row["scope2_file"],
        scope3_file=row["scope3_file"],
        merged_file=row["merged_file"],
        fault_type=row["fault_type"],
        fault_confidence=row["fault_confidence"],
        beam_voltage=row["beam_voltage"],
        beam_current=row["beam_current"],
        analog_metrics=json.loads(row["analog_metrics"]) if row["analog_metrics"] is not None else None,
        digital_pattern=json.loads(row["digital_pattern"]) if row["digital_pattern"] is not None else None,
        time_groups=json.loads(row["time_groups"]) if row["time_groups"] is not None else None,
        graphs_path=row["graphs_path"],
        report_path=row["report_path"],
        report_md=row["report_md"],
        case_id=row["case_id"],
        case_description=row["case_description"],
        case_fault=row["case_fault"],
        user_beam_time=row["user_beam_time"],
        notes=row["notes"] if "notes" in row.keys() else "",
        user_fault_type=row["user_fault_type"] if "user_fault_type" in row.keys() else "",
        created_at=row["created_at"],
    )


def _row_to_summary(row: sqlite3.Row) -> EventSummary:
    return EventSummary(
        id=row["id"],
        timestamp=row["timestamp"],
        fault_type=row["fault_type"],
        fault_confidence=row["fault_confidence"],
        beam_voltage=row["beam_voltage"],
        beam_current=row["beam_current"],
        case_id=row["case_id"],
        case_description=row["case_description"],
        user_beam_time=row["user_beam_time"],
        notes=row["notes"] if "notes" in row.keys() else "",
        user_fault_type=row["user_fault_type"] if "user_fault_type" in row.keys() else "",
        created_at=row["created_at"],
    )


def _dict_to_event(d: Dict[str, Any]) -> Event:
    return Event(
        id=d["id"],
        timestamp=d["timestamp"],
        scope1_file=d.get("scope1_file"),
        scope2_file=d.get("scope2_file"),
        scope3_file=d.get("scope3_file"),
        merged_file=d.get("merged_file"),
        fault_type=d.get("fault_type"),
        fault_confidence=d.get("fault_confidence", 0.0),
        beam_voltage=d.get("beam_voltage"),
        beam_current=d.get("beam_current"),
        analog_metrics=d.get("analog_metrics"),
        digital_pattern=d.get("digital_pattern"),
        time_groups=d.get("time_groups"),
        graphs_path=d.get("graphs_path"),
        report_path=d.get("report_path"),
        report_md=d.get("report_md"),
        created_at=d.get("created_at", datetime.now(timezone.utc).isoformat()),
    )


# ── Event CRUD ───────────────────────────────────────────────

def create_event(conn: sqlite3.Connection, data: EventCreate) -> Event:
    """Insert a new event. Returns the created Event."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO events (
            id, timestamp,
            scope1_file, scope2_file, scope3_file, merged_file,
            fault_type, fault_confidence,
            beam_voltage, beam_current,
            analog_metrics, digital_pattern, time_groups,
            graphs_path, report_path, report_md,
            case_id, case_description, case_fault, user_beam_time,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            data.id,
            data.timestamp,
            data.scope1_file,
            data.scope2_file,
            data.scope3_file,
            data.merged_file,
            data.fault_type,
            data.fault_confidence,
            data.beam_voltage,
            data.beam_current,
            json.dumps(data.analog_metrics) if data.analog_metrics else None,
            json.dumps(data.digital_pattern) if data.digital_pattern else None,
            json.dumps(data.time_groups) if data.time_groups else None,
            data.graphs_path,
            data.report_path,
            data.report_md,
            data.case_id,
            data.case_description,
            data.case_fault,
            data.user_beam_time,
            now,
        ),
    )
    conn.commit()
    return get_event(conn, data.id)


def get_event(conn: sqlite3.Connection, event_id: str) -> Optional[Event]:
    """Get a single event by ID."""
    cursor = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,))
    row = cursor.fetchone()
    return _row_to_event(row) if row else None


def update_event(
    conn: sqlite3.Connection,
    event_id: str,
    updates: Dict[str, Any],
) -> Optional[Event]:
    """Partially update an event. Returns updated Event or None."""
    allowed = {
        "fault_type", "fault_confidence",
        "beam_voltage", "beam_current",
        "analog_metrics", "digital_pattern", "time_groups",
        "graphs_path", "report_path", "report_md",
        "case_id", "case_description", "case_fault",
        "user_beam_time", "notes", "user_fault_type",
    }
    fields = {k: v for k, v in updates.items() if k in allowed}

    if not fields:
        return get_event(conn, event_id)

    # Serialize JSON fields
    json_fields = {"analog_metrics", "digital_pattern", "time_groups"}
    for f in json_fields & fields.keys():
        if fields[f] is not None:
            fields[f] = json.dumps(fields[f])

    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [event_id]

    conn.execute(f"UPDATE events SET {set_clause} WHERE id = ?", values)
    conn.commit()
    return get_event(conn, event_id)


def delete_event(conn: sqlite3.Connection, event_id: str) -> bool:
    """Delete an event and its links. Returns True if deleted."""
    conn.execute("DELETE FROM event_links WHERE event_id = ? OR related_event_id = ?",
                 (event_id, event_id))
    cursor = conn.execute("DELETE FROM events WHERE id = ?", (event_id,))
    conn.commit()
    return cursor.rowcount > 0


# ── Event Listing / Search ───────────────────────────────────

def list_events(
    conn: sqlite3.Connection,
    page: int = 1,
    page_size: int = 20,
    fault_type: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    search: Optional[str] = None,
    user_beam_time: Optional[str] = None,
    hide_ms: bool = False,
) -> EventListResponse:
    """Paginated event list with optional filters."""
    conditions: List[str] = []
    params: List[Any] = []

    if fault_type:
        conditions.append("fault_type = ?")
        params.append(fault_type)

    if date_from:
        conditions.append("timestamp >= ?")
        params.append(date_from)

    if date_to:
        conditions.append("timestamp <= ?")
        params.append(date_to)

    if search:
        conditions.append("notes LIKE ?")
        params.append(f"%{search}%")

    if user_beam_time:
        if "," in user_beam_time:
            values = [v.strip() for v in user_beam_time.split(",") if v.strip()]
            placeholders = ",".join(["?"] * len(values))
            conditions.append(f"user_beam_time IN ({placeholders})")
            params.extend(values)
        else:
            conditions.append("user_beam_time = ?")
            params.append(user_beam_time)

    if hide_ms:
        conditions.append("(user_beam_time IS NULL OR user_beam_time = '' OR user_beam_time NOT LIKE '%MS%')")

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    # Total count
    count_row = conn.execute(
        f"SELECT COUNT(*) FROM events {where_clause}", params
    ).fetchone()
    total = count_row[0]

    # Paginated results
    offset = (page - 1) * page_size
    cursor = conn.execute(
        f"""
        SELECT id, timestamp, fault_type, fault_confidence,
               beam_voltage, beam_current, case_id, case_description,
               user_beam_time, user_fault_type, notes, created_at
        FROM events {where_clause}
        ORDER BY timestamp DESC
        LIMIT ? OFFSET ?
        """,
        params + [page_size, offset],
    )

    events = [_row_to_summary(row) for row in cursor.fetchall()]
    total_pages = max(1, (total + page_size - 1) // page_size)

    return EventListResponse(
        events=events,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )


def search_events_by_time(
    conn: sqlite3.Connection,
    start: str,
    end: str,
) -> List[EventSummary]:
    """Get events within a time range (ISO8601 strings)."""
    cursor = conn.execute(
        """
        SELECT id, timestamp, fault_type, fault_confidence,
               beam_voltage, beam_current, case_id, case_description, user_beam_time, created_at
        FROM events
        WHERE timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp DESC
        """,
        (start, end),
    )
    return [_row_to_summary(row) for row in cursor.fetchall()]


def get_events_by_fault_type(
    conn: sqlite3.Connection,
    fault_type: str,
    limit: int = 50,
) -> List[EventSummary]:
    """Get events filtered by fault type."""
    cursor = conn.execute(
        """
        SELECT id, timestamp, fault_type, fault_confidence,
               beam_voltage, beam_current, created_at
        FROM events
        WHERE fault_type = ?
        ORDER BY timestamp DESC
        LIMIT ?
        """,
        (fault_type, limit),
    )
    return [_row_to_summary(row) for row in cursor.fetchall()]


# ── Fault Type CRUD ──────────────────────────────────────────

def list_fault_types(conn: sqlite3.Connection, hide_ms: bool = False) -> List[FaultType]:
    """List all fault types with event counts."""
    ms_filter = ""
    if hide_ms:
        ms_filter = "AND (e.user_beam_time IS NULL OR e.user_beam_time = '' OR e.user_beam_time NOT LIKE '%MS%')"
    cursor = conn.execute(
        f"""
        SELECT ft.name, ft.description, ft.typical_pattern, ft.severity, ft.created_at,
               COUNT(e.id) as event_count
        FROM fault_types ft
        LEFT JOIN events e ON e.fault_type = ft.name {ms_filter}
        GROUP BY ft.name
        ORDER BY event_count DESC
        """
    )
    return [
        FaultType(
            name=row["name"],
            description=row["description"],
            typical_pattern=json.loads(row["typical_pattern"]) if row["typical_pattern"] is not None else None,
            severity=row["severity"],
            event_count=row["event_count"],
            created_at=row["created_at"],
        )
        for row in cursor.fetchall()
    ]


def get_fault_type(conn: sqlite3.Connection, name: str) -> Optional[FaultType]:
    """Get a fault type by name."""
    cursor = conn.execute(
        """
        SELECT ft.name, ft.description, ft.typical_pattern, ft.severity, ft.created_at,
               COUNT(e.id) as event_count
        FROM fault_types ft
        LEFT JOIN events e ON e.fault_type = ft.name
        WHERE ft.name = ?
        GROUP BY ft.name
        """,
        (name,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    return FaultType(
        name=row["name"],
        description=row["description"],
        typical_pattern=json.loads(row["typical_pattern"])
            if row["typical_pattern"] is not None else None,
        severity=row["severity"],
        event_count=row["event_count"],
        created_at=row["created_at"],
    )


# ── Event Links / Similarity ─────────────────────────────────

def create_event_link(
    conn: sqlite3.Connection,
    event_id: str,
    related_event_id: str,
    similarity_score: float,
) -> EventLink:
    """Create a similarity link between two events."""
    conn.execute(
        """
        INSERT OR REPLACE INTO event_links
            (event_id, related_event_id, similarity_score)
        VALUES (?, ?, ?)
        """,
        (event_id, related_event_id, similarity_score),
    )
    conn.commit()

    cursor = conn.execute(
        "SELECT * FROM event_links WHERE event_id = ? AND related_event_id = ?",
        (event_id, related_event_id),
    )
    row = cursor.fetchone()
    return EventLink(
        id=row["id"],
        event_id=row["event_id"],
        related_event_id=row["related_event_id"],
        similarity_score=row["similarity_score"],
        created_at=row["created_at"],
    )


def get_similar_events(
    conn: sqlite3.Connection,
    event_id: str,
    limit: int = 5,
) -> List[SimilarEvent]:
    """Get similar events from stored links."""
    cursor = conn.execute(
        """
        SELECT e.id, e.timestamp, e.fault_type, e.fault_confidence,
               e.beam_voltage, e.beam_current, e.created_at,
               el.similarity_score
        FROM event_links el
        JOIN events e ON e.id = el.related_event_id
        WHERE el.event_id = ?
        ORDER BY el.similarity_score DESC
        LIMIT ?
        """,
        (event_id, limit),
    )
    return [
        SimilarEvent(
            event=EventSummary(
                id=row["id"],
                timestamp=row["timestamp"],
                fault_type=row["fault_type"],
                fault_confidence=row["fault_confidence"],
                beam_voltage=row["beam_voltage"],
                beam_current=row["beam_current"],
                created_at=row["created_at"],
            ),
            similarity_score=row["similarity_score"],
        )
        for row in cursor.fetchall()
    ]


def clear_event_links(conn: sqlite3.Connection, event_id: str) -> None:
    """Remove all similarity links for an event."""
    conn.execute(
        "DELETE FROM event_links WHERE event_id = ? OR related_event_id = ?",
        (event_id, event_id),
    )
    conn.commit()


def get_adjacent_events(
    conn: sqlite3.Connection,
    event_id: str,
    fault_type: Optional[str] = None,
    search: Optional[str] = None,
    user_beam_time: Optional[str] = None,
    hide_ms: bool = False,
) -> tuple[Optional[str], Optional[str]]:
    """
    Returns (prev_event_id, next_event_id) within the filtered set.

    Events sorted by timestamp DESC (same order as list_events).
    prev = comes before current in the list (later timestamp)
    next = comes after current in the list (earlier timestamp)
    """
    cursor = conn.execute("SELECT timestamp FROM events WHERE id = ?", (event_id,))
    row = cursor.fetchone()
    if not row:
        return (None, None)

    current_ts = row["timestamp"]

    # Build filter conditions
    conditions = []
    params: list[Any] = []

    if fault_type:
        conditions.append("fault_type = ?")
        params.append(fault_type)
    if search:
        conditions.append("notes LIKE ?")
        params.append(f"%{search}%")
    if user_beam_time:
        conditions.append("user_beam_time = ?")
        params.append(user_beam_time)
    if hide_ms:
        conditions.append("(user_beam_time IS NULL OR user_beam_time = '' OR user_beam_time NOT LIKE '%MS%')")

    filter_clause = ""
    if conditions:
        filter_clause = "AND " + " AND ".join(conditions)

    # Previous event (later in time → appears earlier in DESC list)
    cursor = conn.execute(
        f"""
        SELECT id FROM events
        WHERE (timestamp > ? OR (timestamp = ? AND id > ?))
        {filter_clause}
        ORDER BY timestamp ASC, id ASC
        LIMIT 1
        """,
        [current_ts, current_ts, event_id] + params,
    )
    prev_row = cursor.fetchone()
    prev_id = prev_row["id"] if prev_row else None

    # Next event (earlier in time → appears later in DESC list)
    cursor = conn.execute(
        f"""
        SELECT id FROM events
        WHERE (timestamp < ? OR (timestamp = ? AND id < ?))
        {filter_clause}
        ORDER BY timestamp DESC, id DESC
        LIMIT 1
        """,
        [current_ts, current_ts, event_id] + params,
    )
    next_row = cursor.fetchone()
    next_id = next_row["id"] if next_row else None

    return (prev_id, next_id)


# ── Attachment CRUD ───────────────────────────────────────────


def _row_to_attachment(row: sqlite3.Row) -> EventAttachment:
    return EventAttachment(
        id=row["id"],
        event_id=row["event_id"],
        original_name=row["original_name"],
        stored_name=row["stored_name"],
        mime_type=row["mime_type"],
        file_size=row["file_size"],
        uploaded_at=row["uploaded_at"],
    )


def create_attachment(
    conn: sqlite3.Connection,
    event_id: str,
    original_name: str,
    stored_name: str,
    mime_type: Optional[str] = None,
    file_size: Optional[int] = None,
) -> EventAttachment:
    cursor = conn.execute(
        """
        INSERT INTO event_attachments (event_id, original_name, stored_name, mime_type, file_size)
        VALUES (?, ?, ?, ?, ?)
        """,
        (event_id, original_name, stored_name, mime_type, file_size),
    )
    conn.commit()
    attachment_id = cursor.lastrowid
    cursor = conn.execute("SELECT * FROM event_attachments WHERE id = ?", (attachment_id,))
    return _row_to_attachment(cursor.fetchone())


def list_attachments_by_event(
    conn: sqlite3.Connection,
    event_id: str,
) -> List[EventAttachment]:
    cursor = conn.execute(
        "SELECT * FROM event_attachments WHERE event_id = ? ORDER BY uploaded_at DESC",
        (event_id,),
    )
    return [_row_to_attachment(row) for row in cursor.fetchall()]


def get_attachment_by_id(
    conn: sqlite3.Connection,
    attachment_id: int,
) -> Optional[EventAttachment]:
    cursor = conn.execute("SELECT * FROM event_attachments WHERE id = ?", (attachment_id,))
    row = cursor.fetchone()
    return _row_to_attachment(row) if row else None


def delete_attachment(
    conn: sqlite3.Connection,
    attachment_id: int,
) -> bool:
    cursor = conn.execute("DELETE FROM event_attachments WHERE id = ?", (attachment_id,))
    conn.commit()
    return cursor.rowcount > 0


def get_attachment_counts(
    conn: sqlite3.Connection,
    event_ids: List[str],
) -> Dict[str, int]:
    if not event_ids:
        return {}
    placeholders = ",".join(["?"] * len(event_ids))
    cursor = conn.execute(
        f"SELECT event_id, COUNT(*) as cnt FROM event_attachments WHERE event_id IN ({placeholders}) GROUP BY event_id",
        event_ids,
    )
    counts = {row["event_id"]: row["cnt"] for row in cursor.fetchall()}
    # Ensure all requested IDs have an entry
    for eid in event_ids:
        counts.setdefault(eid, 0)
    return counts
