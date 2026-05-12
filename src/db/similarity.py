"""
Similarity search engine for SRF events.

Strategy (per v2-plan):
1. Same fault_type preferred
2. Recent time proximity (within N days)
3. Analog metrics cosine similarity (B/C/D/E channels)
4. Top 5 → store in event_links table
"""

import json
import math
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from ..db.repository import (
    create_event_link,
    clear_event_links,
    get_event,
    list_events,
    search_events_by_time,
)


# ── Vector Helpers ───────────────────────────────────────────

def _extract_analog_vector(event) -> Optional[List[float]]:
    """Extract a numeric vector from analog_metrics for similarity comparison.

    Builds a flat vector from all channel peak/mean/std values.
    Returns None if no analog metrics available.
    """
    if not event or not event.analog_metrics:
        return None

    channels = event.analog_metrics
    vec = []
    for ch_name, ch_data in sorted(channels.items()):
        if isinstance(ch_data, dict):
            vec.append(float(ch_data.get("peak", 0.0)))
            vec.append(float(ch_data.get("mean", 0.0)))
            vec.append(float(ch_data.get("std", 0.0)))
            vec.append(float(ch_data.get("p2p", 0.0)))
    return vec


def cosine_similarity(a: List[float], b: List[float]) -> float:
    """Cosine similarity between two vectors."""
    if not a or not b or len(a) != len(b):
        return 0.0

    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))

    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0

    return dot / (norm_a * norm_b)


# ── Similarity Search ────────────────────────────────────────

def find_similar_events(
    conn: sqlite3.Connection,
    event_id: str,
    limit: int = 5,
    max_days: int = 30,
) -> List[Tuple[str, float]]:
    """Find similar events and return (event_id, score) pairs.

    Scoring:
    - Same fault_type: +0.3 base bonus
    - Within 24h: +0.2 time proximity bonus
    - Analog cosine similarity: scaled 0.0–0.5
    - Total: 0.0–1.0
    """
    target = get_event(conn, event_id)
    if not target:
        return []

    candidates = _collect_candidates(conn, target, max_days)
    if not candidates:
        return []

    target_vec = _extract_analog_vector(target)
    scored: List[Tuple[str, float]] = []

    for cand_id, cand in candidates:
        if cand_id == event_id:
            continue

        score = 0.0

        # Same fault_type bonus
        if cand.fault_type and target.fault_type and cand.fault_type == target.fault_type:
            score += 0.3

        # Time proximity bonus
        try:
            t_target = datetime.fromisoformat(target.timestamp)
            t_cand = datetime.fromisoformat(cand.timestamp)
            hours_diff = abs((t_target - t_cand).total_seconds()) / 3600
            if hours_diff <= 24:
                score += 0.2
            elif hours_diff <= 72:
                score += 0.1
        except (ValueError, TypeError):
            pass

        # Analog similarity
        cand_vec = _extract_analog_vector(cand)
        if target_vec and cand_vec:
            sim = cosine_similarity(target_vec, cand_vec)
            score += sim * 0.5  # scale to max 0.5

        scored.append((cand_id, round(score, 4)))

    # Sort by score descending, take top N
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:limit]


def _collect_candidates(
    conn: sqlite3.Connection,
    target_event,
    max_days: int,
) -> List[Tuple[str, Any]]:
    """Collect candidate events for similarity comparison.

    Strategy:
    - Get events with same fault_type (recent N days)
    - If not enough, expand to all recent events
    """
    candidates: List[Tuple[str, Any]] = []

    # First: same fault_type within max_days
    if target_event.fault_type:
        try:
            t = datetime.fromisoformat(target_event.timestamp)
            start = (t - timedelta(days=max_days)).isoformat()
            end = (t + timedelta(days=max_days)).isoformat()
        except (ValueError, TypeError):
            start = ""
            end = ""

        same_type_events = []
        if start and target_event.fault_type:
            cursor = conn.execute(
                """
                SELECT * FROM events
                WHERE fault_type = ? AND timestamp >= ? AND timestamp <= ?
                ORDER BY timestamp DESC
                """,
                (target_event.fault_type, start, end),
            )
            same_type_events = [
                (row["id"], _row_to_event(row))
                for row in cursor.fetchall()
            ]
        candidates.extend(same_type_events)

    # If not enough, add recent events from other types
    if len(candidates) < 20:
        try:
            t = datetime.fromisoformat(target_event.timestamp)
            start = (t - timedelta(days=max_days)).isoformat()
            end = t.isoformat()
        except (ValueError, TypeError):
            start = ""
            end = ""

        if start:
            cursor = conn.execute(
                """
                SELECT * FROM events
                WHERE id != ? AND timestamp >= ? AND timestamp <= ?
                ORDER BY timestamp DESC
                LIMIT 50
                """,
                (target_event.id, start, end),
            )
            other_events = [
                (row["id"], _row_to_event(row))
                for row in cursor.fetchall()
            ]
            # Deduplicate
            existing_ids = {e[0] for e in candidates}
            candidates.extend(
                e for e in other_events if e[0] not in existing_ids
            )

    return candidates


def _row_to_event(row) -> Any:
    """Convert a sqlite3.Row to an Event-like object for similarity comparison."""
    from ..db.models import Event
    import json

    return Event(
        id=row["id"],
        timestamp=row["timestamp"],
        fault_type=row["fault_type"],
        fault_confidence=row["fault_confidence"],
        beam_voltage=row["beam_voltage"],
        beam_current=row["beam_current"],
        analog_metrics=json.loads(row["analog_metrics"]) if row["analog_metrics"] is not None else None,
        digital_pattern=json.loads(row["digital_pattern"]) if row["digital_pattern"] is not None else None,
        time_groups=json.loads(row["time_groups"]) if row["time_groups"] is not None else None,
        created_at=row["created_at"],
    )


# ── Public API ───────────────────────────────────────────────

def update_similarity_links(
    conn: sqlite3.Connection,
    event_id: str,
    limit: int = 5,
) -> List[Tuple[str, float]]:
    """Find similar events and store links in the DB.

    Returns list of (related_event_id, similarity_score).
    """
    # Clear old links for this event
    clear_event_links(conn, event_id)

    # Find similar
    similar = find_similar_events(conn, event_id, limit=limit)

    # Store links
    for related_id, score in similar:
        create_event_link(conn, event_id, related_id, score)

    return similar


def update_all_similarity_links(conn: sqlite3.Connection, limit: int = 5) -> int:
    """Refresh similarity links for ALL events in the DB.

    Returns total number of links created.
    """
    cursor = conn.execute("SELECT id FROM events")
    event_ids = [row["id"] for row in cursor.fetchall()]
    total_links = 0
    for i, eid in enumerate(event_ids):
        links = update_similarity_links(conn, eid, limit=limit)
        total_links += len(links)
        if (i + 1) % 50 == 0:
            print(f"    Similarity: [{i+1}/{len(event_ids)}] {total_links} links")
    return total_links
