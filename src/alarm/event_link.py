"""Attach alarm messages to a beam-dump event.

The alarm monitor keeps a compact transition log.  When a new event is
imported we take a snapshot of the -60s .. +10s window around the beam-current
drop (t0), store it in `event_alarms`, and write a sidecar used by the event
page.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ARCHIVER_BASE = "https://plsarchiver.postech.ac.kr"
KST = timezone(timedelta(hours=9))

WINDOW_PRE_S = 60.0
WINDOW_POST_S = 10.0

SNAPSHOT_DIR = PROJECT_ROOT / "data" / "event_alarms"
SERIES_DIR = PROJECT_ROOT / "data" / "alarm_series"


def _archiver_json(url: str):
    with urllib.request.urlopen(url, timeout=20) as r:
        return json.loads(r.read().decode("utf-8"))


def _fetch_pv_range(pv: str, start: datetime, end: datetime):
    q = urllib.parse.urlencode({
        "pv": pv,
        "from": start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "to": end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
    })
    d = _archiver_json(f"{ARCHIVER_BASE}/api/v1/retrieval/data?{q}")
    pts = [
        (p["secs"] + p.get("nanos", 0) / 1e9, p["val"])
        for p in d.get("data", [])
        if p.get("val") is not None
    ]
    return pts, d.get("meta", {})


def resolve_t0(timestamp: str) -> tuple[float, datetime]:
    """Return (t0 offset from the event timestamp, event base in KST).

    Uses the same beam-current-drop rule as the pv-explorer.  If the archiver
    is unreachable, t0 falls back to 0 (the event timestamp).
    """
    try:
        base = datetime.fromisoformat(timestamp).replace(tzinfo=KST)
    except (TypeError, ValueError):
        base = datetime.now(KST)
    t0 = 0.0
    try:
        pts, _ = _fetch_pv_range(
            "SR:G00:BEAMCURRENT_T",
            base - timedelta(minutes=10),
            base + timedelta(minutes=3),
        )
        b = base.timestamp()
        pre = [(t, v) for t, v in pts if b - 600 <= t <= b + 60]
        if pre:
            mx = max(pre, key=lambda x: x[1])
            hit = next((t for t, v in pts if t > mx[0] and v < mx[1] * 0.99), None)
            if hit is not None:
                t0 = round(hit - b, 3)
    except Exception:
        pass
    return t0, base


def _parse_utc(ts: str) -> Optional[datetime]:
    if not ts:
        return None
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def _split(value: Optional[str]) -> list[str]:
    if not value:
        return []
    return [x.strip() for x in value.split(";") if x.strip()]


def attach_event_alarms(conn, event_id: str, timestamp: str) -> dict[str, Any]:
    """Snapshot alarm onsets for an event and write the page sidecar."""
    t0, base = resolve_t0(timestamp)
    t0_abs = base + timedelta(seconds=t0)
    start = t0_abs - timedelta(seconds=WINDOW_PRE_S)
    end = t0_abs + timedelta(seconds=WINDOW_POST_S)
    start_s = start.astimezone(timezone.utc).isoformat(timespec="milliseconds")
    end_s = end.astimezone(timezone.utc).isoformat(timespec="milliseconds")

    total = conn.execute("SELECT COUNT(*) FROM alarm_transitions").fetchone()[0]

    onsets = conn.execute(
        """
        SELECT pv, MIN(ts_utc) AS ts
          FROM alarm_transitions
         WHERE ts_utc >= ? AND ts_utc <= ? AND state = 'alarm'
         GROUP BY pv
        """,
        (start_s, end_s),
    ).fetchall()

    active = conn.execute(
        """
        SELECT t.pv, t.ts_utc
          FROM alarm_transitions t
          JOIN (
              SELECT pv, MAX(ts_utc) AS ts
                FROM alarm_transitions
               WHERE ts_utc < ?
               GROUP BY pv
          ) m ON t.pv = m.pv AND t.ts_utc = m.ts
         WHERE t.state = 'alarm'
        """,
        (start_s,),
    ).fetchall()

    onset_map = {r["pv"]: r["ts"] for r in onsets}
    active_map = {r["pv"]: r["ts_utc"] for r in active}
    pv_set = set(onset_map) | set(active_map)

    items: list[dict[str, Any]] = []
    for pv in sorted(pv_set):
        ts_str = onset_map.get(pv) or active_map.get(pv)
        ts_dt = _parse_utc(ts_str)
        rel = round((ts_dt - t0_abs).total_seconds(), 3) if ts_dt else 0.0
        active_at_start = pv in active_map

        cat = conn.execute(
            """
            SELECT description, subsystem, graph_pv, graph_description, severity
              FROM alarm_pv_catalog WHERE pv = ?
            """,
            (pv,),
        ).fetchone()
        description = cat["description"] if cat else pv
        subsystem = cat["subsystem"] if cat else ""
        graph_pv = cat["graph_pv"] if cat else ""
        graph_desc = cat["graph_description"] if cat else ""
        severity = cat["severity"] if cat else "medium"

        conn.execute(
            """
            INSERT OR REPLACE INTO event_alarms
                (event_id, pv, description, subsystem, graph_pv, graph_description,
                 first_seen_utc, first_seen_rel, active_at_start, severity)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (event_id, pv, description, subsystem, graph_pv, graph_desc,
             ts_str, rel, 1 if active_at_start else 0, severity),
        )

        items.append({
            "pv": pv,
            "description": description,
            "subsystem": subsystem,
            "graph_pv": _split(graph_pv),
            "graph_description": _split(graph_desc),
            "first_seen_utc": ts_str,
            "first_seen_rel": rel,
            "active_at_start": bool(active_at_start),
            "severity": severity,
        })
    conn.commit()

    items.sort(key=lambda x: (x["first_seen_rel"], x["pv"]))
    doc = {
        "event_id": event_id,
        "available": bool(total > 0),
        "reason": None if total > 0 else "no alarm transitions recorded (monitor not running for this event)",
        "t0_offset_sec": t0,
        "window": {"pre": WINDOW_PRE_S, "post": WINDOW_POST_S},
        "items": items,
    }
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    (SNAPSHOT_DIR / f"{event_id}.json").write_text(
        json.dumps(doc, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    return doc


def load_event_alarms(event_id: str) -> Optional[dict[str, Any]]:
    path = SNAPSHOT_DIR / f"{event_id}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def load_alarm_series(event_id: str) -> Optional[dict[str, Any]]:
    path = SERIES_DIR / f"{event_id}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def save_alarm_series(event_id: str, doc: dict[str, Any]) -> None:
    SERIES_DIR.mkdir(parents=True, exist_ok=True)
    (SERIES_DIR / f"{event_id}.json").write_text(
        json.dumps(doc, ensure_ascii=False, indent=1), encoding="utf-8"
    )
