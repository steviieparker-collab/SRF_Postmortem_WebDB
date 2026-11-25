"""Attach alarm messages to a beam-dump event.

The alarm monitor keeps a compact transition log.  When a new event is
imported we take a snapshot of the -60s .. +10s window around the beam-current
drop (t0), store it in `event_alarms`, and write a sidecar used by the event
page.
"""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

_EVENT_ID_RE = re.compile(r"^\d{8}_\d{6}$")


def is_valid_event_id(event_id: str) -> bool:
    """event_id는 YYYYMMDD_HHMMSS 형식만 허용한다(경로 traversal 방지)."""
    return bool(_EVENT_ID_RE.fullmatch(str(event_id or "")))
from pathlib import Path
from typing import Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ARCHIVER_ROOT = "https://plsarchiver.postech.ac.kr"
ARCHIVER_BASE = ARCHIVER_ROOT  # backwards-compatible alias for the current API
KST = timezone(timedelta(hours=9))

# The current archiver API only covers data from this point on.  Earlier
# periods live in per-year legacy viewers (e.g. /2015/data/plotQueryData.php).
ARCHIVER_CUTOVER = datetime(2025, 9, 18, 9, 0, tzinfo=KST)
LEGACY_MIN_YEAR = 2015

WINDOW_PRE_S = 60.0
WINDOW_POST_S = 10.0

SNAPSHOT_DIR = PROJECT_ROOT / "data" / "event_alarms"
SERIES_DIR = PROJECT_ROOT / "data" / "alarm_series"

# Legacy viewer table names.  The old PHP endpoint needs the subsystem table
# that owns the PV; the graph PVs in alarm_pv_catalog use these prefixes.
_LEGACY_TABLE_PREFIXES = (
    ("SR:RF:KSU1:", "SR_RF_KSU1"),
    ("SR:RF:KSU2:", "SR_RF_KSU2"),
    ("SR:RF:KSU3:", "SR_RF_KSU3"),
    ("SR:RF:CRM1:", "SR_RF_CM1"),
    ("SR:RF:CRM2:", "SR_RF_CM2"),
    ("SR:RF:CRM3:", "SR_RF_CM3"),
    ("SR:RF:CRG", "SR_RF_CRG"),
    ("SR:RF:LRF", "SR_RF_LLRF"),
    ("SR:RF:INT_", "SR_RF_MAIN"),
    ("SR:G00:", "SR_BSTATUS"),
    ("SR:BEAMDOSE", "SR_BSTATUS"),
)


def _archiver_json(url: str):
    with urllib.request.urlopen(url, timeout=20) as r:
        return json.loads(r.read().decode("utf-8"))


def _to_kst(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=KST)
    return value.astimezone(KST)


def archiver_base(ref: Optional[datetime] = None) -> str:
    """Return the archiver base URL for an event reference time."""
    if ref is None:
        return ARCHIVER_ROOT
    ref = _to_kst(ref)
    if ref < ARCHIVER_CUTOVER and ref.year >= LEGACY_MIN_YEAR:
        return f"{ARCHIVER_ROOT}/{ref.year}"
    return ARCHIVER_ROOT


def _legacy_table_for_pv(pv: str) -> Optional[str]:
    for prefix, table in _LEGACY_TABLE_PREFIXES:
        if pv.startswith(prefix):
            return table
    return None


def _legacy_fetch_pv_range(pv: str, start: datetime, end: datetime, year: int):
    table = _legacy_table_for_pv(pv)
    if not table:
        raise ValueError(f"no legacy archiver table mapping for PV: {pv}")
    body = [
        ("table[]", f"{table}_PVINFO"),
        ("table[]", f"{table}_PVDATA"),
        ("PVs[]", pv),
        ("prevTime", _to_kst(start).strftime("%Y-%m-%d %H:%M:%S.0")),
        ("nowTime", _to_kst(end).strftime("%Y-%m-%d %H:%M:%S.0")),
    ]
    req = urllib.request.Request(
        f"{ARCHIVER_ROOT}/{year}/data/plotQueryData.php",
        data=urllib.parse.urlencode(body).encode("utf-8"),
        headers={"User-Agent": "Mozilla/5.0"},
    )
    with urllib.request.urlopen(req, timeout=40) as r:
        raw = r.read().decode("utf-8", "replace")
    if not raw:
        return [], {}
    try:
        d = json.loads(raw)
    except json.JSONDecodeError:
        return [], {}
    times = []
    for t in d.get("time", []):
        try:
            times.append(float(t) / 1000.0)
        except (TypeError, ValueError):
            continue
    keys = [k for k in d if k.startswith("d_")]
    vals = d.get(keys[0], []) if keys else []
    pts = []
    for i, v in enumerate(vals):
        if i >= len(times):
            break
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        pts.append((times[i], fv))
    units = d.get("unit", [])
    return pts, {"unit": units[0] if units else ""}


def _lrf_name_variant(pv: str, legacy: bool) -> Optional[str]:
    """Return the LRF PV spelling used by the requested archive era.

    The current API names LLRF PVs with an ``_ARCH`` suffix
    (``SR:RF:LRF1:GMES_ARCH``); the per-year legacy viewer stores the same
    signal without it (``SR:RF:LRF1:GMES``).  Asking the current API for the
    legacy spelling makes it return HTTP 500, so callers should not have to
    know which era a PV name belongs to.
    """
    if not pv.startswith("SR:RF:LRF"):
        return None
    if legacy:
        return pv[:-5] if pv.endswith("_ARCH") else None
    return None if pv.endswith("_ARCH") else f"{pv}_ARCH"


def _fetch_current(pv: str, start: datetime, end: datetime):
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


def fetch_pv_range(pv: str, start: datetime, end: datetime,
                   ref: Optional[datetime] = None):
    """Fetch a PV range from the current or legacy archiver.

    ``ref`` is the event reference time used to choose the archive period.
    LRF PV names are normalized to the era's spelling automatically, so both
    ``SR:RF:LRF1:GMES`` and ``SR:RF:LRF1:GMES_ARCH`` work for any year.
    """
    ref = ref or start
    base = archiver_base(ref)
    if base != ARCHIVER_ROOT:
        year = _to_kst(ref).year
        pts, meta = _legacy_fetch_pv_range(pv, start, end, year)
        alt = _lrf_name_variant(pv, legacy=True)
        if not pts and alt:
            try:
                pts, meta = _legacy_fetch_pv_range(alt, start, end, year)
            except ValueError:
                pass
        return pts, meta

    try:
        pts, meta = _fetch_current(pv, start, end)
    except urllib.error.HTTPError:
        alt = _lrf_name_variant(pv, legacy=False)
        if not alt:
            raise
        pts, meta = _fetch_current(alt, start, end)
        return pts, meta

    if not pts:
        alt = _lrf_name_variant(pv, legacy=False)
        if alt:
            try:
                apts, ameta = _fetch_current(alt, start, end)
                if apts:
                    return apts, ameta
            except urllib.error.HTTPError:
                pass
    return pts, meta


def _fetch_pv_range(pv: str, start: datetime, end: datetime,
                    ref: Optional[datetime] = None):
    """Backwards-compatible wrapper for fetch_pv_range."""
    return fetch_pv_range(pv, start, end, ref=ref)


def _sustained_drop(pts, threshold: float):
    """First sample below ``threshold`` that is confirmed by the next sample.

    A lone dip is not a beam dump.  The confirmation step matters because the
    legacy archive is 1 Hz and a 1% fluctuation just before the dump used to
    move t0 1-2 s earlier.
    """
    for i, (t, v) in enumerate(pts):
        if v < threshold and (i + 1 >= len(pts) or pts[i + 1][1] < threshold):
            return t
    return None


def resolve_t0(timestamp: str) -> tuple[float, datetime]:
    """Return (t0 offset from the event timestamp, event base in KST).

    t0 is the first sustained drop below 50% of the beam current maximum in
    the 10-minute pre-window.  The old 1%-drop rule triggered on small
    pre-dump dips (e.g. 20241002 was 2 s early) and misaligned Archiver series
    against the oscilloscope waveform.  If the archiver is unreachable, or no
    50% drop exists, t0 falls back to 0 (the event timestamp).
    """
    try:
        base = datetime.fromisoformat(timestamp).replace(tzinfo=KST)
    except (TypeError, ValueError):
        base = datetime.now(KST)
    t0 = 0.0
    try:
        pts, _ = fetch_pv_range(
            "SR:G00:BEAMCURRENT_T",
            base - timedelta(minutes=10),
            base + timedelta(minutes=3),
            ref=base,
        )
        b = base.timestamp()
        pre = [(t, v) for t, v in pts if b - 600 <= t <= b + 60]
        if pre:
            mx_t, mx_v = max(pre, key=lambda x: x[1])
            if mx_v > 0:
                after = [(t, v) for t, v in pts if t > mx_t]
                hit = _sustained_drop(after, mx_v * 0.5)
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
    if not is_valid_event_id(event_id):
        return None
    path = SNAPSHOT_DIR / f"{event_id}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def load_alarm_series(event_id: str) -> Optional[dict[str, Any]]:
    if not is_valid_event_id(event_id):
        return None
    path = SERIES_DIR / f"{event_id}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def save_alarm_series(event_id: str, doc: dict[str, Any]) -> None:
    if not is_valid_event_id(event_id):
        raise ValueError(f"invalid event_id: {event_id}")
    SERIES_DIR.mkdir(parents=True, exist_ok=True)
    (SERIES_DIR / f"{event_id}.json").write_text(
        json.dumps(doc, ensure_ascii=False, indent=1), encoding="utf-8"
    )
