"""Build RAG documents from the read-only SRF Postmortem DB."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Iterable


@dataclass
class RagDocument:
    doc_key: str
    doc_type: str
    text: str
    title: str
    event_id: str = ""
    pv: str = ""
    year: str = ""
    fault_type: str = ""
    subsystem: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


def _json_count(value: str | None) -> int:
    if not value:
        return 0
    try:
        obj = json.loads(value)
    except (TypeError, ValueError):
        return 0
    return len(obj) if hasattr(obj, "__len__") else 0


def _one_line(value: str | None, limit: int = 400) -> str:
    if not value:
        return ""
    text = " ".join(str(value).split())
    return text[:limit]


def _event_cards(conn: sqlite3.Connection,
                 attachments: dict[str, list[str]],
                 links: dict[str, list[str]]) -> Iterable[RagDocument]:
    rows = conn.execute(
        """
        SELECT id, timestamp, fault_type, fault_confidence, case_id,
               case_description, case_fault, user_fault_type, user_beam_time,
               notes, merged_file, analog_metrics, digital_pattern, beam_current
          FROM events
         ORDER BY timestamp
        """
    )
    for r in rows:
        eid = r["id"]
        year = (r["timestamp"] or "")[:4]
        analog_n = _json_count(r["analog_metrics"])
        digital_n = _json_count(r["digital_pattern"])
        atts = attachments.get(eid, [])
        related = links.get(eid, [])
        notes = _one_line(r["notes"], 1200) or "(no note)"
        att_text = "; ".join(atts[:8]) if atts else "(none)"
        sim_text = ", ".join(related[:6]) if related else "(none)"
        waveform = (
            f"analog {analog_n}, digital {digital_n}, merged {'yes' if r['merged_file'] else 'no'}"
        )
        text = (
            f"[E:{eid}] {r['timestamp']} | {r['fault_type'] or 'Unclassified'} | "
            f"{r['case_description'] or ''}\n"
            f"Case: {r['case_id']} | Confidence: {r['fault_confidence'] or 0:.2f}\n"
            f"Fault detail: {r['case_fault'] or '-'}\n"
            f"User fault: {r['user_fault_type'] or '-'} | Beam time: {r['user_beam_time'] or '-'}\n"
            f"Notes: {notes}\n"
            f"Attachments: {att_text}\n"
            f"Waveform: {waveform}\n"
            f"Similar: {sim_text}"
        )
        yield RagDocument(
            doc_key=f"event:{eid}",
            doc_type="event_card",
            text=text,
            title=f"{eid} {r['timestamp']}",
            event_id=eid,
            year=year,
            fault_type=r["fault_type"] or "",
            metadata={
                "kind": "event",
                "event_id": eid,
                "timestamp": r["timestamp"],
                "fault_type": r["fault_type"] or "",
                "case_id": r["case_id"],
                "user_beam_time": r["user_beam_time"] or "",
                "url": f"/events/{eid}",
            },
        )


def _event_alarm_docs(conn: sqlite3.Connection) -> Iterable[RagDocument]:
    rows = conn.execute(
        """
        SELECT a.event_id, a.pv, a.description, a.subsystem, a.graph_pv,
               a.graph_description, a.first_seen_utc, a.first_seen_rel,
               a.active_at_start, a.severity,
               e.timestamp, e.fault_type
          FROM event_alarms a
          JOIN events e ON e.id = a.event_id
         ORDER BY e.timestamp, a.pv
        """
    )
    for r in rows:
        eid = r["event_id"]
        text = (
            f"[A:{eid}/{r['pv']}] {r['timestamp']} | {r['fault_type'] or ''}\n"
            f"Alarm: {r['pv']} - {r['description']} "
            f"({r['subsystem']}, severity {r['severity']})\n"
            f"Graph PV: {r['graph_pv']} ({r['graph_description']})\n"
            f"First seen: {r['first_seen_rel']}s from t0; "
            f"active at start: {'yes' if r['active_at_start'] else 'no'}"
        )
        yield RagDocument(
            doc_key=f"alarm:{eid}:{r['pv']}",
            doc_type="event_alarm",
            text=text,
            title=f"{eid} alarm {r['pv']}",
            event_id=eid,
            pv=r["pv"],
            year=(r["timestamp"] or "")[:4],
            fault_type=r["fault_type"] or "",
            subsystem=r["subsystem"] or "",
            metadata={
                "kind": "alarm",
                "event_id": eid,
                "pv": r["pv"],
                "description": r["description"],
                "subsystem": r["subsystem"],
                "graph_pv": r["graph_pv"],
                "graph_description": r["graph_description"],
                "first_seen_rel": r["first_seen_rel"],
                "active_at_start": bool(r["active_at_start"]),
                "severity": r["severity"],
                "url": f"/events/{eid}",
            },
        )


def _event_pv_selection_docs(conn: sqlite3.Connection) -> Iterable[RagDocument]:
    rows = conn.execute(
        """
        SELECT s.event_id, s.pv, s.description, s.position,
               e.timestamp, e.fault_type
          FROM event_pv_selection s
          JOIN events e ON e.id = s.event_id
         ORDER BY e.timestamp, s.position, s.pv
        """
    )
    for r in rows:
        eid = r["event_id"]
        text = (
            f"[PVS:{eid}/{r['pv']}] {r['timestamp']} | {r['fault_type'] or ''}\n"
            f"Selected PV: {r['pv']} - {r['description'] or ''}"
        )
        yield RagDocument(
            doc_key=f"pvsel:{eid}:{r['pv']}",
            doc_type="event_pv_selection",
            text=text,
            title=f"{eid} selected PV {r['pv']}",
            event_id=eid,
            pv=r["pv"],
            year=(r["timestamp"] or "")[:4],
            fault_type=r["fault_type"] or "",
            metadata={
                "kind": "pv_selection",
                "event_id": eid,
                "pv": r["pv"],
                "description": r["description"] or "",
                "position": r["position"],
                "url": f"/events/{eid}/pv-explorer?pv={r['pv']}",
            },
        )


def _catalog_docs(conn: sqlite3.Connection) -> Iterable[RagDocument]:
    rows = conn.execute(
        """
        SELECT pv, description, subsystem, graph_pv, graph_description,
               severity, state_mode, alarm_state, normal_state
          FROM alarm_pv_catalog
         ORDER BY subsystem, pv
        """
    )
    for r in rows:
        text = (
            f"[PV:{r['pv']}] {r['description']} | subsystem {r['subsystem']} | "
            f"graph PV {r['graph_pv'] or '-'} ({r['graph_description'] or '-'}) | "
            f"severity {r['severity']} | state {r['state_mode'] or '-'}"
        )
        yield RagDocument(
            doc_key=f"catalog:{r['pv']}",
            doc_type="alarm_catalog",
            text=text,
            title=f"{r['pv']} {r['description']}",
            pv=r["pv"],
            subsystem=r["subsystem"] or "",
            metadata={
                "kind": "pv",
                "pv": r["pv"],
                "description": r["description"],
                "subsystem": r["subsystem"],
                "graph_pv": r["graph_pv"] or "",
                "graph_description": r["graph_description"] or "",
                "severity": r["severity"],
                "state_mode": r["state_mode"] or "",
                "url": "/alarm-dashboard",
            },
        )


def _fault_type_docs(conn: sqlite3.Connection) -> Iterable[RagDocument]:
    rows = conn.execute(
        "SELECT name, description, severity FROM fault_types ORDER BY name"
    )
    for r in rows:
        text = f"[FT:{r['name']}] {r['description']} | severity {r['severity']}"
        yield RagDocument(
            doc_key=f"fault:{r['name']}",
            doc_type="fault_type",
            text=text,
            title=r["name"],
            fault_type=r["name"],
            metadata={
                "kind": "fault",
                "fault_type": r["name"],
                "description": r["description"],
                "severity": r["severity"],
                "url": "/stats",
            },
        )


def build_documents(conn: sqlite3.Connection) -> list[RagDocument]:
    attachments: dict[str, list[str]] = {}
    for r in conn.execute(
        "SELECT event_id, original_name FROM event_attachments ORDER BY id"
    ):
        attachments.setdefault(r["event_id"], []).append(r["original_name"])

    links: dict[str, list[str]] = {}
    for r in conn.execute(
        """
        SELECT event_id, related_event_id
          FROM event_links
         ORDER BY event_id, similarity_score DESC
        """
    ):
        links.setdefault(r["event_id"], []).append(r["related_event_id"])

    docs: list[RagDocument] = []
    docs.extend(_event_cards(conn, attachments, links))
    docs.extend(_event_alarm_docs(conn))
    docs.extend(_event_pv_selection_docs(conn))
    docs.extend(_catalog_docs(conn))
    docs.extend(_fault_type_docs(conn))
    return docs
