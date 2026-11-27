"""SRF Postmortem MCP server.

Exposes the read-only Postmortem DB and RAG tools over MCP (stdio or HTTP).
The server is intentionally tool-oriented: clients call semantic operations
such as ``postmortem_search_events`` instead of sending raw SQL.

Run:
    venv-mcp/bin/python scripts/run_mcp.py --transport stdio
    venv-mcp/bin/python scripts/run_mcp.py --transport streamable-http --port 8765
"""

from __future__ import annotations

import json
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional, Union

from mcp.server.mcpserver import MCPServer, Image
from mcp.types import TextContent

from ..alarm.event_link import fetch_pv_range, resolve_t0
from ..features.physics import (
    interpret_sequence,
    normalize_sequence,
    reference_bundle as physics_reference_bundle,
)
from ..features.store import (
    feature_store_stats,
    find_similar_waveforms,
    get_waveform_features,
)
from ..rag.config import get_rag_config
from ..rag.db import connect_events
from ..rag.index import search as index_search
from ..rag.index import stats as index_stats
from ..rag.plan import plan_query
from ..rag.tools import (
    aggregate_events as _aggregate_events,
    get_alarm_status as _get_alarm_status,
    get_event_context as _get_event_context,
    search_alarm_catalog as _search_alarm_catalog,
    search_events as _search_events,
    similar_events as _similar_events,
)

KST = timezone(timedelta(hours=9))

_ROOT = Path(__file__).resolve().parent.parent.parent
_WF_SCOPES = ("forward", "cavity", "reflect")


def _waveform_image_paths(event_id: str) -> dict[str, Path]:
    """Pre-rendered narrow-scope JPEGs produced by the web pipeline."""
    base = _ROOT / "data" / "waveforms" / event_id
    out: dict[str, Path] = {}
    for scope in _WF_SCOPES:
        path = base / f"{scope}_narrow.jpg"
        try:
            if path.is_file() and path.stat().st_size > 0:
                out[scope] = path
        except OSError:
            continue
    return out


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() not in {"", "false", "0", "no", "none", "off"}
    return bool(value)


def _parse_image_scopes(scope: Optional[str]) -> Optional[list[str]]:
    if not scope or scope.strip().lower() in {"all", "*"}:
        return None
    wanted = [s.strip().lower() for s in scope.split(",") if s.strip()]
    selected = [s for s in wanted if s in _WF_SCOPES]
    return selected or None


def _with_images(
    payload: dict[str, Any],
    event_id: str,
    scope: Optional[str] = None,
) -> Any:
    """Attach image metadata and MCP Image blocks to a tool result.

    The first content block stays JSON text so existing clients keep parsing
    ``content[0].text``; image blocks follow for clients that support MCP images.
    """
    images = _waveform_image_paths(event_id)
    selected = _parse_image_scopes(scope)
    if selected is not None:
        images = {k: v for k, v in images.items() if k in selected}
    payload["images"] = [
        {
            "scope": key,
            "mime_type": "image/jpeg",
            "source": str(path.relative_to(_ROOT)),
            "url": f"/api/waveforms/{event_id}/{key}_narrow.jpg",
        }
        for key, path in images.items()
    ]
    payload["image_available"] = bool(images)
    if not images:
        payload["image_hint"] = (
            "No pre-rendered narrow-scope JPEG for this event. "
            "Use the event URL or regenerate waveform images in the web pipeline."
        )
        return payload
    blocks: list[Any] = [
        TextContent(
            type="text",
            text=json.dumps(payload, ensure_ascii=False, default=str),
        )
    ]
    blocks.extend(Image(path=path) for path in images.values())
    return blocks

server = MCPServer(
    "srf-postmortem",
    version="0.1.0",
    title="SRF Postmortem DB",
    description=(
        "Read-only MCP access to the PLS-II SRF beam-dump postmortem database: "
        "events, notes, fault types, alarm snapshots, PV selections, PV catalog, "
        "similar events, and (for supported PVs) Archiver time-series summaries."
    ),
    instructions=(
        "Use the postmortem_* tools. For single-event questions call "
        "postmortem_get_event first. For waveform/quench shape questions call "
        "postmortem_analyze_waveform and postmortem_find_similar_waveforms "
        "(events with parquet only; call postmortem_get_capabilities for current "
        "coverage). For integrated quench questions call "
        "postmortem_analyze_quench or postmortem_compare_quench_events. "
        "For report-text questions call "
        "postmortem_search_reports. For lists/trends call postmortem_aggregate_events "
        "or postmortem_search_events. Distinguish recorded alarms (event_alarms) "
        "from PV catalog mappings and manually registered PV selections. Cite "
        "[E:event_id], [A:event_id/PV], [PVS:event_id/PV], [PV:PV]. "
        "Event lists/counts exclude Machine Study (*MS) periods by default "
        "(hide_ms=true); MS is not a user beam-service period, so pass "
        "hide_ms=false only when MS periods are explicitly requested. "
        "Fault classification always uses the manual user_fault_type "
        "when present, then the automatic fault_type. case_id and "
        "effective_case_id follow the manual classification; auto_case_id and "
        "auto_fault_type are reference only. When manual and automatic differ, "
        "show the manual case and append the automatic reference, e.g. "
        "'이 이벤트는 Case 3: RF_Interlock입니다. (자동분류는 Case 4: MIS로 판정)'. "
        "Before interpreting waveform features or writing a causal explanation, "
        "call postmortem_get_glossary (or read postmortem://domain-guide) for the "
        "SRF interlock hierarchy, signal definitions, and the first-interlock rule. "
        "Device-name abbreviations are in postmortem_get_capabilities.abbreviations "
        "and postmortem_get_glossary(topic='abbreviations'): CM, LLRF, HPRF, KSU, "
        "IC, MIS, PSI, MPS, BLM, TFS, FOFB, BCL01/BCL03, BPR01, BPS01, and the "
        "channel prefixes VAC/QUEN/ARC/HE-PR/RDY/INT. Read that table before "
        "expanding a device tag; do not guess an abbreviation. "
        "Do not call a cavity probe dip-and-rise 'recovery'; it can be a "
        "beam-loading/phase-slip oscillation. "
        "For waveform pictures, set include_images=true or generate_plot=true: "
        "content[0].text stays JSON and following content blocks are image/png|jpeg. "
        "fault_type accepts a string or a list of strings. "
        "When reporting counts or percentages, copy the key and count values from "
        "the tool response verbatim. Never estimate, merge, or recompute them; "
        "sum_of_counts must equal total. Quote the data_as_of date when stating a "
        "total, because the DB is refreshed only every few months. "
        "If evidence is missing, say so; do not invent event IDs, PVs, or numbers."
    ),
)

_CAVEATS = [
    "Report text and image captions/OCR cover parsed report attachments; "
    "VLM-transcribed numbers should be checked against the source image. Call "
    "postmortem_get_capabilities for current document counts.",
    "report_md is auto-generated boilerplate and is not used as knowledge.",
    "event_alarms snapshots exist for only a few events; most events have none.",
    "event_pv_selection is a manually registered PV list, not a recorded alarm.",
    "Waveform numeric features cover events with parquet (2023+); a few events "
    "have missing channels. Events without parquet cannot be analyzed. Call "
    "postmortem_get_capabilities for current coverage.",
    "Event lists/counts exclude Machine Study (*MS) periods by default "
    "(hide_ms=true). MS is not a user beam-service period, so it is not data. "
    "Fault counts prefer the manual user_fault_type over the automatic fault_type.",
    "Analog waveform values are normalized so nominal operation is 1.0; they are "
    "not physical units. Call postmortem_get_glossary or read "
    "postmortem://domain-guide before interpreting waveform features.",
    "The RF interlock fault is the first digital interlock in the trip chain "
    "(time_groups FIRST), not the most frequent channel. MIS is dual-redundant "
    "(INT_MIS1_IC / INT_MIS2_IC).",
    "A cavity probe voltage that dips and then rises is not necessarily "
    "'recovering'; it can oscillate because of beam phase slip and beam loading. "
    "PLS-II over-input protection limits forward power to about 1.2x nominal.",
]

TOOL_DESCRIPTIONS = {
    "postmortem_get_capabilities":
        "Return live data coverage counts (events, notes, attachments, alarms, "
        "PV selections, PV catalog), the data_as_of snapshot date and total, "
        "index freshness, available tools, examples, and known limitations. "
        "Call this first. It also returns abbreviations: the device-name table "
        "for CM, LLRF, HPRF, KSU, IC, MIS, PSI, MPS, BLM, BPM, DCCT, TFS, FOFB, "
        "BCL01/BCL03, BPR01, BPS01, and channel prefixes. Read it before "
        "interpreting device tags. The DB is refreshed every few months, so quote "
        "the data_as_of date whenever you report totals.",
    "postmortem_search_events":
        "Search beam-dump events by natural-language query and/or structured filters. "
        "Filters: year, year_from/year_to, year_min/year_max, date (YYYY-MM-DD), "
        "subsystem (KSU1-3/CM1-3/CRG/MPS...), fault_type (string or list), "
        "fault_types (list), event_id, "
        "pv (matches event_alarms/event_pv_selection PV names), beam_time (partial "
        "beam-time label such as '2026-11차'), has_waveform (bool), hide_ms (default "
        "true; excludes Machine Study beam times such as 2026-01MS), sort "
        "('relevance'|'recent'), limit, offset. "
        "fault_type/fault_types match the manual user_fault_type first, then the "
        "automatic fault_type. "
        "Questions containing 최근/요즘/최신 automatically use sort='recent'. Returns "
        "response_type='event_list' with items[]; each item has id, event_id, timestamp, "
        "fault_type (auto), auto_fault_type, effective_fault_type (manual override "
        "first), case_id (auto), effective_case_id (manual override first), "
        "classification_note (set only when manual differs from auto), "
        "user_fault_type, notes, alarm_count, pv_selection_count, has_waveform, url, "
        "citation. "
        "Use aggregate for counts.",
    "postmortem_get_event":
        "Return one event's full context: notes, fault classification "
        "(effective_fault_type applies the manual override first), recorded alarms, "
        "manual PV selections, catalog mappings, attachments, similar events, waveform "
        "availability. Returns response_type='event_detail' with event, alarms[], "
        "pv_selection[], catalog[], attachments[], similar[], waveform, caveats. "
        "An empty similar[] means the DB has no event_links for this event, not an API error. "
        "Set include_images=true (or waveform_image=true) to append MCP Image blocks "
        "for the pre-rendered forward/cavity/reflect JPEGs; image_scope=all|forward|"
        "cavity|reflect or a comma list. JSON stays in content[0].text.",
    "postmortem_aggregate_events":
        "Count events grouped by one or more fields. group_by accepts year, month "
        "(YYYY-MM), fault_type, case_id, auto_case_id, auto_fault_type, "
        "user_fault_type, beam_time, or a comma-separated composite such as "
        "'year,beam_time'. case_id follows the manual classification first; "
        "auto_case_id is the automatic reference. fault_type grouping uses the "
        "manual user_fault_type first, then the automatic fault_type. Filters: "
        "year, year_from, year_to, subsystem, fault_type (string or list), "
        "fault_types (list), "
        "text, pv, beam_time. hide_ms (default true) excludes Machine Study "
        "periods. include_percent defaults to true. "
        "Rows carry key, count, percent; copy them verbatim and never estimate or "
        "recompute. Returns response_type='aggregate' with group_by[], total, "
        "sum_of_counts, rows:[{key, keys, count, percent?}], and data_as_of "
        "(latest event timestamp, DB total, and an 'as of YYYY-MM-DD, total N "
        "events' statement). Quote the data_as_of date when reporting totals. "
        "Use this for counts/trends; do not use vector search for counts.",
    "postmortem_similar_events":
        "Return events linked to the given event by the DB event_links similarity graph. "
        "Returns response_type='event_list' with event_id, returned, items[]; each item "
        "has both id and event_id. Empty items means no links exist in the DB. This is "
        "not a vector similarity score.",
    "postmortem_search_alarm_catalog":
        "Search the 524-entry alarm PV catalog by PV name, description, or graph PV. "
        "Returns response_type='pv_list' with items[]; each item has pv, description, "
        "subsystem, graph_pv, graph_description, severity, citation. Results are catalog "
        "mappings, not evidence that an alarm occurred in an event.",
    "postmortem_get_alarm_status":
        "Return the live alarm monitor status, last-state counts, and recent transitions. "
        "Returns response_type='alarm_status' with monitor, last_state_counts, "
        "last_state_updated, recent_transitions, catalog, caveats. Check last_heartbeat "
        "and last_state_updated before calling this current.",
    "postmortem_resolve_event":
        "Resolve a date (YYYY-MM-DD) and optional subsystem to candidate event IDs. "
        "Returns response_type='event_list' with returned and items[] (id + event_id). "
        "If multiple events match, choose one and call postmortem_get_event.",
    "postmortem_list_fault_types":
        "List fault types with descriptions, severity, and event counts. "
        "Counts use the manual user_fault_type first, then the automatic fault_type. "
        "hide_ms (default true) excludes Machine Study beam times. "
        "Returns response_type='fault_type_list' with items[].",
    "postmortem_list_beam_times":
        "List user beam-time labels with event counts. "
        "hide_ms (default true) omits Machine Study (*MS) beam times. "
        "Returns response_type='beam_time_list' with items[].",
    "postmortem_get_pv_series":
        "Return an Archiver time-series summary for a PV around an event's t0. "
        "Parameters: event_id, pv, pre=60, post=10, include_points=false. Supports "
        "KSU/CRM/CRG/LRF/G00 PVs. Returns response_type='pv_series' with available, "
        "quality, summary:{count,min,max,median,peak_value,peak_rel_sec,"
        "max_deviation_pct,resolution_s}, and optional points[].",
    "postmortem_analyze_waveform":
        "Return numeric waveform features for one event from the static feature store: "
        "t0, beam-current fall timing, CM1/2/3 cavity sag, reflect rise, forward trip, "
        "digital interlock transitions, quench unit, and data caveats. The "
        "'sequence' block adds cavity 90/50/10% crossing times, the cavity-to-beam "
        "delay, the 3-cavity collapse spread, and a sequence pattern (rf_trip, "
        "beam_first_rf_trip, beam_only, single_cavity_quench, mixed). The pattern "
        "is descriptive; root cause is not assigned. The "
        "'physics' block adds a deterministic textbook-grounded interpretation "
        "with PLS-II parameter checks, citations, cause_candidates (possible "
        "causes, not a root-cause assignment), and next_logs_ko. A cavity probe "
        "dip-and-rise is an oscillation, not necessarily 'recovery'. Covers 313/866 "
        "events (2023+, parquet only). Values are computed by code, not an LLM. Returns "
        "response_type='waveform_analysis'. Set generate_plot=true (or "
        "include_images=true) to append MCP Image blocks for the pre-rendered "
        "narrow-scope JPEGs; image_scope=all|forward|cavity|reflect. JSON stays in "
        "content[0].text. Use postmortem_find_similar_waveforms to "
        "compare waveforms; use postmortem_get_event for notes/alarms.",
    "postmortem_find_similar_waveforms":
        "Return the top_k events whose waveform feature vector is closest (cosine) to "
        "the given event. Model waveform_manual_v1 is a 78-dim hand-crafted vector "
        "(sag/reflect/forward/beam-fall/digital), not a learned embedding. Returns "
        "response_type='waveform_similarity' with target and items[] (event_id, "
        "similarity, signature, delta_vs_target, citation). Only events with parquet "
        "are searchable. This is not the DB event_links graph.",
    "postmortem_search_reports":
        "Search the indexed maintenance/fault reports (91 unique HWP/HWPX/PDF/DOCX "
        "documents from 96 attachments) by text and image captions. Hybrid FTS5 "
        "trigram + dense cosine (multilingual-e5-small, CPU only), reciprocal-rank "
        "fused. Parameters: query, event_id (optional), top_k (<=30). Returns "
        "response_type='report_search' with items[]: kind='chunk' (table/narrative "
        "text) or kind='image' (Qwen2.5-VL caption + OCR), title, event_id/event_ids, "
        "text/caption/ocr_text, citation [R:event/doc] or [R:event/doc#imgN], "
        "dense_score, fts_score. Treat VLM-transcribed numbers as pointers to the "
        "source image, not ground truth. Use postmortem_get_event for notes/alarms "
        "and postmortem_search_events for event lists.",
    "postmortem_analyze_quench":
        "Integrated quench analysis for one event: 100 ms waveform features "
        "(cavity sag, reflect rise, forward trip, beam fall, QUEN asserts), "
        "Archiver BCL01/BCL03 cavity temperatures and beam current around the "
        "Archiver t0, and report text/image-caption evidence. Parameters: "
        "event_id, include_pv=true, include_reports=true, include_images=false, "
        "image_scope=all|forward|cavity|reflect, pre=60, post=120. "
        "Returns response_type='quench_analysis' with event, waveform, pv.series, "
        "reports.items, highlights, caveats. PVs are ~1 Hz; waveform t0 and "
        "Archiver t0 are different clocks.",
    "postmortem_compare_quench_events":
        "Compare waveform features across up to 20 quench events. Parameters: "
        "event_ids (list), include_reports=false. Returns "
        "response_type='quench_comparison' with rows[]: timestamp, quench unit, "
        "t0, cavity sag/pre-state, reflect rise, forward trip, beam fall, QUEN "
        "assert, report titles. Use postmortem_analyze_quench for PV/temperature "
        "evidence; this table is fast and does not call the Archiver.",
    "postmortem_get_glossary":
        "Return curated background knowledge for PLS-II SRF beam-dump postmortems. "
        "This is a static, textbook-grounded reference library, not event data. "
        "Call it before interpreting waveform features, writing a causal chain, or "
        "answering any SRF/beam-physics question. "
        "DOMAIN GLOSSARY (config/domain_glossary.json), 10 addressable topics: "
        "abbreviations (device names and channel prefixes: CM, LLRF, HPRF, KSU, "
        "IC, MIS, PSI, MPS, BLM, BPM, DCCT, TFS, FOFB, BCL01/BCL03, BPR01, BPS01; "
        "read this before expanding a device tag), "
        "system_overview (ring/RF layout and operating context), "
        "interlock_hierarchy (external MIS/PSI vs internal CM/KSU trip chain), "
        "trip_sequence (standard MIS beam-dump sequence, first-interlock rule), "
        "signals (analog/digital definitions, active_low/active_high polarity), "
        "naming (actual PV/channel names), "
        "data_rules (MS exclusion, manual vs auto classification, relay delay), "
        "answer_style (required caveats and quoting rules), "
        "authority (document/data authority order), caveats (standing limitations). "
        "PHYSICS LIBRARY (config/physics_references.json), 8 topics and 31 "
        "references: "
        "cavity_figures_of_merit (Q0, QL, R/Q, beta, U, Pc, Pbeam, beta_opt), "
        "cavity_decay_after_rf_off (tau=2QL/omega; PLS-II 95-108 us natural, "
        "24-32 us beam-loaded, midpoint 28 us), "
        "beam_loading_llrf (V_c=V_g+V_b; beam phase slip/beam loading can make "
        "the probe voltage oscillate; over-input protection limits forward to "
        "about 1.2x nominal; He vessel/rupture disk protection), "
        "mis_machine_interlock (PLC 3-layer MIS; RF Ready = MIS healthy AND PSI "
        "healthy; PSI handles radiation/access; MPS is separate; BLM is monitoring), "
        "beam_energy_loss_after_rf_trip (dE/dt=-U0/T_rev=1.32 GeV/ms; PLS-II RF "
        "acceptance 2.8% at 4.5 MV), "
        "transverse_instability (exponential growth; PLS-II TFS and FOFB; BPM "
        "limits), "
        "beam_dump_causes (empirical PLS-II taxonomy: RF single station, RF common, "
        "MPS, MIS, CONTROL/TFS/FOFB, BEAMLINE, MECHVAC, KICKER, MAGNET, PSI, "
        "LINAC, external; each class also lists cause_candidates), "
        "pls2_operating_point (3 GeV, 400 mA, 499.973 MHz, 3 cavities at 1.5 MV "
        "each = 4.5 MV, Q0/Qext, R/Q=89 ohm, RF acceptance 2.8%). "
        "The full physics document also carries one internal test note "
        "(2020-04-05 vertical test, 500 MHz CESR-B): it is measured test data, "
        "not a textbook citation, and is under 'internal_test_notes'. "
        "A physics topic returns title, summary_ko, equations[{name,expr,note}], "
        "categories[] (for beam_dump_causes), "
        "citations[{id,title,authors,year,doi,url,locator,supports}], and caveats. "
        "A domain topic returns its JSON object or entry list. "
        "Parameters: topic (optional). topic='list' returns the topic index only. "
        "Omit topic to return both documents (physics under 'physics'). An unknown "
        "topic returns the valid keys. Quote equations and parameter values from "
        "the response; do not substitute values from memory.",
}

TOOL_META = {
    "postmortem_get_capabilities": {
        "response_shape": "capabilities",
        "examples": [{}],
    },
    "postmortem_search_events": {
        "response_shape": "event_list",
        "examples": [
            {"query": "2026년 KSU관련 빔덤프", "limit": 5},
            {"year_min": 2024, "year_max": 2026,
             "fault_types": ["Case 3: RF_Interlock"],
             "has_waveform": True, "limit": 10},
            {"beam_time": "2026-11차"},
            {"pv": "SR:RF:CRM1:BCL03"},
            {"fault_type": ["Case 3: RF_Interlock", "Case 9: Cavity_quench"],
             "limit": 10},
        ],
    },
    "postmortem_get_event": {
        "response_shape": "event_detail",
        "examples": [
            {"event_id": "20251103_115812"},
            {"event_id": "20260921_162300", "include_images": True,
             "image_scope": "cavity"},
        ],
    },
    "postmortem_aggregate_events": {
        "response_shape": "aggregate",
        "examples": [
            {"group_by": "year"},
            {"group_by": "year,beam_time", "include_percent": True},
            {"group_by": "fault_type", "year": 2026},
            {"group_by": "case_id", "year": 2026},
            {"group_by": "fault_type", "beam_time": "2026-11차"},
        ],
    },
    "postmortem_similar_events": {
        "response_shape": "event_list",
        "examples": [{"event_id": "20251103_115812", "limit": 5}],
    },
    "postmortem_search_alarm_catalog": {
        "response_shape": "pv_list",
        "examples": [{"query": "water flow", "subsystem": "KSU3"}],
    },
    "postmortem_get_alarm_status": {
        "response_shape": "alarm_status",
        "examples": [{}],
    },
    "postmortem_resolve_event": {
        "response_shape": "event_list",
        "examples": [{"date": "2025-11-03", "subsystem": "KSU3"}],
    },
    "postmortem_list_fault_types": {
        "response_shape": "fault_type_list",
        "examples": [{}],
    },
    "postmortem_list_beam_times": {
        "response_shape": "beam_time_list",
        "examples": [{}],
    },
    "postmortem_get_pv_series": {
        "response_shape": "pv_series",
        "examples": [
            {"event_id": "20241111_044552", "pv": "SR:RF:CRM1:BCL03"},
        ],
    },
    "postmortem_analyze_waveform": {
        "response_shape": "waveform_analysis",
        "examples": [
            {"event_id": "20240907_025313"},
            {"event_id": "20251205_014253"},
            {"event_id": "20240907_025313", "generate_plot": True},
        ],
    },
    "postmortem_find_similar_waveforms": {
        "response_shape": "waveform_similarity",
        "examples": [
            {"event_id": "20240907_025313", "top_k": 5},
            {"event_id": "20251205_014253", "top_k": 5},
        ],
    },
    "postmortem_search_reports": {
        "response_shape": "report_search",
        "examples": [
            {"query": "켄치 발생 시 RF 반사파가 증가한 원인", "top_k": 5},
            {"query": "이온 펌프 전류 상승", "event_id": "20180521_185400",
             "top_k": 5},
        ],
    },
    "postmortem_analyze_quench": {
        "response_shape": "quench_analysis",
        "examples": [
            {"event_id": "20240907_025313"},
            {"event_id": "20251205_014253", "include_pv": True},
            {"event_id": "20240907_025313", "include_images": True},
        ],
    },
    "postmortem_compare_quench_events": {
        "response_shape": "quench_comparison",
        "examples": [
            {"event_ids": ["20240907_025313", "20241002_171719",
                           "20251205_014253"]},
        ],
    },
    "postmortem_get_glossary": {
        "response_shape": "glossary",
        "examples": [
            {},
            {"topic": "abbreviations"},
            {"topic": "trip_sequence"},
            {"topic": "signals"},
        ],
    },
}


def _event_item(ev: dict[str, Any]) -> dict[str, Any]:
    eid = ev.get("id") or ev.get("event_id")
    out = dict(ev)
    out["event_id"] = eid
    out["citation"] = f"[E:{eid}]" if eid else ""
    out.setdefault("url", f"/events/{eid}" if eid else "")
    return out


def _pv_citation(pv: str) -> str:
    return f"[PV:{pv}]"


def _reports_stats() -> dict[str, Any]:
    """Lazy: report search needs numpy/onnxruntime, which are optional."""
    try:
        from ..features.reports import report_store_stats
        return report_store_stats()
    except Exception as exc:  # noqa: BLE001 - capabilities must not fail
        return {"available": False, "error": f"{type(exc).__name__}: {exc}"}


_GLOSSARY_PATH = (
    Path(__file__).resolve().parent.parent.parent / "config" / "domain_glossary.json"
)
_GLOSSARY_CACHE: dict[str, Any] = {"mtime": None, "data": None}


def _domain_glossary() -> dict[str, Any]:
    """Load config/domain_glossary.json, reloading when the file changes."""
    try:
        mtime = _GLOSSARY_PATH.stat().st_mtime
    except OSError:
        return {"available": False, "error": f"missing {_GLOSSARY_PATH}"}
    if _GLOSSARY_CACHE["mtime"] != mtime:
        try:
            _GLOSSARY_CACHE["data"] = json.loads(
                _GLOSSARY_PATH.read_text(encoding="utf-8")
            )
            _GLOSSARY_CACHE["mtime"] = mtime
        except (OSError, ValueError) as exc:
            return {"available": False, "error": f"{type(exc).__name__}: {exc}"}
    return _GLOSSARY_CACHE["data"]


def _glossary_index() -> dict[str, Any]:
    """Return an addressable-topic index for both glossary documents."""
    doc = _domain_glossary()
    phys = physics_reference_bundle()
    domain: dict[str, Any] = {}
    for key, value in doc.items():
        if key in ("version", "updated", "title", "scope"):
            continue
        if isinstance(value, dict):
            domain[key] = {
                "kind": "object",
                "entries": len(value),
                "entry_keys": list(value)[:12],
            }
        elif isinstance(value, list):
            domain[key] = {"kind": "list", "entries": len(value)}
        else:
            domain[key] = {"kind": type(value).__name__}
    topics = phys.get("topics") or {}
    return {
        "response_type": "glossary_index",
        "usage": (
            "Pass topic=<key> to postmortem_get_glossary for the full entry. "
            "topic='list' returns only this index."
        ),
        "addressable_topic_count": len(domain) + len(topics),
        "domain": {
            "source": "config/domain_glossary.json",
            "version": doc.get("version"),
            "updated": doc.get("updated"),
            "topic_count": len(domain),
            "topics": domain,
        },
        "physics": {
            "source": "config/physics_references.json",
            "version": phys.get("version"),
            "updated": phys.get("updated"),
            "topic_count": len(topics),
            "reference_count": len(phys.get("references") or []),
            "internal_test_notes": [
                {"id": n.get("id"), "title": n.get("title"), "date": n.get("date")}
                for n in (phys.get("internal_test_notes") or [])
            ],
            "topics": {
                key: {
                    "title": value.get("title"),
                    "summary_ko": value.get("summary_ko"),
                    "citation_ids": [
                        c.get("id") if isinstance(c, dict) else c
                        for c in value.get("citations", [])
                    ],
                }
                for key, value in topics.items()
            },
        },
    }


@server.tool(description=TOOL_DESCRIPTIONS["postmortem_get_capabilities"],
             meta=TOOL_META["postmortem_get_capabilities"])
def postmortem_get_capabilities() -> dict[str, Any]:
    """Return data coverage, index freshness, available tools, and known limitations."""
    cfg = get_rag_config()
    conn = connect_events()
    try:
        counts = {
            "events": conn.execute("SELECT count(*) FROM events").fetchone()[0],
            "notes": conn.execute(
                "SELECT count(*) FROM events WHERE trim(coalesce(notes,'')) <> ''"
            ).fetchone()[0],
            "attachments": conn.execute("SELECT count(*) FROM event_attachments").fetchone()[0],
            "event_alarms": conn.execute("SELECT count(*) FROM event_alarms").fetchone()[0],
            "event_alarm_events": conn.execute(
                "SELECT count(DISTINCT event_id) FROM event_alarms"
            ).fetchone()[0],
            "event_pv_selection": conn.execute(
                "SELECT count(*) FROM event_pv_selection"
            ).fetchone()[0],
            "event_pv_selection_events": conn.execute(
                "SELECT count(DISTINCT event_id) FROM event_pv_selection"
            ).fetchone()[0],
            "alarm_pv_catalog": conn.execute(
                "SELECT count(*) FROM alarm_pv_catalog"
            ).fetchone()[0],
            "alarm_pv_mapped": conn.execute(
                "SELECT count(*) FROM alarm_pv_catalog "
                "WHERE trim(coalesce(graph_pv,'')) <> ''"
            ).fetchone()[0],
            "event_links": conn.execute("SELECT count(*) FROM event_links").fetchone()[0],
            "waveform_events": conn.execute(
                "SELECT count(*) FROM events WHERE trim(coalesce(merged_file,'')) <> ''"
            ).fetchone()[0],
        }
        year_span = conn.execute(
            "SELECT min(substr(timestamp,1,4)), max(substr(timestamp,1,4)) FROM events"
        ).fetchone()
        freshness = conn.execute(
            "SELECT max(timestamp) AS latest, max(created_at) AS latest_created, "
            "count(*) AS total, "
            "sum(CASE WHEN user_beam_time IS NOT NULL AND user_beam_time <> '' "
            "AND user_beam_time LIKE '%MS%' THEN 0 ELSE 1 END) AS total_non_ms "
            "FROM events"
        ).fetchone()
    finally:
        conn.close()
    latest = freshness["latest"] or ""
    latest_date = latest[:10] if latest else ""
    as_of_statement = (
        f"as of {latest_date}, total {freshness['total']} events in DB "
        f"({freshness['total_non_ms']} non-MS)"
        if latest_date else "DB is empty"
    )
    return {
        "response_type": "capabilities",
        "server": "srf-postmortem",
        "version": "0.1.0",
        "model_for_reference": cfg.llm_model,
        "data": counts,
        "data_as_of": {
            "latest_event_timestamp": latest,
            "latest_created_at": freshness["latest_created"],
            "total_events": freshness["total"],
            "total_events_non_ms": freshness["total_non_ms"],
            "statement": as_of_statement,
            "note": (
                "The DB is refreshed every few months. Counts reflect the DB "
                "contents as of the latest event timestamp above. MCP reads the "
                "same events.db as the Postmortem web app, so no separate web "
                "pull is needed; both reflect the same snapshot."
            ),
        },
        "year_span": {"from": year_span[0], "to": year_span[1]},
        "index": index_stats(),
        "feature_store": feature_store_stats(),
        "reports": _reports_stats(),
        "query_defaults": {
            "hide_ms": True,
            "hide_ms_meaning": (
                "Exclude Machine Study (*MS) periods from event lists/counts. "
                "MS is not a user beam-service period."
            ),
            "fault_classification": (
                "COALESCE(user_fault_type, fault_type): the manual user case "
                "designation overrides the automatic classifier."
            ),
            "interlock_fault_rule": (
                "The RF interlock fault is the first digital interlock in the "
                "trip chain (time_groups FIRST), not the most frequent channel."
            ),
        },
        "domain_glossary": {
            "tool": "postmortem_get_glossary",
            "resource": "postmortem://domain-guide",
            "version": _domain_glossary().get("version"),
            "updated": _domain_glossary().get("updated"),
            "topics": [
                "abbreviations",
                "system_overview",
                "interlock_hierarchy",
                "trip_sequence",
                "signals",
                "naming",
                "data_rules",
                "answer_style",
                "authority",
                "caveats",
            ],
        },
        "abbreviations": {
            "topic": "abbreviations",
            "tool": "postmortem_get_glossary",
            "resource": "postmortem://abbreviations",
            "note": (
                "Read this before interpreting device names. It defines the "
                "facility/SRF/protection terms, channel prefixes, instrument "
                "tags, and numeric suffixes used in events, alarms, and notes."
            ),
            "count": sum(
                len(value)
                for value in (
                    _domain_glossary().get("abbreviations") or {}
                ).values()
                if isinstance(value, dict)
            ),
            "entries": _domain_glossary().get("abbreviations", {}),
        },
        "physics_glossary": {
            "tool": "postmortem_get_glossary",
            "source": "config/physics_references.json",
            "version": physics_reference_bundle().get("version"),
            "updated": physics_reference_bundle().get("updated"),
            "topic_count": len(
                physics_reference_bundle().get("topics") or {}
            ),
            "reference_count": len(
                physics_reference_bundle().get("references") or []
            ),
            "internal_test_note_count": len(
                physics_reference_bundle().get("internal_test_notes") or []
            ),
            "topics": {
                key: value.get("title")
                for key, value in (
                    physics_reference_bundle().get("topics") or {}
                ).items()
            },
            "usage": (
                "Call postmortem_get_glossary(topic=<key>) for equations, "
                "citations, and caveats. topic='list' returns the full index."
            ),
        },
        "tools": [
            "postmortem_search_events",
            "postmortem_get_event",
            "postmortem_aggregate_events",
            "postmortem_similar_events",
            "postmortem_search_alarm_catalog",
            "postmortem_get_alarm_status",
            "postmortem_resolve_event",
            "postmortem_list_fault_types",
            "postmortem_list_beam_times",
            "postmortem_get_pv_series",
            "postmortem_analyze_waveform",
            "postmortem_find_similar_waveforms",
            "postmortem_search_reports",
            "postmortem_analyze_quench",
            "postmortem_compare_quench_events",
            "postmortem_get_glossary",
            "postmortem_get_capabilities",
        ],
        "services": [{"name": "postmortem", "tool_prefix": "postmortem_"}],
        "tool_descriptions": TOOL_DESCRIPTIONS,
        "examples": [
            {"tool": "postmortem_search_events",
             "arguments": {"query": "2026년 KSU관련 빔덤프", "limit": 5}},
            {"tool": "postmortem_get_event",
             "arguments": {"event_id": "20251103_115812"}},
            {"tool": "postmortem_aggregate_events",
             "arguments": {"group_by": "year,beam_time", "include_percent": True}},
            {"tool": "postmortem_search_alarm_catalog",
             "arguments": {"query": "water flow", "subsystem": "KSU3"}},
            {"tool": "postmortem_get_pv_series",
             "arguments": {"event_id": "20241111_044552",
                           "pv": "SR:RF:CRM1:BCL03"}},
            {"tool": "postmortem_analyze_waveform",
             "arguments": {"event_id": "20240907_025313"}},
            {"tool": "postmortem_find_similar_waveforms",
             "arguments": {"event_id": "20240907_025313", "top_k": 5}},
            {"tool": "postmortem_search_reports",
             "arguments": {"query": "켄치 발생 시 RF 반사파가 증가한 원인",
                           "top_k": 5}},
            {"tool": "postmortem_analyze_quench",
             "arguments": {"event_id": "20240907_025313"}},
            {"tool": "postmortem_compare_quench_events",
             "arguments": {"event_ids": ["20240907_025313",
                                         "20241002_171719",
                                         "20251205_014253"]}},
        ],
        "resources": [
            "postmortem://capabilities",
            "postmortem://schema",
            "postmortem://fault-types",
            "postmortem://domain-guide",
            "postmortem://glossary",
            "postmortem://quickstart",
        ],
        "response_shapes": {
            "event_list": {
                "returned": "int",
                "items": "array of event refs; each has id and event_id",
            },
            "event_detail": {
                "event": "object",
                "alarms": "array",
                "pv_selection": "array",
                "catalog": "array",
                "similar": "array",
                "similar_count": "int",
                "links_available": "bool",
            },
            "aggregate": {
                "group_by": "array of field names",
                "total": "int",
                "rows": "array of {key, keys, count, percent?}",
            },
            "pv_list": {
                "items": "array of {pv, description, subsystem, graph_pv, citation}",
            },
            "alarm_status": {
                "monitor": "object",
                "last_state_counts": "object",
                "recent_transitions": "array",
            },
            "pv_series": {
                "available": "bool",
                "quality": "ok|no_data|error",
                "summary": "object",
            },
            "waveform_analysis": {
                "available": "bool",
                "event_id": "string",
                "snapshot_id": "string",
                "waveform": {
                    "t0": "object",
                    "beam_fall": "object",
                    "signature": "CM1/CM2/CM3 objects",
                    "analog": "10 channel objects",
                    "digital": "channels with transitions/asserts",
                },
                "caveats": "array",
            },
            "waveform_similarity": {
                "available": "bool",
                "event_id": "string",
                "metric": "cosine",
                "target": "object",
                "items": "array of {event_id, similarity, signature, delta_vs_target}",
            },
            "report_search": {
                "available": "bool",
                "query": "string",
                "returned": "int",
                "items": "array of {kind: chunk|image, title, event_id, "
                         "event_ids, text|caption, score, dense_score, "
                         "fts_score, citation}",
                "dense_available": "bool",
            },
            "quench_analysis": {
                "available": "bool",
                "event": "object",
                "waveform": "object|null",
                "pv": "object (t0_arch_s, series.BCL03/BCL01/BEAM)",
                "reports": "object",
                "highlights": "array of strings",
            },
            "quench_comparison": {
                "available": "bool",
                "rows": "array of per-event feature rows",
                "with_waveform": "int",
                "with_reports": "int",
            },
        },
        "encoding": "utf-8",
        "session": {
            "idle_timeout_default_sec": 1800,
            "note": "Re-initialize the MCP session after expiry.",
        },
        "limitations": _CAVEATS,
    }


@server.tool(description=TOOL_DESCRIPTIONS["postmortem_search_events"],
             meta=TOOL_META["postmortem_search_events"])
def postmortem_search_events(
    query: Optional[str] = None,
    year: Optional[int] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    date: Optional[str] = None,
    subsystem: Optional[str] = None,
    fault_type: Optional[Union[str, list[str]]] = None,
    fault_types: Optional[list[str]] = None,
    event_id: Optional[str] = None,
    pv: Optional[str] = None,
    beam_time: Optional[str] = None,
    has_waveform: Optional[bool] = None,
    hide_ms: bool = True,
    sort: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
) -> dict[str, Any]:
    """Search beam-dump events by keyword and/or structured filters.

    Use this for lists and exploratory searches. For a single known event use
    postmortem_get_event. For counts/trends use postmortem_aggregate_events.
    """
    limit = max(1, min(int(limit or 20), 100))
    offset = max(0, int(offset or 0))
    if isinstance(fault_type, (list, tuple)):
        if not fault_types:
            fault_types = [str(x).strip() for x in fault_type if str(x).strip()]
        fault_type = None
    if isinstance(fault_types, str):
        fault_types = [fault_types]
    if year_min is not None and year_from is None:
        year_from = year_min
    if year_max is not None and year_to is None:
        year_to = year_max
    filters: dict[str, Any] = {}
    for key, value in {
        "year": year, "year_from": year_from, "year_to": year_to,
        "date": date, "subsystem": subsystem, "fault_type": fault_type,
        "event_id": event_id, "pv": pv, "beam_time": beam_time,
    }.items():
        if value not in (None, ""):
            filters[key] = value
    if fault_types:
        filters["fault_types"] = [str(t) for t in fault_types if t]
    if has_waveform is not None:
        filters["has_waveform"] = bool(has_waveform)
    filters["hide_ms"] = bool(hide_ms)
    if beam_time and "MS" in str(beam_time).upper() and hide_ms:
        return {
            "response_type": "event_list",
            "returned": 0,
            "items": [],
            "filters": filters,
            "error": (
                f"beam_time={beam_time!r} looks like a Machine Study period, "
                "but hide_ms=true."
            ),
            "hint": "MS periods are not user beam service. Pass hide_ms=false to include them.",
        }

    if event_id:
        ctx = _get_event_context(str(event_id))
        if "error" in ctx:
            return {"returned": 0, "items": [], "error": ctx["error"], "filters": filters}
        return {
            "response_type": "event_list",
            "returned": 1,
            "items": [_event_item(ctx["event"])],
            "filters": filters,
            "caveats": _CAVEATS,
        }

    # Extract year/subsystem/date/topic from a natural-language query so a
    # query-only call does not return arbitrary recent events.
    plan = plan_query(query) if query else None
    if plan:
        allowed = {
            "event_id", "pv", "year", "year_from", "year_to", "date",
            "subsystem", "fault_type", "text",
        }
        for key, value in plan.filters.items():
            if key in allowed and key not in filters:
                filters[key] = value

    sort_mode = (sort or "").strip().lower()
    if sort_mode not in {"relevance", "recent"}:
        sort_mode = "recent" if plan and "sort=recent" in plan.notes else "relevance"

    events = _search_events(filters, limit=limit + offset)
    if query:
        search_filters = {
            k: v for k, v in filters.items()
            if k in {"event_id", "pv", "beam_time", "year", "year_from",
                     "year_to", "fault_type"}
        }
        hits = index_search(query, filters=search_filters, top_k=limit + offset)
        seen = {e.get("id") for e in events}
        fts_events: list[dict[str, Any]] = []
        for hit in hits:
            eid = hit.get("event_id") or (hit.get("metadata") or {}).get("event_id")
            if not eid or eid in seen:
                continue
            # Re-check FTS hits against the same SQL filters (hide_ms,
            # manual-first fault type, year) so vector/lexical hits cannot
            # reintroduce events the structured query excluded.
            extra = _search_events(
                {**search_filters, "event_id": eid, "hide_ms": hide_ms},
                limit=1,
            )
            if extra:
                fts_events.append(extra[0])
                seen.add(eid)
        # Structured filters are more precise; otherwise trust FTS order.
        events = (events + fts_events) if filters else (fts_events + events)
    if sort_mode == "recent":
        # "최근" questions must not fall back to arbitrary BM25 order.
        events = sorted(
            events, key=lambda e: str(e.get("timestamp") or ""), reverse=True
        )
    page = events[offset:offset + limit]
    result = {
        "response_type": "event_list",
        "returned": len(page),
        "has_more": len(events) > offset + limit,
        "items": [_event_item(e) for e in page],
        "filters": filters,
        "sort": sort_mode,
        "query": query or "",
        "caveats": _CAVEATS,
    }
    if plan and plan.intent == "aggregate":
        result["hint"] = (
            "This question asks for counts/trends, not an event list. Call "
            "postmortem_aggregate_events with group_by='year', 'month', "
            "'fault_type', 'beam_time', or a combination (e.g. "
            "'year,beam_time'), passing year/fault_type/subsystem to narrow it."
        )
    return result


@server.tool(description=TOOL_DESCRIPTIONS["postmortem_get_event"],
             meta=TOOL_META["postmortem_get_event"])
def postmortem_get_event(
    event_id: str,
    include_images: bool = False,
    waveform_image: Union[bool, str] = False,
    image_scope: Optional[str] = None,
) -> Any:
    """Return one event with notes, fault classification, recorded alarms,
    manually selected PVs, catalog mappings, attachments, similar events, and
    waveform availability. Use this before explaining a specific event.

    Set include_images=true (or waveform_image=true) to append MCP Image
    content blocks for the pre-rendered forward/cavity/reflect JPEGs.
    image_scope accepts all, forward, cavity, reflect, or a comma list.
    """
    ctx = _get_event_context(str(event_id))
    if "error" in ctx:
        return {"error": ctx["error"], "event_id": event_id}
    ev = _event_item(ctx.get("event") or {})
    alarms = []
    for a in ctx.get("alarms") or []:
        item = dict(a)
        item["citation"] = f"[A:{event_id}/{a.get('pv')}]"
        item["graph_pv_list"] = [
            p.strip() for p in (a.get("graph_pv") or "").split(";") if p.strip()
        ]
        alarms.append(item)
    selection = []
    for s in ctx.get("pv_selection") or []:
        item = dict(s)
        item["citation"] = f"[PVS:{event_id}/{s.get('pv')}]"
        item["url"] = f"/events/{event_id}/pv-explorer?pv={s.get('pv')}"
        selection.append(item)
    catalog = []
    for c in ctx.get("catalog") or []:
        item = dict(c)
        item["citation"] = _pv_citation(c.get("pv", ""))
        catalog.append(item)
    caveats = list(_CAVEATS)
    if not alarms:
        caveats.append("No recorded alarm snapshot for this event (event_alarms is empty).")
    if not selection:
        caveats.append("No manually registered PV selection for this event.")
    similar_items = [
        {
            **s,
            "event_id": s.get("id"),
            "citation": f"[E:{s.get('id')}]",
            "url": f"/events/{s.get('id')}",
        }
        for s in (ctx.get("similar") or [])
    ]
    physics = interpret_sequence(None)
    try:
        record = get_waveform_features(str(event_id))
        if record:
            feats = record.get("features") or {}
            physics = interpret_sequence(
                feats.get("sequence"), feats.get("beam_fall")
            )
    except Exception as exc:  # noqa: BLE001 - physics is optional context
        physics = {
            "available": False,
            "error": f"physics interpretation unavailable: {type(exc).__name__}: {exc}",
            "caveats": ["Use postmortem_get_glossary for the physics background."],
        }
    result = {
        "response_type": "event_detail",
        "event": ev,
        "classification": ctx.get("classification") or {},
        "waveform": ctx.get("waveform") or {},
        "physics": physics,
        "attachments": ctx.get("attachments") or [],
        "similar": similar_items,
        "similar_count": len(similar_items),
        "similar_note": (
            "An empty similar[] means the DB has no event_links for this event; "
            "it does not indicate an API error."
        ),
        "links_available": bool(similar_items),
        "alarms": alarms,
        "pv_selection": selection,
        "catalog": catalog,
        "caveats": caveats,
    }
    if _truthy(include_images) or _truthy(waveform_image):
        return _with_images(result, str(event_id), image_scope)
    return result


@server.tool(description=TOOL_DESCRIPTIONS["postmortem_aggregate_events"],
             meta=TOOL_META["postmortem_aggregate_events"])
def postmortem_aggregate_events(
    group_by: str = "year",
    year: Optional[int] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    subsystem: Optional[str] = None,
    fault_type: Optional[Union[str, list[str]]] = None,
    fault_types: Optional[list[str]] = None,
    text: Optional[str] = None,
    pv: Optional[str] = None,
    beam_time: Optional[str] = None,
    hide_ms: bool = True,
    include_percent: bool = True,
) -> dict[str, Any]:
    """Count events grouped by one or more fields.

    ``group_by`` accepts a single field or comma-separated fields:
    year, month, fault_type, case_id, auto_case_id, auto_fault_type,
    user_fault_type, beam_time. ``case_id`` follows the manual classification
    first; ``auto_case_id`` is the automatic reference. ``fault_type`` uses the
    manual user_fault_type first (override), then the automatic classification.
    ``hide_ms`` (default true) excludes Machine Study beam times.
    """
    if isinstance(fault_type, (list, tuple)):
        if not fault_types:
            fault_types = [str(x).strip() for x in fault_type if str(x).strip()]
        fault_type = None
    if isinstance(fault_types, str):
        fault_types = [fault_types]
    filters: dict[str, Any] = {}
    for key, value in {
        "year": year, "year_from": year_from, "year_to": year_to,
        "subsystem": subsystem, "fault_type": fault_type, "text": text,
        "pv": pv, "beam_time": beam_time,
    }.items():
        if value not in (None, ""):
            filters[key] = value
    if fault_types:
        filters["fault_types"] = [str(t) for t in fault_types if t]
    filters["hide_ms"] = bool(hide_ms)
    if beam_time and "MS" in str(beam_time).upper() and hide_ms:
        return {
            "response_type": "aggregate",
            "group_by": [group_by],
            "rows": [],
            "filters": filters,
            "error": (
                f"beam_time={beam_time!r} looks like a Machine Study period, "
                "but hide_ms=true."
            ),
            "hint": "MS periods are not user beam service. Pass hide_ms=false to include them.",
        }
    result = _aggregate_events(
        group_by, filters, include_percent=include_percent
    )
    result["response_type"] = "aggregate"
    result["filters"] = filters
    result["citation"] = "aggregate:postmortem_events"
    return result


@server.tool(description=TOOL_DESCRIPTIONS["postmortem_similar_events"],
             meta=TOOL_META["postmortem_similar_events"])
def postmortem_similar_events(event_id: str, limit: int = 10) -> dict[str, Any]:
    """Return events linked to the given event by the DB similarity graph."""
    limit = max(1, min(int(limit or 10), 50))
    rows = _similar_events(str(event_id), limit=limit)
    items = [
        {
            **r,
            "event_id": r.get("id"),
            "citation": f"[E:{r.get('id')}]",
            "url": f"/events/{r.get('id')}",
        }
        for r in rows
    ]
    return {
        "response_type": "event_list",
        "event_id": event_id,
        "returned": len(items),
        "links_available": bool(items),
        "items": items,
        "note": (
            "Empty items means no event_links exist for this event. "
            "Similarity is the DB event_links score, not a vector similarity."
        ),
    }


@server.tool(description=TOOL_DESCRIPTIONS["postmortem_search_alarm_catalog"],
             meta=TOOL_META["postmortem_search_alarm_catalog"])
def postmortem_search_alarm_catalog(
    query: str = "",
    subsystem: Optional[str] = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Search the 524-entry alarm PV catalog by PV name, description, or graph PV.

    Results are catalog mappings, not evidence that an alarm occurred in an event.
    """
    limit = max(1, min(int(limit or 20), 100))
    rows = _search_alarm_catalog(query, subsystem=subsystem, limit=limit)
    items = [{**r, "citation": _pv_citation(r.get("pv", ""))} for r in rows]
    return {
        "response_type": "pv_list",
        "returned": len(items),
        "items": items,
        "query": query,
        "subsystem": subsystem or "",
        "note": "PV catalog mapping only; not a recorded event alarm.",
    }


@server.tool(description=TOOL_DESCRIPTIONS["postmortem_get_alarm_status"],
             meta=TOOL_META["postmortem_get_alarm_status"])
def postmortem_get_alarm_status() -> dict[str, Any]:
    """Return the live alarm monitor status, last-state counts, and recent transitions.

    Check last_heartbeat and last_state_updated before calling this "current".
    """
    result = _get_alarm_status()
    result["response_type"] = "alarm_status"
    result["citation"] = "alarm_status:monitor"
    result["caveats"] = [
        "alarm_last_state is the monitor's last known state; check last_heartbeat.",
        "Historical events have very few recorded transitions.",
    ]
    return result


@server.tool(description=TOOL_DESCRIPTIONS["postmortem_resolve_event"],
             meta=TOOL_META["postmortem_resolve_event"])
def postmortem_resolve_event(date: str, subsystem: Optional[str] = None) -> dict[str, Any]:
    """Resolve a date (YYYY-MM-DD) and optional subsystem to candidate event IDs."""
    events = _search_events({"date": date, "subsystem": subsystem}, limit=10)
    return {
        "response_type": "event_list",
        "date": date,
        "subsystem": subsystem or "",
        "returned": len(events),
        "items": [_event_item(e) for e in events],
        "note": "If multiple events match, choose one and call postmortem_get_event.",
    }


@server.tool(description=TOOL_DESCRIPTIONS["postmortem_list_fault_types"],
             meta=TOOL_META["postmortem_list_fault_types"])
def postmortem_list_fault_types(hide_ms: bool = True) -> dict[str, Any]:
    """List fault types with their descriptions and event counts.

    Counts use the manual user_fault_type first (override), then the automatic
    classification. hide_ms (default true) excludes Machine Study beam times.
    """
    ms_filter = "" if not hide_ms else (
        " AND (e.user_beam_time IS NULL OR e.user_beam_time = '' "
        "OR e.user_beam_time NOT LIKE '%MS%')"
    )
    conn = connect_events()
    try:
        rows = conn.execute(
            f"""
            SELECT f.name, f.description, f.severity, count(e.id) AS event_count
              FROM fault_types f
              LEFT JOIN events e
                ON COALESCE(NULLIF(e.user_fault_type,''), e.fault_type, '') = f.name
               {ms_filter}
             GROUP BY f.name, f.description, f.severity
             ORDER BY event_count DESC, f.name
            """
        ).fetchall()
        return {"response_type": "fault_type_list",
                "hide_ms": bool(hide_ms),
                "items": [dict(r) for r in rows]}
    finally:
        conn.close()


@server.tool(description=TOOL_DESCRIPTIONS["postmortem_list_beam_times"],
             meta=TOOL_META["postmortem_list_beam_times"])
def postmortem_list_beam_times(hide_ms: bool = True) -> dict[str, Any]:
    """List user beam-time labels with event counts.

    hide_ms (default true) omits Machine Study (``*MS``) beam times.
    """
    ms_filter = "" if not hide_ms else (
        " WHERE (user_beam_time IS NULL OR user_beam_time = '' "
        "OR user_beam_time NOT LIKE '%MS%')"
    )
    conn = connect_events()
    try:
        rows = conn.execute(
            f"""
            SELECT COALESCE(NULLIF(user_beam_time,''),'(none)') AS beam_time,
                   count(*) AS event_count
              FROM events{ms_filter}
             GROUP BY beam_time
             ORDER BY beam_time
            """
        ).fetchall()
        return {"response_type": "beam_time_list",
                "hide_ms": bool(hide_ms),
                "items": [dict(r) for r in rows]}
    finally:
        conn.close()


@server.tool(description=TOOL_DESCRIPTIONS["postmortem_get_pv_series"],
             meta=TOOL_META["postmortem_get_pv_series"])
def postmortem_get_pv_series(
    event_id: str,
    pv: str,
    pre: float = 60,
    post: float = 10,
    include_points: bool = False,
) -> dict[str, Any]:
    """Return an Archiver time-series summary for a PV around an event's t0.

    Supports graph PVs in the KSU/CRM/CRG/LRF/G00 legacy tables. Use this for
    numeric evidence (min/max/deviation); use postmortem_get_event for context.
    """
    pre = max(0.0, min(float(pre or 60), 600.0))
    post = max(0.0, min(float(post or 10), 600.0))
    conn = connect_events()
    try:
        row = conn.execute(
            "SELECT timestamp FROM events WHERE id = ?", (event_id,)
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return {"response_type": "pv_series", "available": False,
                "event_id": event_id, "pv": pv, "error": "event not found"}
    timestamp = row["timestamp"]
    try:
        t0, base = resolve_t0(timestamp)
        t0_abs = base + timedelta(seconds=t0)
        pts, meta = fetch_pv_range(
            pv,
            t0_abs - timedelta(seconds=pre),
            t0_abs + timedelta(seconds=post),
            ref=base,
        )
    except Exception as e:  # noqa: BLE001
        return {
            "response_type": "pv_series",
            "available": False,
            "event_id": event_id,
            "pv": pv,
            "error": str(e),
            "url": f"/events/{event_id}/pv-explorer?pv={pv}",
        }
    if not pts:
        return {
            "response_type": "pv_series",
            "available": False,
            "event_id": event_id,
            "pv": pv,
            "quality": "no_data",
            "t0_offset_sec": t0,
            "url": f"/events/{event_id}/pv-explorer?pv={pv}",
            "note": "Archiver returned no data for this PV/period.",
        }
    vals = [v for _, v in pts]
    med = statistics.median(vals)
    tpk, vpk = max(pts, key=lambda tv: abs(tv[1] - med))
    dev = abs(vpk - med)
    scale = abs(med) if abs(med) > 1e-9 else 1e-9
    times = [t for t, _ in pts]
    diffs = sorted(b - a for a, b in zip(times, times[1:]) if b > a)
    resolution = round(diffs[len(diffs) // 2], 3) if diffs else None
    result = {
        "response_type": "pv_series",
        "available": True,
        "event_id": event_id,
        "pv": pv,
        "unit": meta.get("unit") or meta.get("name") or "",
        "quality": "ok",
        "t0_offset_sec": t0,
        "window": {"pre": pre, "post": post},
        "summary": {
            "count": len(pts),
            "min": min(vals),
            "max": max(vals),
            "median": med,
            "peak_value": vpk,
            "peak_rel_sec": round(tpk - t0_abs.timestamp(), 3),
            "max_deviation_pct": round(100 * dev / scale, 2),
            "resolution_s": resolution,
        },
        "url": f"/events/{event_id}/pv-explorer?pv={pv}",
        "note": "Archiver resolution is typically 1 s; t0 is the beam-current drop.",
    }
    if include_points:
        result["points"] = [
            [round(t - t0_abs.timestamp(), 3), v] for t, v in pts
        ]
    return result


@server.tool(description=TOOL_DESCRIPTIONS["postmortem_analyze_waveform"],
             meta=TOOL_META["postmortem_analyze_waveform"])
def postmortem_analyze_waveform(
    event_id: str,
    generate_plot: Union[bool, str] = False,
    include_images: bool = False,
    image_scope: Optional[str] = None,
) -> Any:
    """Return numeric waveform features for one event from the feature store.

    generate_plot=true (or include_images=true) appends MCP Image content
    blocks for the pre-rendered forward/cavity/reflect narrow-scope JPEGs.
    The JSON payload is still content[0].text.
    """
    try:
        record = get_waveform_features(str(event_id))
    except Exception as exc:  # noqa: BLE001 - report store problems as data
        return {
            "response_type": "waveform_analysis",
            "available": False,
            "event_id": event_id,
            "error": f"feature store unavailable: {exc}",
            "note": "Build data/feature_store.sqlite3 with "
                    "scripts/build_feature_store.py.",
            "physics": interpret_sequence(None),
        }
    if record is None:
        return {
            "response_type": "waveform_analysis",
            "available": False,
            "event_id": event_id,
            "error": "no waveform feature record for this event",
            "note": "Only 313/866 events (2023+) have parquet waveforms. "
                    "Use postmortem_get_event for notes, alarms, and attachments.",
            "physics": interpret_sequence(None),
        }
    features = record.get("features") or {}
    sequence = normalize_sequence(features.get("sequence"))
    t0 = features.get("t0") or {}
    dt = features.get("dt_s")
    missing = features.get("missing_channels") or []

    digital: dict[str, Any] = {}
    for channel, block in (features.get("digital") or {}).items():
        if not (block.get("n_transitions") or block.get("asserted")):
            continue
        digital[channel] = {
            "polarity": block.get("polarity"),
            "asserted": block.get("asserted"),
            "n_transitions": block.get("n_transitions"),
            "first_assert_t_s": block.get("first_assert_t_s"),
            "first_release_t_s": block.get("first_release_t_s"),
            "pulse_width_s": block.get("pulse_width_s"),
            "state_before": block.get("state_before"),
            "state_after": block.get("state_after"),
        }

    caveats = [
        "Waveform features describe a 100 ms window (±50 ms around the scope trigger).",
        "t0 is the trace-local 50% beam-current sustained drop (same rule as "
        "resolve_t0), not the Archiver t0 offset.",
        "sequence times are relative to t0; negative cavity times mean the "
        "cavity moved before the beam current. delay_*_us is positive when "
        "the cavity precedes the beam.",
        "All values are computed by code from the parquet trace; no LLM inference.",
    ]
    if not t0.get("found"):
        caveats.append(
            "No confirmed 50% beam-current drop in the window; t0 fell back to 0.0."
        )
    if missing:
        caveats.append(
            "Missing channels in the source parquet: "
            + ", ".join(missing) + ". Their features are null."
        )
    if features.get("quench_unit_source") == "default_cm1":
        caveats.append(
            "quench_unit defaults to CM1 because no QUEN channel asserted; "
            "cavity collapse alone cannot identify the quench unit."
        )
    if dt is not None and dt > 5e-6:
        caveats.append(
            f"Sampling interval is {dt * 1e6:.1f} µs (2023-era); timing "
            "resolution is coarser than the 2 µs traces."
        )

    result = {
        "response_type": "waveform_analysis",
        "available": True,
        "event_id": record["event_id"],
        "snapshot_id": record["snapshot_id"],
        "model": record.get("model"),
        "timestamp": record.get("timestamp"),
        "fault_type": features.get("fault_type", ""),
        "user_fault_type": features.get("user_fault_type", ""),
        "waveform": {
            "n_samples": features.get("n_samples"),
            "dt_s": dt,
            "dt_us": round(dt * 1e6, 3) if dt is not None else None,
            "duration_s": features.get("duration_s"),
            "t0": t0,
            "windows": features.get("windows"),
            "quench_unit": record.get("quench_unit") or features.get("quench_unit"),
            "quench_unit_source": features.get("quench_unit_source"),
            "quench_units_with_quen_assert": features.get(
                "quench_units_with_quen_assert"
            ),
            "missing_channels": missing,
            "beam_fall": features.get("beam_fall"),
            "sequence": sequence,
            "physics": interpret_sequence(
                sequence, features.get("beam_fall")
            ),
            "signature": features.get("signature"),
            "analog": features.get("analog"),
            "digital": digital,
            "warnings": features.get("warnings") or [],
        },
        "citation": f"[E:{record['event_id']}]",
        "url": f"/events/{record['event_id']}",
        "caveats": caveats,
    }
    if _truthy(generate_plot) or _truthy(include_images):
        result["plot_source"] = "pre_rendered_narrow_jpg"
        return _with_images(result, str(event_id), image_scope)
    return result


@server.tool(description=TOOL_DESCRIPTIONS["postmortem_find_similar_waveforms"],
             meta=TOOL_META["postmortem_find_similar_waveforms"])
def postmortem_find_similar_waveforms(
    event_id: str, top_k: int = 5
) -> dict[str, Any]:
    """Return the closest waveforms by cosine over the manual feature vector."""
    try:
        result = find_similar_waveforms(str(event_id), top_k=top_k)
    except Exception as exc:  # noqa: BLE001 - report store problems as data
        return {
            "response_type": "waveform_similarity",
            "available": False,
            "event_id": event_id,
            "error": f"feature store unavailable: {exc}",
            "note": "Build data/feature_store.sqlite3 with "
                    "scripts/build_feature_store.py.",
        }
    result.setdefault("url", f"/events/{event_id}")
    return result


@server.tool(description=TOOL_DESCRIPTIONS["postmortem_search_reports"],
             meta=TOOL_META["postmortem_search_reports"])
def postmortem_search_reports(
    query: str, event_id: Optional[str] = None, top_k: int = 8
) -> dict[str, Any]:
    """Search indexed reports by hybrid lexical + dense text retrieval."""
    try:
        from ..features.reports import search_reports
        return search_reports(query, event_id=event_id, top_k=top_k)
    except Exception as exc:  # noqa: BLE001 - report as data, not a crash
        return {
            "response_type": "report_search",
            "available": False,
            "query": query,
            "event_id": event_id or "",
            "error": f"report search unavailable: {type(exc).__name__}: {exc}",
            "note": "Report search needs the feature store and "
                    "numpy/onnxruntime/tokenizers in the MCP venv.",
        }


@server.tool(description=TOOL_DESCRIPTIONS["postmortem_analyze_quench"],
             meta=TOOL_META["postmortem_analyze_quench"])
def postmortem_analyze_quench(
    event_id: str,
    include_pv: bool = True,
    include_reports: bool = True,
    include_images: Union[bool, str] = False,
    image_scope: Optional[str] = None,
    pre: float = 60,
    post: float = 120,
) -> Any:
    """Integrate waveform, Archiver PVs, and report evidence for one quench."""
    try:
        from ..features.quench import analyze_quench
        result = analyze_quench(
            event_id,
            include_pv=bool(include_pv),
            include_reports=bool(include_reports),
            pre=max(0.0, min(float(pre or 60), 600.0)),
            post=max(0.0, min(float(post or 120), 600.0)),
        )
        if _truthy(include_images):
            return _with_images(result, str(event_id), image_scope)
        return result
    except Exception as exc:  # noqa: BLE001
        return {
            "response_type": "quench_analysis",
            "available": False,
            "event_id": event_id,
            "error": f"quench analysis unavailable: {type(exc).__name__}: {exc}",
        }


@server.tool(description=TOOL_DESCRIPTIONS["postmortem_compare_quench_events"],
             meta=TOOL_META["postmortem_compare_quench_events"])
def postmortem_compare_quench_events(
    event_ids: list[str],
    include_reports: bool = False,
) -> dict[str, Any]:
    """Compare waveform features across quench events (no Archiver calls)."""
    try:
        from ..features.quench import compare_quench_events
        return compare_quench_events(
            event_ids, include_reports=bool(include_reports)
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "response_type": "quench_comparison",
            "available": False,
            "error": f"quench comparison unavailable: {type(exc).__name__}: {exc}",
        }


@server.tool(description=TOOL_DESCRIPTIONS["postmortem_get_glossary"],
             meta=TOOL_META["postmortem_get_glossary"])
def postmortem_get_glossary(topic: Optional[str] = None) -> dict[str, Any]:
    """Return SRF interlock and beam-dump physics background knowledge.

    topic: optional key. Domain keys: abbreviations, system_overview,
    interlock_hierarchy, trip_sequence, signals, naming, data_rules,
    answer_style, authority, caveats. Physics keys: cavity_figures_of_merit,
    cavity_decay_after_rf_off, beam_loading_llrf, mis_machine_interlock,
    beam_energy_loss_after_rf_trip, transverse_instability,
    beam_dump_causes, pls2_operating_point. Omit to return both
    documents (the physics library is under ``physics``).
    """
    if topic and topic.strip().lower() in {"list", "index", "topics", "__list__"}:
        return _glossary_index()
    if topic:
        phys = physics_reference_bundle(topic.strip())
        if phys.get("available"):
            return {"response_type": "glossary", "source": "physics_references", **phys}

    doc = _domain_glossary()
    if not doc.get("available", True):
        return {"response_type": "glossary", **doc}
    if topic:
        key = topic.strip()
        if key in doc:
            return {
                "response_type": "glossary",
                "topic": key,
                "content": doc[key],
                "version": doc.get("version"),
                "updated": doc.get("updated"),
            }
        return {
            "response_type": "glossary",
            "topic": key,
            "available": False,
            "error": "unknown topic",
            "topics": [
                k for k in doc
                if k not in ("version", "updated", "title", "scope")
            ],
            "physics_topics": list((physics_reference_bundle().get("topics") or {})),
            "hint": "Call with topic='list' for an index of both documents.",
        }
    result = {"response_type": "glossary", **doc}
    phys = physics_reference_bundle()
    if phys.get("available"):
        result["physics"] = phys
    return result


# ── Resources ───────────────────────────────────────────────

@server.resource("postmortem://capabilities", mime_type="application/json",
                 description="Data coverage, index freshness, and limitations")
def resource_capabilities() -> str:
    return json.dumps(postmortem_get_capabilities(), ensure_ascii=False)


@server.resource("postmortem://schema", mime_type="application/json",
                 description="Tool and table overview")
def resource_schema() -> str:
    return json.dumps(
        {
            "tables": {
                "events": "id, timestamp, fault_type, case_id, case_description, case_fault, user_fault_type, user_beam_time, notes, merged_file, analog_metrics, digital_pattern",
                "event_alarms": "event_id, pv, description, subsystem, graph_pv, first_seen_rel, active_at_start, severity",
                "event_pv_selection": "event_id, pv, description, position",
                "alarm_pv_catalog": "pv, description, subsystem, graph_pv, graph_description, severity, state_mode",
                "fault_types": "name, description, severity",
                "event_links": "event_id, related_event_id, similarity_score",
            },
            "tools": postmortem_get_capabilities()["tools"],
            "citation_formats": [
                "[E:event_id]",
                "[A:event_id/PV] recorded alarm",
                "[PVS:event_id/PV] manually registered PV",
                "[PV:PV] catalog mapping",
                "[FT:fault_type]",
            ],
            "event_reference_schema": {
                "id": "event id",
                "event_id": "same value as id (kept for client compatibility)",
                "timestamp": "ISO8601",
                "fault_type": "Case N: ...",
                "url": "/events/<id>",
                "citation": "[E:<id>]",
            },
            "response_shapes": postmortem_get_capabilities()["response_shapes"],
            "encoding": "utf-8",
        },
        ensure_ascii=False,
    )


@server.resource("postmortem://fault-types", mime_type="application/json",
                 description="Fault type taxonomy and event counts")
def resource_fault_types() -> str:
    return json.dumps(postmortem_list_fault_types(), ensure_ascii=False)


@server.resource("postmortem://abbreviations", mime_type="application/json",
                 description="Device-name abbreviations and channel prefixes")
def resource_abbreviations() -> str:
    return json.dumps(
        _domain_glossary().get("abbreviations", {}),
        ensure_ascii=False,
    )


@server.resource("postmortem://domain-guide", mime_type="application/json",
                 description="SRF interlock background knowledge (config/domain_glossary.json)")
def resource_domain_guide() -> str:
    return json.dumps(_domain_glossary(), ensure_ascii=False)


@server.resource("postmortem://glossary", mime_type="application/json",
                 description="Common SRF/PV aliases plus a pointer to the domain guide")
def resource_glossary() -> str:
    return json.dumps(
        {
            "domain_guide": "postmortem://domain-guide",
            "domain_guide_tool": "postmortem_get_glossary",
            "abbreviations": "postmortem://abbreviations",
            "abbreviations_tool": "postmortem_get_glossary(topic='abbreviations')",
            "aliases": {
                "쿼치": "quench",
                "퀜치": "quench",
                "KSU#1": "KSU1",
                "KSU#2": "KSU2",
                "KSU#3": "KSU3",
                "SRF#1": "CM1",
                "SRF#2": "CM2",
                "SRF#3": "CM3",
                "CM#1": "CRM1",
                "CM#2": "CRM2",
                "CM#3": "CRM3",
                "KLY1": "KSU1",
                "KLY2": "KSU2",
                "KLY3": "KSU3",
            },
            "notes": [
                "KSU and CM/CRM are different subsystems.",
                "RDY_KSUx_IC is a ready-chain interlock; AI_LOW_FAULT40 is a KSU water-flow alarm.",
                "event_alarms is a recorded alarm snapshot; event_pv_selection is manual.",
            ],
        },
        ensure_ascii=False,
    )


@server.resource("postmortem://quickstart", mime_type="application/json",
                 description="Example tool calls and expected usage")
def resource_quickstart() -> str:
    return json.dumps(
        {
            "search_events": {
                "tool": "postmortem_search_events",
                "arguments": {"query": "2026년 KSU관련 빔덤프", "limit": 5},
            },
            "event_detail": {
                "tool": "postmortem_get_event",
                "arguments": {"event_id": "20251103_115812"},
            },
            "aggregate": {
                "tool": "postmortem_aggregate_events",
                "arguments": {"group_by": "year,beam_time",
                              "include_percent": True},
            },
            "alarm_catalog": {
                "tool": "postmortem_search_alarm_catalog",
                "arguments": {"query": "water flow", "subsystem": "KSU3"},
            },
            "pv_series": {
                "tool": "postmortem_get_pv_series",
                "arguments": {"event_id": "20241111_044552",
                              "pv": "SR:RF:CRM1:BCL03"},
            },
            "waveform_analysis": {
                "tool": "postmortem_analyze_waveform",
                "arguments": {"event_id": "20240907_025313"},
            },
            "waveform_similarity": {
                "tool": "postmortem_find_similar_waveforms",
                "arguments": {"event_id": "20240907_025313", "top_k": 5},
            },
            "report_search": {
                "tool": "postmortem_search_reports",
                "arguments": {"query": "켄치 발생 시 RF 반사파가 증가한 원인",
                              "top_k": 5},
            },
            "quench_analysis": {
                "tool": "postmortem_analyze_quench",
                "arguments": {"event_id": "20240907_025313"},
            },
            "quench_comparison": {
                "tool": "postmortem_compare_quench_events",
                "arguments": {"event_ids": ["20240907_025313",
                                            "20241002_171719",
                                            "20251205_014253"]},
            },
            "notes": [
                "Use postmortem_get_capabilities first.",
                "Cite [E:], [A:], [PVS:], [PV:], [R:].",
                "Use aggregate tools for counts, not vector search.",
                "Only 313/866 events have waveform features; check available.",
                "Reports are text-only; image captions are pending.",
            ],
        },
        ensure_ascii=False,
    )


# ── Prompts ─────────────────────────────────────────────────

@server.prompt(name="postmortem_event_analysis",
               description="Analyze one beam-dump event with DB evidence")
def prompt_event_analysis(event_id: str) -> str:
    return (
        "Analyze SRF beam-dump event {event_id}. "
        "Call postmortem_get_event first. Then, if useful, call "
        "postmortem_analyze_waveform (numeric waveform features), "
        "postmortem_find_similar_waveforms, postmortem_search_reports, "
        "postmortem_similar_events, postmortem_search_alarm_catalog, or "
        "postmortem_get_pv_series. "
        "Distinguish recorded alarms [A:...] from "
        "catalog mappings [PV:...] and manual PV selections [PVS:...]. "
        "Say explicitly when an event has no waveform (only 313/866 do). "
        "Cite every factual claim with [E:...] or [PV:...]. If the event has no "
        "recorded alarm snapshot, say so explicitly."
    )


@server.prompt(name="postmortem_trend_analysis",
               description="Analyze event counts/trends for a subsystem")
def prompt_trend_analysis(subsystem: str, year_from: int, year_to: int) -> str:
    return (
        f"Analyze SRF beam-dump trends for {subsystem} from {year_from} to {year_to}. "
        "Call postmortem_aggregate_events with group_by='year' and the filters. "
        "Then list the underlying events with postmortem_search_events. "
        "Do not infer counts from retrieved text; use the aggregate tool result."
    )


@server.prompt(name="postmortem_compare_events",
               description="Compare two beam-dump events")
def prompt_compare_events(event_a: str, event_b: str) -> str:
    return (
        f"Compare SRF beam-dump events {event_a} and {event_b}. "
        "Call postmortem_get_event for both. If both have parquet waveforms "
        "(313/866 events, 2023+), call postmortem_analyze_waveform for each and "
        "compare t0, beam fall, cavity sag, reflect rise, forward trip, and "
        "digital transitions; then call postmortem_find_similar_waveforms. "
        "Call postmortem_search_reports for the maintenance reports attached to "
        "both events and compare the recorded actions. "
        "Compare timestamp, fault type, notes, "
        "recorded alarms, PV selections, and similar events. Cite both event IDs."
    )


@server.prompt(name="postmortem_pv_investigation",
               description="Investigate a PV around an event")
def prompt_pv_investigation(event_id: str, pv: str) -> str:
    return (
        f"Investigate PV {pv} for event {event_id}. Call postmortem_get_event and "
        "postmortem_get_pv_series (include_points=false first). Explain whether the "
        "PV is a recorded alarm, a manual selection, or only a catalog mapping. "
        "Cite [E:...], [PVS:...], and [PV:...] as appropriate."
    )
