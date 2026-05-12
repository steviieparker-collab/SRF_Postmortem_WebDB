"""
FastAPI web server for SRF Postmortem Event Viewer.

Provides:
- `/` — Event list (main page)
- `/events/{id}` — Event detail page
- `/api/events` — JSON API
"""

import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlencode

from fastapi import FastAPI, Query, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from ..core.config import get_config, reset_config
from ..db.schema import init_db_sync, get_sync_connection, seed_fault_types
from ..db.repository import (
    get_event,
    list_events,
    get_similar_events,
    list_fault_types,
    get_fault_type,
    update_event,
    get_adjacent_events,
    create_attachment,
    list_attachments_by_event,
    get_attachment_by_id,
    delete_attachment,
    get_attachment_counts,
)
from ..db.similarity import update_similarity_links, update_all_similarity_links
from ..import_job import run_import, CASE_TO_FAULT

import threading
import yaml
from pathlib import Path
from datetime import datetime, timezone
import uuid
import mimetypes
from typing import List as TypingList


# ── Simple Auth (password prompt) ────────────────────────────

def _check_password(input_pw: str) -> bool:
    """Verify password against config."""
    cfg = get_config()
    expected = getattr(cfg.access, "password", "")
    if not expected:
        return True  # no password configured = no auth required
    return input_pw == expected


def _event_id_to_utc_file_stem(event_id: str) -> str:
    """Convert a KST event_id to UTC-based file stem for matching old merged files.
    
    Event IDs are formatted as YYYYMMDD_HHMMSS in KST (UTC+9).
    Old merged files were named with UTC timestamps, so subtract 9h for fallback.
    """
    try:
        from datetime import datetime, timedelta
        kst = datetime.strptime(event_id, "%Y%m%d_%H%M%S")
        utc = kst - timedelta(hours=9)
        return utc.strftime("%Y%m%d_%H%M%S")
    except ValueError:
        return None


def _cleanup_event_files(event_id: str):
    """Remove merged parquet and results files for a deleted event.
    
    Tries KST-based name first, falls back to UTC-based name
    for backwards compatibility with old merged files.
    """
    try:
        cfg = get_config()
        
        # Collect candidate file stems to try
        stems = [event_id]
        utc_stem = _event_id_to_utc_file_stem(event_id)
        if utc_stem and utc_stem != event_id:
            stems.append(utc_stem)
        
        merged_dir = cfg.paths.merged_dir
        for stem in stems:
            merged_path = merged_dir / f"event_{stem}.parquet"
            if merged_path.exists():
                merged_path.unlink()
                break
        
        results_dir = cfg.paths.results_dir
        for stem in stems:
            results_path = results_dir / f"event_{stem}_classification.json"
            if results_path.exists():
                results_path.unlink()
                break
    except Exception:
        pass  # non-critical; don't fail the delete


def _get_attachments_dir(event_id: str) -> Path:
    """Get the directory for storing attachments of a specific event."""
    cfg = get_config()
    project_root = Path(__file__).resolve().parent.parent.parent
    att_dir = project_root / "data" / "attachments" / event_id
    att_dir.mkdir(parents=True, exist_ok=True)
    return att_dir


app = FastAPI(title="SRF Postmortem Viewer")

# ── Auth API ──────────────────────────────────────────────────

@app.post("/api/verify-password")
async def api_verify_password(request: Request):
    """Verify password (simple check, no sessions)."""
    data = await request.json()
    return {"ok": _check_password(data.get("password", ""))}


# ── Static / Templates ───────────────────────────────────────

_templates_dir = Path(__file__).parent / "templates"
_static_dir = Path(__file__).parent / "static"

templates = Jinja2Templates(directory=str(_templates_dir))

# Register custom Jinja2 filters
def extract_case_id(fault_type: str) -> int:
    """Extract case number from fault_type string like 'Case 3: RF_Interlock'."""
    m = re.match(r"^Case (\d+)", fault_type or "")
    return int(m.group(1)) if m else 0

templates.env.filters["extract_case_id"] = extract_case_id

app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")


# ── Startup ──────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    """Initialize DB on first run."""
    conn = init_db_sync()
    seed_fault_types(conn)
    conn.close()


# ── Web Pages ────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def index(
    request: Request,
    page: int = Query(1, ge=1),
    fault_type: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    user_beam_time: Optional[List[str]] = Query(None),
    bt_sort: Optional[str] = Query("desc"),
    hide_ms: Optional[str] = Query("true"),
    year: Optional[str] = Query(None),
):
    """Main event list page."""
    # Convert multiple user_beam_time params to comma-separated string
    bt_str = ",".join(filter(None, user_beam_time)) if user_beam_time else ""
    if year is None:
        year = ""
    conn = get_sync_connection()
    try:
        result = list_events(
            conn,
            page=page,
            page_size=20,
            fault_type=fault_type,
            search=search,
            user_beam_time=bt_str or None,
            hide_ms=(hide_ms == "true"),
            date_from=f"{year}-01-01" if year else None,
            date_to=f"{int(year)+1}-01-01" if year else None,
        )
        fault_types = list_fault_types(conn, hide_ms=(hide_ms == "true"))
        # Get distinct user_beam_time values (default DESC)
        order = "DESC" if bt_sort == "desc" else "ASC"
        # MS를 포함한 항목을 제외하도록 수정 (user_beam_time IS NULL OR ... 생략 가능)
        ms_filter = " AND (user_beam_time NOT LIKE '%MS%' OR user_beam_time IS NULL OR user_beam_time = '')" if hide_ms == "true" else ""
        year_filter = f" AND timestamp LIKE '{year}%'" if year else ""
        cursor = conn.execute(
            f"SELECT DISTINCT user_beam_time FROM events WHERE user_beam_time != ''{ms_filter}{year_filter} ORDER BY user_beam_time {order}"
        )
        user_beam_times = [row["user_beam_time"] for row in cursor.fetchall()]
        # Get distinct case_ids
        cursor = conn.execute(
            "SELECT DISTINCT case_id FROM events WHERE case_id > 0 ORDER BY case_id"
        )
        case_ids = [row["case_id"] for row in cursor.fetchall()]
        cursor = conn.execute("SELECT DISTINCT SUBSTR(timestamp,1,4) as yr FROM events ORDER BY yr DESC")
        years = [row["yr"] for row in cursor.fetchall()]
        # Get attachment counts for displayed events
        event_ids = [e.id for e in result.events]
        attachment_counts = get_attachment_counts(conn, event_ids)
    finally:
        conn.close()

    # Get graph thumbnails
    cfg = get_config()
    graphs_dir = ""
    if cfg.monitor.data_root:
        graphs_dir = str(Path(cfg.monitor.data_root) / cfg.monitor.graphs_dir)

    # Build filter query string for prev/next on detail page
    filter_parts = {}
    if fault_type:
        filter_parts["fault_type"] = fault_type
    if search:
        filter_parts["search"] = search
    if bt_str:
        filter_parts["user_beam_time"] = bt_str
    if bt_sort and bt_sort != "desc":
        filter_parts["bt_sort"] = bt_sort
    if hide_ms == "true":
        filter_parts["hide_ms"] = "true"
    if year:
        filter_parts["year"] = year
    filter_query = urlencode(filter_parts)

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "events": result.events,
            "total": result.total,
            "page": result.page,
            "page_size": result.page_size,
            "total_pages": result.total_pages,
            "fault_types": fault_types,
            "current_fault_type": fault_type or "",
            "search": search or "",
            "user_beam_time": bt_str or "",
            "user_beam_times": user_beam_times,
            "case_ids": case_ids,
            "graphs_dir": graphs_dir,
            "attachment_counts": attachment_counts,
            "filter_query": filter_query,
            "bt_sort": bt_sort,
            "hide_ms": hide_ms,
            "year": year or "",
            "years": years,
        },
    )


@app.get("/events/{event_id}", response_class=HTMLResponse, include_in_schema=False)
async def event_detail(
    request: Request,
    event_id: str,
    fault_type: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    user_beam_time: Optional[str] = Query(None),
    hide_ms: Optional[str] = Query("true"),
):
    """Event detail page with plots and similar events."""
    conn = get_sync_connection()
    try:
        event = get_event(conn, event_id)
        if not event:
            return HTMLResponse("Event not found", status_code=404)

        # Get adjacent events within filter context
        prev_id, next_id = get_adjacent_events(
            conn, event_id,
            fault_type=fault_type or None,
            search=search or None,
            user_beam_time=user_beam_time or None,
            hide_ms=(hide_ms == "true"),
        )

        # Get or update similarity links
        similar = get_similar_events(conn, event_id)
        if not similar:
            update_similarity_links(conn, event_id)
            similar = get_similar_events(conn, event_id)

        # Fetch fault types for dropdowns
        ft_list = list_fault_types(conn)

        # Fetch existing user_beam_time values (sorted DESC) for datalist
        cursor = conn.execute(
            "SELECT DISTINCT user_beam_time FROM events WHERE user_beam_time != '' ORDER BY user_beam_time DESC"
        )
        user_beam_times = [row["user_beam_time"] for row in cursor.fetchall()]

        # Parse JSON fields for template
        analog_metrics = event.analog_metrics
        digital_pattern = event.digital_pattern
        time_groups = event.time_groups
    finally:
        conn.close()

    # Check graphs exist
    cfg = get_config()
    graphs_path = ""
    if event.graphs_path and os.path.isdir(event.graphs_path):
        graphs_path = event.graphs_path
    elif cfg.monitor.data_root and event.id:
        candidate = Path(cfg.monitor.data_root) / cfg.monitor.graphs_dir / event.id
        if candidate.is_dir():
            graphs_path = str(candidate)

    merged_filename = os.path.basename(event.merged_file) if event.merged_file else ""

    # Build filter query string for prev/next and back links
    filter_parts = {}
    if fault_type:
        filter_parts["fault_type"] = fault_type
    if search:
        filter_parts["search"] = search
    if user_beam_time:
        filter_parts["user_beam_time"] = user_beam_time
    if hide_ms == "true":
        filter_parts["hide_ms"] = "true"
    filter_query = urlencode(filter_parts)
    back_url = "/" + ("?" + filter_query if filter_query else "")

    return templates.TemplateResponse(
        request,
        "event_detail.html",
        {
            "event": event,
            "analog_metrics": analog_metrics,
            "digital_pattern": digital_pattern,
            "time_groups": time_groups,
            "similar": similar,
            "graphs_path": graphs_path,
            "merged_filename": merged_filename,
            "prev_event_id": prev_id,
            "next_event_id": next_id,
            "filter_query": filter_query,
            "back_url": back_url,
            "fault_types": ft_list,
            "case_fault_types": CASE_TO_FAULT,
            "user_beam_times": user_beam_times,
        },
    )


@app.get("/stats", response_class=HTMLResponse, include_in_schema=False)
async def stats_page(request: Request, hide_ms: Optional[str] = Query("true")):
    """Statistics page."""
    conn = get_sync_connection()
    try:
        ms_filter = " AND (user_beam_time NOT LIKE '%MS%' OR user_beam_time IS NULL OR user_beam_time = '')" if hide_ms == "true" else ""
        cursor = conn.execute(
            f"SELECT DISTINCT user_beam_time FROM events WHERE user_beam_time != ''{ms_filter} ORDER BY user_beam_time DESC"
        )
        user_beam_times = [row["user_beam_time"] for row in cursor.fetchall()]
        cursor = conn.execute("SELECT DISTINCT case_id FROM events WHERE case_id > 0 ORDER BY case_id")
        case_ids = [row["case_id"] for row in cursor.fetchall()]
        fault_types = list_fault_types(conn)
        cursor = conn.execute("SELECT DISTINCT SUBSTR(timestamp,1,4) as yr FROM events ORDER BY yr DESC")
        years = [row["yr"] for row in cursor.fetchall()]
    finally:
        conn.close()
    return templates.TemplateResponse(
        request,
        "stats.html",
        {
            "user_beam_times": user_beam_times,
            "case_ids": case_ids,
            "fault_types": fault_types,
            "years": years,
            "hide_ms": hide_ms,
        },
    )


# ── JSON API ─────────────────────────────────────────────────

@app.get("/api/events")
async def api_list_events(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    fault_type: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    user_beam_time: Optional[str] = Query(None),
):
    """JSON endpoint: paginated event list."""
    conn = get_sync_connection()
    try:
        result = list_events(
            conn,
            page=page,
            page_size=page_size,
            fault_type=fault_type,
            search=search,
            user_beam_time=user_beam_time,
        )
    finally:
        conn.close()
    return result.model_dump()


@app.get("/api/events/{event_id}")
async def api_get_event(event_id: str):
    """JSON endpoint: single event detail."""
    conn = get_sync_connection()
    try:
        event = get_event(conn, event_id)
    finally:
        conn.close()

    if not event:
        return JSONResponse({"error": "Event not found"}, status_code=404)
    return event.model_dump()


@app.get("/api/events/{event_id}/similar")
async def api_get_similar(event_id: str):
    """JSON endpoint: similar events."""
    conn = get_sync_connection()
    try:
        similar = get_similar_events(conn, event_id)
        if not similar:
            update_similarity_links(conn, event_id)
            similar = get_similar_events(conn, event_id)
    finally:
        conn.close()
    return {"similar": [s.model_dump() for s in similar]}


@app.get("/api/fault-types")
async def api_fault_types():
    """JSON endpoint: list of fault types with counts."""
    conn = get_sync_connection()
    try:
        types = list_fault_types(conn)
    finally:
        conn.close()
    return {"fault_types": [t.model_dump() for t in types]}


@app.post("/api/events/{event_id}/user-beam-time")
async def api_set_user_beam_time(event_id: str, request: Request):
    """Update user_beam_time for an event."""
    data = await request.json()
    value = data.get("value", "")
    conn = get_sync_connection()
    try:
        event = update_event(conn, event_id, {"user_beam_time": value})
        if not event:
            return JSONResponse({"error": "Event not found"}, status_code=404)
        return {"ok": True, "user_beam_time": value}
    finally:
        conn.close()


@app.post("/api/events/{event_id}/notes")
async def api_set_notes(event_id: str, request: Request):
    """Update notes for an event."""
    data = await request.json()
    value = data.get("value", "")
    conn = get_sync_connection()
    try:
        event = update_event(conn, event_id, {"notes": value})
        if not event:
            return JSONResponse({"error": "Event not found"}, status_code=404)
        return {"ok": True, "notes": value}
    finally:
        conn.close()


@app.delete("/api/events/{event_id}")
async def api_delete_event(event_id: str, request: Request):
    """Delete an event. Password in query param or body."""
    data = await request.json() if request.headers.get("content-type") == "application/json" else {}
    pw = request.query_params.get("password", "") or data.get("password", "")
    if not _check_password(pw):
        return JSONResponse({"error": "Authentication required"}, status_code=401)
    conn = get_sync_connection()
    try:
        from ..db.repository import delete_event as delete_event_db
        deleted = delete_event_db(conn, event_id)
        if not deleted:
            return JSONResponse({"error": "Event not found"}, status_code=404)
        # Also clean up merged and results files
        _cleanup_event_files(event_id)
        return {"ok": True, "message": f"Event {event_id} deleted"}
    finally:
        conn.close()


# ── Attachment API ───────────────────────────────────────────


@app.post("/api/events/{event_id}/attachments")
async def api_upload_attachments(event_id: str, request: Request):
    """Upload file attachments for an event. Accepts multipart/form-data."""
    conn = get_sync_connection()
    try:
        event = get_event(conn, event_id)
        if not event:
            return JSONResponse({"error": "Event not found"}, status_code=404)
    finally:
        conn.close()

    # Parse multipart form
    form = await request.form()
    files = form.getlist("files")
    if not files:
        return JSONResponse({"error": "No files provided"}, status_code=400)

    conn = get_sync_connection()
    try:
        att_dir = _get_attachments_dir(event_id)
        created = []
        for upload_file in files:
            if not hasattr(upload_file, "filename") or not upload_file.filename:
                continue

            original_name = upload_file.filename
            ext = Path(original_name).suffix or ""
            stored_name = f"{uuid.uuid4().hex}{ext}"
            file_path = att_dir / stored_name

            # Read and write file
            content = await upload_file.read()
            file_path.write_bytes(content)

            # Detect MIME type
            mime_type, _ = mimetypes.guess_type(original_name)
            file_size = len(content)

            # Create DB record
            record = create_attachment(
                conn, event_id, original_name, stored_name, mime_type, file_size
            )
            created.append({
                "id": record.id,
                "original_name": record.original_name,
                "stored_name": record.stored_name,
                "mime_type": record.mime_type,
                "file_size": record.file_size,
                "uploaded_at": record.uploaded_at,
            })
    finally:
        conn.close()

    return {"ok": True, "attachments": created}


@app.get("/api/events/{event_id}/attachments")
async def api_list_attachments(event_id: str):
    """List all attachments for an event."""
    conn = get_sync_connection()
    try:
        attachments = list_attachments_by_event(conn, event_id)
        result = [
            {
                "id": a.id,
                "original_name": a.original_name,
                "stored_name": a.stored_name,
                "mime_type": a.mime_type,
                "file_size": a.file_size,
                "uploaded_at": a.uploaded_at,
            }
            for a in attachments
        ]
    finally:
        conn.close()
    return {"ok": True, "attachments": result}


@app.get("/api/attachments/{attachment_id}/download")
async def api_download_attachment(attachment_id: int):
    """Download an attachment file."""
    conn = get_sync_connection()
    try:
        attachment = get_attachment_by_id(conn, attachment_id)
    finally:
        conn.close()

    if not attachment:
        return JSONResponse({"error": "Attachment not found"}, status_code=404)

    att_dir = _get_attachments_dir(attachment.event_id)
    file_path = att_dir / attachment.stored_name

    if not file_path.exists():
        return JSONResponse({"error": "File not found on disk"}, status_code=404)

    return FileResponse(
        str(file_path),
        media_type=attachment.mime_type or "application/octet-stream",
        filename=attachment.original_name,
    )


@app.delete("/api/attachments/{attachment_id}")
async def api_delete_attachment(attachment_id: int, request: Request):
    """Delete an attachment. Password in query param."""
    pw = request.query_params.get("password", "")
    if not _check_password(pw):
        return JSONResponse({"error": "Authentication required"}, status_code=401)

    conn = get_sync_connection()
    try:
        attachment = get_attachment_by_id(conn, attachment_id)
        if not attachment:
            return JSONResponse({"error": "Attachment not found"}, status_code=404)

        # Delete file from disk
        att_dir = _get_attachments_dir(attachment.event_id)
        file_path = att_dir / attachment.stored_name
        if file_path.exists():
            file_path.unlink()

        # Clean up empty event dir
        try:
            if att_dir.exists() and not any(att_dir.iterdir()):
                att_dir.rmdir()
        except Exception:
            pass

        # Delete DB record
        deleted = delete_attachment(conn, attachment_id)
    finally:
        conn.close()

    return {"ok": bool(deleted)}


@app.post("/api/events/batch-delete")
async def api_batch_delete(request: Request):
    """Batch delete events. Password in query param or body."""
    data = await request.json()
    pw = request.query_params.get("password", "") or data.get("password", "")
    if not _check_password(pw):
        return JSONResponse({"error": "Authentication required"}, status_code=401)
    event_ids = data.get("event_ids", [])
    if not event_ids:
        return {"ok": True, "deleted": 0, "message": "No events specified"}
    conn = get_sync_connection()
    try:
        from ..db.repository import delete_event as delete_event_db
        deleted_count = 0
        errors = []
        for eid in event_ids:
            try:
                if delete_event_db(conn, eid):
                    deleted_count += 1
                    _cleanup_event_files(eid)
                else:
                    errors.append(eid)
            except Exception as ex:
                errors.append(f"{eid}: {ex}")
        return {"ok": True, "deleted": deleted_count, "files_cleaned": True if deleted_count > 0 else False, "errors": errors}
    finally:
        conn.close()


@app.post("/api/events/batch-beamtime")
async def api_batch_beamtime(request: Request):
    """Batch set user_beam_time for events."""
    data = await request.json()
    pw = request.query_params.get("password", "") or data.get("password", "")
    if not _check_password(pw):
        return JSONResponse({"error": "Authentication required"}, status_code=401)
    event_ids = data.get("event_ids", [])
    beam_time = data.get("beam_time", "")
    if not event_ids:
        return {"ok": True, "updated": 0, "message": "No events specified"}
    conn = get_sync_connection()
    try:
        updated_count = 0
        errors = []
        for eid in event_ids:
            try:
                ev = update_event(conn, eid, {"user_beam_time": beam_time})
                if ev:
                    updated_count += 1
                else:
                    errors.append(eid)
            except Exception as ex:
                errors.append(f"{eid}: {ex}")
        return {"ok": True, "updated": updated_count, "errors": errors}
    finally:
        conn.close()


@app.post("/api/events/{event_id}/user-fault-type")
async def api_set_user_fault_type(event_id: str, request: Request):
    """Update user_fault_type for an event."""
    data = await request.json()
    value = data.get("value", "")
    conn = get_sync_connection()
    try:
        event = update_event(conn, event_id, {"user_fault_type": value})
        if not event:
            return JSONResponse({"error": "Event not found"}, status_code=404)
        return {"ok": True, "user_fault_type": value}
    finally:
        conn.close()


@app.get("/api/stats/cases")
async def api_stats_cases(
    period: Optional[str] = Query(None),
    user_beam_time: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    hide_ms: Optional[str] = Query("false"),
):
    """Case-based statistics."""
    conn = get_sync_connection()
    conditions = []
    params = []

    if period == "yearly":
        conditions.append("timestamp >= datetime('now', '-1 year')")
    if user_beam_time:
        if "," in user_beam_time:
            values = [v.strip() for v in user_beam_time.split(",") if v.strip()]
            placeholders = ",".join(["?"] * len(values))
            conditions.append(f"user_beam_time IN ({placeholders})")
            params.extend(values)
        else:
            conditions.append("user_beam_time = ?")
            params.append(user_beam_time)
    if hide_ms == "true":
        conditions.append("(user_beam_time NOT LIKE '%MS%' OR user_beam_time IS NULL OR user_beam_time = '')")
    if date_from:
        conditions.append("timestamp >= ?")
        params.append(date_from)
    if date_to:
        conditions.append("timestamp <= ?")
        params.append(date_to)

    where = ""
    if conditions:
        where = "WHERE " + " AND ".join(conditions)

    try:
        # Use user_fault_type if set, otherwise fault_type
        eff = "CASE WHEN user_fault_type != '' THEN user_fault_type ELSE COALESCE(fault_type, 'Unknown') END"

        # Case/fault distribution (using effective fault type)
        cursor = conn.execute(f"""
            SELECT {eff} as effective_fault, COUNT(*) as cnt
            FROM events {where}
            GROUP BY effective_fault
            ORDER BY cnt DESC
        """, params)
        case_stats = []
        fault_stats = []
        for r in cursor.fetchall():
            ft = r["effective_fault"] or "Unknown"
            cid = extract_case_id(ft)
            case_stats.append({
                "case_id": cid,
                "case_description": ft,
                "count": r["cnt"],
            })
            fault_stats.append({
                "fault_type": ft,
                "count": r["cnt"],
            })
        total = sum(c["count"] for c in case_stats)

        # User beam time distribution (if no filter)
        bt_stats = []
        if not user_beam_time:
            bt_where = f"{where}"
            bt_params = list(params)
            if conditions:
                bt_where = f"{where} AND user_beam_time != ''"
            else:
                bt_where = "WHERE user_beam_time != ''"
            cursor = conn.execute(f"""
                SELECT user_beam_time, COUNT(*) as cnt
                FROM events {bt_where}
                GROUP BY user_beam_time
                ORDER BY user_beam_time
            """, bt_params)
            bt_stats = [
                {"user_beam_time": r["user_beam_time"], "count": r["cnt"]}
                for r in cursor.fetchall()
            ]

        # Digital channel breakdown (which digital channels most common per case)
        digital_by_case = {}
        digital_cols = [
            "INT_MIS1_IC", "INT_MIS2_IC", "INT_IN_FC3", "INT_IC_FC2", "INT_IC_FC1",
            "RDY_KSU3_IC", "RDY_KSU2_IC", "RDY_KSU1_IC",
            "INT_PSI2_IC", "INT_PSI1_IC",
            "RDY_CM3_FC3", "RDY_CM2_FC2", "RDY_CM1_FC1",
            "HE-PR_CM3_FC3", "HE-PR_CM2_FC2", "HE-PR_CM1_FC1",
            "ARC_CM3_FC3", "ARC_CM2_FC2", "ARC_CM1_FC1",
            "QUEN_CM3_FC3", "QUEN_CM2_FC2", "QUEN_CM1_FC1",
            "VAC_CM3_FC3", "VAC_CM2_FC2", "VAC_CM1_FC1",
        ]
        # For events with case_id > 0, extract digital_pattern and count channel occurrences
        dig_where = f"{where}"
        if conditions:
            dig_where = f"{where} AND digital_pattern IS NOT NULL"
        else:
            dig_where = "WHERE digital_pattern IS NOT NULL"
        cursor = conn.execute(f"""
            SELECT case_id, digital_pattern FROM events {dig_where}
        """, params)
        for r in cursor.fetchall():
            cid = r["case_id"]
            if cid not in digital_by_case:
                digital_by_case[cid] = {ch: 0 for ch in digital_cols}
            try:
                pat = json.loads(r["digital_pattern"])
                for ch, val in pat.items():
                    ch_key = ch.replace("_d", "")
                    if val == 1 and ch_key in digital_by_case[cid]:
                        digital_by_case[cid][ch_key] += 1
            except (json.JSONDecodeError, TypeError):
                pass

    finally:
        conn.close()

    return {
        "total": total,
        "case_stats": case_stats,
        "fault_stats": fault_stats,
        "user_beam_time_stats": bt_stats,
        "digital_by_case": digital_by_case,
    }


@app.get("/api/events/{event_id}/waveforms")
async def api_get_waveforms(event_id: str):
    """JSON endpoint: actual waveform data from merged parquet file."""
    import polars as pl

    conn = get_sync_connection()
    try:
        event = get_event(conn, event_id)
    finally:
        conn.close()

    if not event or not event.merged_file:
        return JSONResponse({"error": "No merged file"}, status_code=404)

    mf = Path(event.merged_file)
    if not mf.is_file():
        return JSONResponse({"error": f"File not found: {mf}"}, status_code=404)

    try:
        df = pl.read_parquet(str(mf))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

    # Time axis in ms
    time_ms = (df["t_rel_s"].to_numpy() * 1000).round(6).tolist()

    # Analog channels (ending with _v, excluding baseline variants)
    analog_cols = sorted([
        c for c in df.columns
        if c.endswith("_v") and not c.endswith("_baseline_std")
    ])
    analog = {}
    for col in analog_cols:
        label = col.replace("_v", "")
        vals = df[col].fill_null(0.0).to_numpy().astype(float).round(6).tolist()
        analog[label] = {
            "time": time_ms,
            "value": vals,
        }

    # Digital channels (ending with _d)
    digital_cols = sorted([c for c in df.columns if c.endswith("_d")])
    digital = {}
    for col in digital_cols:
        label = col.replace("_d", "")
        digital[label] = {
            "time": time_ms,
            "value": df[col].fill_null(0).to_numpy().round(0).astype(int).tolist(),
        }

    return {
        "event_id": event_id,
        "time_ms": time_ms,
        "analog": analog,
        "digital": digital,
        "num_points": len(df),
    }


@app.get("/api/stats/histogram")
async def api_stats_histogram(
    year: Optional[str] = Query(None),
    user_beam_time: Optional[str] = Query(None),
    hide_ms: Optional[str] = Query("false"),
):
    """Histogram data: fault type breakdown by user_beam_time.
    
    Always groups by user_beam_time.
    hide_ms=true: filter out MS periods.
    """
    conn = get_sync_connection()
    try:
        conditions = ["user_beam_time != ''"]
        params = []
        if year:
            conditions.append("SUBSTR(timestamp,1,4) = ?")
            params.append(year)
        if user_beam_time:
            if "," in user_beam_time:
                values = [v.strip() for v in user_beam_time.split(",") if v.strip()]
                placeholders = ",".join(["?"] * len(values))
                conditions.append(f"user_beam_time IN ({placeholders})")
                params.extend(values)
            else:
                conditions.append("user_beam_time = ?")
                params.append(user_beam_time)
        if hide_ms == "true":
            conditions.append("(user_beam_time NOT LIKE '%MS%' OR user_beam_time IS NULL OR user_beam_time = '')")
        
        where_clause = "WHERE " + " AND ".join(conditions)
        
        eff = "CASE WHEN user_fault_type != '' THEN user_fault_type ELSE COALESCE(fault_type, 'Unknown') END"
        cursor = conn.execute(
            f"""
            SELECT user_beam_time as period, {eff} as fault_type, COUNT(*) as cnt
            FROM events
            {where_clause}
            GROUP BY user_beam_time, fault_type
            ORDER BY user_beam_time, fault_type
            """,
            params,
        )
        
        # Build matrix: period x fault_type
        rows = cursor.fetchall()
        periods = []
        ft_map = {}
        data_matrix = {}
        
        for r in rows:
            period = r["period"]
            ft = r["fault_type"] or "Unknown"
            cnt = r["cnt"]
            
            # Skip MS periods if hide_ms is true
            if hide_ms == "true" and " MS" in period:
                continue
            
            if period not in periods:
                periods.append(period)
            if ft not in ft_map:
                ft_map[ft] = len(ft_map)
            
            if period not in data_matrix:
                data_matrix[period] = {}
            data_matrix[period][ft] = cnt
        
        fault_types_sorted = sorted(ft_map.keys())
        
        # Sort periods: by beam time number DESC, MS first per number
        def sort_key(p):
            parts = p.split(" ")
            base = parts[0]
            is_ms = len(parts) > 1 and "MS" in parts[1]
            y, n_raw = base.split("-")
            n = "".join(c for c in n_raw if c.isdigit())
            return (int(y), int(n), 0 if is_ms else 1)
        
        periods.sort(key=sort_key)
        
        # Build traces
        traces = []
        for i, ft in enumerate(fault_types_sorted):
            trace = {
                "name": ft,
                "x": periods,
                "y": [data_matrix.get(p, {}).get(ft, 0) for p in periods],
                "type": "bar",
            }
            traces.append(trace)
    finally:
        conn.close()
    
    return {"traces": traces, "periods": periods, "fault_types": fault_types_sorted}


# ── Import / Classify ─────────────────────────────────────────

_import_status: dict = {"running": False, "last_result": None}


@app.post("/api/import")
async def api_import_events():
    """Import parquet files from data/merged/, classify, and store in DB."""
    global _import_status

    if _import_status.get("running"):
        return JSONResponse({"error": "Import already in progress"}, status_code=409)

    # paths.merged_dir은 상대경로("./data/merged")로 되어있음.
    cfg = get_config()
    project_root = Path(__file__).resolve().parent.parent.parent
    # ./data/merged 에서 ./ 제거하고 project_root와 결합
    path_str = str(cfg.paths.merged_dir).lstrip("./").lstrip(".\\")
    merged_dir = (project_root / path_str).resolve()
    config_path = project_root / "config" / "config.yaml"

    if not merged_dir.exists():
        return JSONResponse({"error": f"Merged directory not found: {merged_dir}"}, status_code=404)

    result = run_import(merged_dir, config_path)
    _import_status["last_result"] = result
    return result


@app.get("/api/import/status")
async def api_import_status():
    """Check last import result."""
    return _import_status


# ── Settings / Pipeline Control ───────────────────────────────

from .pipeline_manager import (
    get_pipeline_status,
    run_batch_pipeline,
    run_import_only,
    start_monitor,
    stop_monitor,
    stop_batch_pipeline,
)


@app.get("/settings", response_class=HTMLResponse, include_in_schema=False)
async def settings_page(request: Request, password: Optional[str] = Query(None)):
    """Settings and pipeline control page."""
    authed = password and _check_password(password)
    if not authed:
        return templates.TemplateResponse(request, "settings.html", {"auth_required": True})
    cfg = get_config()
    status = get_pipeline_status()
    config_path = Path(__file__).parent.parent.parent / "config" / "config.yaml"
    config_text = ""
    if config_path.exists():
        config_text = config_path.read_text()
    return templates.TemplateResponse(
        request,
        "settings.html",
        {
            "config": cfg,
            "config_text": config_text,
            "pipeline_status": status,
            "watch_folders": [str(wf) for wf in cfg.paths.watch_folders],
            "watch_folder_str": ", ".join(str(wf) for wf in cfg.paths.watch_folders),
            "email": cfg.email,
            "system": cfg.system,
        },
    )


@app.get("/api/config")
async def api_get_config():
    """Get current configuration as JSON."""
    cfg = get_config()
    config_path = Path(__file__).parent.parent.parent / "config" / "config.yaml"
    config_text = config_path.read_text() if config_path.exists() else ""
    return {
        "yaml": config_text,
        "watch_folders": [str(wf) for wf in cfg.paths.watch_folders],
        "email": {
            "smtp_server": cfg.email.smtp_server,
            "smtp_port": cfg.email.smtp_port,
            "sender_email": cfg.email.sender_email,
            "receiver_emails": cfg.email.receiver_emails,
            "subject_template": cfg.email.subject_template,
        },
        "web": {"host": cfg.web.host, "port": cfg.web.port},
        "system": {"mode": cfg.system.mode, "check_interval": cfg.system.check_interval},
    }


@app.post("/api/config")
async def api_save_config(request: Request):
    """Update config.yaml with new values."""
    pw = request.query_params.get("password", "")
    if not _check_password(pw):
        return JSONResponse({"error": "Authentication required"}, status_code=401)
    try:
        data = await request.json()
        config_path = Path(__file__).parent.parent.parent / "config" / "config.yaml"

        # Base: load from raw_yaml first (if provided), else load from file
        if "raw_yaml" in data:
            cfg = yaml.safe_load(data["raw_yaml"]) or {}
        else:
            if config_path.exists():
                with open(config_path, "r") as f:
                    cfg = yaml.safe_load(f) or {}
            else:
                cfg = {}

        # Apply individual field overrides (on top of raw_yaml or file)
        if "watch_folders" in data:
            cfg.setdefault("paths", {})["watch_folders"] = data["watch_folders"]

        if "email" in data:
            cfg["email"] = cfg.get("email", {})
            for k, v in data["email"].items():
                cfg["email"][k] = v

        if "web" in data:
            cfg["web"] = cfg.get("web", {})
            cfg["web"].update(data["web"])

        if "system" in data:
            cfg["system"] = cfg.get("system", {})
            cfg["system"].update(data["system"])

        # Write
        config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(config_path, "w") as f:
            yaml.safe_dump(cfg, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

        reset_config()
        return {"ok": True, "message": "Config saved"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/pipeline/batch")
async def api_pipeline_batch(request: Request):
    """Run full batch pipeline."""
    pw = request.query_params.get("password", "")
    if not _check_password(pw):
        return JSONResponse({"error": "Authentication required"}, status_code=401)
    data = await request.json() if request.headers.get("content-type") == "application/json" else {}
    input_dirs = data.get("input_dirs")
    config_path = str(Path(__file__).parent.parent.parent / "config" / "config.yaml")
    return run_batch_pipeline(config_path=config_path, input_dirs=input_dirs)


@app.post("/api/pipeline/import")
async def api_pipeline_import(request: Request):
    """Run import only."""
    pw = request.query_params.get("password", "")
    if not _check_password(pw):
        return JSONResponse({"error": "Authentication required"}, status_code=401)
    config_path = str(Path(__file__).parent.parent.parent / "config" / "config.yaml")
    return run_import_only(config_path=config_path)


@app.post("/api/pipeline/monitor/start")
async def api_monitor_start(request: Request):
    """Start folder monitoring."""
    pw = request.query_params.get("password", "")
    if not _check_password(pw):
        return JSONResponse({"error": "Authentication required"}, status_code=401)
    config_path = str(Path(__file__).parent.parent.parent / "config" / "config.yaml")
    return start_monitor(config_path=config_path)


@app.post("/api/pipeline/monitor/stop")
async def api_monitor_stop(request: Request):
    """Stop folder monitoring."""
    pw = request.query_params.get("password", "")
    if not _check_password(pw):
        return JSONResponse({"error": "Authentication required"}, status_code=401)
    return stop_monitor()


@app.get("/api/pipeline/status")
async def api_pipeline_status():
    """Get pipeline status."""
    return get_pipeline_status()


@app.post("/api/pipeline/stop")
async def api_pipeline_stop(request: Request):
    """Force stop batch or monitor pipeline."""
    pw = request.query_params.get("password", "")
    if not _check_password(pw):
        return JSONResponse({"error": "Authentication required"}, status_code=401)
    
    stop_monitor()
    stop_batch_pipeline()
    
    return {"ok": True, "message": "Pipeline stop requested"}


@app.post("/api/db/backup")
async def api_db_backup(request: Request):
    pw = request.query_params.get("password", "")
    if not _check_password(pw):
        return JSONResponse({"error": "Auth"}, status_code=401)
    
    cfg = get_config()
    from src.orchestrator import SRFOrchestrator
    orch = SRFOrchestrator(cfg)
    path = orch.backup_database()
    return {"ok": True, "path": path}


@app.get("/api/db/backups")
async def api_list_backups(request: Request):
    """List available backup files."""
    pw = request.query_params.get("password", "")
    if not _check_password(pw):
        return JSONResponse({"error": "Auth"}, status_code=401)
    
    cfg = get_config()
    from pathlib import Path
    db_path = Path(cfg.db.path)
    backup_dir = db_path.parent / "backups"
    backups = []
    if backup_dir.exists():
        for f in sorted(backup_dir.glob("*.tar.gz"), reverse=True):
            stat = f.stat()
            backups.append({
                "filename": f.name,
                "path": str(f),
                "size": stat.st_size,
                "modified": stat.st_mtime,
            })
    return {"ok": True, "backups": backups}


@app.post("/api/db/restore")
async def api_db_restore(request: Request):
    """Restore database from a backup file."""
    pw = request.query_params.get("password", "")
    if not _check_password(pw):
        return JSONResponse({"error": "Auth"}, status_code=401)
    
    data = await request.json()
    backup_file = data.get("backup_file", "")
    if not backup_file:
        return JSONResponse({"error": "No backup_file specified"}, status_code=400)
    
    cfg = get_config()
    from src.orchestrator import SRFOrchestrator
    orch = SRFOrchestrator(cfg)
    result = orch.restore_database(backup_file)
    return result

@app.post("/api/pipeline/reset")
async def api_pipeline_reset(request: Request):
    """Reload config from file."""
    pw = request.query_params.get("password", "")
    if not _check_password(pw):
        return JSONResponse({"error": "Authentication required"}, status_code=401)
    try:
        reset_config()
        return {"ok": True, "message": "Config reloaded"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── CLI entry point ───────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    from ..core.config import get_config

    cfg = get_config()
    uvicorn.run(app, host=cfg.web.host, port=cfg.web.port)
