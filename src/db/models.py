"""
Pydantic models for SRF Event DB entities.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class FaultType(BaseModel):
    name: str
    description: str = ""
    typical_pattern: Optional[Dict[str, Any]] = None
    severity: str = "medium"
    event_count: int = 0
    created_at: str = ""


class EventCreate(BaseModel):
    id: str
    timestamp: str
    scope1_file: Optional[str] = None
    scope2_file: Optional[str] = None
    scope3_file: Optional[str] = None
    merged_file: Optional[str] = None
    fault_type: Optional[str] = None
    fault_confidence: float = 0.0
    beam_voltage: Optional[float] = None
    beam_current: Optional[float] = None
    analog_metrics: Optional[Dict[str, Any]] = None
    digital_pattern: Optional[Dict[str, Any]] = None
    time_groups: Optional[Dict[str, Any]] = None
    graphs_path: Optional[str] = None
    report_path: Optional[str] = None
    report_md: Optional[str] = None
    case_id: int = 0
    case_description: str = ""
    case_fault: str = ""
    user_beam_time: str = ""
    notes: str = ""
    user_fault_type: str = ""


class Event(BaseModel):
    id: str
    timestamp: str
    scope1_file: Optional[str] = None
    scope2_file: Optional[str] = None
    scope3_file: Optional[str] = None
    merged_file: Optional[str] = None
    fault_type: Optional[str] = None
    fault_confidence: float = 0.0
    beam_voltage: Optional[float] = None
    beam_current: Optional[float] = None
    analog_metrics: Optional[Dict[str, Any]] = None
    digital_pattern: Optional[Dict[str, Any]] = None
    time_groups: Optional[Dict[str, Any]] = None
    graphs_path: Optional[str] = None
    report_path: Optional[str] = None
    report_md: Optional[str] = None
    case_id: int = 0
    case_description: str = ""
    case_fault: str = ""
    user_beam_time: str = ""
    notes: str = ""
    user_fault_type: str = ""
    created_at: str = ""


class EventLink(BaseModel):
    id: int = 0
    event_id: str
    related_event_id: str
    similarity_score: float = 0.0
    created_at: str = ""


class EventAttachment(BaseModel):
    id: int = 0
    event_id: str
    original_name: str
    stored_name: str
    mime_type: Optional[str] = None
    file_size: Optional[int] = None
    uploaded_at: str = ""


class EventSummary(BaseModel):
    """Lightweight event model for list views."""
    id: str
    timestamp: str
    fault_type: Optional[str] = None
    fault_confidence: float = 0.0
    beam_voltage: Optional[float] = None
    beam_current: Optional[float] = None
    case_id: int = 0
    case_description: str = ""
    user_beam_time: str = ""
    notes: str = ""
    user_fault_type: str = ""
    created_at: str = ""


class SimilarEvent(BaseModel):
    event: EventSummary
    similarity_score: float


class EventListResponse(BaseModel):
    events: List[EventSummary]
    total: int
    page: int
    page_size: int
    total_pages: int
