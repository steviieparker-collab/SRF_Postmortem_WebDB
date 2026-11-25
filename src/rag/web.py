"""FastAPI routes for the RAG chat page."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from .answer import answer_question
from .config import get_rag_config
from .index import stats as index_stats
from .tools import get_event_context

SUGGESTIONS = [
    "2025-11-03 KSU3 유량 부족 이벤트의 원인과 알람 PV는?",
    "KSU3 ion pump current too high 이력을 연도별로 보여줘.",
    "2024년 CM1 quench 이벤트 목록을 보여줘.",
    "CM2 quench와 유사한 사례를 찾아줘.",
    "KSU3 water flow low 알람에 매핑된 graph PV는?",
    "현재 알람 모니터 상태와 마지막 heartbeat는?",
    "2026-10-25 HEX 온도 인터락의 PV와 임계값은?",
    "2023-12-10 KSU1 water flow sensor error 조치 내용은?",
]


def build_router(templates) -> APIRouter:
    router = APIRouter()

    @router.get("/chat", response_class=HTMLResponse)
    async def chat_page(request: Request):
        return templates.TemplateResponse(
            request,
            "chat.html",
            {
                "rag_status": index_stats(),
                "model": get_rag_config().llm_model,
            },
        )

    @router.post("/api/chat")
    def api_chat(payload: dict[str, Any]):
        message = str(payload.get("message") or "").strip()
        filters = payload.get("filters") or {}
        if not isinstance(filters, dict):
            filters = {}
        if not message:
            return JSONResponse(
                {"answer": "질문이 비어 있습니다.", "sources": [], "intent": "none"},
                status_code=400,
            )
        return JSONResponse(answer_question(message, filters=filters))

    @router.get("/api/rag/status")
    def api_rag_status():
        return JSONResponse(index_stats())

    @router.get("/api/chat/suggestions")
    def api_suggestions():
        return JSONResponse({"suggestions": SUGGESTIONS})

    @router.get("/api/rag/event/{event_id}/context")
    def api_event_context(event_id: str):
        return JSONResponse(get_event_context(event_id))

    return router
