"""Deterministic query planning for the RAG assistant.

The planner extracts explicit filters and a coarse intent.  It intentionally
does not ask the LLM to choose SQL: exact filters and aggregates are handled
by the backend, which is more reliable and easier to audit.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional

EVENT_ID_RE = re.compile(r"\b(20\d{6}_\d{6})\b")
DATE_RE = re.compile(r"\b(20\d{2})-(\d{2})-(\d{2})\b")
PV_RE = re.compile(r"\b(SR:[A-Z0-9]+:[A-Z0-9_]+)\b")
YEAR_RE = re.compile(r"(20\d{2})")
YEAR_RANGE_RE = re.compile(r"(20\d{2})\s*[~\-–]\s*(20\d{2})")
BEAM_TIME_RE = re.compile(r"\b(20\d{2})\s*[- ]?\s*(\d{1,2})\s*차\b")
SUBSYSTEM_RE = re.compile(
    r"\b(KSU\s*#?\s*[123]|CM\s*#?\s*[123]|CRM\s*#?\s*[123]|CRG|MPS|VAC|LLRF|Tuner|Magnet|Kicker)\b",
    re.IGNORECASE,
)
# "KSU 관련", "CM 관련"처럼 번호 없이 쓰는 경우도 필터로 인식한다.
BARE_SUBSYSTEM_RE = re.compile(
    r"\b(KSU|CRM|CM|CRG|MPS|VAC|LLRF|Tuner|Magnet|Kicker)\b",
    re.IGNORECASE,
)
CASE_RE = re.compile(r"\bcase\s*(\d{1,2})\b", re.IGNORECASE)


def _normalize_subsystem(value: str) -> str:
    v = re.sub(r"\s+", "", value).upper().replace("#", "")
    if v.startswith("CM") and not v.startswith("CRM"):
        v = "CRM" + v[2:]
    return v


def extract_event_id(text: str) -> Optional[str]:
    m = EVENT_ID_RE.search(text)
    return m.group(1) if m else None


def extract_date(text: str) -> Optional[str]:
    m = DATE_RE.search(text)
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None


def extract_pv(text: str) -> Optional[str]:
    m = PV_RE.search(text)
    return m.group(1) if m else None


def extract_subsystem(text: str) -> Optional[str]:
    # "KSU관련" -> "KSU 관련" so the word boundary matches.
    text = re.sub(r"([A-Za-z0-9])([가-힣])", r"\1 \2", text)
    text = re.sub(r"([가-힣])([A-Za-z0-9])", r"\1 \2", text)
    m = SUBSYSTEM_RE.search(text)
    if m:
        return _normalize_subsystem(m.group(1))
    m = BARE_SUBSYSTEM_RE.search(text)
    if m:
        value = m.group(1).upper()
        if value in {"CM", "CRM"}:
            return "CRM"
        return value
    return None


def extract_years(text: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    text = EVENT_ID_RE.sub(" ", text)
    m = YEAR_RANGE_RE.search(text)
    if m:
        a, b = sorted([int(m.group(1)), int(m.group(2))])
        out["year_from"] = a
        out["year_to"] = b
        return out
    m = YEAR_RE.search(text)
    if m:
        out["year"] = int(m.group(1))
    return out


def extract_beam_time(text: str) -> Optional[str]:
    m = BEAM_TIME_RE.search(text)
    if not m:
        return None
    return f"{m.group(1)}-{int(m.group(2))}차"


def extract_fault_type(text: str) -> Optional[str]:
    m = CASE_RE.search(text)
    if m:
        return f"Case {int(m.group(1))}"
    # Korean spellings appear in user questions (인터락/인터록), while the DB
    # fault_type column uses English names such as RF_Interlock.
    synonyms = (
        ("RF_Interlock", ("rf_interlock", "rf interlock", "rf인터락", "rf 인터락",
                          "rf인터록", "rf 인터록")),
        ("Beam_loss", ("beam_loss", "beam loss", "빔로스", "빔 로스")),
        ("Cavity_quench", ("cavity_quench", "cavity quench", "캐비티 켄치",
                           "캐비티 퀜치", "켄치", "퀜치")),
    )
    lower = text.lower()
    compact = lower.replace(" ", "")
    for name, variants in synonyms:
        for variant in variants:
            if variant.replace(" ", "") in compact:
                return name
    for name in (
        "RF_Interlock", "RF_interlock", "Beam_loss", "Cavity_quench",
        "Cavity_blip", "Cavity_detune", "RF_source_fault", "MIS",
    ):
        if name.lower() in lower:
            return name.replace("RF_interlock", "RF_Interlock")
    return None


QUESTION_WORDS = (
    "이력을", "이력", "연도별로", "연도별", "보여줘", "알려줘", "몇 건", "건수",
    "추세", "분포", "비율", "가장 많은", "통계", "집계", "목록", "사례",
    "유사한", "비슷한", "관련", "대해", "대한", "원인", "조치", "내용",
    "요약", "정리", "있는", "있나", "인가요", "인가", "무엇인가요", "무엇",
    "어떤", "어떻게", "뭐야", "뭐지", "뭔가요", "뭔지",
    "그리고", "해줘", "줘", "이벤트", "인터록", "인터락", "알람", "그래프",
    "최근에", "최근", "최신", "요즘", "근래",
    "많이", "발생한", "발생", "많은", "가장", "달은", "달별", "월은", "월별",
)
# "최근 빔덤프"처럼 시점만 말하는 질문은 텍스트 검색어가 아니라
# 최신순 정렬 의도로 해석해야 한다. extract_topic()이 이 단어들을 지우고,
# plan_query()가 sort=recent 노트를 남긴다.
TEMPORAL_WORDS = ("최근", "최신", "요즘", "근래")
GENERIC_TOPIC_TOKENS = {
    "빔덤프", "빔", "덤프", "beam", "dump", "이벤트", "event", "관련",
    "대한", "대해", "목록", "사례", "알람", "인터록", "인터락", "그래프", "pv",
    "graph", "rf", "알려줘", "보여줘", "이력", "추세", "통계", "집계",
}


def extract_topic(text: str) -> str:
    """Remove question boilerplate and filters, leaving a search topic."""
    t = text
    for pattern in (EVENT_ID_RE, DATE_RE, PV_RE, YEAR_RANGE_RE, YEAR_RE,
                    BEAM_TIME_RE, SUBSYSTEM_RE, CASE_RE):
        t = pattern.sub(" ", t)
    for word in QUESTION_WORDS:
        t = t.replace(word, " ")
    t = re.sub(r"[?？.,!(){}\[\]:;'\"]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    particles = (
        "으로는", "에서", "으로", "과의", "은", "는", "이", "가", "을", "를",
        "의", "도", "만", "에", "로", "과", "와",
    )

    def strip_particle(word: str) -> str:
        for p in particles:
            if word.endswith(p) and len(word) > len(p) + 1:
                return word[: -len(p)]
        return word

    tokens = [strip_particle(w) for w in t.split() if len(w) >= 2]
    tokens = [
        w for w in tokens
        if len(w) >= 2 and w.lower() not in GENERIC_TOPIC_TOKENS
    ]
    return " ".join(tokens)


@dataclass
class QueryPlan:
    question: str
    intent: str
    filters: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


STATUS_WORDS = ("현재 알람", "지금 알람", "알람 상태", "모니터", "heartbeat", "last heartbeat", "연결 상태")
AGG_WORDS = (
    "몇 건", "건수", "총 몇", "추세", "연도별", "분포", "비율",
    "가장 많은", "가장 많이", "많이 발생", "많은 달", "월별", "달별",
    "통계", "집계", "count",
)
SIMILAR_WORDS = ("유사", "비슷", "사례", "같은", "similar")
LIST_WORDS = ("목록", "리스트", "list")
ALARM_PV_WORDS = ("알람", "pv", "그래프", "인터록", "interlock", "graph", "매핑", "카탈로그")


def plan_query(question: str) -> QueryPlan:
    q = question.strip()
    filters: dict[str, Any] = {}
    notes: list[str] = []

    event_id = extract_event_id(q)
    if event_id:
        filters["event_id"] = event_id
    date = extract_date(q)
    if date:
        filters["date"] = date
    pv = extract_pv(q)
    if pv:
        filters["pv"] = pv
    subsystem = extract_subsystem(q)
    if subsystem:
        filters["subsystem"] = subsystem
    filters.update(extract_years(q))
    beam_time = extract_beam_time(q)
    if beam_time:
        filters["beam_time"] = beam_time
    fault_type = extract_fault_type(q)
    if fault_type:
        filters["fault_type"] = fault_type

    lower = q.lower()
    if any(w in lower for w in TEMPORAL_WORDS):
        notes.append("sort=recent")
    if any(w in lower for w in STATUS_WORDS):
        intent = "alarm_status"
    elif any(w in lower for w in AGG_WORDS):
        intent = "aggregate"
    elif any(w in lower for w in SIMILAR_WORDS):
        intent = "similar"
    elif any(w in lower for w in LIST_WORDS):
        intent = "search"
    elif event_id:
        intent = "event_detail"
    elif any(w in lower for w in ALARM_PV_WORDS):
        intent = "alarm_pv"
    elif "이벤트" in q:
        intent = "event_detail"
    else:
        intent = "search"

    if intent == "aggregate":
        if "fault" in lower or "케이스" in q or "case" in lower:
            notes.append("group_by=fault_type")
        elif "beam" in lower or "빔타임" in q or "회차" in q:
            notes.append("group_by=beam_time")
        elif "월별" in q or "달별" in q or "month" in lower or "달" in q:
            notes.append("group_by=month")
        else:
            notes.append("group_by=year")

    topic = extract_topic(q)
    generic_topics = {
        "fault type", "fault", "case", "빔덤프", "빔 덤프", "beam dump",
        "event", "이벤트", "사례",
    }
    if (
        topic
        and topic.lower() not in generic_topics
        and (intent in {"aggregate", "search"} or (intent == "event_detail" and not event_id))
    ):
        filters["text"] = topic

    return QueryPlan(question=q, intent=intent, filters=filters, notes=notes)
