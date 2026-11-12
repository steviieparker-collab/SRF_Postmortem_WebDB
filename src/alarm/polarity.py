"""Evaluate CSS OPI alarm-rule expressions.

The OPI rules use a tiny expression language such as ``pv0>=-1 && pv0<0``.
We parse the same forms here instead of using eval() so the monitor can
classify raw CA values safely and deterministically.
"""

from __future__ import annotations

import json
import math
import re
from typing import Any, Optional

_COND_RE = re.compile(
    r"pv0\s*(==|!=|>=|<=|>|<)\s*(-?\d+(?:\.\d+)?)"
)


def _json_list(raw: Any) -> list[str]:
    if not raw:
        return []
    if isinstance(raw, (list, tuple)):
        return [str(x) for x in raw]
    try:
        val = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    if isinstance(val, list):
        return [str(x) for x in val]
    if isinstance(val, str):
        return [val]
    return []


def eval_expression(expr: str, value: float) -> Optional[bool]:
    """Return True/False for a parsed expression, or None if unparseable."""
    parts = [p.strip() for p in (expr or "").split("&&") if p.strip()]
    if not parts:
        return None
    for part in parts:
        m = _COND_RE.fullmatch(part)
        if not m:
            return None
        op, num = m.group(1), float(m.group(2))
        if op == "==":
            ok = value == num
        elif op == "!=":
            ok = value != num
        elif op == ">=":
            ok = value >= num
        elif op == "<=":
            ok = value <= num
        elif op == ">":
            ok = value > num
        elif op == "<":
            ok = value < num
        else:
            return None
        if not ok:
            return False
    return True


def classify(row: dict[str, Any], value: Any) -> str:
    """Classify a raw CA value as alarm / normal / complex / unknown."""
    mode = (row.get("state_mode") or "").strip()
    if mode == "complex":
        return "complex"

    try:
        v = float(value)
    except (TypeError, ValueError):
        return "unknown"
    if not math.isfinite(v):
        return "unknown"

    for expr in _json_list(row.get("alarm_expr")):
        if eval_expression(expr, v) is True:
            return "alarm"
    for expr in _json_list(row.get("normal_expr")):
        if eval_expression(expr, v) is True:
            return "normal"

    default = (row.get("default_state") or "").strip()
    if default in ("alarm", "normal"):
        return default
    return "unknown"
