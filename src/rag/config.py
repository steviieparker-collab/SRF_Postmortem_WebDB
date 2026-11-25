"""Configuration for the SRF Postmortem RAG assistant.

Secrets are read from environment variables or a git-ignored ``.env`` file.
No key is ever written to the repository or logged.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_MODEL = "deepseek/deepseek-v4.1-flash-1017"


def _load_env_file(path: Path) -> None:
    """Minimal .env loader (no external dependency).

    Existing environment variables always win; the first file loaded wins
    over later fallback files.
    """
    if not path.exists():
        return
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value


# Repo-local .env first, then the telegram-bot .env as a fallback for
# OPENROUTER_TOKEN (kept outside the repo).
_load_env_file(PROJECT_ROOT / ".env")
_load_env_file(Path.home() / "claude" / "telegram-bot" / ".env")


def _as_bool(value: Optional[str], default: bool = True) -> bool:
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def _as_float(value: Optional[str], default: float) -> float:
    try:
        return float(value) if value is not None else default
    except ValueError:
        return default


def _as_int(value: Optional[str], default: int) -> int:
    try:
        return int(value) if value is not None else default
    except ValueError:
        return default


@dataclass(frozen=True)
class RagConfig:
    enabled: bool
    db_path: Path
    index_path: Path
    llm_base_url: str
    llm_model: str
    api_key: str
    temperature: float
    max_tokens: int
    top_k: int
    http_referer: str
    app_title: str
    request_timeout: int
    reasoning_effort: str


@lru_cache(maxsize=1)
def get_rag_config() -> RagConfig:
    db_path = Path(os.getenv("RAG_DB_PATH", str(PROJECT_ROOT / "db" / "events.db")))
    index_path = Path(
        os.getenv("RAG_INDEX_PATH", str(PROJECT_ROOT / "data" / "rag" / "rag.sqlite3"))
    )
    return RagConfig(
        enabled=_as_bool(os.getenv("RAG_ENABLED"), True),
        db_path=db_path,
        index_path=index_path,
        llm_base_url=os.getenv(
            "OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"
        ).rstrip("/"),
        llm_model=os.getenv("OPENROUTER_MODEL", DEFAULT_MODEL).strip(),
        api_key=(
            os.getenv("OPENROUTER_API_KEY")
            or os.getenv("OPENROUTER_TOKEN")
            or ""
        ).strip(),
        temperature=_as_float(os.getenv("RAG_TEMPERATURE"), 0.0),
        max_tokens=_as_int(os.getenv("RAG_MAX_TOKENS"), 1500),
        top_k=_as_int(os.getenv("RAG_TOP_K"), 8),
        http_referer=os.getenv("RAG_HTTP_REFERER", "").strip(),
        app_title=os.getenv("RAG_APP_TITLE", "SRF Postmortem Assistant").strip(),
        request_timeout=_as_int(os.getenv("RAG_REQUEST_TIMEOUT"), 120),
        reasoning_effort=os.getenv("RAG_REASONING_EFFORT", "low").strip(),
    )
