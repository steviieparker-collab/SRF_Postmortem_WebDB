"""OpenRouter client for the RAG assistant.

Only the configured model is used; the API key is never logged.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from .config import get_rag_config


class LLMError(RuntimeError):
    pass


def _post(payload: dict[str, Any]) -> dict[str, Any]:
    cfg = get_rag_config()
    if not cfg.api_key:
        raise LLMError(
            "OPENROUTER_API_KEY is not configured. "
            "Add it to the git-ignored .env file or the process environment."
        )
    url = f"{cfg.llm_base_url}/chat/completions"
    headers = {
        "Authorization": f"Bearer {cfg.api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if cfg.http_referer:
        headers["HTTP-Referer"] = cfg.http_referer
    if cfg.app_title:
        headers["X-Title"] = cfg.app_title
    req = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=cfg.request_timeout) as r:
            raw = r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:500]
        raise LLMError(f"OpenRouter HTTP {e.code}: {body}") from e
    except urllib.error.URLError as e:
        raise LLMError(f"OpenRouter connection error: {e.reason}") from e
    except TimeoutError as e:
        raise LLMError("OpenRouter request timed out") from e
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise LLMError(f"OpenRouter returned invalid JSON: {raw[:300]}") from e


def _post_with_policy_retry(payload: dict[str, Any]) -> dict[str, Any]:
    try:
        return _post(payload)
    except LLMError as e:
        # Some OpenRouter providers/models reject the provider policy block.
        # Retry once without it rather than failing the whole answer.
        if "HTTP 400" not in str(e):
            raise
        payload.pop("provider", None)
        return _post(payload)


def _extract_content(data: dict[str, Any]) -> str:
    choices = data.get("choices") or []
    if not choices:
        raise LLMError(f"OpenRouter returned no choices: {str(data)[:300]}")
    message = choices[0].get("message") or {}
    content = message.get("content")
    if isinstance(content, list):
        content = "".join(
            part.get("text", "") for part in content if isinstance(part, dict)
        )
    return str(content or "").strip()


def chat(messages: list[dict[str, str]], max_tokens: int | None = None,
         temperature: float | None = None) -> str:
    cfg = get_rag_config()
    payload: dict[str, Any] = {
        "model": cfg.llm_model,
        "messages": messages,
        "temperature": cfg.temperature if temperature is None else temperature,
        "max_tokens": cfg.max_tokens if max_tokens is None else max_tokens,
        "provider": {"data_collection": "deny", "allow_fallbacks": False},
    }
    if cfg.reasoning_effort:
        payload["reasoning_effort"] = cfg.reasoning_effort

    data = _post_with_policy_retry(payload)
    content = _extract_content(data)
    if content:
        return content

    # DeepSeek Flash can occasionally spend the whole completion budget on
    # reasoning and return an empty content field. Retry once without the
    # reasoning hint and with a larger budget.
    payload.pop("reasoning_effort", None)
    payload["max_tokens"] = max(int(payload.get("max_tokens") or 0), 2000)
    data = _post_with_policy_retry(payload)
    content = _extract_content(data)
    if not content:
        raise LLMError("OpenRouter returned an empty answer")
    return content
