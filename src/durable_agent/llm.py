"""Language-model access: a small provider protocol and an OpenRouter client.

The OpenRouter client tries each configured model once (a busy model is skipped rather than
retried, which saves the free-tier quota), caches successful replies on disk by request hash and
validates structured replies against a Pydantic model with one repair round-trip.

Inside a workflow, model calls are *activities*: their output is recorded in the journal, so a
resumed run never calls the model again for a step that already completed. Retries across whole
failures (every model busy) are the activity's retry policy's job.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from pathlib import Path
from typing import Any, Protocol, TypeVar

from pydantic import BaseModel, ValidationError

from .config import Settings, load_settings
from .errors import NonRetryableError

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
DEFAULT_CACHE_DIR = Path(".cache") / "llm"
T = TypeVar("T", bound=BaseModel)
Message = dict[str, str]

_THINK = re.compile(r"<think>.*?</think>", re.S | re.I)
_FENCE = re.compile(r"```(?:json|markdown|md)?\s*(.*?)```", re.S | re.I)


class LLMError(RuntimeError):
    """No configured model produced a usable answer (retryable by the activity's policy)."""


class LLMAuthError(NonRetryableError):
    """The provider rejected the key: retrying cannot help."""


class LLM(Protocol):
    """What the agent needs from a model."""

    @property
    def name(self) -> str: ...

    def complete(self, messages: list[Message]) -> str: ...

    def complete_json(self, messages: list[Message], schema: type[T]) -> T: ...


def extract_json(text: str) -> dict[str, Any]:
    """Pull the first JSON object out of a reply (handles code fences, prose and reasoning tags)."""
    text = _THINK.sub("", text or "").strip()
    fenced = _FENCE.search(text)
    if fenced:
        text = fenced.group(1).strip()
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        start, end = text.find("{"), text.rfind("}")
        if start == -1 or end <= start:
            raise ValueError("no JSON object in the model reply") from None
        value = json.loads(text[start : end + 1])
    if not isinstance(value, dict):
        raise ValueError("the model reply is JSON but not an object")
    return value


class OpenRouterLLM:
    """OpenRouter's OpenAI-compatible API with model fallback, a disk cache and JSON repair."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        cache_dir: Path | None = DEFAULT_CACHE_DIR,
        timeout: float = 90.0,
        app_name: str = "durable-agent-state-machine",
    ) -> None:
        self.settings = settings or load_settings()
        if not self.settings.api_key:
            raise LLMAuthError("OPENROUTER_API_KEY is not set")
        from openai import OpenAI  # imported lazily: offline use never needs the SDK

        self._client = OpenAI(
            base_url=OPENROUTER_BASE_URL,
            api_key=self.settings.api_key,
            default_headers={"X-Title": app_name},
            timeout=timeout,
            max_retries=0,
        )
        self.cache_dir = cache_dir
        self.last_model: str | None = None
        self.last_errors: list[str] = []

    @property
    def name(self) -> str:
        return f"openrouter:{self.last_model or self.settings.models[0]}"

    def _cache_file(self, messages: list[Message], json_mode: bool) -> Path | None:
        if self.cache_dir is None:
            return None
        key = {"models": list(self.settings.models), "messages": messages, "json": json_mode}
        digest = hashlib.sha256(json.dumps(key, sort_keys=True).encode("utf-8")).hexdigest()[:40]
        return self.cache_dir / f"{digest}.json"

    def complete(self, messages: list[Message], *, json_mode: bool = False, max_tokens: int = 2500) -> str:
        path = self._cache_file(messages, json_mode)
        if path is not None and path.exists():
            cached = json.loads(path.read_text(encoding="utf-8"))
            self.last_model = cached.get("model")
            return str(cached["content"])
        content, model = self._call_with_fallback(messages, json_mode, max_tokens)
        self.last_model = model
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps({"model": model, "content": content}), encoding="utf-8")
        return content

    def _forget(self, messages: list[Message], json_mode: bool) -> None:
        path = self._cache_file(messages, json_mode)
        if path is not None and path.exists():
            path.unlink()

    def _call_with_fallback(self, messages: list[Message], json_mode: bool, max_tokens: int) -> tuple[str, str]:
        from openai import APIConnectionError, APIStatusError, RateLimitError

        self.last_errors = []
        retryable: list[str] = []
        for round_ in range(2):
            candidates = list(self.settings.models) if round_ == 0 else retryable
            if round_ == 1:
                if not retryable:
                    break
                time.sleep(5)
            retryable = []
            for model in candidates:
                use_json_format = json_mode
                for _ in range(2):  # the second pass only when JSON mode itself was rejected
                    kwargs: dict[str, Any] = {
                        "model": model,
                        "messages": messages,
                        "temperature": 0.2,
                        "max_tokens": max_tokens,
                    }
                    if use_json_format:
                        kwargs["response_format"] = {"type": "json_object"}
                    try:
                        response = self._client.chat.completions.create(**kwargs)
                    except RateLimitError as exc:
                        self.last_errors.append(f"{model}: busy (HTTP 429)")
                        if "per-day" not in str(exc) and "per day" not in str(exc):
                            retryable.append(model)
                        break
                    except APIConnectionError as exc:  # includes timeouts
                        self.last_errors.append(f"{model}: {type(exc).__name__}")
                        retryable.append(model)
                        break
                    except APIStatusError as exc:
                        if exc.status_code == 401:
                            raise LLMAuthError("OpenRouter rejected the API key (HTTP 401)") from exc
                        self.last_errors.append(f"{model}: HTTP {exc.status_code}")
                        if exc.status_code == 400 and use_json_format:
                            use_json_format = False
                            continue
                        if exc.status_code >= 500:
                            retryable.append(model)
                        break
                    choices = getattr(response, "choices", None) or []
                    choice = choices[0] if choices else None
                    content = ((choice.message.content if choice else None) or "").strip()
                    if json_mode and getattr(choice, "finish_reason", None) == "length":
                        self.last_errors.append(f"{model}: reply cut off at the token limit")
                        break
                    if content:
                        return _THINK.sub("", content).strip(), model
                    self.last_errors.append(f"{model}: empty reply")
                    break
        raise LLMError("no model returned an answer: " + "; ".join(self.last_errors[-6:]))

    def complete_json(self, messages: list[Message], schema: type[T]) -> T:
        """Ask for JSON matching ``schema``; one repair round-trip when the first reply is invalid."""
        hint = json.dumps(schema.model_json_schema(), separators=(",", ":"))
        system = {
            "role": "system",
            "content": "Reply with one JSON object only, no prose and no code fences. "
            f"It must validate against this JSON Schema: {hint}",
        }
        request = [system, *messages]
        raw = self.complete(request, json_mode=True)
        try:
            return schema.model_validate(extract_json(raw))
        except (ValueError, ValidationError) as first_error:
            self._forget(request, json_mode=True)
            repair = [
                *request,
                {"role": "assistant", "content": raw[:4000]},
                {"role": "user", "content": f"That reply could not be used ({str(first_error)[:400]}). "
                 "Return the corrected JSON object only."},
            ]
            raw = self.complete(repair, json_mode=True)
            try:
                return schema.model_validate(extract_json(raw))
            except (ValueError, ValidationError) as exc:
                self._forget(repair, json_mode=True)
                raise LLMError(f"the model's reply did not match {schema.__name__}: {exc}") from exc
