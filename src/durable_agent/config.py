"""Runtime settings: the optional OpenRouter key and model list, read from env vars or a ``.env`` file."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

# Free OpenRouter models, tried in order. Override with OPENROUTER_MODELS (comma-separated).
DEFAULT_MODELS: tuple[str, ...] = (
    "nvidia/nemotron-3.5-super-vl:free",
    "google/gemma-4-26b-a4b-it:free",
    "inclusionai/ling-3.2-flash:free",
    "google/gemma-4-31b-it:free",
)
OFFLINE_ENV = "DURABLE_AGENT_OFFLINE"


@dataclass(frozen=True)
class Settings:
    api_key: str | None = None
    models: tuple[str, ...] = DEFAULT_MODELS

    @property
    def live(self) -> bool:
        """True when a key is configured and offline mode is not forced."""
        return bool(self.api_key) and not offline_forced()


def offline_forced() -> bool:
    """``DURABLE_AGENT_OFFLINE=1`` forces the offline stand-in even when a key is set."""
    return os.getenv(OFFLINE_ENV, "").strip().lower() in {"1", "true", "yes"}


def load_settings() -> Settings:
    load_dotenv(override=False)
    key = (os.getenv("OPENROUTER_API_KEY") or "").strip()
    raw_models = os.getenv("OPENROUTER_MODELS") or ""
    models = tuple(m.strip() for m in raw_models.split(",") if m.strip()) or DEFAULT_MODELS
    return Settings(api_key=key or None, models=models)
