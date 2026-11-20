from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from hypothesis.configuration import set_hypothesis_home_dir

# Hypothesis caches examples and source constants (with absolute paths); keep them out of the repository.
set_hypothesis_home_dir(Path(tempfile.gettempdir()) / "durable-agent-hypothesis")


@pytest.fixture(autouse=True)
def _offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tests never call a real model, even when a key is set in the environment or a .env file."""
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    monkeypatch.setenv("DURABLE_AGENT_OFFLINE", "1")
