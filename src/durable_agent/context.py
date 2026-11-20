"""What an activity sees while it runs: identity, idempotency key, resources and the outbox."""

from __future__ import annotations

import json
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class OutboxMessage:
    """A side effect to deliver after the step commits."""

    key: str
    topic: str
    payload: dict[str, Any]


class Outbox:
    """Buffers an attempt's outgoing messages.

    Messages are written to the ``outbox`` table in the same SQLite transaction as the step's
    ``StepCompleted`` event, so a message exists if and only if the step result was recorded.
    Messages from failed, timed-out or fenced-off attempts are dropped with the attempt.
    """

    def __init__(self, base_key: str) -> None:
        self._base_key = base_key
        self.messages: list[OutboxMessage] = []

    def send(self, topic: str, payload: Mapping[str, Any]) -> str:
        """Queue ``payload`` for ``topic``; returns the message's idempotency key (stable across retries)."""
        key = f"{self._base_key}/{len(self.messages)}"
        body = json.loads(json.dumps(dict(payload)))  # copy, and fail now if it is not JSON-serialisable
        self.messages.append(OutboxMessage(key=key, topic=topic, payload=body))
        return key


@dataclass
class ActivityContext:
    """Passed to every activity call."""

    run_id: str
    step: str
    attempt: int
    worker_id: str
    resources: Mapping[str, Any]
    outbox: Outbox
    cancelled: threading.Event = field(default_factory=threading.Event)

    @property
    def idempotency_key(self) -> str:
        """``"<run>/<state>#<visit>"``: identical for every attempt and every worker that runs this step."""
        return f"{self.run_id}/{self.step}"

    def resource(self, name: str) -> Any:
        """A shared resource registered on the engine (an LLM client, a tool, a sink)."""
        try:
            return self.resources[name]
        except KeyError:
            raise KeyError(f"resource {name!r} is not registered on the engine") from None
