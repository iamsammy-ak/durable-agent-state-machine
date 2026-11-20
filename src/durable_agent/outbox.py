"""Transactional outbox relay: delivers committed side effects to their sinks, at least once.

A step never performs a side effect directly. It queues an outbox message, which commits in
the same transaction as the step result. The relay then delivers it and marks it delivered. A
crash between "delivered" and "marked" makes the message due again after its claim expires, so
the sink can see the same key twice: sinks must deduplicate by key (an *idempotent receiver*).
Together that gives effectively-once effects.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from .crash import CrashInjector, crash_hit
from .retry import RetryPolicy, stable_rng
from .store import Store

DELIVERY_RETRY = RetryPolicy(max_attempts=8, initial_interval=0.2, max_interval=30.0)


class Sink(Protocol):
    """Receives outbox messages for one topic."""

    def deliver(self, key: str, payload: dict[str, Any]) -> None:
        """Apply the side effect. Must be idempotent by ``key``: the same message can arrive twice."""
        ...


class OutboxRelay:
    """Claims due messages, hands them to their sinks and records the outcome."""

    def __init__(
        self,
        store: Store,
        sinks: Mapping[str, Sink],
        worker_id: str,
        *,
        claim_ttl: float = 10.0,
        retry: RetryPolicy = DELIVERY_RETRY,
        crash: CrashInjector | None = None,
    ) -> None:
        self.store = store
        self.sinks = sinks
        self.worker_id = worker_id
        self.claim_ttl = claim_ttl
        self.retry = retry
        self.crash = crash

    def deliver_due(self, *, limit: int = 10, run_id: str | None = None) -> int:
        """Deliver up to ``limit`` due messages; returns how many were delivered and acknowledged."""
        delivered = 0
        for message in self.store.claim_outbox(self.worker_id, self.claim_ttl, limit=limit, run_id=run_id):
            sink = self.sinks.get(message.topic)
            try:
                if sink is None:
                    raise LookupError(f"no sink registered for topic {message.topic!r}")
                sink.deliver(message.key, message.payload)
            except Exception as exc:
                delay = self.retry.delay(message.attempts, stable_rng(message.key, message.attempts))
                self.store.outbox_failed(
                    message.id,
                    self.worker_id,
                    f"{type(exc).__name__}: {exc}",
                    retry_at=self.store.now() + delay,
                    dead=message.attempts >= self.retry.max_attempts,
                )
                continue
            crash_hit(self.crash, "outbox.after_deliver", message.step)
            if self.store.outbox_delivered(message.id, self.worker_id):
                delivered += 1
        return delivered
