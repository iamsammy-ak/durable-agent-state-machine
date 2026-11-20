"""Small workflows and helpers shared by the tests."""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from durable_agent import ActivityContext, Engine, RetryPolicy, Workflow
from durable_agent.workflow import Signal

FAST_RETRY = RetryPolicy(max_attempts=3, initial_interval=0.01, max_interval=0.02)


class Count(BaseModel):
    n: int = 0
    target: int = 3
    note: str = ""


class Ok(BaseModel):
    note: str = ""


Hook = Callable[[ActivityContext, Count], None]


def counter_workflow(
    *,
    retry: RetryPolicy = FAST_RETRY,
    timeout: float | None = None,
    gate: bool = False,
    gate_timeout: float | None = None,
    sleep: float | None = None,
    failure_state: bool = False,
    emit: bool = False,
) -> Workflow[Count]:
    """``tick`` loops until ``n == target``; optionally a review gate, a sleep and an outbox message."""
    wf: Workflow[Count] = Workflow("counter", Count, start="tick")

    @wf.activity("tick", retry=retry, timeout=timeout)
    def tick(ctx: ActivityContext, state: Count) -> dict[str, Any]:
        ctx.resources["calls"][ctx.step] += 1
        hook: Hook | None = ctx.resources.get("hook")
        if hook is not None:
            hook(ctx, state)
        if emit:
            ctx.outbox.send("audit", {"run": ctx.run_id, "n": state.n + 1})
        return {"n": state.n + 1}

    after_loop = "review" if gate else ("pause" if sleep is not None else "done")
    wf.transition("tick", "tick", guard=lambda s: s.n < s.target, label="n < target")
    wf.transition("tick", after_loop)
    if failure_state:
        wf.terminal("gave_up", failed=True)
        wf.transition("tick", "gave_up", on="failure")
    if gate:

        def review(state: Count, signal: Signal) -> dict[str, Any]:
            return {"note": f"{signal.name} by {signal.sender}"}

        wf.gate("review", signals={"approve": Ok, "reject": Ok}, timeout=gate_timeout, apply=review)
        wf.transition("review", "pause" if sleep is not None else "done", on="approve")
        wf.transition("review", "rejected", on="reject")
        wf.terminal("rejected")
        if gate_timeout is not None:
            wf.transition("review", "timed_out", on="timeout")
            wf.terminal("timed_out")
    if sleep is not None:
        wf.sleep("pause", seconds=sleep)
        wf.transition("pause", "done")
    wf.terminal("done")
    return wf


class MemorySink:
    """An idempotent receiver: records every call, applies each key once."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.applied: dict[str, dict[str, Any]] = {}

    def deliver(self, key: str, payload: dict[str, Any]) -> None:
        self.calls.append(key)
        self.applied.setdefault(key, payload)


def make_engine(tmp_path: Path, wf: Workflow[Any], **kwargs: Any) -> Engine:
    resources = {"calls": Counter(), **kwargs.pop("resources", {})}
    kwargs.setdefault("lease_ttl", 0.3)
    return Engine(tmp_path / "runs.db", [wf], resources=resources, **kwargs)


def wait_for(predicate: Callable[[], bool], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition not reached in time")
