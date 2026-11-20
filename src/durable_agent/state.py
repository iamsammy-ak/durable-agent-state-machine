"""Run state as a fold over the event journal.

The executor never mutates a run directly: it appends events and applies them with
:func:`apply_event`, the same function used when a worker rebuilds a run after a crash. Live
execution and replay therefore cannot disagree. Transitions are read from the events (the
``next`` field), not re-evaluated, so replay does not depend on guard code staying the same.

A completed step's result is merged into ``data`` and its record is dropped: the journal keeps the
full history, while the state (and so every checkpoint) stays proportional to the workflow's data,
not to the number of steps a long-running run has executed.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Iterable
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError

from .errors import ReplayError
from .events import TERMINAL_STATUSES, Event, EventType, RunStatus

SNAPSHOT_VERSION = 1


class StepRecord(BaseModel):
    """The in-flight step (one visit of an activity state), keyed ``"<state>#<visit>"``."""

    key: str
    node: str
    attempt: int = 1
    status: Literal["scheduled", "started", "failed", "completed"] = "scheduled"
    not_before: float | None = None
    started_by: str | None = None
    failures: int = 0
    last_error: str | None = None


class GateRecord(BaseModel):
    """An open human-in-the-loop gate."""

    node: str
    key: str
    opened_at: float
    signals: list[str]
    timeout_at: float | None = None
    timer_id: str | None = None
    assignee: str | None = None


class TimerRecord(BaseModel):
    """A durable timer: the wake-up time is persisted in the journal, not held in memory."""

    timer_id: str
    kind: Literal["retry", "gate_timeout", "sleep"]
    fire_at: float
    step: str | None = None


class RunState(BaseModel):
    """Everything a worker needs to continue a run, rebuilt from a checkpoint plus later events."""

    run_id: str
    workflow: str = ""
    version: int = 1
    status: RunStatus = RunStatus.PENDING
    current: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)
    visits: dict[str, int] = Field(default_factory=dict)
    steps: dict[str, StepRecord] = Field(default_factory=dict)
    gate: GateRecord | None = None
    timers: dict[str, TimerRecord] = Field(default_factory=dict)
    last_seq: int = 0
    started_at: float | None = None
    updated_at: float | None = None
    finished_at: float | None = None
    outcome: str | None = None
    error: str | None = None
    recoveries: int = 0
    completed_steps: int = 0
    since_checkpoint: int = 0

    @property
    def terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    def step_key(self, node: str | None = None) -> str:
        """Key of the current (or given) state's latest visit, e.g. ``"draft#2"``."""
        name = node if node is not None else self.current
        if name is None:
            raise ReplayError("run has no current state")
        return f"{name}#{self.visits.get(name, 0)}"


def _enter(state: RunState, target: str) -> None:
    state.current = target
    state.visits[target] = state.visits.get(target, 0) + 1
    state.gate = None
    state.timers.clear()
    state.since_checkpoint += 1


def _step(state: RunState, event: Event) -> StepRecord:
    if event.step is None or event.step not in state.steps:
        raise ReplayError(f"seq {event.seq}: {event.type.value} for unknown step {event.step!r}")
    return state.steps[event.step]


def apply_event(state: RunState, event: Event) -> RunState:
    """Fold one event into ``state`` (in place) and return it."""
    if event.run_id != state.run_id:
        raise ReplayError(f"event for run {event.run_id!r} applied to run {state.run_id!r}")
    if event.seq != state.last_seq + 1:
        raise ReplayError(f"journal gap: expected seq {state.last_seq + 1}, got {event.seq}")
    if state.terminal and event.type is not EventType.CHECKPOINT_WRITTEN:
        raise ReplayError(f"seq {event.seq}: {event.type.value} after the run finished")
    data = event.data
    kind = event.type

    if kind is EventType.RUN_STARTED:
        if state.last_seq != 0:
            raise ReplayError("RunStarted must be the first event")
        state.workflow = data["workflow"]
        state.version = int(data.get("version", 1))
        state.data = copy.deepcopy(data["input"])
        state.started_at = event.ts
        state.status = RunStatus.RUNNING
        _enter(state, data["start"])
    elif kind is EventType.STEP_SCHEDULED:
        if event.step is None or event.attempt is None:
            raise ReplayError(f"seq {event.seq}: StepScheduled without a step key or attempt")
        record = state.steps.get(event.step)
        if record is None:
            state.steps[event.step] = StepRecord(
                key=event.step,
                node=event.step.split("#", 1)[0],
                attempt=event.attempt,
                not_before=data.get("not_before"),
            )
        else:
            record.attempt = event.attempt
            record.status = "scheduled"
            record.not_before = data.get("not_before")
    elif kind is EventType.STEP_STARTED:
        record = _step(state, event)
        record.status = "started"
        record.attempt = event.attempt or record.attempt
        record.started_by = event.worker
        record.not_before = None
        state.timers = {k: t for k, t in state.timers.items() if not (t.kind == "retry" and t.step == event.step)}
    elif kind is EventType.STEP_FAILED:
        record = _step(state, event)
        record.status = "failed"
        record.failures += 1
        record.last_error = f"{data.get('error_type', 'Error')}: {data.get('message', '')}"
        if data.get("final") and data.get("next"):
            del state.steps[record.key]
            _enter(state, data["next"])
    elif kind is EventType.STEP_COMPLETED:
        record = _step(state, event)
        state.data.update(copy.deepcopy(data.get("result") or {}))
        del state.steps[record.key]  # the result now lives in ``data`` and in the journal
        state.completed_steps += 1
        _enter(state, data["next"])
    elif kind is EventType.TIMER_SET:
        state.timers[data["timer_id"]] = TimerRecord(
            timer_id=data["timer_id"], kind=data["kind"], fire_at=data["fire_at"], step=event.step
        )
    elif kind is EventType.TIMER_FIRED:
        state.timers.pop(data["timer_id"], None)
        if data.get("next"):
            _enter(state, data["next"])
    elif kind is EventType.GATE_OPENED:
        state.gate = GateRecord(
            node=data["gate"],
            key=event.step or state.step_key(data["gate"]),
            opened_at=event.ts,
            signals=list(data["signals"]),
            timeout_at=data.get("timeout_at"),
            timer_id=data.get("timer_id"),
            assignee=data.get("assignee"),
        )
    elif kind is EventType.SIGNAL_RECEIVED:
        state.data.update(copy.deepcopy(data.get("result") or {}))
        if data.get("next"):
            _enter(state, data["next"])
    elif kind is EventType.CHECKPOINT_WRITTEN:
        state.since_checkpoint = 0
    elif kind is EventType.RUN_RECOVERED:
        state.recoveries += 1
    elif kind is EventType.RUN_COMPLETED:
        state.status = RunStatus.COMPLETED
        state.outcome = data.get("outcome")
        state.finished_at = event.ts
    elif kind is EventType.RUN_FAILED:
        state.status = RunStatus.FAILED
        state.outcome = data.get("outcome")
        state.error = data.get("error")
        state.finished_at = event.ts
    elif kind is EventType.RUN_CANCELLED:
        state.status = RunStatus.CANCELLED
        state.error = data.get("reason") or "cancelled"
        state.finished_at = event.ts
    else:  # pragma: no cover - exhaustive over EventType
        raise ReplayError(f"unknown event type {kind!r}")

    state.last_seq = event.seq
    state.updated_at = event.ts
    return state


def replay(events: Iterable[Event], *, run_id: str | None = None, base: RunState | None = None) -> RunState:
    """Fold ``events`` onto ``base`` (a checkpoint snapshot) or onto an empty run."""
    items = list(events)
    if base is not None:
        state = base.model_copy(deep=True)
    else:
        rid = run_id or (items[0].run_id if items else None)
        if rid is None:
            raise ReplayError("cannot replay an empty journal without a run id")
        state = RunState(run_id=rid)
    for event in items:
        apply_event(state, event)
    return state


def snapshot(state: RunState) -> str:
    """Serialise a run state for the checkpoints table."""
    return json.dumps({"v": SNAPSHOT_VERSION, "state": state.model_dump(mode="json")}, separators=(",", ":"))


def restore(text: str) -> RunState | None:
    """Load a snapshot; ``None`` when it was written by an incompatible version (callers replay fully)."""
    try:
        raw = json.loads(text)
        if raw.get("v") != SNAPSHOT_VERSION:
            return None
        return RunState.model_validate(raw["state"])
    except (ValueError, KeyError, ValidationError, AttributeError):
        return None
