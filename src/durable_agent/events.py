"""The event journal's vocabulary: every change to a run is one of these append-only events."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class EventType(str, Enum):
    """Event kinds, in roughly the order a run produces them."""

    RUN_STARTED = "RunStarted"
    STEP_SCHEDULED = "StepScheduled"
    STEP_STARTED = "StepStarted"
    STEP_COMPLETED = "StepCompleted"
    STEP_FAILED = "StepFailed"
    TIMER_SET = "TimerSet"
    TIMER_FIRED = "TimerFired"
    GATE_OPENED = "GateOpened"
    SIGNAL_RECEIVED = "SignalReceived"
    CHECKPOINT_WRITTEN = "CheckpointWritten"
    RUN_RECOVERED = "RunRecovered"
    RUN_COMPLETED = "RunCompleted"
    RUN_FAILED = "RunFailed"
    RUN_CANCELLED = "RunCancelled"


TERMINAL_EVENTS = frozenset({EventType.RUN_COMPLETED, EventType.RUN_FAILED, EventType.RUN_CANCELLED})


class RunStatus(str, Enum):
    """Operational status of a run, as stored in the ``runs`` table."""

    PENDING = "pending"  # created, never claimed by a worker
    RUNNING = "running"  # claimable now, or held by a worker
    WAITING = "waiting"  # released by its worker until a timer fires or a signal arrives
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in TERMINAL_STATUSES


TERMINAL_STATUSES = frozenset({RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED})


class Event(BaseModel):
    """One journal entry. ``seq`` is contiguous per run and starts at 1."""

    model_config = ConfigDict(frozen=True)

    run_id: str
    seq: int = Field(ge=1)
    type: EventType
    ts: float
    step: str | None = None
    attempt: int | None = None
    worker: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)
