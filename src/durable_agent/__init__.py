"""Durable Agent State Machine: a small durable-execution runtime for agent workflows."""

from __future__ import annotations

from .context import ActivityContext, Outbox, OutboxMessage
from .crash import CRASH_POINTS, CrashInjector, CrashMode, CrashPlan, SimulatedCrash
from .engine import Engine, new_run_id
from .errors import (
    DurableAgentError,
    InvalidResult,
    LeaseLost,
    NonRetryableError,
    ReplayError,
    RunFinished,
    RunNotFound,
    SignalRejected,
    TransitionError,
    WorkflowDefinitionError,
)
from .events import Event, EventType, RunStatus
from .executor import AdvanceReport, load_state
from .outbox import OutboxRelay, Sink
from .retry import RetryPolicy
from .state import RunState, replay
from .store import Lease, RunRow, Store
from .verify import Violation, check_journal, check_run
from .worker import Worker
from .workflow import Signal, Transition, Workflow

__version__ = "0.1.0"

__all__ = [
    "CRASH_POINTS",
    "ActivityContext",
    "AdvanceReport",
    "CrashInjector",
    "CrashMode",
    "CrashPlan",
    "DurableAgentError",
    "Engine",
    "Event",
    "EventType",
    "InvalidResult",
    "Lease",
    "LeaseLost",
    "NonRetryableError",
    "Outbox",
    "OutboxMessage",
    "OutboxRelay",
    "ReplayError",
    "RetryPolicy",
    "RunFinished",
    "RunNotFound",
    "RunRow",
    "RunState",
    "RunStatus",
    "Signal",
    "SignalRejected",
    "SimulatedCrash",
    "Sink",
    "Store",
    "Transition",
    "TransitionError",
    "Violation",
    "Worker",
    "Workflow",
    "WorkflowDefinitionError",
    "__version__",
    "check_journal",
    "check_run",
    "load_state",
    "new_run_id",
    "replay",
]
