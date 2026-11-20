"""The public entry point: register workflows, start runs, send signals, inspect history."""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ValidationError

from .crash import CrashInjector
from .errors import SignalRejected, WorkflowDefinitionError
from .events import Event, EventType, RunStatus
from .executor import CANCEL, enter_node, load_state
from .outbox import Sink
from .state import RunState
from .store import OutboxRow, RunRow, SignalRow, Store
from .worker import Worker
from .workflow import Workflow


def new_run_id(prefix: str = "run") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


class Engine:
    """A durable-execution engine bound to one SQLite database.

    Args:
        db_path: SQLite file shared by every worker (threads or processes).
        workflows: workflow definitions this engine can start and run (validated here).
        sinks: outbox topic -> sink that applies the side effect (must be idempotent by key).
        resources: shared objects handed to activities via ``ctx.resource(name)``.
        checkpoint_every: write a state snapshot after this many state transitions (0 = never).
        synchronous: SQLite ``synchronous`` pragma (``NORMAL`` or ``FULL``).
        lease_ttl: seconds a worker's lease lives without a heartbeat; the crash-recovery delay.
    """

    def __init__(
        self,
        db_path: str | Path,
        workflows: Iterable[Workflow[Any]],
        *,
        sinks: Mapping[str, Sink] | None = None,
        resources: Mapping[str, Any] | None = None,
        checkpoint_every: int = 1,
        synchronous: str = "NORMAL",
        lease_ttl: float = 10.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if lease_ttl <= 0:
            raise ValueError("lease_ttl must be positive")
        self.db_path = Path(db_path)
        self.workflows: dict[str, Workflow[Any]] = {}
        for wf in workflows:
            wf.validate()
            self.workflows[wf.name] = wf
        self.sinks: dict[str, Sink] = dict(sinks or {})
        self.resources: dict[str, Any] = dict(resources or {})
        self.checkpoint_every = checkpoint_every
        self.synchronous = synchronous
        self.lease_ttl = lease_ttl
        self.clock = clock
        self.store = self.open_store()

    def open_store(self) -> Store:
        """A new connection with this engine's settings (one per thread)."""
        return Store(self.db_path, synchronous=self.synchronous, clock=self.clock)

    def close(self) -> None:
        self.store.close()

    def __enter__(self) -> Engine:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def workflow(self, name: str) -> Workflow[Any]:
        try:
            return self.workflows[name]
        except KeyError:
            raise WorkflowDefinitionError(f"workflow {name!r} is not registered on this engine") from None

    # ------------------------------------------------------------------ control
    def start(
        self, workflow: str, input: BaseModel | Mapping[str, Any] | None = None, *, run_id: str | None = None
    ) -> str:
        """Create a run: journal ``RunStarted`` plus the first state's entry events. Returns the run id."""
        wf = self.workflow(workflow)
        model = wf.parse_state(input.model_dump(mode="json") if isinstance(input, BaseModel) else (input or {}))
        state = RunState(run_id=run_id or new_run_id())
        with self.store.run_tx(state, worker=None, create_workflow=wf.name) as tx:
            tx.append(
                EventType.RUN_STARTED,
                data={"workflow": wf.name, "version": wf.version, "start": wf.start,
                      "input": model.model_dump(mode="json")},
            )
            enter_node(tx, wf, model, checkpoint_every=0)
        return state.run_id

    def signal(self, run_id: str, name: str, payload: Mapping[str, Any] | None = None, *, sender: str = "api") -> int:
        """Deliver a signal to a run's inbox. It is buffered until the run reaches a gate that accepts it."""
        wf = self.workflow(self.store.get_run(run_id).workflow)
        if name == CANCEL:
            body = {"reason": str((payload or {}).get("reason", ""))}
        else:
            models = wf.signal_models
            if name not in models:
                raise SignalRejected(f"workflow {wf.name!r} has no gate that accepts {name!r} (try {sorted(models)})")
            try:
                body = models[name].model_validate(dict(payload or {})).model_dump(mode="json")
            except ValidationError as exc:
                raise SignalRejected(f"invalid {name!r} payload: {exc.errors()[0]['msg']}") from exc
        return self.store.add_signal(run_id, name, body, sender)

    def cancel(self, run_id: str, reason: str = "", *, sender: str = "api") -> int:
        """Request cancellation; the run stops at its next step boundary."""
        return self.signal(run_id, CANCEL, {"reason": reason}, sender=sender)

    # ---------------------------------------------------------------- inspection
    def state(self, run_id: str) -> RunState:
        """Current state, rebuilt from the latest checkpoint and the events after it."""
        return load_state(self.store, run_id)[0]

    def history(self, run_id: str) -> list[Event]:
        return self.store.events(run_id)

    def run(self, run_id: str) -> RunRow:
        return self.store.get_run(run_id)

    def runs(self, statuses: Iterable[RunStatus] | None = None) -> list[RunRow]:
        return self.store.list_runs(statuses=statuses)

    def signals(self, run_id: str) -> list[SignalRow]:
        return self.store.signals(run_id)

    def outbox(self, run_id: str | None = None) -> list[OutboxRow]:
        return self.store.outbox(run_id=run_id)

    # ------------------------------------------------------------------ workers
    def worker(
        self,
        worker_id: str | None = None,
        *,
        poll_interval: float = 0.1,
        heartbeat: bool = True,
        crash: CrashInjector | None = None,
        log: bool = False,
    ) -> Worker:
        return Worker(self, worker_id, poll_interval=poll_interval, heartbeat=heartbeat, crash=crash, log=log)

    def drain(self, *, worker_id: str = "inline", timeout: float = 30.0, horizon: float = 5.0) -> int:
        """Process work in this thread until nothing is ready within ``horizon`` seconds."""
        with self.worker(worker_id) as worker:
            return worker.run_until_idle(timeout=timeout, horizon=horizon)
