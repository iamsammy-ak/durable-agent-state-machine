"""Drives one leased run forward until it finishes, parks (timer or signal) or loses its lease.

Delivery semantics, precisely:

* **Steps commit exactly once.** A step's result is written as ``StepCompleted`` in one fenced
  transaction; a completed step is never executed again, on any worker.
* **Activities run at least once.** If a worker dies after ``StepStarted`` and before the outcome
  commits, the attempt is *in doubt*: the next worker records it as ``StepFailed(worker_lost)`` and
  runs a new attempt. Code inside an activity may therefore run more than once.
* **Side effects are effectively once** when they go through the outbox: messages are committed
  atomically with ``StepCompleted`` and carry a deterministic idempotency key; the relay delivers
  at least once and the receiver deduplicates by key.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ValidationError

from .context import ActivityContext, Outbox
from .crash import CrashInjector, SimulatedCrash, crash_hit
from .errors import ActivityTimeout, InvalidResult, LeaseLost, ReplayError, RunNotFound, TransitionError
from .events import EventType, RunStatus
from .retry import stable_rng
from .state import RunState, replay, restore
from .store import Lease, RunTx, Store
from .workflow import ActivityNode, GateNode, Signal, SleepNode, TerminalNode, Workflow

if TYPE_CHECKING:
    from .engine import Engine

CANCEL = "cancel"


def load_state(store: Store, run_id: str) -> tuple[RunState, int | None, int]:
    """Rebuild a run: latest checkpoint + the events after it.

    Returns ``(state, checkpoint_seq or None, number of events replayed)``. A checkpoint written by
    an incompatible version is ignored and the journal is replayed from the start.
    """
    checkpoint = store.latest_checkpoint(run_id)
    base = restore(checkpoint[1]) if checkpoint else None
    if base is None:
        events = store.events(run_id)
        if not events:
            raise RunNotFound(run_id)
        return replay(events, run_id=run_id), None, len(events)
    events = store.events(run_id, after_seq=base.last_seq)
    return replay(events, base=base), base.last_seq, len(events)


def enter_node(tx: RunTx, wf: Workflow[Any], model: BaseModel, checkpoint_every: int) -> None:
    """Entry actions for the state the run just moved to, in the same transaction as the move."""
    state = tx.state
    if state.current is None:
        raise ReplayError("no current state to enter")
    node = wf.node(state.current)
    key = state.step_key()
    if isinstance(node, ActivityNode):
        tx.append(EventType.STEP_SCHEDULED, step=key, attempt=1)
    elif isinstance(node, GateNode):
        timeout = node.timeout_for(model)
        timeout_at = None if timeout is None else tx.now + timeout
        timer_id = None if timeout_at is None else f"timeout:{key}"
        tx.append(
            EventType.GATE_OPENED,
            step=key,
            data={
                "gate": node.name,
                "signals": list(node.signals),
                "assignee": node.assignee,
                "timeout_at": timeout_at,
                "timer_id": timer_id,
            },
        )
        if timeout_at is not None:
            timer = {"timer_id": timer_id, "kind": "gate_timeout", "fire_at": timeout_at}
            tx.append(EventType.TIMER_SET, step=key, data=timer)
    elif isinstance(node, SleepNode):
        fire_at = tx.now + node.seconds_for(model)
        tx.append(EventType.TIMER_SET, step=key, data={"timer_id": f"sleep:{key}", "kind": "sleep", "fire_at": fire_at})
    elif isinstance(node, TerminalNode):
        kind = EventType.RUN_FAILED if node.failed else EventType.RUN_COMPLETED
        data: dict[str, Any] = {"outcome": node.name}
        if node.failed:
            data["error"] = f"ended in failure state {node.name!r}"
        tx.append(kind, data=data)
    if checkpoint_every > 0 and (state.since_checkpoint >= checkpoint_every or state.terminal):
        tx.checkpoint()


@dataclass
class AdvanceReport:
    """What one :meth:`Executor.advance` call did (used by the CLI and the benchmarks)."""

    run_id: str
    status: RunStatus
    current: str | None
    waiting_on: str | None = None
    ready_at: float | None = None
    recovered_from: str | None = None
    checkpoint_seq: int | None = None
    replayed_events: int = 0
    replay_ms: float = 0.0
    attempts_run: list[str] = field(default_factory=list)


class _Stop(Exception):
    """Internal: the run is parked or finished; stop driving it."""


class Executor:
    """Runs one leased run. Create one per lease."""

    def __init__(
        self,
        engine: Engine,
        store: Store,
        lease: Lease,
        *,
        worker_id: str,
        crash: CrashInjector | None = None,
        lease_lost: threading.Event | None = None,
    ) -> None:
        self.engine = engine
        self.store = store
        self.lease = lease
        self.worker_id = worker_id
        self.crash = crash
        self.lease_lost = lease_lost or threading.Event()
        self.state: RunState | None = None
        self.wf: Workflow[Any] | None = None

    # ------------------------------------------------------------------ helpers
    def _tx(self) -> Any:
        assert self.state is not None
        return self.store.run_tx(
            self.state, worker=self.worker_id, lease=self.lease, lease_ttl=self.engine.lease_ttl
        )

    def _model(self) -> BaseModel:
        assert self.state is not None and self.wf is not None
        return self.wf.parse_state(self.state.data)

    def _enter(self, tx: RunTx, model: BaseModel) -> None:
        assert self.wf is not None
        enter_node(tx, self.wf, model, self.engine.checkpoint_every)

    # ------------------------------------------------------------------- driver
    def advance(self) -> AdvanceReport:
        """Drive the run until it parks or finishes. Raises :class:`LeaseLost` if fenced off."""
        started = time.perf_counter()
        state, checkpoint_seq, replayed = load_state(self.store, self.lease.run_id)
        replay_ms = (time.perf_counter() - started) * 1000
        self.state = state
        self.wf = self.engine.workflow(state.workflow)
        report = AdvanceReport(
            run_id=state.run_id,
            status=state.status,
            current=state.current,
            checkpoint_seq=checkpoint_seq,
            replayed_events=replayed,
            replay_ms=replay_ms,
        )
        if self.lease.previous_owner is not None and not state.terminal:
            report.recovered_from = self.lease.previous_owner
            with self._tx() as tx:
                tx.append(
                    EventType.RUN_RECOVERED,
                    data={
                        "from_worker": self.lease.previous_owner,
                        "lease_expired_at": self.lease.previous_expires_at,
                        "checkpoint_seq": checkpoint_seq,
                        "replayed_events": replayed,
                        "replay_ms": round(replay_ms, 3),
                    },
                )
        crash_hit(self.crash, "run.after_claim", state.step_key() if state.current else None)
        try:
            while not state.terminal:
                if self.lease_lost.is_set():
                    raise LeaseLost(f"heartbeat lost the lease on {state.run_id}")
                self._check_cancel()
                node = self.wf.node(state.current or "")
                try:
                    if isinstance(node, ActivityNode):
                        self._activity(node, report)
                    elif isinstance(node, GateNode):
                        self._gate(node)
                    elif isinstance(node, SleepNode):
                        self._sleep(node)
                    else:
                        raise ReplayError(f"run is in terminal state {node.name!r} without a terminal event")
                except TransitionError as exc:  # a guard set that does not cover this state: a definition bug
                    with self._tx() as tx:
                        tx.append(EventType.RUN_FAILED, data={"error": str(exc)})
        except _Stop:
            pass
        row = self.store.get_run(state.run_id)
        report.status, report.current = row.status, row.current_state
        report.waiting_on, report.ready_at = row.waiting_on, row.ready_at
        return report

    def _check_cancel(self) -> None:
        assert self.state is not None
        signal = self.store.next_signal(self.state.run_id, [CANCEL])
        if signal is None:
            return
        with self._tx() as tx:
            tx.consume_signal(signal.id)
            tx.append(
                EventType.RUN_CANCELLED,
                data={"reason": signal.payload.get("reason", ""), "by": signal.sender, "signal_id": signal.id},
            )
            if self.engine.checkpoint_every > 0:
                tx.checkpoint()
        raise _Stop

    def _park(self, *, ready_at: float | None, waiting_on: str, wake_on: list[str]) -> None:
        with self._tx() as tx:
            tx.release(ready_at=ready_at, waiting_on=waiting_on, wake_on=[*wake_on, CANCEL])
        raise _Stop

    # --------------------------------------------------------------- activities
    def _activity(self, node: ActivityNode, report: AdvanceReport) -> None:
        state = self.state
        assert state is not None
        key = state.step_key()
        record = state.steps.get(key)
        if record is None:
            raise ReplayError(f"{key}: current activity has no scheduled attempt")
        if record.status == "started":
            # The previous holder died (or was fenced off) mid-attempt: the outcome is unknown.
            self._fail(
                node,
                kind="worker_lost",
                error_type="WorkerLost",
                message=f"{record.started_by or 'a worker'} stopped during attempt {record.attempt}",
                retryable=True,
                duration_ms=None,
            )
            return
        if record.status == "failed":
            raise ReplayError(f"{key}: failed attempt without a scheduled retry")
        now = self.store.now()
        if record.not_before is not None and record.not_before > now:
            self._park(ready_at=record.not_before, waiting_on=f"retry {key}", wake_on=[])
        attempt = record.attempt
        with self._tx() as tx:
            tx.append(EventType.STEP_STARTED, step=key, attempt=attempt)
        report.attempts_run.append(f"{key}/{attempt}")
        crash_hit(self.crash, "step.after_start", key)

        model = self._model()
        ctx = ActivityContext(
            run_id=state.run_id,
            step=key,
            attempt=attempt,
            worker_id=self.worker_id,
            resources=self.engine.resources,
            outbox=Outbox(f"{state.run_id}/{key}"),
        )
        outcome, value, duration_ms = self._invoke(node, ctx, model)
        if outcome == "timeout":
            self._fail(
                node,
                kind="timeout",
                error_type="ActivityTimeout",
                message=f"attempt exceeded its {node.timeout:g}s timeout",
                retryable=node.retry.is_retryable_name(ActivityTimeout.__name__),
                duration_ms=duration_ms,
            )
            return
        if outcome == "error":
            assert isinstance(value, BaseException)
            self._fail(
                node,
                kind="error",
                error_type=type(value).__name__,
                message=str(value)[:500],
                retryable=node.retry.is_retryable(value),
                duration_ms=duration_ms,
            )
            return
        assert self.wf is not None
        try:
            patch, new_model = self.wf.apply_result(model, value)
        except InvalidResult as exc:
            self._fail(node, kind="error", error_type="InvalidResult", message=str(exc)[:500], retryable=False,
                       duration_ms=duration_ms)
            return
        try:
            target = self.wf.next_state(node.name, new_model)
        except TransitionError as exc:
            self._fail(node, kind="error", error_type="TransitionError", message=str(exc), retryable=False,
                       duration_ms=duration_ms, allow_failure_transition=False)
            return
        crash_hit(self.crash, "step.before_commit", key)
        with self._tx() as tx:
            tx.append(
                EventType.STEP_COMPLETED,
                step=key,
                attempt=attempt,
                data={"result": patch, "next": target, "duration_ms": duration_ms, "outbox": len(ctx.outbox.messages)},
            )
            for message in ctx.outbox.messages:
                tx.enqueue(message, key)
            self._enter(tx, new_model)
        crash_hit(self.crash, "step.after_commit", key)

    def _invoke(self, node: ActivityNode, ctx: ActivityContext, model: BaseModel) -> tuple[str, Any, float]:
        """Call the activity, in a helper thread when it has a timeout.

        A timed-out attempt is abandoned, not killed (Python cannot kill threads): ``ctx.cancelled``
        is set so cooperative code can stop, and its result and outbox messages are discarded.
        """
        started = time.perf_counter()
        if node.timeout is None:
            try:
                value = node.fn(ctx, model)
            except Exception as exc:
                return "error", exc, _ms(started)
            return "ok", value, _ms(started)

        box: dict[str, Any] = {}
        done = threading.Event()

        def target() -> None:
            try:
                box["value"] = node.fn(ctx, model)
            except BaseException as exc:  # crashes are re-raised in the worker thread below
                box["error"] = exc
            finally:
                done.set()

        threading.Thread(target=target, name=f"activity-{ctx.step}", daemon=True).start()
        if not done.wait(node.timeout):
            ctx.cancelled.set()
            return "timeout", None, _ms(started)
        error = box.get("error")
        if isinstance(error, SimulatedCrash):
            raise error
        if error is not None:
            if not isinstance(error, Exception):
                raise error
            return "error", error, _ms(started)
        return "ok", box.get("value"), _ms(started)

    def _fail(
        self,
        node: ActivityNode,
        *,
        kind: str,
        error_type: str,
        message: str,
        retryable: bool,
        duration_ms: float | None,
        allow_failure_transition: bool = True,
    ) -> None:
        state = self.state
        assert state is not None and self.wf is not None
        key = state.step_key()
        attempt = state.steps[key].attempt
        final = not retryable or attempt >= node.retry.max_attempts
        data: dict[str, Any] = {
            "kind": kind,
            "error_type": error_type,
            "message": message,
            "retryable": retryable,
            "final": final,
            "duration_ms": duration_ms,
        }
        if not final:
            delay = 0.0 if kind == "worker_lost" else node.retry.delay(attempt, stable_rng(state.run_id, key, attempt))
            with self._tx() as tx:
                retry_at = tx.now + delay
                data.update(retry_in=round(delay, 4), retry_at=retry_at)
                tx.append(EventType.STEP_FAILED, step=key, attempt=attempt, data=data)
                if delay > 0:
                    tx.append(
                        EventType.TIMER_SET,
                        step=key,
                        data={"timer_id": f"retry:{key}:{attempt + 1}", "kind": "retry", "fire_at": retry_at},
                    )
                tx.append(EventType.STEP_SCHEDULED, step=key, attempt=attempt + 1, data={"not_before": retry_at})
            return
        model = self._model()
        target = None
        if allow_failure_transition and self.wf.has_trigger(node.name, "failure"):
            target = self.wf.next_state(node.name, model, on="failure")
        data["next"] = target
        with self._tx() as tx:
            tx.append(EventType.STEP_FAILED, step=key, attempt=attempt, data=data)
            if target is not None:
                self._enter(tx, model)
            else:
                tx.append(
                    EventType.RUN_FAILED,
                    data={"error": f"{key} failed after {attempt} attempt(s): {error_type}: {message}"},
                )
                if self.engine.checkpoint_every > 0:
                    tx.checkpoint()

    # -------------------------------------------------------------------- gates
    def _gate(self, node: GateNode) -> None:
        state = self.state
        assert state is not None and self.wf is not None
        gate = state.gate
        if gate is None:
            raise ReplayError(f"{state.step_key()}: gate state without GateOpened")
        signal = self.store.next_signal(state.run_id, gate.signals)
        if signal is not None:
            crash_hit(self.crash, "gate.before_signal", gate.key)
            model = self._model()
            data: dict[str, Any] = {
                "signal": signal.name,
                "signal_id": signal.id,
                "sender": signal.sender,
                "payload": signal.payload,
                "waited_s": round(self.store.now() - gate.opened_at, 3),
            }
            try:
                payload = node.signals[signal.name].model_validate(signal.payload)
                raw = node.apply(model, Signal(signal.name, payload, signal.sender)) if node.apply else None
                patch, new_model = self.wf.apply_result(model, raw)
                target: str | None = self.wf.next_state(node.name, new_model, on=signal.name)
            except (ValidationError, InvalidResult, TransitionError) as exc:
                # A payload the gate cannot use is consumed and recorded, and the gate keeps waiting.
                patch, new_model, target = {}, model, None
                data["ignored"] = str(exc)[:300]
            data.update(result=patch, next=target)
            with self._tx() as tx:
                tx.consume_signal(signal.id)
                tx.append(EventType.SIGNAL_RECEIVED, step=gate.key, data=data)
                if target is not None:
                    self._enter(tx, new_model)
            return
        now = self.store.now()
        if gate.timeout_at is not None and gate.timeout_at <= now:
            crash_hit(self.crash, "timer.before_fire", gate.key)
            model = self._model()
            target = self.wf.next_state(node.name, model, on="timeout")
            with self._tx() as tx:
                tx.append(
                    EventType.TIMER_FIRED,
                    step=gate.key,
                    data={"timer_id": gate.timer_id, "kind": "gate_timeout", "next": target,
                          "late_ms": round((now - gate.timeout_at) * 1000, 1)},
                )
                self._enter(tx, model)
            return
        self._park(ready_at=gate.timeout_at, waiting_on=f"signal:{node.name}", wake_on=list(gate.signals))

    # ------------------------------------------------------------------- sleeps
    def _sleep(self, node: SleepNode) -> None:
        state = self.state
        assert state is not None and self.wf is not None
        key = state.step_key()
        timer = state.timers.get(f"sleep:{key}")
        if timer is None:
            raise ReplayError(f"{key}: sleep state without a TimerSet")
        now = self.store.now()
        if timer.fire_at > now:
            self._park(ready_at=timer.fire_at, waiting_on=f"timer:{node.name}", wake_on=[])
        crash_hit(self.crash, "timer.before_fire", key)
        model = self._model()
        target = self.wf.next_state(node.name, model)
        with self._tx() as tx:
            tx.append(
                EventType.TIMER_FIRED,
                step=key,
                data={"timer_id": timer.timer_id, "kind": "sleep", "next": target,
                      "late_ms": round((now - timer.fire_at) * 1000, 1)},
            )
            self._enter(tx, model)


def _ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000, 3)
