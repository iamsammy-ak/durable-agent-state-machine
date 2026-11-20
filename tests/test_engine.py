from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest
from helpers import Count, counter_workflow, make_engine

from durable_agent import (
    ActivityContext,
    Engine,
    EventType,
    NonRetryableError,
    RetryPolicy,
    RunStatus,
    SignalRejected,
    check_run,
    load_state,
)


def _types(engine: Engine, run_id: str) -> list[str]:
    return [e.type.value for e in engine.history(run_id)]


def test_a_run_completes_with_one_attempt_per_step(tmp_path: Path) -> None:
    engine = make_engine(tmp_path, counter_workflow())
    run_id = engine.start("counter", {"target": 3})
    engine.drain()
    row = engine.run(run_id)
    assert row.status is RunStatus.COMPLETED and row.outcome == "done"
    assert engine.state(run_id).data["n"] == 3
    assert dict(engine.resources["calls"]) == {"tick#1": 1, "tick#2": 1, "tick#3": 1}
    types = _types(engine, run_id)
    assert types[:4] == ["RunStarted", "StepScheduled", "StepStarted", "StepCompleted"]
    assert types[-2:] == ["RunCompleted", "CheckpointWritten"]
    assert check_run(engine.store, run_id) == []


def test_transient_failures_are_retried_with_persisted_backoff(tmp_path: Path) -> None:
    def flaky(ctx: ActivityContext, state: Count) -> None:
        if ctx.attempt < 3:
            raise ConnectionError("try again")

    engine = make_engine(tmp_path, counter_workflow(), resources={"hook": flaky})
    run_id = engine.start("counter", {"target": 1})
    engine.drain()
    assert engine.run(run_id).outcome == "done"
    failed = [e for e in engine.history(run_id) if e.type is EventType.STEP_FAILED]
    assert [e.attempt for e in failed] == [1, 2]
    assert all(e.data["retryable"] and not e.data["final"] for e in failed)
    assert all(0 <= e.data["retry_in"] <= 0.02 for e in failed)
    timers = [e for e in engine.history(run_id) if e.type is EventType.TIMER_SET]
    assert all(t.data["kind"] == "retry" for t in timers)


def test_non_retryable_errors_fail_the_run_at_once(tmp_path: Path) -> None:
    def broken(ctx: ActivityContext, state: Count) -> None:
        raise NonRetryableError("bad input")

    engine = make_engine(tmp_path, counter_workflow(), resources={"hook": broken})
    run_id = engine.start("counter")
    engine.drain()
    row = engine.run(run_id)
    assert row.status is RunStatus.FAILED and "bad input" in (row.error or "")
    assert engine.resources["calls"]["tick#1"] == 1


def test_exhausted_retries_follow_the_failure_transition(tmp_path: Path) -> None:
    def always(ctx: ActivityContext, state: Count) -> None:
        raise TimeoutError("down")

    engine = make_engine(tmp_path, counter_workflow(failure_state=True), resources={"hook": always})
    run_id = engine.start("counter")
    engine.drain()
    row = engine.run(run_id)
    assert row.status is RunStatus.FAILED and row.outcome == "gave_up"
    final = [e for e in engine.history(run_id) if e.type is EventType.STEP_FAILED][-1]
    assert final.attempt == 3 and final.data["final"] and final.data["next"] == "gave_up"


def test_a_timed_out_attempt_is_abandoned_and_its_result_discarded(tmp_path: Path) -> None:
    finished_late = threading.Event()

    def slow_first(ctx: ActivityContext, state: Count) -> None:
        if ctx.attempt == 1:
            ctx.cancelled.wait(2.0)
            finished_late.set()

    engine = make_engine(tmp_path, counter_workflow(timeout=0.2), resources={"hook": slow_first})
    run_id = engine.start("counter", {"target": 1})
    engine.drain()
    assert finished_late.wait(2.0)  # the abandoned attempt saw the cancellation
    failed = [e for e in engine.history(run_id) if e.type is EventType.STEP_FAILED]
    assert [e.data["error_type"] for e in failed] == ["ActivityTimeout"]
    completed = [e for e in engine.history(run_id) if e.type is EventType.STEP_COMPLETED]
    assert [e.attempt for e in completed] == [2] and engine.state(run_id).data["n"] == 1


def test_a_result_that_does_not_fit_the_state_fails_without_retrying(tmp_path: Path) -> None:
    from durable_agent import Workflow

    wf: Workflow[Count] = Workflow("counter", Count, start="tick")
    wf.add_activity("tick", lambda ctx, s: {"n": "many"})
    wf.terminal("done")
    wf.transition("tick", "done")
    engine = make_engine(tmp_path, wf)
    run_id = engine.start("counter")
    engine.drain()
    failed = [e for e in engine.history(run_id) if e.type is EventType.STEP_FAILED]
    assert failed[0].data["error_type"] == "InvalidResult" and failed[0].data["final"]
    assert engine.run(run_id).status is RunStatus.FAILED


def test_loops_get_one_step_key_per_visit(tmp_path: Path) -> None:
    engine = make_engine(tmp_path, counter_workflow())
    run_id = engine.start("counter", {"target": 4})
    engine.drain()
    state = engine.state(run_id)
    completed = [e.step for e in engine.history(run_id) if e.type is EventType.STEP_COMPLETED]
    assert completed == ["tick#1", "tick#2", "tick#3", "tick#4"]
    assert state.visits["tick"] == 4 and state.completed_steps == 4
    assert state.steps == {}  # finished steps live in the journal, not in the state or its snapshots


def test_signals_sent_early_are_buffered_until_the_gate_opens(tmp_path: Path) -> None:
    engine = make_engine(tmp_path, counter_workflow(gate=True))
    run_id = engine.start("counter", {"target": 2})
    engine.signal(run_id, "approve", {"note": "pre-approved"}, sender="lead")
    engine.drain()
    assert engine.run(run_id).outcome == "done"
    received = [e for e in engine.history(run_id) if e.type is EventType.SIGNAL_RECEIVED][0]
    assert received.data["sender"] == "lead" and received.data["next"] == "done"
    assert engine.state(run_id).data["note"] == "approve by lead"


def test_a_gate_parks_the_run_without_holding_a_worker(tmp_path: Path) -> None:
    engine = make_engine(tmp_path, counter_workflow(gate=True))
    run_id = engine.start("counter", {"target": 1})
    engine.drain()
    row = engine.run(run_id)
    assert row.status is RunStatus.WAITING and row.waiting_on == "signal:review"
    assert row.lease_owner is None and row.ready_at is None
    engine.signal(run_id, "reject", sender="lead")
    assert engine.run(run_id).ready_at is not None  # the signal woke it
    engine.drain()
    assert engine.run(run_id).outcome == "rejected"


def test_gate_timeouts_fire_from_persisted_timers(tmp_path: Path) -> None:
    engine = make_engine(tmp_path, counter_workflow(gate=True, gate_timeout=0.2))
    run_id = engine.start("counter", {"target": 1})
    engine.drain()
    assert engine.run(run_id).outcome == "timed_out"
    fired = [e for e in engine.history(run_id) if e.type is EventType.TIMER_FIRED]
    assert fired[0].data["kind"] == "gate_timeout" and fired[0].data["next"] == "timed_out"


def test_invalid_signals_are_rejected_before_they_reach_the_inbox(tmp_path: Path) -> None:
    from durable_agent.research import build_engine

    engine = build_engine(tmp_path / "r.db", offline=True)
    run_id = engine.start("research_report", {"topic": "solid-state batteries"})
    with pytest.raises(SignalRejected, match="no gate"):
        engine.signal(run_id, "launch")
    with pytest.raises(SignalRejected, match="invalid 'edit' payload"):
        engine.signal(run_id, "edit", {})
    assert engine.signals(run_id) == []


def test_cancellation_stops_a_parked_run(tmp_path: Path) -> None:
    engine = make_engine(tmp_path, counter_workflow(gate=True))
    run_id = engine.start("counter", {"target": 1})
    engine.drain()
    engine.cancel(run_id, "no longer needed", sender="ops")
    engine.drain()
    row = engine.run(run_id)
    assert row.status is RunStatus.CANCELLED
    last = [e for e in engine.history(run_id) if e.type is EventType.RUN_CANCELLED][0]
    assert last.data == {"reason": "no longer needed", "by": "ops", "signal_id": 1}


def test_a_sleep_survives_a_worker_restart_and_keeps_its_wake_up_time(tmp_path: Path) -> None:
    engine = make_engine(tmp_path, counter_workflow(sleep=0.5))
    run_id = engine.start("counter", {"target": 1})
    with engine.worker("first") as worker:
        worker.run_until_idle(horizon=0.0)  # parks at the sleep, then this worker goes away
    timer = [e for e in engine.history(run_id) if e.type is EventType.TIMER_SET][0]
    row = engine.run(run_id)
    assert row.waiting_on == "timer:pause" and row.ready_at == pytest.approx(timer.data["fire_at"])
    engine.drain(worker_id="second")
    fired = [e for e in engine.history(run_id) if e.type is EventType.TIMER_FIRED][0]
    assert fired.worker == "second" and fired.ts >= timer.data["fire_at"]
    assert engine.run(run_id).outcome == "done"


def test_checkpoints_can_be_disabled_and_bad_snapshots_are_ignored(tmp_path: Path) -> None:
    engine = make_engine(tmp_path, counter_workflow(), checkpoint_every=0)
    run_id = engine.start("counter", {"target": 2})
    engine.drain()
    assert engine.store.checkpoints(run_id) == []
    state, checkpoint_seq, replayed = load_state(engine.store, run_id)
    assert checkpoint_seq is None and replayed == len(engine.history(run_id))
    engine.store.conn.execute(
        "INSERT INTO checkpoints (run_id, seq, state, ts) VALUES (?, ?, ?, ?)", (run_id, 99, '{"v": 0}', time.time())
    )
    again, checkpoint_seq, _ = load_state(engine.store, run_id)
    assert checkpoint_seq is None and again == state


def test_checkpoint_frequency_controls_replay_length(tmp_path: Path) -> None:
    engine = make_engine(tmp_path, counter_workflow(), checkpoint_every=3)
    run_id = engine.start("counter", {"target": 7})
    engine.drain()
    seqs = [seq for seq, _ in engine.store.checkpoints(run_id)]
    assert len(seqs) == 3  # transitions 3 and 6, plus the terminal one
    assert check_run(engine.store, run_id) == []


def test_retry_policy_limits_are_respected(tmp_path: Path) -> None:
    def always(ctx: ActivityContext, state: Count) -> None:
        raise ConnectionError("nope")

    wf = counter_workflow(retry=RetryPolicy(max_attempts=5, initial_interval=0.0, max_interval=0.0))
    engine = make_engine(tmp_path, wf, resources={"hook": always})
    run_id = engine.start("counter")
    engine.drain()
    assert engine.resources["calls"]["tick#1"] == 5
    assert engine.run(run_id).status is RunStatus.FAILED
