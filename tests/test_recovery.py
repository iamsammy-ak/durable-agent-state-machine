"""Crash recovery, fencing and effectively-once side effects, in-process (fast)."""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest
from helpers import Count, MemorySink, counter_workflow, make_engine, wait_for
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from durable_agent import (
    ActivityContext,
    CrashInjector,
    CrashPlan,
    Engine,
    EventType,
    RunStatus,
    SimulatedCrash,
    check_run,
    replay,
)
from durable_agent.research import CmsSink, ReportOptions, ReportState, build_engine, cms_path


def _crash_then_recover(engine: Engine, plan: CrashPlan) -> None:
    worker = engine.worker("doomed", crash=CrashInjector(plan))
    with pytest.raises(SimulatedCrash):
        worker.run_until_idle(horizon=5.0)
    worker.close()  # the dead worker's lease is left to expire
    engine.drain(worker_id="rescuer")


@pytest.mark.parametrize(
    "spec", ["run.after_claim:1", "step.after_start:2", "step.before_commit:2", "step.after_commit:2"]
)
def test_a_crash_at_any_step_boundary_is_recovered_without_rerunning_completed_steps(
    tmp_path: Path, spec: str
) -> None:
    engine = make_engine(tmp_path, counter_workflow(), lease_ttl=0.2)
    run_id = engine.start("counter", {"target": 3})
    _crash_then_recover(engine, CrashPlan.parse(spec))
    assert engine.run(run_id).outcome == "done"
    assert engine.state(run_id).data["n"] == 3
    calls = engine.resources["calls"]
    assert calls["tick#1"] == 1  # executed exactly once, whoever ran it
    in_doubt = spec.startswith(("step.after_start", "step.before_commit"))
    assert calls["tick#2"] == (2 if spec.startswith("step.before_commit") else 1)
    events = engine.history(run_id)
    recovered = [e for e in events if e.type is EventType.RUN_RECOVERED]
    assert len(recovered) == 1 and recovered[0].data["from_worker"] == "doomed"
    lost = [e for e in events if e.type is EventType.STEP_FAILED and e.data["kind"] == "worker_lost"]
    assert len(lost) == (1 if in_doubt else 0)
    assert check_run(engine.store, run_id) == []


def test_a_stalled_worker_is_fenced_off_after_another_takes_over(tmp_path: Path) -> None:
    release = threading.Event()

    def stall_first(ctx: ActivityContext, state: Count) -> None:
        if ctx.worker_id == "zombie":
            release.wait(5)

    engine = make_engine(tmp_path, counter_workflow(), lease_ttl=0.3, resources={"hook": stall_first})
    run_id = engine.start("counter", {"target": 1})
    outcome: list[object] = []

    def zombie_main() -> None:
        # A stop-the-world pause: the zombie sends no heartbeats while its activity stalls.
        with engine.worker("zombie", heartbeat=False) as zombie:
            lease = zombie.store.claim("zombie", ttl=0.3)
            assert lease is not None
            outcome.append(zombie.process(lease))
            outcome.append(zombie.stats.leases_lost)

    thread = threading.Thread(target=zombie_main)
    thread.start()
    wait_for(lambda: any(e.type is EventType.STEP_STARTED for e in engine.history(run_id)))
    time.sleep(0.35)  # the zombie's lease expires
    engine.drain(worker_id="successor")
    release.set()
    thread.join(5)
    assert outcome == [None, 1]  # its late commit was refused: lease lost
    completed = [e for e in engine.history(run_id) if e.type is EventType.STEP_COMPLETED]
    assert [e.worker for e in completed] == ["successor"]
    assert engine.state(run_id).data["n"] == 1
    assert check_run(engine.store, run_id) == []


def test_a_crash_after_delivery_is_absorbed_by_the_idempotent_receiver(tmp_path: Path) -> None:
    sink = MemorySink()
    engine = make_engine(tmp_path, counter_workflow(emit=True), lease_ttl=0.2, sinks={"audit": sink})
    run_id = engine.start("counter", {"target": 2})
    _crash_then_recover(engine, CrashPlan.parse("outbox.after_deliver:1"))
    assert engine.run(run_id).outcome == "done"
    assert sink.calls.count(f"{run_id}/tick#1/0") == 2  # delivered twice ...
    assert len(sink.applied) == 2  # ... applied once per step
    assert all(m.status == "delivered" for m in engine.outbox(run_id))


def _report_engine(tmp_path: Path, mode: str) -> tuple[Engine, str]:
    engine = build_engine(tmp_path / f"{mode}.db", offline=True, lease_ttl=0.2)
    run_id = engine.start(
        "research_report",
        ReportState(topic="solid-state batteries", options=ReportOptions(publish_mode=mode)),  # type: ignore[arg-type]
    )
    engine.drain()
    engine.signal(run_id, "approve", {"note": "ship it"}, sender="editor")
    return engine, run_id


@pytest.mark.parametrize(("mode", "articles"), [("direct", 2), ("outbox", 1)])
def test_outbox_prevents_the_duplicate_that_a_direct_call_produces(tmp_path: Path, mode: str, articles: int) -> None:
    engine, run_id = _report_engine(tmp_path, mode)
    _crash_then_recover(engine, CrashPlan.parse("step.before_commit@publish:1"))
    assert engine.run(run_id).outcome == "done"
    published = CmsSink(cms_path(tmp_path / f"{mode}.db")).articles(run_id)
    assert len(published) == articles
    if mode == "outbox":
        assert published[0].request_id == f"{run_id}/publish#1/0"


@settings(max_examples=6, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    point=st.sampled_from(["run.after_claim", "step.after_start", "step.before_commit", "step.after_commit"]),
    nth=st.integers(1, 4),
    failures=st.lists(st.booleans(), min_size=4, max_size=4),
)
def test_any_single_crash_leads_to_the_same_final_state(
    tmp_path_factory: pytest.TempPathFactory, point: str, nth: int, failures: list[bool]
) -> None:
    """Property: whatever the crash point, the final state equals that of a run without crashes."""

    def scripted(ctx: ActivityContext, state: Count) -> None:
        if ctx.attempt == 1 and failures[state.n % len(failures)]:
            raise ConnectionError("injected")

    final = []
    for crash in (None, CrashPlan(point, nth=nth)):
        engine = make_engine(tmp_path_factory.mktemp("p"), counter_workflow(), lease_ttl=0.1,
                             resources={"hook": scripted})
        run_id = engine.start("counter", {"target": 3})
        if crash is None:
            engine.drain()
        else:
            worker = engine.worker("doomed", crash=CrashInjector(crash))
            try:
                worker.run_until_idle(horizon=5.0)
            except SimulatedCrash:
                pass
            worker.close()
            engine.drain(worker_id="rescuer")
        assert check_run(engine.store, run_id) == []
        assert engine.run(run_id).status is RunStatus.COMPLETED
        final.append(replay(engine.history(run_id)).data)
        engine.close()
    assert final[0] == final[1]
