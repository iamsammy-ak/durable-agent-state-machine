from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest
from helpers import Count, counter_workflow, make_engine

from durable_agent import (
    Event,
    EventType,
    LeaseLost,
    ReplayError,
    RunFinished,
    RunNotFound,
    RunState,
    RunStatus,
    Store,
    replay,
)
from durable_agent.state import restore, snapshot


def _started(run_id: str = "r1") -> list[Event]:
    return [
        Event(run_id=run_id, seq=1, type=EventType.RUN_STARTED, ts=1.0,
              data={"workflow": "counter", "start": "tick", "input": {"n": 0, "target": 1}}),
        Event(run_id=run_id, seq=2, type=EventType.STEP_SCHEDULED, ts=1.0, step="tick#1", attempt=1),
    ]


def test_replay_folds_events_into_state() -> None:
    state = replay(_started())
    assert state.current == "tick" and state.visits == {"tick": 1}
    assert state.steps["tick#1"].status == "scheduled" and state.last_seq == 2


def test_replay_rejects_gaps_foreign_events_and_events_after_the_end() -> None:
    events = _started()
    with pytest.raises(ReplayError, match="gap"):
        replay([events[0], events[1].model_copy(update={"seq": 3})])
    with pytest.raises(ReplayError):
        replay(events, base=RunState(run_id="other"))
    done = Event(run_id="r1", seq=3, type=EventType.RUN_COMPLETED, ts=2.0, data={"outcome": "done"})
    late = Event(run_id="r1", seq=4, type=EventType.STEP_STARTED, ts=2.0, step="tick#1", attempt=1)
    with pytest.raises(ReplayError, match="after the run finished"):
        replay([*events, done, late])


def test_snapshot_round_trip_and_version_guard() -> None:
    state = replay(_started())
    assert restore(snapshot(state)) == state
    assert restore(snapshot(state).replace('"v":1', '"v":999')) is None
    assert restore("not json") is None


def test_store_uses_wal_and_the_journal_is_append_only(tmp_path: Path) -> None:
    engine = make_engine(tmp_path, counter_workflow())
    run_id = engine.start("counter", {"target": 1})
    store = engine.store
    assert store.conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    with pytest.raises(sqlite3.DatabaseError, match="append-only"):
        store.conn.execute("UPDATE events SET type = 'RunCompleted' WHERE run_id = ?", (run_id,))
    with pytest.raises(sqlite3.DatabaseError, match="append-only"):
        store.conn.execute("DELETE FROM events WHERE run_id = ?", (run_id,))


def test_only_one_worker_can_hold_a_lease_and_expiry_hands_it_over(tmp_path: Path) -> None:
    clock = [1000.0]
    store = Store(tmp_path / "s.db", clock=lambda: clock[0])
    engine = make_engine(tmp_path, counter_workflow())
    engine.store.close()
    engine.store = store  # drive time by hand
    run_id = engine.start("counter")
    first = store.claim("a", ttl=5)
    assert first is not None and first.epoch == 1 and first.previous_owner is None
    assert store.claim("b", ttl=5) is None
    clock[0] += 6
    second = store.claim("b", ttl=5)
    assert second is not None and second.run_id == run_id
    assert second.epoch == 2 and second.previous_owner == "a"
    assert not store.extend_lease(first, ttl=5)  # the old holder's heartbeat now fails
    with pytest.raises(LeaseLost), store.run_tx(engine.state(run_id), worker="a", lease=first):
        pass


def test_signals_need_a_live_run(tmp_path: Path) -> None:
    engine = make_engine(tmp_path, counter_workflow(gate=True))
    with pytest.raises(RunNotFound):
        engine.store.add_signal("missing", "approve", {}, "x")
    run_id = engine.start("counter", {"target": 1})
    engine.cancel(run_id, "stop")
    engine.drain()
    assert engine.run(run_id).status is RunStatus.CANCELLED
    with pytest.raises(RunFinished):
        engine.signal(run_id, "approve")


def test_a_signal_that_races_ahead_of_parking_wakes_the_run(tmp_path: Path) -> None:
    engine = make_engine(tmp_path, counter_workflow(gate=True))
    run_id = engine.start("counter", {"target": 1})
    store = engine.store
    lease = store.claim("w", ttl=5)
    assert lease is not None
    state = engine.state(run_id)
    store.add_signal(run_id, "approve", {}, "fast human")  # arrives while the run is still held
    with store.run_tx(state, worker="w", lease=lease) as tx:
        tx.release(ready_at=None, waiting_on="signal:review", wake_on=["approve"])
    row = engine.run(run_id)
    assert row.status is RunStatus.WAITING and row.ready_at is not None  # not parked forever


def test_outbox_claims_are_exclusive_until_they_expire(tmp_path: Path) -> None:
    engine = make_engine(tmp_path, counter_workflow(emit=True))
    run_id = engine.start("counter", {"target": 1})
    with engine.worker("w") as worker:
        lease = worker.store.claim("w", ttl=5)
        assert lease is not None
        worker.process(lease)  # runs the step; leaves its outbox message undelivered
    store = engine.store
    first = store.claim_outbox("a", ttl=0.2)
    assert [m.key for m in first] == [f"{run_id}/tick#1/0"]
    assert store.claim_outbox("b", ttl=0.2) == []
    time.sleep(0.3)
    again = store.claim_outbox("b", ttl=5)
    assert len(again) == 1 and again[0].attempts == 2
    assert not store.outbox_delivered(first[0].id, "a")  # a's claim expired
    assert store.outbox_delivered(again[0].id, "b")


def test_count_model_default_is_valid() -> None:
    assert Count().target == 3
