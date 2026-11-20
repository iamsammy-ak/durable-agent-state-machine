from __future__ import annotations

import io
from pathlib import Path

from helpers import Count, counter_workflow, make_engine
from rich.console import Console

from durable_agent import ActivityContext, EventType
from durable_agent.render import EVENT_STYLE, describe, history_table, step_summary


def _flaky(ctx: ActivityContext, state: Count) -> None:
    if ctx.attempt == 1:
        raise ConnectionError("blip")


def test_step_summary_counts_attempts_and_failures(tmp_path: Path) -> None:
    engine = make_engine(tmp_path, counter_workflow(), resources={"hook": _flaky})
    run_id = engine.start("counter", {"target": 2})
    engine.drain()
    lines = step_summary(engine.history(run_id))
    assert [(line.key, line.status, line.attempts, line.failures) for line in lines] == [
        ("tick#1", "completed", 2, 1),
        ("tick#2", "completed", 2, 1),
    ]
    assert lines[0].last_error == "ConnectionError: blip"


def test_every_event_type_renders_in_the_timeline(tmp_path: Path) -> None:
    engine = make_engine(tmp_path, counter_workflow(gate=True, gate_timeout=0.1), resources={"hook": _flaky})
    timed_out = engine.start("counter", {"target": 1})
    cancelled = engine.start("counter", {"target": 1, "note": "x"})
    engine.cancel(cancelled, "not needed", sender="ops")
    engine.drain()
    events = engine.history(timed_out) + engine.history(cancelled)
    assert set(EVENT_STYLE) == set(EventType)
    console = Console(record=True, width=160, file=io.StringIO())
    console.print(history_table(engine.history(timed_out)))
    text = console.export_text()
    for expected in ("RunStarted", "retry in", "gate_timeout timer fired → timed_out", "outcome: timed_out"):
        assert expected in text
    cancel = [e for e in events if e.type is EventType.RUN_CANCELLED][0]
    assert describe(cancel, cancel.ts) == "cancelled by ops: not needed"
