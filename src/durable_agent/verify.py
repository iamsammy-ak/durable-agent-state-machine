"""Invariant checks over a run's journal, checkpoints, index row and outbox.

Used by the tests and the chaos benchmark to show that crashes leave every run consistent.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence

from pydantic import BaseModel

from .errors import ReplayError
from .events import TERMINAL_EVENTS, Event, EventType
from .state import replay, restore
from .store import Store


class Violation(BaseModel):
    run_id: str
    rule: str
    detail: str


def check_journal(events: Sequence[Event]) -> list[Violation]:
    """Structural rules every journal must satisfy, whatever crashed along the way."""
    if not events:
        return [Violation(run_id="?", rule="non_empty", detail="the journal has no events")]
    run_id = events[0].run_id
    found: list[Violation] = []

    def add(rule: str, detail: str) -> None:
        found.append(Violation(run_id=run_id, rule=rule, detail=detail))

    for position, event in enumerate(events, start=1):
        if event.seq != position:
            add("contiguous_seq", f"position {position} holds seq {event.seq}")
            break
    if events[0].type is not EventType.RUN_STARTED:
        add("starts_with_run_started", f"first event is {events[0].type.value}")
    if sum(e.type is EventType.RUN_STARTED for e in events) != 1:
        add("single_run_started", "RunStarted appears more than once")
    terminal_at = [i for i, e in enumerate(events) if e.type in TERMINAL_EVENTS]
    if len(terminal_at) > 1:
        add("single_terminal_event", f"{len(terminal_at)} terminal events")
    if terminal_at and any(e.type is not EventType.CHECKPOINT_WRITTEN for e in events[terminal_at[0] + 1 :]):
        add("terminal_is_last", "events were appended after the run finished")

    scheduled: dict[str, set[int]] = defaultdict(set)
    started: dict[str, list[int]] = defaultdict(list)
    outcomes: dict[tuple[str, int], int] = defaultdict(int)
    completed: set[str] = set()
    for e in events:
        key, attempt = e.step or "", e.attempt or 0
        if e.type is EventType.STEP_SCHEDULED:
            scheduled[key].add(attempt)
        elif e.type is EventType.STEP_STARTED:
            if key in completed:
                add("completed_step_never_reruns", f"{key} attempt {attempt} started after the step completed")
            if attempt not in scheduled[key]:
                add("start_follows_schedule", f"{key} attempt {attempt} started without being scheduled")
            if started[key] and attempt <= started[key][-1]:
                add("attempts_increase", f"{key} attempt {attempt} started after attempt {started[key][-1]}")
            started[key].append(attempt)
        elif e.type in (EventType.STEP_COMPLETED, EventType.STEP_FAILED):
            if attempt not in started[key]:
                add("outcome_follows_start", f"{key} attempt {attempt}: {e.type.value} without StepStarted")
            outcomes[(key, attempt)] += 1
            if outcomes[(key, attempt)] > 1:
                add("one_outcome_per_attempt", f"{key} attempt {attempt} has several outcomes")
            if e.type is EventType.STEP_COMPLETED:
                if key in completed:
                    add("single_completion", f"{key} completed twice")
                completed.add(key)

    consumed = [e.data["signal_id"] for e in events if e.data.get("signal_id") is not None]
    if len(consumed) != len(set(consumed)):
        add("signal_consumed_once", "a signal was consumed more than once")
    try:
        replay(events)
    except ReplayError as exc:
        add("replayable", str(exc))
    return found


def check_run(store: Store, run_id: str) -> list[Violation]:
    """Journal rules plus: every checkpoint replays to the same state, the index row agrees with
    the journal, and every outbox message belongs to a committed step."""
    events = store.events(run_id)
    found = check_journal(events)
    if not events or any(v.rule == "replayable" for v in found):
        return found

    def add(rule: str, detail: str) -> None:
        found.append(Violation(run_id=run_id, rule=rule, detail=detail))

    full = replay(events).model_dump()
    for seq, text in store.checkpoints(run_id):
        base = restore(text)
        if base is None or base.last_seq != seq:
            add("checkpoint_readable", f"checkpoint @{seq} cannot be restored")
            continue
        if replay([e for e in events if e.seq > seq], base=base).model_dump() != full:
            add("checkpoint_replays_to_same_state", f"checkpoint @{seq} + tail differs from a full replay")
    row = store.get_run(run_id)
    if row.last_seq != full["last_seq"]:
        add("index_matches_journal", f"runs.last_seq={row.last_seq}, journal={full['last_seq']}")
    if row.status.terminal != (full["status"] in ("completed", "failed", "cancelled")):
        add("index_matches_journal", f"runs.status={row.status.value}, journal status={full['status']}")
    if row.current_state != full["current"]:
        add("index_matches_journal", f"runs.current_state={row.current_state}, journal={full['current']}")
    committed = {e.step for e in events if e.type is EventType.STEP_COMPLETED}
    for message in store.outbox(run_id=run_id):
        if message.step not in committed:
            add("outbox_from_committed_steps", f"message {message.key} has no StepCompleted")
    return found
