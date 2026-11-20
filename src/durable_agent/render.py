"""Rich renderings: the audit timeline of a run, the runs table and a run's detail panel."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from rich.console import Group, RenderableType
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from .events import Event, EventType, RunStatus
from .state import RunState
from .store import OutboxRow, RunRow, SignalRow

EVENT_STYLE: dict[EventType, tuple[str, str]] = {
    EventType.RUN_STARTED: ("▶", "bold cyan"),
    EventType.STEP_SCHEDULED: ("○", "grey62"),
    EventType.STEP_STARTED: ("●", "blue"),
    EventType.STEP_COMPLETED: ("✔", "green"),
    EventType.STEP_FAILED: ("✖", "red"),
    EventType.TIMER_SET: ("◷", "magenta"),
    EventType.TIMER_FIRED: ("◉", "magenta"),
    EventType.GATE_OPENED: ("◆", "yellow"),
    EventType.SIGNAL_RECEIVED: ("⇒", "bold yellow"),
    EventType.CHECKPOINT_WRITTEN: ("▣", "grey50"),
    EventType.RUN_RECOVERED: ("↻", "bold magenta"),
    EventType.RUN_COMPLETED: ("■", "bold green"),
    EventType.RUN_FAILED: ("■", "bold red"),
    EventType.RUN_CANCELLED: ("□", "bold red"),
}
STEP_EVENTS = frozenset(
    {EventType.STEP_SCHEDULED, EventType.STEP_STARTED, EventType.STEP_COMPLETED, EventType.STEP_FAILED}
)
STATUS_STYLE = {
    RunStatus.PENDING: "grey62",
    RunStatus.RUNNING: "blue",
    RunStatus.WAITING: "yellow",
    RunStatus.COMPLETED: "green",
    RunStatus.FAILED: "red",
    RunStatus.CANCELLED: "red",
}


def _short(value: Any, limit: int = 48) -> str:
    text = str(value)
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _rel(ts: float | None, t0: float) -> str:
    if ts is None:
        return "-"
    delta = ts - t0
    return f"+{delta / 3600:.1f}h" if delta >= 3600 else f"+{delta:.2f}s"


def describe(event: Event, t0: float) -> str:
    """One-line human description of an event."""
    d = event.data
    kind = event.type
    if kind is EventType.RUN_STARTED:
        first_text = next((f"{k}={_short(v, 40)!r}" for k, v in d.get("input", {}).items() if isinstance(v, str)), "")
        return f"{d.get('workflow')} v{d.get('version', 1)} → {d.get('start')}  {first_text}".rstrip()
    if kind is EventType.STEP_SCHEDULED:
        nb = d.get("not_before")
        return f"attempt {event.attempt}" + (f" · not before {_rel(nb, t0)}" if nb and event.attempt != 1 else "")
    if kind is EventType.STEP_STARTED:
        return f"attempt {event.attempt}"
    if kind is EventType.STEP_COMPLETED:
        keys = ", ".join(d.get("result", {})) or "no changes"
        extra = f" · {d['outbox']} outbox msg" if d.get("outbox") else ""
        took = f"{d.get('duration_ms', 0):.0f} ms"
        return f"attempt {event.attempt} · {took} · set {_short(keys, 30)}{extra} → {d.get('next')}"
    if kind is EventType.STEP_FAILED:
        retry = "retry now" if not d.get("retry_in") else f"retry in {d['retry_in']:.2f}s"
        tail = f" → {d['next']}" if d.get("next") else (" · final" if d.get("final") else f" · {retry}")
        return f"attempt {event.attempt} · {d.get('error_type')}: {_short(d.get('message', ''), 46)}{tail}"
    if kind is EventType.TIMER_SET:
        return f"{d.get('kind')} timer for {_rel(d.get('fire_at'), t0)}"
    if kind is EventType.TIMER_FIRED:
        return f"{d.get('kind')} timer fired → {d.get('next')}"
    if kind is EventType.GATE_OPENED:
        who = f" · assignee {d['assignee']}" if d.get("assignee") else ""
        until = f" · times out at {_rel(d.get('timeout_at'), t0)}" if d.get("timeout_at") else ""
        return f"waiting for {' / '.join(d.get('signals', []))}{who}{until}"
    if kind is EventType.SIGNAL_RECEIVED:
        if d.get("ignored"):
            return f"{d.get('signal')} from {d.get('sender')} ignored: {_short(d['ignored'], 40)}"
        changed = f" · set {', '.join(d['result'])}" if d.get("result") else ""
        return f"{d.get('signal')} from {d.get('sender')} after {d.get('waited_s', 0):.2f}s{changed} → {d.get('next')}"
    if kind is EventType.CHECKPOINT_WRITTEN:
        return f"snapshot at seq {d.get('checkpoint_seq')} ({d.get('bytes', 0):,} bytes)"
    if kind is EventType.RUN_RECOVERED:
        where = f"checkpoint @{d['checkpoint_seq']}" if d.get("checkpoint_seq") else "full journal"
        return (f"took over from {d.get('from_worker')} · {where} + {d.get('replayed_events')} events "
                f"replayed in {d.get('replay_ms', 0):.1f} ms")
    if kind is EventType.RUN_COMPLETED:
        return f"outcome: {d.get('outcome')}"
    if kind is EventType.RUN_FAILED:
        return _short(d.get("error", ""), 70)
    return f"cancelled by {d.get('by')}: {d.get('reason') or 'no reason given'}"


def history_table(events: Sequence[Event], *, show_checkpoints: bool = True) -> Table:
    """The audit timeline of one run."""
    table = Table(box=None, pad_edge=False, show_edge=False, header_style="bold", expand=False)
    table.add_column("time", justify="right", style="grey62", no_wrap=True)
    table.add_column("seq", justify="right", style="grey50", no_wrap=True)
    table.add_column("worker", style="cyan", no_wrap=True)
    table.add_column("event", no_wrap=True)
    table.add_column("step", style="bold", no_wrap=True)
    table.add_column("detail", overflow="fold")
    if not events:
        return table
    t0 = events[0].ts
    for event in events:
        if event.type is EventType.CHECKPOINT_WRITTEN and not show_checkpoints:
            continue
        icon, style = EVENT_STYLE[event.type]
        dim = event.type in (EventType.CHECKPOINT_WRITTEN, EventType.STEP_SCHEDULED)
        table.add_row(
            f"{event.ts - t0:+.2f}s",
            str(event.seq),
            event.worker or "client",
            Text(f"{icon} {event.type.value}", style=style),
            event.step or "",
            Text(describe(event, t0), style="grey50" if dim else ""),
        )
    return table


def runs_table(rows: Sequence[RunRow]) -> Table:
    table = Table(header_style="bold", box=None, pad_edge=False)
    for name in ("run", "workflow", "status", "state", "waiting on", "lease", "events", "outcome"):
        table.add_column(name, no_wrap=name != "outcome")
    for r in rows:
        table.add_row(
            r.run_id,
            r.workflow,
            Text(r.status.value, style=STATUS_STYLE[r.status]),
            r.current_state or "",
            r.waiting_on or "",
            f"{r.lease_owner} (epoch {r.lease_epoch})" if r.lease_owner else "",
            str(r.last_seq),
            r.outcome or (_short(r.error, 40) if r.error else ""),
        )
    return table


@dataclass
class StepLine:
    key: str
    status: str = "scheduled"
    attempts: int = 0
    failures: int = 0
    last_error: str = ""


def step_summary(events: Sequence[Event]) -> list[StepLine]:
    """Per step key: last status, attempts started, failures and the last error, from the journal."""
    lines: dict[str, StepLine] = {}
    for e in events:
        if e.step is None or e.type not in STEP_EVENTS:
            continue
        line = lines.setdefault(e.step, StepLine(e.step))
        if e.type is EventType.STEP_STARTED:
            line.status, line.attempts = "started", max(line.attempts, e.attempt or 0)
        elif e.type is EventType.STEP_COMPLETED:
            line.status = "completed"
        elif e.type is EventType.STEP_FAILED:
            line.status, line.failures = "failed", line.failures + 1
            line.last_error = f"{e.data.get('error_type')}: {e.data.get('message', '')}"
    return list(lines.values())


def run_panel(
    row: RunRow, state: RunState, events: Sequence[Event], outbox: Sequence[OutboxRow], signals: Sequence[SignalRow]
) -> Panel:
    """Detail view of one run: status, steps with attempts, gate, outbox and signals."""
    head = Table.grid(padding=(0, 2))
    head.add_column(style="grey62")
    head.add_column()
    head.add_row("status", Text(row.status.value, style=STATUS_STYLE[row.status]))
    waiting = f"  (waiting on {row.waiting_on})" if row.waiting_on else ""
    head.add_row("state", f"{row.current_state or '-'}{waiting}")
    if row.lease_owner:
        head.add_row("lease", f"{row.lease_owner}, epoch {row.lease_epoch}")
    recoveries = f"{state.recoveries} crash recover{'y' if state.recoveries == 1 else 'ies'}"
    head.add_row("journal", f"{row.last_seq} events, {recoveries}")
    if row.outcome or row.error:
        head.add_row("result", f"{row.outcome or ''} {row.error or ''}".strip())
    steps = Table(header_style="bold", box=None, pad_edge=False)
    for name in ("step", "status", "attempts", "failures", "last error"):
        steps.add_column(name)
    for line in step_summary(events):
        steps.add_row(line.key, line.status, str(line.attempts), str(line.failures), _short(line.last_error, 50))
    parts: list[RenderableType] = [head, Text(""), steps]
    if outbox:
        box = Table(header_style="bold", box=None, pad_edge=False)
        for name in ("outbox key", "topic", "status", "attempts"):
            box.add_column(name)
        for m in outbox:
            box.add_row(m.key, m.topic, m.status, str(m.attempts))
        parts += [Text(""), box]
    if signals:
        sig = Table(header_style="bold", box=None, pad_edge=False)
        for name in ("signal", "from", "consumed at seq"):
            sig.add_column(name)
        for s in signals:
            sig.add_row(s.name, s.sender, str(s.consumed_seq) if s.consumed_seq else "pending")
        parts += [Text(""), sig]
    return Panel(Group(*parts), title=f"{row.run_id} · {row.workflow}", title_align="left", border_style="grey50")
