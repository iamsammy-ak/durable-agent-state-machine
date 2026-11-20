"""Command-line interface: ``durable-agent`` (or ``python -m durable_agent``)."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import NoReturn

import typer
from rich.console import Console

from .crash import CRASH_POINTS, CrashInjector, CrashMode, CrashPlan
from .engine import Engine
from .errors import DurableAgentError
from .events import RunStatus
from .executor import AdvanceReport
from .render import history_table, run_panel, runs_table

DEFAULT_DB = Path(os.getenv("DURABLE_AGENT_DB", ".durable-agent/runs.db"))
CLI_WORKER = "cli"

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Durable execution for agent workflows: checkpoints, crash recovery, retries and approval gates.",
)
console = Console()

DbOption = typer.Option(DEFAULT_DB, "--db", help="SQLite database shared by all workers.")
TtlOption = typer.Option(5.0, "--ttl", help="Lease TTL in seconds: how long a dead worker's run stays blocked.")
OfflineOption = typer.Option(False, "--offline", help="Use the offline stand-in even if an API key is set.")


def _engine(db: Path, *, ttl: float = 5.0, offline: bool = False, checkpoint_every: int = 1) -> Engine:
    from .research import build_engine

    return build_engine(db, offline=True if offline else None, lease_ttl=ttl, checkpoint_every=checkpoint_every)


def _fail(message: str) -> NoReturn:
    console.print(f"[red]error:[/] {message}")
    raise typer.Exit(1)


def _explain(engine: Engine, run_id: str, reports: list[AdvanceReport]) -> None:
    """Print what a drive did and where the run stands now."""
    for report in reports:
        if report.recovered_from:
            where = f"checkpoint @seq {report.checkpoint_seq}" if report.checkpoint_seq else "the full journal"
            console.print(f"[magenta]↻[/] recovered from [bold]{report.recovered_from}[/]: {where} + "
                          f"{report.replayed_events} events replayed in {report.replay_ms:.1f} ms")
    row = engine.run(run_id)
    if row.status is RunStatus.WAITING and (row.waiting_on or "").startswith("signal:"):
        gate = (row.waiting_on or "").split(":", 1)[1]
        console.print(f"[yellow]◆[/] [bold]{run_id}[/] is waiting at [bold]{gate}[/]. Decide with:\n"
                      f"    durable-agent approve {run_id} --by <name>   (add --title/--body to edit)\n"
                      f"    durable-agent reject {run_id} --by <name> --reason <text>")
    elif row.status.terminal:
        style = "green" if row.status is RunStatus.COMPLETED else "red"
        detail = row.outcome or ""
        if row.error:
            detail += f" ({row.error})"
        console.print(f"[{style}]■[/] [bold]{run_id}[/] {row.status.value}: {detail}")
    else:
        console.print(f"[blue]●[/] [bold]{run_id}[/] is {row.status.value} at {row.current_state} "
                      f"({row.waiting_on or 'in progress'}); a worker will continue it.")


def _drive(engine: Engine, run_id: str, *, steal: bool = False) -> None:
    with engine.worker(CLI_WORKER) as worker:
        reports = worker.drive(run_id, steal=steal)
    _explain(engine, run_id, reports)


@app.command()
def demo(
    db: Path = typer.Option(Path(".durable-agent/demo.db"), "--db", help="Demo database (recreated)."),
    ttl: float = typer.Option(1.0, "--ttl", help="Lease TTL for the demo workers."),
) -> None:
    """Scripted scenario: a killed worker, a takeover, an approval gate and an exactly-once publish."""
    from .demo import run_demo

    ok = run_demo(console, db, lease_ttl=ttl)
    raise typer.Exit(0 if ok else 1)


@app.command()
def run(
    topic: str = typer.Argument(..., help="What the research brief is about."),
    db: Path = DbOption,
    run_id: str | None = typer.Option(None, "--run-id", help="Explicit run id."),
    failure_rate: float = typer.Option(0.0, "--failure-rate", help="Injected search failure probability."),
    fail_first: str = typer.Option("", "--fail-first", help="Scripted search failures, e.g. 'rate_limit,timeout'."),
    approval_timeout: float = typer.Option(86400.0, "--approval-timeout", help="Seconds before escalation."),
    escalation_timeout: float = typer.Option(86400.0, "--escalation-timeout", help="Seconds before auto-reject."),
    crash_at: str = typer.Option(
        "", "--crash-at", help="Kill this process at a crash point, e.g. step.before_commit@draft."
    ),
    no_drive: bool = typer.Option(False, "--no-drive", help="Only create the run; leave it to workers."),
    ttl: float = TtlOption,
    offline: bool = OfflineOption,
) -> None:
    """Start a research run and drive it in this process until it waits for a person or finishes."""
    engine = _engine(db, ttl=ttl, offline=offline)
    options = {
        "failure_rate": failure_rate,
        "scripted_failures": [f.strip() for f in fail_first.split(",") if f.strip()],
        "approval_timeout": approval_timeout,
        "escalation_timeout": escalation_timeout,
    }
    try:
        rid = engine.start("research_report", {"topic": topic, "options": options}, run_id=run_id)
    except (DurableAgentError, ValueError) as exc:
        _fail(str(exc))
    console.print(f"[bold cyan]▶[/] started [bold]{rid}[/] ({topic})")
    if no_drive:
        return
    crash = CrashInjector(CrashPlan.parse(crash_at), CrashMode.EXIT) if crash_at else None
    if crash is not None:
        console.print(f"[red]armed:[/] this process will die at {crash.plan}; continue with "
                      f"[bold]durable-agent resume {rid}[/]")
    with engine.worker(CLI_WORKER, crash=crash) as worker:
        reports = worker.drive(rid)
    _explain(engine, rid, reports)


@app.command()
def worker(
    db: Path = DbOption,
    worker_id: str | None = typer.Option(None, "--id", help="Worker id (default: random)."),
    ttl: float = TtlOption,
    poll: float = typer.Option(0.2, "--poll", help="Seconds between polls when idle."),
    max_seconds: float | None = typer.Option(None, "--max-seconds", help="Exit after this long."),
    crash_at: str = typer.Option("", "--crash-at", help=f"Fault injection: one of {', '.join(CRASH_POINTS)}."),
    crash_mode: CrashMode = typer.Option(CrashMode.EXIT, "--crash-mode", help="exit, or wait to be killed."),
    checkpoint_every: int = typer.Option(1, "--checkpoint-every", help="Snapshot after this many transitions."),
    offline: bool = OfflineOption,
    quiet: bool = typer.Option(False, "--quiet", help="No log output."),
) -> None:
    """Poll the database for ready runs and drive them; deliver outbox messages. Ctrl+C stops it."""
    from .worker_main import run_worker

    if not quiet:
        console.print(f"polling {db} (lease TTL {ttl:g}s). Ctrl+C to stop.")
    stats = run_worker(db, worker_id=worker_id, lease_ttl=ttl, poll=poll, max_seconds=max_seconds,
                       crash_at=crash_at, crash_mode=crash_mode, checkpoint_every=checkpoint_every,
                       offline=offline, log=not quiet)
    if not quiet:
        console.print(f"claims {stats.claims} · recoveries {stats.recoveries} · attempts {stats.attempts} · "
                      f"messages delivered {stats.messages_delivered} · leases lost {stats.leases_lost}")


@app.command()
def status(
    run_id: str | None = typer.Argument(None, help="Show one run in detail."),
    db: Path = DbOption,
) -> None:
    """List runs, or show one run's steps, attempts, outbox and signals."""
    engine = _engine(db, offline=True)
    if run_id is None:
        rows = engine.runs()
        console.print(runs_table(rows) if rows else "no runs yet")
        return
    try:
        console.print(run_panel(engine.run(run_id), engine.state(run_id), engine.history(run_id),
                                engine.outbox(run_id), engine.signals(run_id)))
    except DurableAgentError as exc:
        _fail(f"{type(exc).__name__}: {exc}")


@app.command()
def history(
    run_id: str = typer.Argument(..., help="Run id."),
    db: Path = DbOption,
    checkpoints: bool = typer.Option(True, "--checkpoints/--no-checkpoints", help="Show CheckpointWritten rows."),
) -> None:
    """The run's audit timeline: every event in the journal, in order."""
    engine = _engine(db, offline=True)
    events = engine.history(run_id)
    if not events:
        _fail(f"no run {run_id!r} in {db}")
    console.print(history_table(events, show_checkpoints=checkpoints))


def _signal(run_id: str, name: str, payload: dict[str, str], by: str, db: Path, drive: bool, ttl: float,
            offline: bool) -> None:
    engine = _engine(db, ttl=ttl, offline=offline)
    try:
        engine.signal(run_id, name, payload, sender=by)
    except DurableAgentError as exc:
        _fail(str(exc))
    console.print(f"[bold yellow]⇒[/] {name} from {by} delivered to {run_id}")
    if drive:
        _drive(engine, run_id)


@app.command()
def approve(
    run_id: str = typer.Argument(..., help="Run id."),
    by: str = typer.Option(..., "--by", help="Who approves (recorded in the audit trail)."),
    note: str = typer.Option("", "--note"),
    title: str | None = typer.Option(None, "--title", help="Approve with a new title (an edit)."),
    body: str | None = typer.Option(None, "--body", help="Approve with a new body (an edit)."),
    db: Path = DbOption,
    drive: bool = typer.Option(True, "--drive/--no-drive", help="Continue the run in this process."),
    ttl: float = TtlOption,
    offline: bool = OfflineOption,
) -> None:
    """Approve a run waiting at a gate (with --title/--body it becomes an edit)."""
    payload: dict[str, str] = {"note": note}
    if title is not None:
        payload["title"] = title
    if body is not None:
        payload["body"] = body
    _signal(run_id, "edit" if len(payload) > 1 else "approve", payload, by, db, drive, ttl, offline)


@app.command()
def reject(
    run_id: str = typer.Argument(..., help="Run id."),
    by: str = typer.Option(..., "--by", help="Who rejects."),
    reason: str = typer.Option("", "--reason"),
    db: Path = DbOption,
    drive: bool = typer.Option(True, "--drive/--no-drive", help="Continue the run in this process."),
    ttl: float = TtlOption,
    offline: bool = OfflineOption,
) -> None:
    """Reject a run waiting at a gate."""
    _signal(run_id, "reject", {"reason": reason}, by, db, drive, ttl, offline)


@app.command()
def cancel(
    run_id: str = typer.Argument(..., help="Run id."),
    by: str = typer.Option("cli", "--by"),
    reason: str = typer.Option("", "--reason"),
    db: Path = DbOption,
) -> None:
    """Ask a run to stop at its next step boundary."""
    engine = _engine(db, offline=True)
    try:
        engine.cancel(run_id, reason, sender=by)
    except DurableAgentError as exc:
        _fail(str(exc))
    console.print(f"cancellation requested for {run_id}")
    _drive(engine, run_id)


@app.command()
def resume(
    run_id: str = typer.Argument(..., help="Run id."),
    db: Path = DbOption,
    steal: bool = typer.Option(False, "--steal", help="Take the lease now (only if its worker is surely dead)."),
    ttl: float = TtlOption,
    offline: bool = OfflineOption,
) -> None:
    """Continue a run from its last checkpoint (waits for a dead worker's lease to expire)."""
    engine = _engine(db, ttl=ttl, offline=offline)
    try:
        row = engine.run(run_id)
    except DurableAgentError as exc:
        _fail(f"{type(exc).__name__}: {exc}")
    if row.lease_owner and not steal:
        console.print(f"[grey62]waiting for the lease of {row.lease_owner} to expire...[/]")
    _drive(engine, run_id, steal=steal)


@app.command()
def diagram() -> None:
    """Print the research workflow as a Mermaid state diagram (generated from its definition)."""
    from .research import research_workflow

    typer.echo(research_workflow.to_mermaid(), nl=False)


def main() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None and (getattr(stream, "encoding", "") or "").lower() not in ("utf-8", "utf8"):
            reconfigure(encoding="utf-8", errors="replace")
    app()
