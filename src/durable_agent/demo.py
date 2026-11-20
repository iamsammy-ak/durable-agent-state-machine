"""Scripted demo: a research run survives a killed worker, waits for an editor, and publishes once."""

from __future__ import annotations

import time
from collections import Counter
from pathlib import Path

from rich.console import Console
from rich.table import Table

from .crash import CrashInjector, CrashMode, CrashPlan, SimulatedCrash
from .events import Event, EventType
from .research import CmsSink, ReportOptions, ReportState, build_engine, cms_path
from .supervisor import WorkerProcess, worker_command
from .verify import check_run

DEMO_RUN_ID = "brief-001"
DEMO_TOPIC = "heat pumps in cold climates"


def _remove_db(db_path: Path) -> None:
    for base in (db_path, cms_path(db_path)):
        for suffix in ("", "-wal", "-shm"):
            Path(str(base) + suffix).unlink(missing_ok=True)
    db_path.with_suffix(".worker-a.log").unlink(missing_ok=True)


def _attempt_lines(events: list[Event], worker: str) -> list[str]:
    """Summarise what one worker did, step by step."""
    lines: dict[str, list[str]] = {}
    for e in events:
        if e.worker != worker or e.step is None:
            continue
        marks = lines.setdefault(e.step, [])
        if e.type is EventType.STEP_COMPLETED:
            marks.append(f"[green]✔[/] attempt {e.attempt}")
        elif e.type is EventType.STEP_FAILED:
            marks.append(f"[red]✖[/] attempt {e.attempt} {e.data.get('error_type')}")
    started = {e.step for e in events if e.type is EventType.STEP_STARTED and e.worker == worker}
    finished = {e.step for e in events if e.type is EventType.STEP_COMPLETED}
    for step in started - finished:
        lines.setdefault(step or "", []).append("[blue]●[/] attempt 1 in progress")
    return [f"    {step:<10} " + "  ".join(marks) for step, marks in lines.items() if marks]


def run_demo(console: Console, db_path: Path, *, lease_ttl: float = 1.0) -> bool:
    """Run the scripted scenario. Returns True when every invariant held."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    _remove_db(db_path)
    clock = time.perf_counter()
    engine = build_engine(db_path, offline=True, lease_ttl=lease_ttl)
    console.rule("[bold]Durable Agent State Machine[/] · demo")
    console.print("[grey62]Offline run: the language model is a deterministic stand-in and the search corpus is "
                  "synthetic.[/]\n")

    options = ReportOptions(scripted_failures=["rate_limit", "timeout"])
    run_id = engine.start("research_report", ReportState(topic=DEMO_TOPIC, options=options), run_id=DEMO_RUN_ID)
    console.print(f"[bold cyan]▶[/] Started [bold]{run_id}[/]: a research brief on [italic]{DEMO_TOPIC}[/]. "
                  "The search tool is set to fail twice (HTTP 429, then a hang).")

    # 1. worker-a runs in its own process and is killed while the draft result is uncommitted.
    plan = CrashPlan("step.before_commit", node="draft")
    worker_a = WorkerProcess(
        worker_command(db_path, "worker-a", lease_ttl=lease_ttl, poll=0.05, crash=plan, max_seconds=60),
        "worker-a",
        log_path=db_path.with_suffix(".worker-a.log"),
    )
    reached = worker_a.wait_for_marker(timeout=60)
    if reached is None:
        worker_a.kill()
        raise RuntimeError("worker-a exited before reaching its crash point")
    killed_at = worker_a.kill()
    console.print("\n[bold]worker-a[/] (separate process):")
    for line in _attempt_lines(engine.history(run_id), "worker-a"):
        console.print(line)
    console.print(f"[bold red]✖ worker-a killed[/] with Process.kill() at [bold]{reached.split()[0]}[/] in "
                  f"{reached.split()[1]}: the draft was written but never committed, and the lease is still held.")

    # 2. worker-b takes over once the lease expires, and parks the run at the approval gate.
    worker_b = engine.worker("worker-b", crash=CrashInjector(CrashPlan("outbox.after_deliver"), CrashMode.RAISE))
    reports = worker_b.drive(run_id, horizon=0.5)
    recovered = next(r for r in reports if r.recovered_from)
    takeover = next(e.ts for e in engine.history(run_id) if e.type is EventType.RUN_RECOVERED) - killed_at
    console.print(f"\n[bold magenta]↻ worker-b[/] took over {takeover:.2f}s after the kill (lease TTL {lease_ttl:g}s): "
                  f"checkpoint @seq {recovered.checkpoint_seq} + {recovered.replayed_events} events replayed in "
                  f"{recovered.replay_ms:.1f} ms.")
    console.print("    plan#1 and gather#1 did not run again; draft#1 attempt 1 was in doubt, so it was recorded "
                  "as WorkerLost and attempt 2 ran.")
    row = engine.run(run_id)
    console.print(f"[yellow]◆ Waiting[/] at gate [bold]{row.current_state}[/] for the editor. The run holds no "
                  "worker while it waits; its timeout is a persisted timer.")

    # 3. A person edits and approves; the publish step queues its side effect in the outbox.
    engine.signal(run_id, "edit", {"title": "Cold-climate heat pumps: what the evidence shows",
                                   "note": "sharper title"}, sender="alice")
    console.print("\n[bold yellow]⇒ alice[/] sends [bold]edit[/] with a new title.")
    try:
        worker_b.drive(run_id, horizon=0.5)
        crashed = False
    except SimulatedCrash:
        crashed = True
    worker_b.close()
    if crashed:
        console.print("[green]✔[/] publish#1 committed with its outbox message in the same transaction.")
        console.print("[bold red]✖ worker-b crashed[/] after the CMS accepted the article and before the outbox "
                      "row was marked delivered.")

    # 4. worker-c redelivers; the CMS deduplicates by the idempotency key.
    with engine.worker("worker-c") as worker_c:
        worker_c.run_until_idle(horizon=lease_ttl * 3)
    cms = CmsSink(cms_path(db_path))
    articles = cms.articles(run_id)
    console.print(f"[green]✔ worker-c[/] redelivered after the claim expired; the CMS received "
                  f"{cms.call_count(run_id)} calls with the same key and kept {len(articles)} article.\n")

    events = engine.history(run_id)
    starts = Counter((e.step or "").split("#")[0] for e in events if e.type is EventType.STEP_STARTED)
    failures = [f"{(e.step or '').split('#')[0]} {e.data.get('error_type')}" for e in events
                if e.type is EventType.STEP_FAILED]
    violations = check_run(engine.store, run_id)
    table = Table(title="Result", title_justify="left", show_header=False, box=None, pad_edge=False)
    table.add_column(style="grey62")
    table.add_column()
    table.add_row("outcome", f"{engine.run(run_id).status.value}: {engine.run(run_id).outcome}")
    table.add_row("activity executions", " · ".join(f"{k} {v}" for k, v in starts.items()))
    table.add_row("failed attempts", ", ".join(failures))
    table.add_row("completed steps re-run", "0" if not any(v.rule == "completed_step_never_reruns"
                                                          for v in violations) else "VIOLATED")
    table.add_row("published articles", f"{len(articles)} (duplicates: {max(0, len(articles) - 1)})")
    table.add_row("journal", f"{len(events)} events · "
                  f"{sum(e.type is EventType.CHECKPOINT_WRITTEN for e in events)} checkpoints · "
                  + ("all invariants hold" if not violations else f"{len(violations)} violations"))
    table.add_row("wall time", f"{time.perf_counter() - clock:.1f}s")
    console.print(table)
    console.print(f"\nFull audit trail: [bold]durable-agent history {run_id} --db {db_path.as_posix()}[/]")
    engine.close()
    return not violations and len(articles) == 1
