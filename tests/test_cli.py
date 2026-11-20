from __future__ import annotations

from pathlib import Path

import pytest
from rich.console import Console
from typer.testing import CliRunner

from durable_agent import CrashInjector, CrashPlan, SimulatedCrash
from durable_agent import cli as cli_module
from durable_agent.cli import app
from durable_agent.research import build_engine

runner = CliRunner()


@pytest.fixture(autouse=True)
def _wide_console(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_module, "console", Console(width=200, force_terminal=False))


def invoke(*args: str) -> str:
    result = runner.invoke(app, list(args))
    assert result.exit_code == 0, result.output
    return result.output


def test_diagram_prints_the_state_machine() -> None:
    out = invoke("diagram")
    assert out.startswith("stateDiagram-v2") and "approval --> escalation : timeout" in out


def test_run_waits_for_approval_then_approve_with_edit_publishes(tmp_path: Path) -> None:
    db = str(tmp_path / "cli.db")
    out = invoke("run", "solid-state batteries", "--db", db, "--run-id", "cli-1", "--fail-first", "rate_limit")
    assert "cli-1 is waiting at approval" in out
    assert "cli-1" in invoke("status", "--db", db)
    out = invoke("approve", "cli-1", "--by", "alice", "--title", "Batteries, briefly", "--db", db)
    assert "edit from alice delivered" in out and "cli-1 completed: done" in out
    history = invoke("history", "cli-1", "--db", db)
    for expected in ("RateLimited", "SignalReceived", "edit from alice", "RunCompleted"):
        assert expected in history
    assert "delivered" in invoke("status", "cli-1", "--db", db)


def test_reject_and_cancel(tmp_path: Path) -> None:
    db = str(tmp_path / "cli.db")
    invoke("run", "urban heat islands", "--db", db, "--run-id", "r-1")
    assert "r-1 completed: rejected" in invoke("reject", "r-1", "--by", "bob", "--reason", "no", "--db", db)
    invoke("run", "urban heat islands", "--db", db, "--run-id", "r-2")
    assert "r-2 cancelled" in invoke("cancel", "r-2", "--reason", "duplicate request", "--db", db)


def test_resume_continues_a_run_whose_worker_crashed(tmp_path: Path) -> None:
    db = tmp_path / "cli.db"
    engine = build_engine(db, offline=True)
    run_id = engine.start("research_report", {"topic": "heat pumps in cold climates"}, run_id="crashy")
    worker = engine.worker("cli", crash=CrashInjector(CrashPlan.parse("step.before_commit@draft")))
    with pytest.raises(SimulatedCrash):
        worker.drive(run_id)
    worker.close()
    out = invoke("resume", run_id, "--db", str(db), "--steal")
    assert "recovered from cli" in out and "crashy is waiting at approval" in out


def test_errors_exit_non_zero(tmp_path: Path) -> None:
    db = str(tmp_path / "cli.db")
    assert runner.invoke(app, ["history", "nope", "--db", db]).exit_code == 1
    assert runner.invoke(app, ["approve", "nope", "--by", "x", "--db", db]).exit_code == 1
    assert runner.invoke(app, ["run", "x", "--db", db]).exit_code == 1  # topic too short for the model
