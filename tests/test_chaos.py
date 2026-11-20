"""Real process kills: worker subprocesses are stopped with Process.kill() (a smaller version of the benchmark)."""

from __future__ import annotations

import io
from pathlib import Path

import pytest
from rich.console import Console

from durable_agent.chaos import ChaosConfig, make_plans, run_chaos
from durable_agent.crash import CrashPlan
from durable_agent.demo import run_demo


def test_kill_schedule_is_seeded() -> None:
    config = ChaosConfig(crashes=50, seed=11)
    plans = make_plans(config)
    assert plans == make_plans(config) and len(plans) == 50
    assert any(isinstance(p, CrashPlan) for p in plans) and any(isinstance(p, float) for p in plans)


@pytest.mark.chaos
def test_killed_worker_processes_lose_nothing_and_duplicate_nothing(tmp_path: Path) -> None:
    config = ChaosConfig(
        runs=5, crashes=4, workers=2, standby_workers=1, seed=3, lease_ttl=0.5,
        approval_timeout=2.0, escalation_timeout=1.0, plan_patience=4.0, max_seconds=150,
    )
    report = run_chaos(config, tmp_path / "chaos")
    assert not report.timed_out
    assert report.kills >= 1
    assert report.completion_rate == 1.0, report.unexpected_runs
    assert report.duplicate_publications == 0
    assert report.missing_publications == 0 and report.content_mismatches == 0
    assert report.completed_step_reexecutions == 0
    assert report.violations == []


@pytest.mark.chaos
def test_demo_survives_a_real_kill(tmp_path: Path) -> None:
    console = Console(record=True, width=120, file=io.StringIO())
    assert run_demo(console, tmp_path / "demo.db", lease_ttl=0.5)
    text = console.export_text()
    assert "worker-a killed with Process.kill()" in text
    assert "kept 1 article" in text and "all invariants hold" in text
