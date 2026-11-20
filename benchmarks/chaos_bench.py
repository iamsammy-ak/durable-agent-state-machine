"""Chaos benchmark: kill worker processes at seeded points across many research runs.

Two experiments with the same seed and kill schedule:

* ``outbox`` - the runtime as designed: the publish step's side effect goes through the outbox;
* ``direct`` - a baseline where the publish activity calls the CMS itself with a fresh request id.

Reproduce (about 10 minutes on a laptop; worker start-up time dominates):

    PYTHONPATH=src python benchmarks/chaos_bench.py --runs 200 --crashes 200

Writes benchmarks/results/chaos.json, benchmarks/results/chaos.md and assets/chaos.png.
"""

from __future__ import annotations

import argparse
import json
import platform
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from durable_agent.chaos import ChaosConfig, ChaosReport, run_chaos  # noqa: E402

RESULTS = ROOT / "benchmarks" / "results"
WORK = ROOT / "benchmarks" / ".work"
BLUE, ORANGE, INK, MUTED, GRID = "#2a78d6", "#eb6834", "#0b0b0b", "#52514e", "#e4e3df"


def environment() -> dict[str, object]:
    return {
        "python": platform.python_version(),
        "sqlite": sqlite3.sqlite_version,
        "os": f"{platform.system()} {platform.release()}",
        "cpu_count": __import__("os").cpu_count(),
    }


def chart(outbox: ChaosReport, direct: ChaosReport, path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, (left, right) = plt.subplots(1, 2, figsize=(11, 4.2), gridspec_kw={"width_ratios": [2.2, 1]})
    fig.patch.set_facecolor("white")
    values = outbox.recovery_seconds
    ttl = outbox.config.lease_ttl
    width = 0.05
    bins = [i * width for i in range(int(max(values + [ttl]) / width) + 3)]
    left.hist(values, bins=bins, color=BLUE, edgecolor="white", linewidth=1)
    left.axvline(ttl, color=MUTED, linestyle="--", linewidth=1.2)
    left.text(ttl - 0.02, left.get_ylim()[1] * 0.97, f"lease TTL {ttl:g}s", color=MUTED, va="top", ha="right",
              fontsize=9)
    left.set_xlim(0, max(values + [ttl]) + 0.15)
    left.set_title("Time from kill to takeover by another worker", loc="left", fontsize=11, color=INK)
    left.set_xlabel("seconds", color=MUTED)
    left.set_ylabel("killed runs", color=MUTED)
    p50, p95 = outbox.recovery.p50 or 0, outbox.recovery.p95 or 0
    left.text(0.98, 0.95, f"n = {outbox.recovery.count}\np50 = {p50:.2f}s\np95 = {p95:.2f}s",
              transform=left.transAxes, ha="right", va="top", fontsize=9, color=INK)

    names = ["direct call\n(baseline)", "outbox +\nidempotency key"]
    counts = [direct.duplicate_publications, outbox.duplicate_publications]
    bars = right.bar(names, counts, color=[ORANGE, BLUE], width=0.55, edgecolor="white", linewidth=2)
    for bar, count in zip(bars, counts, strict=True):
        right.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f"{count}", ha="center", va="bottom",
                   fontsize=11, color=INK, fontweight="bold")
    right.set_title("Runs published twice", loc="left", fontsize=11, color=INK)
    right.set_ylim(0, max(counts + [1]) * 1.3)
    right.set_ylabel("runs with a duplicate article", color=MUTED)

    for ax in (left, right):
        ax.set_facecolor("white")
        ax.grid(axis="y", color=GRID, linewidth=0.8)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        for side in ("left", "bottom"):
            ax.spines[side].set_color(GRID)
        ax.tick_params(colors=MUTED, labelsize=9)
    fig.suptitle(
        f"{outbox.kills} worker processes killed across {outbox.runs} runs (outbox mode; the direct baseline used "
        "the same seeded kill schedule)",
        x=0.01, ha="left", fontsize=10, color=MUTED,
    )
    fig.tight_layout()
    fig.savefig(path, dpi=110, facecolor="white")
    plt.close(fig)


def markdown(outbox: ChaosReport, direct: ChaosReport, command: str, env: dict[str, object]) -> str:
    def row(label: str, a: object, b: object) -> str:
        return f"| {label} | {a} | {b} |"

    cfg = outbox.config
    lines = [
        "# Chaos benchmark",
        "",
        f"Command: `{command}`",
        "",
        f"Environment: Python {env['python']}, SQLite {env['sqlite']}, {env['os']}, {env['cpu_count']} logical CPUs. "
        "Offline: deterministic stand-in LLM and a synthetic corpus; the search tool fails "
        f"{cfg.failure_rate:.0%} of attempts on purpose.",
        "",
        f"Setup: research-workflow runs are admitted two per worker (at least {cfg.runs}, more while kill plans "
        f"remain), {cfg.workers} worker processes that receive kill plans "
        f"plus {cfg.standby_workers} standby worker(s) that are never killed, lease TTL {cfg.lease_ttl:g}s, "
        f"seed {cfg.seed}. "
        f"Kill plans: {1 - cfg.random_kill_share:.0%} at named crash points (seeded point and hit number), "
        f"{cfg.random_kill_share:.0%} at a random time after the worker is ready; a planned worker whose crash "
        f"point never comes is killed after {cfg.plan_patience:g}s. Approver decisions are seeded: "
        f"{', '.join(f'{k} {v:.0%}' for k, v in cfg.decision_weights.items())}.",
        "",
        "| Metric | outbox (as designed) | direct call (baseline) |",
        "|---|---|---|",
        row("Runs", outbox.runs, direct.runs),
        row("Worker processes killed", outbox.kills, direct.kills),
        row("... at a named crash point / random time / unfired plan",
            " / ".join(str(outbox.kills_by_kind.get(k, 0)) for k in ("crash_point", "random_time", "unfired_plan")),
            " / ".join(str(direct.kills_by_kind.get(k, 0)) for k in ("crash_point", "random_time", "unfired_plan"))),
        row("Kills that interrupted a leased run", outbox.kills_holding_a_run, direct.kills_holding_a_run),
        row("Runs ending as their decision implies", f"{outbox.as_expected}/{outbox.runs} "
            f"({outbox.completion_rate:.1%})", f"{direct.as_expected}/{direct.runs} ({direct.completion_rate:.1%})"),
        row("**Runs published twice**", f"**{outbox.duplicate_publications}**", f"**{direct.duplicate_publications}**"),
        row("Approved runs never published", outbox.missing_publications, direct.missing_publications),
        row("CMS content differing from the journal", outbox.content_mismatches, direct.content_mismatches),
        row("Redeliveries absorbed by the idempotency key", outbox.redeliveries_absorbed, "n/a"),
        row("Journal invariant violations", len(outbox.violations), len(direct.violations)),
        row("Completed steps executed again", outbox.completed_step_reexecutions, direct.completed_step_reexecutions),
        row("Activity executions (StepStarted)", outbox.activity_executions, direct.activity_executions),
        row("In-doubt attempts re-run (WorkerLost)", outbox.in_doubt_attempts, direct.in_doubt_attempts),
        row("Retries exhausted", outbox.retries_exhausted, direct.retries_exhausted),
        row("Recovery time p50 / p95 / max (s)",
            f"{outbox.recovery.p50} / {outbox.recovery.p95} / {outbox.recovery.max}",
            f"{direct.recovery.p50} / {direct.recovery.p95} / {direct.recovery.max}"),
        row("Replay on takeover p50 / max (ms)", f"{outbox.replay_ms.p50} / {outbox.replay_ms.max}",
            f"{direct.replay_ms.p50} / {direct.replay_ms.max}"),
        row("Events / checkpoints written", f"{outbox.events} / {outbox.checkpoints}",
            f"{direct.events} / {direct.checkpoints}"),
        row("Wall time (s)", outbox.wall_seconds, direct.wall_seconds),
        "",
        "Failed attempts by type (outbox run): "
        + ", ".join(f"{k} {v}" for k, v in outbox.failures_by_type.items()) + ".",
        "",
        "Attempts needed by the flaky `gather` step (outbox run): "
        + ", ".join(f"{k} attempt{'s' if int(k) > 1 else ''}: {v} runs" for k, v in
                    outbox.gather_attempts_histogram.items()) + ".",
        "",
        "Kills by crash point (outbox run): "
        + ", ".join(f"`{k}` {v}" for k, v in outbox.kills_by_point.items()) + ".",
        "",
        "Recovery time is measured from the kill to the `RunRecovered` event written by the worker that took "
        "over. It is bounded below by the remaining lease (heartbeats renew it every TTL/3) and includes the "
        "poll interval and, when every healthy worker is busy or still starting, the start-up of a replacement.",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--runs", type=int, default=200)
    parser.add_argument("--crashes", type=int, default=200)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--lease-ttl", type=float, default=1.0)
    parser.add_argument("--report-only", action="store_true", help="re-render the chart and Markdown from chaos.json")
    args = parser.parse_args()
    if args.report_only:
        saved = json.loads((RESULTS / "chaos.json").read_text(encoding="utf-8"))
        outbox, direct = (ChaosReport.model_validate(saved[mode]) for mode in ("outbox", "direct"))
        (RESULTS / "chaos.md").write_text(markdown(outbox, direct, saved["command"], saved["environment"]),
                                          encoding="utf-8")
        chart(outbox, direct, ROOT / "assets" / "chaos.png")
        print("re-rendered benchmarks/results/chaos.md and assets/chaos.png")
        return
    command = (f"PYTHONPATH=src python benchmarks/chaos_bench.py --runs {args.runs} --crashes {args.crashes} "
               f"--workers {args.workers} --seed {args.seed} --lease-ttl {args.lease_ttl:g}")
    reports: dict[str, ChaosReport] = {}
    for mode in ("outbox", "direct"):
        config = ChaosConfig(runs=args.runs, crashes=args.crashes, workers=args.workers, seed=args.seed,
                             lease_ttl=args.lease_ttl, publish_mode=mode)
        print(f"== {mode}")
        started = time.time()
        reports[mode] = run_chaos(config, WORK / mode, progress=lambda line: print("  " + line, flush=True))
        r = reports[mode]
        print(f"  done in {time.time() - started:.0f}s: {r.as_expected}/{r.runs} as expected, kills {r.kills}, "
              f"duplicates {r.duplicate_publications}, violations {len(r.violations)}")
    RESULTS.mkdir(parents=True, exist_ok=True)
    env = environment()
    payload = {"command": command, "environment": env,
               **{mode: report.model_dump(mode="json") for mode, report in reports.items()}}
    (RESULTS / "chaos.json").write_text(json.dumps(payload, indent=1), encoding="utf-8")
    (RESULTS / "chaos.md").write_text(markdown(reports["outbox"], reports["direct"], command, env), encoding="utf-8")
    chart(reports["outbox"], reports["direct"], ROOT / "assets" / "chaos.png")
    print("wrote benchmarks/results/chaos.json, chaos.md and assets/chaos.png")


if __name__ == "__main__":
    main()
