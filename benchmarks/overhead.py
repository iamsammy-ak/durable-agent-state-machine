"""Persistence overhead per step, and the cost of rebuilding a run (replay) with and without checkpoints.

A loop workflow runs N no-op steps (state: a counter plus ~1 KB of text, like an agent's notes).
The same steps are also run in memory - activity call, result validation, guard evaluation, no
persistence - and the difference is the cost of durability. For the replay measurement the worker
is crashed five steps before the end, so rebuilding the run is what a recovering worker does.

Reproduce:

    PYTHONPATH=src python benchmarks/overhead.py

Writes benchmarks/results/overhead.json, benchmarks/results/overhead.md and assets/overhead.png.
"""

from __future__ import annotations

import json
import os
import platform
import shutil
import sqlite3
import statistics
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pydantic import BaseModel  # noqa: E402

from durable_agent import (  # noqa: E402
    ActivityContext,
    CrashInjector,
    CrashPlan,
    Engine,
    Outbox,
    SimulatedCrash,
    Workflow,
    load_state,
)
from durable_agent.workflow import ActivityNode  # noqa: E402

RESULTS = ROOT / "benchmarks" / "results"
WORK = ROOT / "benchmarks" / ".work" / "overhead"
STEPS, REPEATS = 300, 7
NOTES = "lorem ipsum " * 85  # ~1 KB of text carried in the state
CONFIGS = [("NORMAL", 1), ("NORMAL", 10), ("NORMAL", 0), ("FULL", 1)]
REPLAY_STEPS = [100, 500, 2000]
BLUE, ORANGE, INK, MUTED, GRID, NEUTRAL = "#2a78d6", "#eb6834", "#0b0b0b", "#52514e", "#e4e3df", "#a8a7a2"


class Loop(BaseModel):
    n: int = 0
    target: int = STEPS
    notes: str = ""


def step(ctx: ActivityContext, state: Loop) -> dict[str, Any]:
    return {"n": state.n + 1}


def workflow() -> Workflow[Loop]:
    wf: Workflow[Loop] = Workflow("loop", Loop, start="step")
    wf.add_activity("step", step)
    wf.terminal("done")
    wf.transition("step", "step", guard=lambda s: s.n < s.target)
    wf.transition("step", "done")
    return wf


def in_memory_ms(steps: int) -> float:
    """Same logic without persistence: call, validate the result, pick the transition."""
    wf = workflow()
    model = Loop(target=steps, notes=NOTES)
    ctx = ActivityContext(run_id="mem", step="step#1", attempt=1, worker_id="mem", resources={}, outbox=Outbox("m"))
    node = wf.node("step")
    assert isinstance(node, ActivityNode)
    started = time.perf_counter()
    current = "step"
    while current != "done":
        value = node.fn(ctx, model)
        _, model = wf.apply_result(model, value)
        current = wf.next_state("step", model)
    return (time.perf_counter() - started) * 1000 / steps


def durable_run(
    db: Path, steps: int, synchronous: str, checkpoint_every: int, *, crash_after: int | None = None
) -> tuple[Engine, str, float]:
    """Run the loop in one worker; with ``crash_after`` the worker dies after that many committed steps."""
    for suffix in ("", "-wal", "-shm"):
        Path(str(db) + suffix).unlink(missing_ok=True)
    engine = Engine(db, [workflow()], synchronous=synchronous, checkpoint_every=checkpoint_every, lease_ttl=600)
    run_id = engine.start("loop", {"target": steps, "notes": NOTES})
    crash = CrashInjector(CrashPlan("step.after_commit", nth=crash_after)) if crash_after else None
    with engine.worker("bench", crash=crash) as worker:
        lease = worker.store.claim("bench", ttl=600)
        assert lease is not None
        started = time.perf_counter()
        try:
            worker.process(lease)
        except SimulatedCrash:
            pass
        elapsed = time.perf_counter() - started
    if crash is None:
        assert engine.run(run_id).outcome == "done"
    return engine, run_id, elapsed * 1000 / steps


def chart(per_step: list[dict[str, Any]], memory: float, replay: list[dict[str, Any]], path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, (left, right) = plt.subplots(1, 2, figsize=(11, 4.2))
    fig.patch.set_facecolor("white")
    labels = [r["label"] for r in per_step] + ["in memory (no persistence)"]
    values = [r["ms_per_step"] for r in per_step] + [memory]
    colors = [BLUE] * len(per_step) + [NEUTRAL]
    ypos = list(range(len(labels)))[::-1]
    left.barh(ypos, values, color=colors, height=0.6, edgecolor="white", linewidth=2)
    for y, value in zip(ypos, values, strict=True):
        left.text(value, y, f"  {value:.2f} ms", va="center", fontsize=9, color=INK)
    left.set_yticks(ypos, labels)
    left.set_xlim(0, max(values) * 1.3)
    left.set_xlabel("milliseconds per step (median)", color=MUTED)
    left.set_title("Cost of one durable step", loc="left", fontsize=11, color=INK)

    events = [r["events"] for r in replay]
    full = [r["full_replay_ms"] for r in replay]
    ckpt = [r["checkpoint_ms"] for r in replay]
    right.plot(events, full, color=ORANGE, linewidth=2, marker="o", markersize=6, label="replay whole journal")
    right.plot(events, ckpt, color=BLUE, linewidth=2, marker="o", markersize=6, label="checkpoint + tail")
    right.text(events[-1], full[-1], "  whole journal", color=INK, fontsize=9, va="center")
    right.text(events[-1], ckpt[-1], "  checkpoint + tail", color=INK, fontsize=9, va="bottom")
    right.set_xlim(0, events[-1] * 1.35)
    right.set_ylim(0, max(full) * 1.15)
    right.set_xlabel("events in the journal when the worker crashed", color=MUTED)
    right.set_ylabel(f"ms to rebuild the run (best of {REPEATS})", color=MUTED)
    right.set_title("Rebuilding a run after a crash", loc="left", fontsize=11, color=INK)
    right.legend(frameon=False, fontsize=9, loc="upper left")
    for ax in (left, right):
        ax.set_facecolor("white")
        ax.grid(axis="x" if ax is left else "y", color=GRID, linewidth=0.8)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        for side in ("left", "bottom"):
            ax.spines[side].set_color(GRID)
        ax.tick_params(colors=MUTED, labelsize=9)
    fig.tight_layout()
    fig.savefig(path, dpi=110, facecolor="white")
    plt.close(fig)


def main() -> None:
    if WORK.exists():
        shutil.rmtree(WORK)
    WORK.mkdir(parents=True)
    memory = statistics.median(in_memory_ms(STEPS) for _ in range(REPEATS))
    samples: dict[tuple[str, int], list[float]] = {config: [] for config in CONFIGS}
    for i in range(REPEATS):  # round-robin, so background load affects every configuration alike
        for synchronous, every in CONFIGS:
            engine, _, ms = durable_run(WORK / f"{synchronous}-{every}-{i}.db", STEPS, synchronous, every)
            engine.close()
            samples[(synchronous, every)].append(ms)
    per_step = []
    for (synchronous, every), values in samples.items():
        label = f"synchronous={synchronous}, " + (f"checkpoint every {every}" if every else "no checkpoints")
        per_step.append({"synchronous": synchronous, "checkpoint_every": every, "label": label,
                         "ms_per_step": round(statistics.median(values), 3),
                         "min_ms_per_step": round(min(values), 3),
                         "overhead_ms": round(statistics.median(values) - memory, 3),
                         "samples_ms": [round(v, 3) for v in values]})
        print(f"{label:<45} median {statistics.median(values):7.3f}  min {min(values):7.3f} ms/step")
    replay = []
    for steps in REPLAY_STEPS:
        row: dict[str, Any] = {"steps": steps}
        for every, key in ((0, "full_replay_ms"), (10, "checkpoint_ms")):
            engine, run_id, _ = durable_run(WORK / f"replay-{steps}-{every}.db", steps, "NORMAL", every,
                                            crash_after=steps - 5)
            timings = []
            for _ in range(REPEATS):
                started = time.perf_counter()
                _, checkpoint_seq, replayed = load_state(engine.store, run_id)
                timings.append((time.perf_counter() - started) * 1000)
            row[key] = round(min(timings), 3)  # best of N, as timeit reports: least disturbed by other load
            row["events"] = len(engine.history(run_id))
            row[key.replace("_ms", "_events_replayed")] = replayed
            engine.close()
        replay.append(row)
        print(f"replay {row['events']:>5} events: full {row['full_replay_ms']:.2f} ms, "
              f"checkpoint {row['checkpoint_ms']:.2f} ms")
    env = {"python": platform.python_version(), "sqlite": sqlite3.sqlite_version,
           "os": f"{platform.system()} {platform.release()}", "cpu_count": os.cpu_count()}
    command = "PYTHONPATH=src python benchmarks/overhead.py"
    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / "overhead.json").write_text(json.dumps(
        {"command": command, "environment": env, "steps": STEPS, "repeats": REPEATS,
         "in_memory_ms_per_step": round(memory, 4), "per_step": per_step, "replay": replay}, indent=1),
        encoding="utf-8")
    lines = [
        "# Persistence overhead",
        "",
        f"Command: `{command}`",
        "",
        f"Environment: Python {env['python']}, SQLite {env['sqlite']}, {env['os']}, {env['cpu_count']} logical CPUs. "
        f"{REPEATS} repeats (configurations interleaved) of a {STEPS}-step loop whose state carries ~1 KB of "
        "text. Each step is two SQLite write transactions (StepStarted; StepCompleted + next StepScheduled + "
        "optional checkpoint).",
        "",
        "| Configuration | median ms per step | best ms per step | median overhead vs in memory (ms) |",
        "|---|---:|---:|---:|",
        *[f"| {r['label']} | {r['ms_per_step']:.3f} | {r['min_ms_per_step']:.3f} | {r['overhead_ms']:.3f} |"
          for r in per_step],
        f"| in memory, no persistence | {memory:.3f} | - | - |",
        "",
        "| Journal length at the crash (events) | full replay, best (ms) | "
        "checkpoint + tail, best (ms) | tail events |",
        "|---:|---:|---:|---:|",
        *[f"| {r['events']} | {r['full_replay_ms']:.2f} | {r['checkpoint_ms']:.2f} | "
          f"{r['checkpoint_events_replayed']} |" for r in replay],
        "",
        "Timings come from one laptop that was also running other work, so treat them as orders of magnitude; "
        "the comparisons between rows are the point.",
    ]
    (RESULTS / "overhead.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    chart(per_step, memory, replay, ROOT / "assets" / "overhead.png")
    print("wrote benchmarks/results/overhead.json, overhead.md and assets/overhead.png")


if __name__ == "__main__":
    main()
