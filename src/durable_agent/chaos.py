"""Chaos harness: many research runs, worker subprocesses killed at seeded points, then checks.

Each worker subprocess is started either with a *crash plan* (a named crash point and which hit
of it to die on - the worker prints a marker there and the harness kills it with
``Process.kill()``) or with a *random kill time* (killed wherever it happens to be, including in
the middle of a SQLite transaction). Dead workers are replaced until the crash budget is used up,
then clean workers finish the remaining work; one or more standby workers are never killed. Runs
are admitted so that every worker always has work, until both the minimum number of runs and the
whole crash budget have been used. A built-in approver plays the human: it approves, edits,
rejects or ignores each run according to a seeded draw.

Afterwards every run must have reached the outcome its decision implies, the CMS must hold
exactly one article per published run with the journal's final title and body, and every journal
must pass :func:`durable_agent.verify.check_run`.
"""

from __future__ import annotations

import random
import shutil
import statistics
import time
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from .crash import CRASH_POINTS, STEP_POINTS, CrashPlan
from .events import EventType, RunStatus
from .research import CmsSink, build_engine, cms_path
from .research.corpus import TOPICS
from .retry import stable_rng
from .state import replay
from .store import Store
from .supervisor import WorkerProcess, worker_command
from .verify import Violation, check_run

Decision = Literal["approve", "edit", "reject", "none"]
ACTIVITY_STATES = ("plan", "gather", "draft", "publish")
LIVE = (RunStatus.PENDING, RunStatus.RUNNING, RunStatus.WAITING)


class ChaosConfig(BaseModel):
    runs: int = Field(default=200, ge=1, description="Minimum number of runs; more join while kills remain.")
    crashes: int = Field(default=200, ge=0, description="Kill plans to use up in total.")
    max_runs: int = Field(default=5000, ge=1, description="Hard cap on admitted runs.")
    workers: int = Field(default=4, ge=1, description="Worker processes that receive kill plans.")
    standby_workers: int = Field(default=1, ge=0, description="Extra workers that are never killed.")
    seed: int = 7
    lease_ttl: float = Field(default=1.0, gt=0)
    poll: float = Field(default=0.05, gt=0)
    publish_mode: Literal["outbox", "direct"] = "outbox"
    failure_rate: float = Field(default=0.3, ge=0, le=1, description="Injected search failure rate per attempt.")
    random_kill_share: float = Field(default=0.25, ge=0, le=1, description="Share of kills at a random time.")
    random_kill_window: tuple[float, float] = Field(
        default=(0.05, 1.5), description="Random kills land this many seconds after the worker is ready."
    )
    plan_patience: float = Field(default=6.0, gt=0, description="Kill a worker whose crash point never comes.")
    search_latency: float = Field(default=0.05, ge=0, description="Simulated seconds per search call.")
    decision_weights: dict[str, float] = Field(
        default_factory=lambda: {"approve": 0.7, "edit": 0.15, "reject": 0.1, "none": 0.05}
    )
    approval_timeout: float = 4.0
    escalation_timeout: float = 2.0
    max_seconds: float = 3600.0


class Kill(BaseModel):
    worker: str
    kind: Literal["crash_point", "random_time", "unfired_plan"]
    plan: str
    reached: str | None = None
    at: float
    held_runs: list[str] = Field(default_factory=list)
    held_messages: list[str] = Field(default_factory=list)


class Summary(BaseModel):
    count: int
    mean: float | None = None
    p50: float | None = None
    p95: float | None = None
    max: float | None = None

    @classmethod
    def of(cls, values: list[float]) -> Summary:
        if not values:
            return cls(count=0)
        ordered = sorted(values)
        return cls(
            count=len(ordered),
            mean=round(statistics.fmean(ordered), 4),
            p50=round(ordered[len(ordered) // 2], 4),
            p95=round(ordered[min(len(ordered) - 1, int(0.95 * len(ordered)))], 4),
            max=round(ordered[-1], 4),
        )


class ChaosReport(BaseModel):
    config: ChaosConfig
    wall_seconds: float
    timed_out: bool
    runs: int
    finished: int
    as_expected: int
    completion_rate: float
    outcomes: dict[str, int]
    unexpected_runs: list[str]
    decisions: dict[str, int]
    workers_spawned: int
    kills: int
    kills_by_kind: dict[str, int]
    kills_by_point: dict[str, int]
    kills_holding_a_run: int
    recovery_seconds: list[float]
    recovery: Summary
    replay_ms: Summary
    articles: int
    cms_calls: int
    redeliveries_absorbed: int
    duplicate_publications: int
    missing_publications: int
    content_mismatches: int
    events: int
    checkpoints: int
    activity_executions: int
    in_doubt_attempts: int
    completed_step_reexecutions: int
    failures_by_type: dict[str, int]
    gather_attempts_histogram: dict[int, int]
    retries_exhausted: int
    violations: list[Violation]
    kill_log: list[Kill]


def make_plans(config: ChaosConfig) -> list[CrashPlan | float]:
    """The seeded kill schedule: crash plans, or a delay (seconds after start) for random-time kills."""
    rng = random.Random(config.seed)
    plans: list[CrashPlan | float] = []
    for _ in range(config.crashes):
        if rng.random() < config.random_kill_share:
            plans.append(round(rng.uniform(*config.random_kill_window), 3))
            continue
        point = rng.choice(CRASH_POINTS)
        node = rng.choice(ACTIVITY_STATES) if point in STEP_POINTS else None
        plans.append(CrashPlan(point, nth=rng.randint(1, 2), node=node))
    return plans


def decision_for(config: ChaosConfig, index: int) -> Decision:
    """The approver's seeded decision for the ``index``-th run."""
    names = list(config.decision_weights)
    weights = [config.decision_weights[n] for n in names]
    choice = stable_rng("decision", config.seed, index).choices(names, weights)[0]
    return choice  # type: ignore[return-value]


class _Slot:
    def __init__(self, proc: WorkerProcess, plan: CrashPlan | float | None) -> None:
        self.proc = proc
        self.plan = plan

    def due(self, now: float, patience: float) -> Literal["random_time", "unfired_plan"] | None:
        """Whether a kill is due that does not come from a crash-point marker."""
        ready = self.proc.ready_at
        if ready is None or self.plan is None:
            return None
        if isinstance(self.plan, float):
            return "random_time" if now >= ready + self.plan else None
        return "unfired_plan" if now >= ready + patience else None


def run_chaos(config: ChaosConfig, workdir: Path, *, progress: Callable[[str], None] | None = None) -> ChaosReport:
    """Run the chaos experiment in ``workdir`` (its previous contents are deleted)."""
    if workdir.exists():
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True)
    db = workdir / "chaos.db"
    engine = build_engine(db, offline=True, lease_ttl=config.lease_ttl)
    store = engine.store
    cms = CmsSink(cms_path(db))
    decisions: dict[str, Decision] = {}
    plans = make_plans(config)
    busy = config.workers + config.standby_workers
    slots: list[_Slot | None] = [None] * busy
    kills: list[Kill] = []
    signalled: set[str] = set()
    spawned = next_plan = 0
    started = last_note = time.time()
    timed_out = False

    def admit() -> None:
        index = len(decisions)
        run_id = f"chaos-{index:04d}"
        decisions[run_id] = decision_for(config, index)
        engine.start(
            "research_report",
            {
                "topic": TOPICS[index % len(TOPICS)],
                "options": {
                    "failure_rate": config.failure_rate,
                    "fault_seed": config.seed,
                    "approval_timeout": config.approval_timeout,
                    "escalation_timeout": config.escalation_timeout,
                    "publish_mode": config.publish_mode,
                    "search_latency": config.search_latency,
                },
            },
            run_id=run_id,
        )

    def kill(slot: _Slot, kind: Literal["crash_point", "random_time", "unfired_plan"], reached: str | None) -> None:
        worker = slot.proc.worker_id
        held = [r.run_id for r in store.list_runs(statuses=LIVE) if r.lease_owner == worker]
        msgs = [m.key for m in store.outbox(status="pending") if m.claimed_by == worker]
        at = slot.proc.kill()
        label = str(slot.plan) if isinstance(slot.plan, CrashPlan) else f"{slot.plan:.2f}s after ready"
        kills.append(Kill(worker=worker, kind=kind, plan=label, reached=reached, at=at, held_runs=held,
                          held_messages=msgs))

    while True:
        now = time.time()
        if now - started > config.max_seconds:
            timed_out = True
            break
        # Admission: two live runs per worker, until the minimum run count is reached and every
        # kill plan has been handed out.
        live = store.list_runs(statuses=LIVE)
        wanted = len(decisions) < config.runs or next_plan < len(plans)
        for _ in range(max(0, 2 * busy - len(live)) if wanted else 0):
            if len(decisions) >= config.max_runs:
                break
            admit()
        # Kill, reap and respawn workers.
        for i, slot in enumerate(slots):
            if slot is not None:
                due = slot.due(now, config.plan_patience)
                if not slot.proc.markers.empty():
                    kill(slot, "crash_point", slot.proc.markers.get()[0])
                    slots[i] = None
                elif due is not None:
                    kill(slot, due, None)
                    slots[i] = None
                elif not slot.proc.alive:
                    slot.proc.kill()
                    slots[i] = None
            if slots[i] is None:
                plan = None
                if i < config.workers:  # chaos slot; the last slots are healthy standbys
                    plan = plans[next_plan] if next_plan < len(plans) else None
                    next_plan += 1
                spawned += 1
                worker_id = f"w{spawned:03d}"
                proc = WorkerProcess(
                    worker_command(db, worker_id, lease_ttl=config.lease_ttl, poll=config.poll,
                                   crash=plan if isinstance(plan, CrashPlan) else None),
                    worker_id,
                    log_path=workdir / "workers.log",
                )
                slots[i] = _Slot(proc, plan)
        # The approver: one decision per run, sent when the run waits at a gate.
        for row in store.list_runs(statuses=[RunStatus.WAITING]):
            decision = decisions[row.run_id]
            if row.run_id in signalled or decision == "none" or not (row.waiting_on or "").startswith("signal:"):
                continue
            payload: dict[str, str] = {"note": "chaos approver"}
            if decision == "edit":
                payload = {"title": f"Edited brief {row.run_id}", "note": "chaos editor"}
            elif decision == "reject":
                payload = {"reason": "chaos reviewer said no"}
            engine.signal(row.run_id, decision, payload, sender="chaos-approver")
            signalled.add(row.run_id)
        if not wanted and not store.list_runs(statuses=LIVE) and not store.outbox(status="pending"):
            break
        if progress is not None and now - last_note >= 10:
            last_note = now
            progress(f"{now - started:5.0f}s  runs {len(decisions):4d}  live {len(live):3d}  kills {len(kills):4d}  "
                     f"workers spawned {spawned:4d}")
        time.sleep(0.02)

    for slot in slots:
        if slot is not None:
            slot.proc.kill()
    wall = time.time() - started
    report = _measure(config, store, cms, decisions, kills, spawned, wall, timed_out)
    engine.close()
    return report


def _measure(
    config: ChaosConfig,
    store: Store,
    cms: CmsSink,
    decisions: dict[str, Decision],
    kills: list[Kill],
    spawned: int,
    wall: float,
    timed_out: bool,
) -> ChaosReport:
    runs = {r.run_id: r for r in store.list_runs()}
    articles = cms.articles()
    by_run: dict[str, list] = {}
    for article in articles:
        by_run.setdefault(article.run_id, []).append(article)

    violations: list[Violation] = []
    unexpected: list[str] = []
    outcomes: Counter[str] = Counter()
    failures: Counter[str] = Counter()
    gather_attempts: Counter[int] = Counter()
    executions = in_doubt = exhausted = events_total = checkpoints = 0
    duplicates = missing = mismatches = 0
    replay_ms: list[float] = []
    for run_id, row in runs.items():
        events = store.events(run_id)
        events_total += len(events)
        violations += check_run(store, run_id)
        outcomes[f"{row.status.value}:{row.outcome or '-'}"] += 1
        decision = decisions[run_id]
        expected = "done" if decision in ("approve", "edit") else "rejected"
        if row.status is not RunStatus.COMPLETED or row.outcome != expected:
            unexpected.append(run_id)
        for e in events:
            if e.type is EventType.STEP_STARTED:
                executions += 1
            elif e.type is EventType.STEP_FAILED:
                failures[e.data.get("error_type", "?")] += 1
                in_doubt += e.data.get("kind") == "worker_lost"
                exhausted += bool(e.data.get("final"))
            elif e.type is EventType.CHECKPOINT_WRITTEN:
                checkpoints += 1
            elif e.type is EventType.RUN_RECOVERED:
                replay_ms.append(float(e.data.get("replay_ms", 0.0)))
        gather_starts = sum(
            1 for e in events if e.type is EventType.STEP_STARTED and (e.step or "").startswith("gather#")
        )
        if gather_starts:
            gather_attempts[gather_starts] += 1
        published = by_run.get(run_id, [])
        if len(published) > 1:
            duplicates += 1
        if expected == "done" and not published:
            missing += 1
        if expected == "rejected" and published:
            unexpected.append(run_id)
        if len(published) == 1 and expected == "done":
            data = replay(events).data  # the journal is the source of truth for the final content
            if published[0].title != data.get("title") or published[0].body != data.get("body"):
                mismatches += 1

    recovery: list[float] = []
    for k in kills:
        for run_id in k.held_runs:
            for e in store.events(run_id):
                if e.ts >= k.at and e.worker not in (None, k.worker):
                    if e.type is EventType.RUN_RECOVERED and e.data.get("from_worker") == k.worker:
                        recovery.append(round(e.ts - k.at, 4))
                    break
    finished = sum(1 for r in runs.values() if r.status.terminal)
    as_expected = len(runs) - len(set(unexpected))
    return ChaosReport(
        config=config,
        wall_seconds=round(wall, 2),
        timed_out=timed_out,
        runs=len(runs),
        finished=finished,
        as_expected=as_expected,
        completion_rate=round(as_expected / len(runs), 4) if runs else 0.0,
        outcomes=dict(sorted(outcomes.items())),
        unexpected_runs=sorted(set(unexpected)),
        decisions=dict(Counter(decisions.values())),
        workers_spawned=spawned,
        kills=len(kills),
        kills_by_kind=dict(Counter(k.kind for k in kills)),
        kills_by_point=dict(sorted(Counter((k.reached or "random time").split(" ")[0] for k in kills).items())),
        kills_holding_a_run=sum(1 for k in kills if k.held_runs),
        recovery_seconds=sorted(recovery),
        recovery=Summary.of(recovery),
        replay_ms=Summary.of(replay_ms),
        articles=len(articles),
        cms_calls=cms.call_count(),
        redeliveries_absorbed=cms.call_count() - len(articles),
        duplicate_publications=duplicates,
        missing_publications=missing,
        content_mismatches=mismatches,
        events=events_total,
        checkpoints=checkpoints,
        activity_executions=executions,
        in_doubt_attempts=in_doubt,
        completed_step_reexecutions=sum(1 for v in violations if v.rule == "completed_step_never_reruns"),
        failures_by_type=dict(failures.most_common()),
        gather_attempts_histogram=dict(sorted(gather_attempts.items())),
        retries_exhausted=exhausted,
        violations=violations[:50],
        kill_log=kills,
    )
