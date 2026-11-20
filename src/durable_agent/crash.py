"""Fault injection: named crash points at the windows where a crash is most dangerous.

The runtime calls :meth:`CrashInjector.hit` at each point. A worker armed with a
:class:`CrashPlan` "crashes" on the plan's n-th hit, in one of three ways:

* ``raise`` - raise :class:`SimulatedCrash` (a ``BaseException``), for fast in-process tests;
* ``exit`` - ``os._exit``: the process dies at once, with no cleanup and its lease still held;
* ``wait`` - print a marker line and block, so a supervising process can kill it with
  ``Process.kill()`` (SIGKILL on Linux, TerminateProcess on Windows). The chaos benchmark uses this.
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from enum import Enum

CRASH_POINTS: tuple[str, ...] = (
    "run.after_claim",  # lease acquired, run state rebuilt, nothing written yet
    "step.after_start",  # StepStarted committed, activity not yet called
    "step.before_commit",  # activity returned; its result is not yet committed
    "step.after_commit",  # StepCompleted committed; lease still held
    "gate.before_signal",  # a signal is waiting in the inbox, not yet consumed
    "timer.before_fire",  # a durable timer is due, TimerFired not yet written
    "outbox.after_deliver",  # the sink applied the side effect; delivery not yet acknowledged
)
STEP_POINTS = frozenset({"step.after_start", "step.before_commit", "step.after_commit"})
MARKER = "@@CRASHPOINT"
READY_MARKER = "@@READY"
CRASH_EXIT_CODE = 137


class SimulatedCrash(BaseException):
    """An in-process stand-in for a hard crash. Not an ``Exception``, so activity error handling ignores it."""


class CrashMode(str, Enum):
    RAISE = "raise"
    EXIT = "exit"
    WAIT = "wait"


@dataclass(frozen=True)
class CrashPlan:
    """Crash at the ``nth`` hit of ``point``, counting only hits in state ``node`` when given."""

    point: str
    nth: int = 1
    node: str | None = None

    def __post_init__(self) -> None:
        if self.point not in CRASH_POINTS:
            raise ValueError(f"unknown crash point {self.point!r}; choose from {', '.join(CRASH_POINTS)}")
        if self.nth < 1:
            raise ValueError("nth must be >= 1")

    @classmethod
    def parse(cls, spec: str) -> CrashPlan:
        """Parse ``"point[@state][:nth]"``, e.g. ``"step.before_commit@draft:1"``."""
        nth = 1
        if ":" in spec:
            spec, _, count = spec.rpartition(":")
            nth = int(count)
        point, _, node = spec.partition("@")
        return cls(point=point, nth=nth, node=node or None)

    def __str__(self) -> str:
        return self.point + (f"@{self.node}" if self.node else "") + f":{self.nth}"


class CrashInjector:
    """Counts hits of one crash point and crashes on the planned one (at most once)."""

    def __init__(self, plan: CrashPlan, mode: CrashMode = CrashMode.RAISE, *, wait_limit: float = 60.0) -> None:
        self.plan = plan
        self.mode = mode
        self.wait_limit = wait_limit
        self.hits = 0
        self.fired = False
        self._lock = threading.Lock()

    def hit(self, point: str, step: str | None = None) -> None:
        with self._lock:
            if self.fired or point != self.plan.point:
                return
            if self.plan.node is not None and (step is None or step.split("#", 1)[0] != self.plan.node):
                return
            self.hits += 1
            if self.hits < self.plan.nth:
                return
            self.fired = True
        where = f"{point} {step or '-'}"
        if self.mode is CrashMode.RAISE:
            raise SimulatedCrash(where)
        if self.mode is CrashMode.WAIT:
            print(f"{MARKER} {where}", flush=True)
            time.sleep(self.wait_limit)  # the supervisor kills us long before this ends
        os._exit(CRASH_EXIT_CODE)


def crash_hit(injector: CrashInjector | None, point: str, step: str | None = None) -> None:
    """Call ``injector.hit`` when fault injection is armed."""
    if injector is not None:
        injector.hit(point, step)
