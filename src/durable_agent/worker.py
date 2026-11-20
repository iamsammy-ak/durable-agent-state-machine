"""Workers: claim runs, keep their leases alive with heartbeats, drive them and relay the outbox."""

from __future__ import annotations

import sqlite3
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .crash import CrashInjector, SimulatedCrash
from .errors import LeaseLost, WorkflowDefinitionError
from .executor import AdvanceReport, Executor
from .outbox import OutboxRelay
from .store import Lease

if TYPE_CHECKING:
    from .engine import Engine


class Heartbeat:
    """Background thread that extends the held lease every ``interval`` seconds, on its own connection.

    If an extension fails, another worker has taken the run over (newer epoch): the lease's
    ``lost`` event is set so the executor stops before doing more work.
    """

    def __init__(self, engine: Engine, worker_id: str, interval: float) -> None:
        self._engine = engine
        self._interval = interval
        self._lock = threading.Lock()
        self._lease: Lease | None = None
        self._lost: threading.Event | None = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name=f"heartbeat-{worker_id}", daemon=True)
        self._started = False

    def hold(self, lease: Lease, lost: threading.Event) -> None:
        with self._lock:
            self._lease, self._lost = lease, lost
        if not self._started:
            self._started = True
            self._thread.start()

    def drop(self) -> None:
        with self._lock:
            self._lease = self._lost = None

    def stop(self) -> None:
        self._stop.set()
        if self._started:
            self._thread.join(timeout=5)

    def _loop(self) -> None:
        store = self._engine.open_store()
        try:
            while not self._stop.wait(self._interval):
                with self._lock:
                    lease, lost = self._lease, self._lost
                if lease is None:
                    continue
                try:
                    alive = store.extend_lease(lease, self._engine.lease_ttl)
                except sqlite3.Error:
                    continue  # busy database: try again on the next beat
                if not alive and lost is not None:
                    lost.set()
        finally:
            store.close()


@dataclass
class WorkerStats:
    claims: int = 0
    recoveries: int = 0
    leases_lost: int = 0
    attempts: int = 0
    messages_delivered: int = 0
    errors: int = 0


class Worker:
    """Processes runs from the shared database. Use one per thread or process."""

    def __init__(
        self,
        engine: Engine,
        worker_id: str | None = None,
        *,
        poll_interval: float = 0.1,
        heartbeat: bool = True,
        crash: CrashInjector | None = None,
        log: bool = False,
    ) -> None:
        self.engine = engine
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:6]}"
        self.poll_interval = poll_interval
        self.crash = crash
        self.log_enabled = log
        self.store = engine.open_store()
        self.relay = OutboxRelay(self.store, engine.sinks, self.worker_id, claim_ttl=engine.lease_ttl, crash=crash)
        self._heartbeat = Heartbeat(engine, self.worker_id, engine.lease_ttl / 3) if heartbeat else None
        self.stats = WorkerStats()

    # ---------------------------------------------------------------- lifecycle
    def close(self) -> None:
        """Stop the heartbeat and close the connection. Held leases are not released (they expire)."""
        if self._heartbeat is not None:
            self._heartbeat.stop()
        self.store.close()

    def __enter__(self) -> Worker:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _log(self, message: str) -> None:
        if self.log_enabled:
            print(f"[{self.worker_id}] {message}", file=sys.stderr, flush=True)

    # ------------------------------------------------------------------ working
    def process(self, lease: Lease) -> AdvanceReport | None:
        """Drive one leased run. Returns ``None`` if the lease was lost or the run cannot be handled here."""
        lost = threading.Event()
        if self._heartbeat is not None:
            self._heartbeat.hold(lease, lost)
        try:
            report = Executor(
                self.engine, self.store, lease, worker_id=self.worker_id, crash=self.crash, lease_lost=lost
            ).advance()
        except LeaseLost as exc:
            self.stats.leases_lost += 1
            self._log(f"lease lost: {exc}")
            return None
        except WorkflowDefinitionError as exc:
            self._log(f"cannot run {lease.run_id}: {exc}")
            self.store.release(lease, ready_at=self.store.now() + 5.0)
            return None
        except SimulatedCrash:
            if self._heartbeat is not None:  # a dead process stops heartbeating
                self._heartbeat.stop()
            raise
        finally:
            if self._heartbeat is not None:
                self._heartbeat.drop()
        self.stats.attempts += len(report.attempts_run)
        if report.recovered_from is not None:
            self.stats.recoveries += 1
            self._log(f"recovered {lease.run_id} from {report.recovered_from}")
        return report

    def run_once(self) -> bool:
        """Claim and drive one ready run, then deliver due outbox messages. True if anything happened."""
        did = False
        lease = self.store.claim(self.worker_id, self.engine.lease_ttl)
        if lease is not None:
            self.stats.claims += 1
            self.process(lease)
            did = True
        delivered = self.relay.deliver_due()
        self.stats.messages_delivered += delivered
        return did or delivered > 0

    def run(self, *, stop: threading.Event | None = None, max_seconds: float | None = None) -> WorkerStats:
        """Poll for work until ``stop`` is set or ``max_seconds`` pass. Errors in one run do not stop the loop."""
        stop = stop or threading.Event()
        deadline = None if max_seconds is None else time.monotonic() + max_seconds
        while not stop.is_set() and (deadline is None or time.monotonic() < deadline):
            try:
                if self.run_once():
                    continue
            except Exception as exc:  # one broken run must not stop the worker; its lease expires
                self.stats.errors += 1
                self._log(f"error: {type(exc).__name__}: {exc}")
            stop.wait(self.poll_interval)
        return self.stats

    def run_until_idle(self, *, timeout: float = 30.0, horizon: float = 5.0) -> int:
        """Work until nothing is ready and nothing becomes ready within ``horizon`` seconds.

        Runs parked on a signal do not keep the worker busy. Returns the number of runs driven.
        """
        deadline = time.monotonic() + timeout
        driven = 0
        while time.monotonic() < deadline:
            if self.run_once():
                driven += 1
                continue
            wake = self._next_wake()
            now = self.store.now()
            if wake is None or wake - now > horizon:
                break
            time.sleep(min(max(wake - now, 0.005), 0.25, max(deadline - time.monotonic(), 0.0)))
        return driven

    def _next_wake(self) -> float | None:
        times = [self.store.next_wakeup(), self.store.next_outbox_due()]
        valid = [t for t in times if t is not None]
        return min(valid) if valid else None

    def drive(
        self, run_id: str, *, steal: bool = False, horizon: float = 5.0, lease_wait: float | None = None
    ) -> list[AdvanceReport]:
        """Drive one run until it finishes or parks for longer than ``horizon`` seconds.

        Waits up to ``lease_wait`` seconds (default: two lease TTLs) for another worker's lease to
        expire; ``steal`` takes the lease at once (only safe when that worker is known to be dead).
        """
        reports: list[AdvanceReport] = []
        wait_limit = time.monotonic() + (lease_wait if lease_wait is not None else 2 * self.engine.lease_ttl + 1)
        while True:
            row = self.store.get_run(run_id)
            if row.status.terminal:
                break
            lease = self.store.claim(self.worker_id, self.engine.lease_ttl, run_id=run_id, steal=steal)
            steal = False
            if lease is not None:
                report = self.process(lease)
                if report is not None:
                    reports.append(report)
                continue
            row = self.store.get_run(run_id)
            now = self.store.now()
            if row.status.terminal:
                break
            if row.lease_owner is not None and (row.lease_expires_at or 0) > now:
                if time.monotonic() >= wait_limit:
                    break  # another live worker holds it
                time.sleep(min((row.lease_expires_at or now) - now + 0.002, 0.25))
                continue
            if row.ready_at is None or row.ready_at - now > horizon:
                break  # parked until a signal arrives, or until a timer beyond the horizon
            time.sleep(max(row.ready_at - now, 0.0) + 0.002)
        self.stats.messages_delivered += self.relay.deliver_due(run_id=run_id, limit=100)
        return reports
