"""SQLite persistence: a runs index, the append-only event journal, checkpoints, a signal inbox and an outbox.

Durability settings: WAL mode with ``synchronous=NORMAL`` (default) means a committed transaction
survives a process crash; after a power failure the most recent commits can be lost, but the
database is never corrupted. ``synchronous=FULL`` also survives power loss, at the cost of an
fsync per commit (measured in ``benchmarks/overhead.py``).

Every journal write happens inside ``BEGIN IMMEDIATE`` and first checks the writer's lease
*epoch* (a fencing token). A worker that lost its lease - because it stalled past the lease
expiry and another worker took over - gets :class:`LeaseLost` instead of corrupting the run.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from .context import OutboxMessage
from .errors import DurableAgentError, LeaseLost, ReplayError, RunFinished, RunNotFound
from .events import TERMINAL_STATUSES, Event, EventType, RunStatus
from .state import RunState, apply_event, snapshot

SCHEMA = """
BEGIN IMMEDIATE;
CREATE TABLE IF NOT EXISTS runs (
    run_id           TEXT PRIMARY KEY,
    workflow         TEXT NOT NULL,
    status           TEXT NOT NULL,
    current_state    TEXT,
    waiting_on       TEXT,
    ready_at         REAL,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL,
    last_seq         INTEGER NOT NULL DEFAULT 0,
    lease_owner      TEXT,
    lease_epoch      INTEGER NOT NULL DEFAULT 0,
    lease_expires_at REAL,
    outcome          TEXT,
    error            TEXT
);
CREATE INDEX IF NOT EXISTS runs_by_readiness ON runs(status, ready_at);
CREATE TABLE IF NOT EXISTS events (
    run_id  TEXT NOT NULL,
    seq     INTEGER NOT NULL,
    type    TEXT NOT NULL,
    step    TEXT,
    attempt INTEGER,
    data    TEXT NOT NULL,
    worker  TEXT,
    ts      REAL NOT NULL,
    PRIMARY KEY (run_id, seq)
) WITHOUT ROWID;
CREATE TRIGGER IF NOT EXISTS events_no_update BEFORE UPDATE ON events
    BEGIN SELECT RAISE(ABORT, 'the event journal is append-only'); END;
CREATE TRIGGER IF NOT EXISTS events_no_delete BEFORE DELETE ON events
    BEGIN SELECT RAISE(ABORT, 'the event journal is append-only'); END;
CREATE TABLE IF NOT EXISTS checkpoints (
    run_id TEXT NOT NULL,
    seq    INTEGER NOT NULL,
    state  TEXT NOT NULL,
    ts     REAL NOT NULL,
    PRIMARY KEY (run_id, seq)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS signals (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id       TEXT NOT NULL,
    name         TEXT NOT NULL,
    payload      TEXT NOT NULL,
    sender       TEXT NOT NULL,
    created_at   REAL NOT NULL,
    consumed_seq INTEGER
);
CREATE INDEX IF NOT EXISTS signals_pending ON signals(run_id, consumed_seq);
CREATE TABLE IF NOT EXISTS outbox (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    key              TEXT NOT NULL UNIQUE,
    run_id           TEXT NOT NULL,
    step             TEXT NOT NULL,
    topic            TEXT NOT NULL,
    payload          TEXT NOT NULL,
    created_at       REAL NOT NULL,
    available_at     REAL NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending',
    attempts         INTEGER NOT NULL DEFAULT 0,
    claimed_by       TEXT,
    claim_expires_at REAL,
    delivered_at     REAL,
    delivered_by     TEXT,
    last_error       TEXT
);
CREATE INDEX IF NOT EXISTS outbox_due ON outbox(status, available_at);
COMMIT;
"""

_LIVE = "status IN ('pending', 'running', 'waiting')"
_READY = "ready_at IS NOT NULL AND ready_at <= :now"
_UNLEASED = "(lease_owner IS NULL OR lease_expires_at <= :now)"


def _dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


class RunRow(BaseModel):
    """One row of the ``runs`` index (derived from the journal, updated in the same transactions)."""

    run_id: str
    workflow: str
    status: RunStatus
    current_state: str | None = None
    waiting_on: str | None = None
    ready_at: float | None = None
    created_at: float
    updated_at: float
    last_seq: int
    lease_owner: str | None = None
    lease_epoch: int = 0
    lease_expires_at: float | None = None
    outcome: str | None = None
    error: str | None = None


class SignalRow(BaseModel):
    id: int
    run_id: str
    name: str
    payload: dict[str, Any]
    sender: str
    created_at: float
    consumed_seq: int | None = None


class OutboxRow(BaseModel):
    id: int
    key: str
    run_id: str
    step: str
    topic: str
    payload: dict[str, Any]
    created_at: float
    available_at: float
    status: str
    attempts: int
    claimed_by: str | None = None
    claim_expires_at: float | None = None
    delivered_at: float | None = None
    delivered_by: str | None = None
    last_error: str | None = None


class Lease(BaseModel):
    """Permission to write a run's journal. ``epoch`` is the fencing token."""

    run_id: str
    owner: str
    epoch: int
    expires_at: float
    previous_owner: str | None = None
    previous_expires_at: float | None = None


class RunTx:
    """Writes for one run inside one SQLite transaction. Appended events are folded into ``state`` at once."""

    def __init__(self, conn: sqlite3.Connection, state: RunState, worker: str | None, now: float) -> None:
        self.conn = conn
        self.state = state
        self.worker = worker
        self.now = now
        self.events: list[Event] = []
        self._release: tuple[float | None, str, tuple[str, ...]] | None = None

    def append(
        self,
        kind: EventType,
        *,
        step: str | None = None,
        attempt: int | None = None,
        data: dict[str, Any] | None = None,
    ) -> Event:
        event = Event(
            run_id=self.state.run_id,
            seq=self.state.last_seq + 1,
            type=kind,
            ts=self.now,
            step=step,
            attempt=attempt,
            worker=self.worker,
            data=data or {},
        )
        self.conn.execute(
            "INSERT INTO events (run_id, seq, type, step, attempt, data, worker, ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (event.run_id, event.seq, kind.value, step, attempt, _dumps(event.data), self.worker, event.ts),
        )
        apply_event(self.state, event)
        self.events.append(event)
        return event

    def consume_signal(self, signal_id: int) -> None:
        """Mark a signal consumed by the next event to be appended (call before that append)."""
        cur = self.conn.execute(
            "UPDATE signals SET consumed_seq = ? WHERE id = ? AND consumed_seq IS NULL",
            (self.state.last_seq + 1, signal_id),
        )
        if cur.rowcount != 1:
            raise ReplayError(f"signal {signal_id} was already consumed")

    def enqueue(self, message: OutboxMessage, step: str) -> None:
        self.conn.execute(
            "INSERT INTO outbox (key, run_id, step, topic, payload, created_at, available_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (message.key, self.state.run_id, step, message.topic, _dumps(message.payload), self.now, self.now),
        )

    def checkpoint(self) -> Event:
        """Store a snapshot of the state as of the last appended event, then journal that fact."""
        text = snapshot(self.state)
        self.conn.execute(
            "INSERT INTO checkpoints (run_id, seq, state, ts) VALUES (?, ?, ?, ?)",
            (self.state.run_id, self.state.last_seq, text, self.now),
        )
        return self.append(
            EventType.CHECKPOINT_WRITTEN, data={"checkpoint_seq": self.state.last_seq, "bytes": len(text)}
        )

    def release(self, *, ready_at: float | None, waiting_on: str, wake_on: Iterable[str] = ()) -> None:
        """Give the lease back on commit and park the run until ``ready_at`` (``None``: until a signal)."""
        self._release = (ready_at, waiting_on, tuple(wake_on))

    def _finish(self, lease: Lease | None, lease_ttl: float, created: bool) -> None:
        state, now = self.state, self.now
        fields: dict[str, Any] = {"last_seq": state.last_seq, "updated_at": now, "current_state": state.current}
        if state.terminal:
            fields.update(
                status=state.status.value, outcome=state.outcome, error=state.error, waiting_on=None, ready_at=None,
                lease_owner=None, lease_expires_at=None,
            )
        elif self._release is not None:
            ready_at, waiting_on, wake_on = self._release
            if wake_on:
                marks = ",".join("?" * len(wake_on))
                pending = self.conn.execute(
                    f"SELECT 1 FROM signals WHERE run_id = ? AND consumed_seq IS NULL AND name IN ({marks}) LIMIT 1",
                    (state.run_id, *wake_on),
                ).fetchone()
                if pending is not None:  # a signal raced in before we parked: no lost wake-up
                    ready_at = now
            fields.update(
                status=RunStatus.WAITING.value, ready_at=ready_at, waiting_on=waiting_on, lease_owner=None,
                lease_expires_at=None,
            )
        elif created:
            fields.update(status=RunStatus.PENDING.value, ready_at=now)
        else:
            fields.update(status=RunStatus.RUNNING.value, waiting_on=None)
            if lease is not None:
                fields["lease_expires_at"] = now + lease_ttl
        assignments = ", ".join(f"{name} = ?" for name in fields)
        self.conn.execute(f"UPDATE runs SET {assignments} WHERE run_id = ?", (*fields.values(), state.run_id))


class Store:
    """One SQLite connection. Not thread-safe: give each thread (worker, heartbeat) its own ``Store``."""

    def __init__(
        self,
        path: str | Path,
        *,
        synchronous: str = "NORMAL",
        clock: Callable[[], float] = time.time,
        busy_timeout: float = 30.0,
    ) -> None:
        sync = synchronous.upper()
        if sync not in {"OFF", "NORMAL", "FULL", "EXTRA"}:
            raise ValueError(f"unknown synchronous mode {synchronous!r}")
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.clock = clock
        self.conn = sqlite3.connect(str(self.path), timeout=busy_timeout, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute(f"PRAGMA synchronous={sync}")
        self.conn.execute(f"PRAGMA busy_timeout={int(busy_timeout * 1000)}")
        self.conn.executescript(SCHEMA)

    def now(self) -> float:
        return self.clock()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> Store:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        self.conn.execute("COMMIT")

    # ------------------------------------------------------------------ journal
    @contextmanager
    def run_tx(
        self,
        state: RunState,
        *,
        worker: str | None,
        lease: Lease | None = None,
        lease_ttl: float = 30.0,
        create_workflow: str | None = None,
    ) -> Iterator[RunTx]:
        """A fenced write transaction for one run.

        With ``create_workflow`` the run row is inserted first (a new run). Otherwise, when a
        ``lease`` is given, the transaction aborts with :class:`LeaseLost` unless the row still
        carries that lease's owner and epoch.
        """
        now = self.now()
        with self._write() as conn:
            if create_workflow is not None:
                try:
                    conn.execute(
                        "INSERT INTO runs (run_id, workflow, status, created_at, updated_at, ready_at) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (state.run_id, create_workflow, RunStatus.PENDING.value, now, now, now),
                    )
                except sqlite3.IntegrityError:
                    raise DurableAgentError(f"run {state.run_id!r} already exists") from None
            else:
                row = conn.execute(
                    "SELECT lease_owner, lease_epoch, last_seq FROM runs WHERE run_id = ?", (state.run_id,)
                ).fetchone()
                if row is None:
                    raise RunNotFound(state.run_id)
                if lease is not None and (row["lease_owner"] != lease.owner or row["lease_epoch"] != lease.epoch):
                    raise LeaseLost(
                        f"run {state.run_id}: lease epoch {lease.epoch} of {lease.owner} is stale "
                        f"(now held by {row['lease_owner']} at epoch {row['lease_epoch']})"
                    )
                if row["last_seq"] != state.last_seq:
                    raise LeaseLost(f"run {state.run_id}: journal is at seq {row['last_seq']}, not {state.last_seq}")
            tx = RunTx(conn, state, worker, now)
            yield tx
            tx._finish(lease, lease_ttl, created=create_workflow is not None)

    def events(self, run_id: str, *, after_seq: int = 0) -> list[Event]:
        rows = self.conn.execute(
            "SELECT * FROM events WHERE run_id = ? AND seq > ? ORDER BY seq", (run_id, after_seq)
        ).fetchall()
        return [
            Event(
                run_id=r["run_id"],
                seq=r["seq"],
                type=EventType(r["type"]),
                ts=r["ts"],
                step=r["step"],
                attempt=r["attempt"],
                worker=r["worker"],
                data=json.loads(r["data"]),
            )
            for r in rows
        ]

    def latest_checkpoint(self, run_id: str) -> tuple[int, str] | None:
        row = self.conn.execute(
            "SELECT seq, state FROM checkpoints WHERE run_id = ? ORDER BY seq DESC LIMIT 1", (run_id,)
        ).fetchone()
        return (row["seq"], row["state"]) if row else None

    def checkpoints(self, run_id: str) -> list[tuple[int, str]]:
        rows = self.conn.execute("SELECT seq, state FROM checkpoints WHERE run_id = ? ORDER BY seq", (run_id,))
        return [(r["seq"], r["state"]) for r in rows]

    # --------------------------------------------------------------------- runs
    def get_run(self, run_id: str) -> RunRow:
        row = self.conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if row is None:
            raise RunNotFound(run_id)
        return RunRow(**dict(row))

    def list_runs(self, *, statuses: Iterable[RunStatus] | None = None, limit: int | None = None) -> list[RunRow]:
        sql, params = "SELECT * FROM runs", []
        if statuses is not None:
            wanted = [s.value for s in statuses]
            sql += f" WHERE status IN ({','.join('?' * len(wanted))})"
            params.extend(wanted)
        sql += " ORDER BY created_at, run_id"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        return [RunRow(**dict(r)) for r in self.conn.execute(sql, params)]

    def next_wakeup(self) -> float | None:
        """Earliest time a live run becomes claimable (ready and unleased): when a worker should look again."""
        row = self.conn.execute(
            f"SELECT MIN(MAX(ready_at, COALESCE(lease_expires_at, 0))) FROM runs WHERE {_LIVE} AND ready_at IS NOT NULL"
        ).fetchone()
        return row[0] if row else None

    # ------------------------------------------------------------------- leases
    def claim(self, worker: str, ttl: float, *, run_id: str | None = None, steal: bool = False) -> Lease | None:
        """Lease a ready run (the oldest, or ``run_id``). ``steal`` ignores a live lease: use only for dead workers."""
        now = self.now()
        if run_id is None:
            rows = self.conn.execute(
                f"SELECT run_id FROM runs WHERE {_LIVE} AND {_READY} AND {_UNLEASED} "
                "ORDER BY ready_at, created_at LIMIT 5",
                {"now": now},
            ).fetchall()
            candidates, steal = [r["run_id"] for r in rows], False
        else:
            candidates = [run_id]
        for candidate in candidates:
            condition = f"run_id = :run_id AND {_LIVE} AND {_READY}" + ("" if steal else f" AND {_UNLEASED}")
            with self._write() as conn:
                row = conn.execute(
                    f"SELECT lease_owner, lease_expires_at, lease_epoch FROM runs WHERE {condition}",
                    {"run_id": candidate, "now": now},
                ).fetchone()
                if row is None:
                    continue  # another worker won the race; try the next candidate
                epoch = row["lease_epoch"] + 1
                conn.execute(
                    "UPDATE runs SET lease_owner = ?, lease_epoch = ?, lease_expires_at = ?, status = ?, "
                    "waiting_on = NULL WHERE run_id = ?",
                    (worker, epoch, now + ttl, RunStatus.RUNNING.value, candidate),
                )
            return Lease(
                run_id=candidate,
                owner=worker,
                epoch=epoch,
                expires_at=now + ttl,
                previous_owner=row["lease_owner"],
                previous_expires_at=row["lease_expires_at"],
            )
        return None

    def extend_lease(self, lease: Lease, ttl: float) -> bool:
        """Heartbeat. False when the lease was lost (another worker holds a newer epoch)."""
        cur = self.conn.execute(
            "UPDATE runs SET lease_expires_at = ? WHERE run_id = ? AND lease_owner = ? AND lease_epoch = ?",
            (self.now() + ttl, lease.run_id, lease.owner, lease.epoch),
        )
        return cur.rowcount == 1

    def release(self, lease: Lease, *, ready_at: float | None) -> bool:
        """Give a lease back without writing events (e.g. a worker that does not know the workflow)."""
        cur = self.conn.execute(
            "UPDATE runs SET lease_owner = NULL, lease_expires_at = NULL, ready_at = ? "
            "WHERE run_id = ? AND lease_owner = ? AND lease_epoch = ?",
            (ready_at, lease.run_id, lease.owner, lease.epoch),
        )
        return cur.rowcount == 1

    # ------------------------------------------------------------------ signals
    def add_signal(self, run_id: str, name: str, payload: dict[str, Any], sender: str) -> int:
        """Put a signal in the run's inbox and wake the run if it is parked."""
        now = self.now()
        with self._write() as conn:
            row = conn.execute("SELECT status FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if row is None:
                raise RunNotFound(run_id)
            if RunStatus(row["status"]) in TERMINAL_STATUSES:
                raise RunFinished(f"run {run_id} is already {row['status']}")
            cur = conn.execute(
                "INSERT INTO signals (run_id, name, payload, sender, created_at) VALUES (?, ?, ?, ?, ?)",
                (run_id, name, _dumps(payload), sender, now),
            )
            if row["status"] == RunStatus.WAITING.value:
                conn.execute(
                    "UPDATE runs SET ready_at = ? WHERE run_id = ? AND (ready_at IS NULL OR ready_at > ?)",
                    (now, run_id, now),
                )
            return int(cur.lastrowid or 0)

    def next_signal(self, run_id: str, names: Iterable[str]) -> SignalRow | None:
        wanted = list(names)
        if not wanted:
            return None
        marks = ",".join("?" * len(wanted))
        row = self.conn.execute(
            f"SELECT * FROM signals WHERE run_id = ? AND consumed_seq IS NULL AND name IN ({marks})"
            " ORDER BY id LIMIT 1",
            (run_id, *wanted),
        ).fetchone()
        return self._signal(row) if row else None

    def signals(self, run_id: str) -> list[SignalRow]:
        rows = self.conn.execute("SELECT * FROM signals WHERE run_id = ? ORDER BY id", (run_id,))
        return [self._signal(r) for r in rows]

    @staticmethod
    def _signal(row: sqlite3.Row) -> SignalRow:
        return SignalRow(**{**dict(row), "payload": json.loads(row["payload"])})

    # ------------------------------------------------------------------- outbox
    def claim_outbox(self, worker: str, ttl: float, *, limit: int = 10, run_id: str | None = None) -> list[OutboxRow]:
        """Claim due, undelivered messages. A claim expires, so a crashed relay's messages are retried."""
        now = self.now()
        due = "status = 'pending' AND available_at <= :now AND (claimed_by IS NULL OR claim_expires_at <= :now)"
        params: dict[str, Any] = {"now": now, "limit": limit}
        where = due + (" AND run_id = :run_id" if run_id else "")
        if run_id:
            params["run_id"] = run_id
        rows = self.conn.execute(f"SELECT id FROM outbox WHERE {where} ORDER BY id LIMIT :limit", params)
        ids = [r["id"] for r in rows]
        claimed: list[OutboxRow] = []
        if not ids:
            return claimed
        with self._write() as conn:
            for msg_id in ids:
                cur = conn.execute(
                    f"UPDATE outbox SET claimed_by = :worker, claim_expires_at = :expires, attempts = attempts + 1 "
                    f"WHERE id = :id AND {due}",
                    {"worker": worker, "expires": now + ttl, "id": msg_id, "now": now},
                )
                if cur.rowcount == 1:
                    row = conn.execute("SELECT * FROM outbox WHERE id = ?", (msg_id,)).fetchone()
                    claimed.append(self._outbox(row))
        return claimed

    def outbox_delivered(self, msg_id: int, worker: str) -> bool:
        cur = self.conn.execute(
            "UPDATE outbox SET status = 'delivered', delivered_at = ?, delivered_by = ?, claimed_by = NULL "
            "WHERE id = ? AND claimed_by = ? AND status = 'pending'",
            (self.now(), worker, msg_id, worker),
        )
        return cur.rowcount == 1

    def outbox_failed(self, msg_id: int, worker: str, error: str, *, retry_at: float, dead: bool) -> None:
        self.conn.execute(
            "UPDATE outbox SET status = ?, last_error = ?, available_at = ?, claimed_by = NULL "
            "WHERE id = ? AND claimed_by = ?",
            ("dead" if dead else "pending", error[:500], retry_at, msg_id, worker),
        )

    def next_outbox_due(self) -> float | None:
        """Earliest time a pending outbox message can be claimed."""
        row = self.conn.execute(
            "SELECT MIN(MAX(available_at, COALESCE(claim_expires_at, 0))) FROM outbox WHERE status = 'pending'"
        ).fetchone()
        return row[0] if row else None

    def outbox(self, *, run_id: str | None = None, status: str | None = None) -> list[OutboxRow]:
        sql, params = "SELECT * FROM outbox WHERE 1 = 1", []
        if run_id:
            sql += " AND run_id = ?"
            params.append(run_id)
        if status:
            sql += " AND status = ?"
            params.append(status)
        return [self._outbox(r) for r in self.conn.execute(sql + " ORDER BY id", params)]

    @staticmethod
    def _outbox(row: sqlite3.Row) -> OutboxRow:
        return OutboxRow(**{**dict(row), "payload": json.loads(row["payload"])})
