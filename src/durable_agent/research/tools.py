"""The research agent's tools: a deliberately flaky search API and a stand-in publishing API (CMS)."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel

from ..errors import NonRetryableError
from ..retry import stable_rng
from . import corpus

FailureMode = Literal["timeout", "rate_limit", "unavailable", "invalid"]
# How an injected failure splits across modes when it is drawn at random.
RANDOM_MODES: tuple[tuple[FailureMode, float], ...] = (("timeout", 0.4), ("rate_limit", 0.35), ("unavailable", 0.25))


class SearchUnavailable(ConnectionError):
    """HTTP 503 from the search API (transient)."""


class RateLimited(ConnectionError):
    """HTTP 429 from the search API (transient)."""


class SearchTimeout(TimeoutError):
    """The search API hung until the caller gave up (transient)."""


class InvalidQuery(NonRetryableError):
    """HTTP 400: the query was rejected; retrying the same request cannot succeed."""


class Source(BaseModel):
    doc_id: str
    title: str
    url: str
    snippet: str
    question: str


class FlakySearch:
    """Keyword search over the synthetic corpus with injected failures.

    Failures are deterministic per (seed, run, step, attempt): a scripted list for the first attempts
    (for demos) and then a random draw with probability ``failure_rate`` (for the chaos benchmark).
    A ``timeout`` failure hangs until the caller cancels the attempt, as a stuck HTTP call would.
    """

    def __init__(self, *, k: int = 3, hang_seconds: float = 30.0) -> None:
        self.k = k
        self.hang_seconds = hang_seconds
        self.calls = 0
        self._lock = threading.Lock()

    def failure_for(
        self, *, seed: int, key: str, attempt: int, failure_rate: float, script: Sequence[FailureMode]
    ) -> FailureMode | None:
        if attempt <= len(script):
            return script[attempt - 1]
        rng = stable_rng("search", seed, key, attempt)
        if rng.random() >= failure_rate:
            return None
        pick, total = rng.random(), 0.0
        for mode, weight in RANDOM_MODES:
            total += weight
            if pick < total:
                return mode
        return RANDOM_MODES[-1][0]

    def search_many(
        self,
        questions: Sequence[str],
        *,
        failure: FailureMode | None = None,
        cancelled: threading.Event | None = None,
        latency: float = 0.0,
    ) -> list[Source]:
        """One batched API call answering every question (so one failure fails the whole call)."""
        with self._lock:
            self.calls += 1
        if latency > 0 and (cancelled or threading.Event()).wait(latency):
            raise SearchTimeout("call abandoned by the caller")
        if failure == "timeout":
            (cancelled or threading.Event()).wait(self.hang_seconds)
            raise SearchTimeout("search API did not answer")
        if failure == "rate_limit":
            raise RateLimited("HTTP 429 Too Many Requests")
        if failure == "unavailable":
            raise SearchUnavailable("HTTP 503 Service Unavailable")
        if failure == "invalid":
            raise InvalidQuery("HTTP 400 query rejected")
        hits: dict[str, Source] = {}
        for question in questions:
            for doc in corpus.search(question, k=self.k):
                hits.setdefault(
                    doc.doc_id,
                    Source(doc_id=doc.doc_id, title=doc.title, url=doc.url, snippet=doc.text, question=question),
                )
        return list(hits.values())


class Article(BaseModel):
    id: int
    request_id: str
    run_id: str
    title: str
    body: str
    sources: list[str]
    published_at: float


class CmsSink:
    """Stand-in for an external publishing API, stored in its own SQLite file.

    Like a well-behaved real API it accepts an idempotency key (``request_id``): a repeated request
    returns the existing article instead of publishing twice. Every call is logged, so the benchmark
    can count redeliveries. It is also the outbox sink for topic ``cms.publish``.
    """

    TOPIC = "cms.publish"

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id TEXT NOT NULL UNIQUE,
                    run_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    body TEXT NOT NULL,
                    sources TEXT NOT NULL,
                    published_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS calls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    ts REAL NOT NULL
                );
                """
            )

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self.path), timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            yield conn
        finally:
            conn.close()

    def publish(self, *, request_id: str, run_id: str, title: str, body: str, sources: Sequence[str]) -> int:
        """Publish an article; returns its id. Idempotent by ``request_id``."""
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO calls (request_id, run_id, ts) VALUES (?, ?, ?)", (request_id, run_id, time.time())
                )
                row = conn.execute("SELECT id FROM articles WHERE request_id = ?", (request_id,)).fetchone()
                if row is None:
                    cur = conn.execute(
                        "INSERT INTO articles (request_id, run_id, title, body, sources, published_at) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (request_id, run_id, title, body, json.dumps(list(sources)), time.time()),
                    )
                    article_id = int(cur.lastrowid or 0)
                else:
                    article_id = int(row["id"])
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
            return article_id

    def deliver(self, key: str, payload: dict[str, Any]) -> None:
        """Outbox sink entry point: the message key is the idempotency key."""
        self.publish(
            request_id=key,
            run_id=payload["run_id"],
            title=payload["title"],
            body=payload["body"],
            sources=payload.get("sources", []),
        )

    def articles(self, run_id: str | None = None) -> list[Article]:
        with self._connect() as conn:
            sql = "SELECT * FROM articles"
            params: tuple[str, ...] = ()
            if run_id is not None:
                sql, params = sql + " WHERE run_id = ?", (run_id,)
            rows = conn.execute(sql + " ORDER BY id", params).fetchall()
        return [Article(**{**dict(r), "sources": json.loads(r["sources"])}) for r in rows]

    def call_count(self, run_id: str | None = None) -> int:
        with self._connect() as conn:
            if run_id is None:
                return int(conn.execute("SELECT COUNT(*) FROM calls").fetchone()[0])
            return int(conn.execute("SELECT COUNT(*) FROM calls WHERE run_id = ?", (run_id,)).fetchone()[0])
