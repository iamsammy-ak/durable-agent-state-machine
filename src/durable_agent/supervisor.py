"""Start worker subprocesses and kill them for real.

``Process.kill()`` sends SIGKILL on Linux and calls TerminateProcess on Windows: no cleanup code runs.
"""

from __future__ import annotations

import os
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path

from .crash import MARKER, READY_MARKER, CrashPlan


def package_root() -> Path:
    """Directory that contains the ``durable_agent`` package (put on the child's ``PYTHONPATH``)."""
    return Path(__file__).resolve().parent.parent


def worker_command(
    db_path: Path,
    worker_id: str,
    *,
    lease_ttl: float,
    poll: float,
    crash: CrashPlan | None = None,
    max_seconds: float = 600.0,
) -> list[str]:
    cmd = [
        sys.executable, "-m", "durable_agent.worker_main",
        "--db", str(db_path), "--id", worker_id, "--ttl", str(lease_ttl), "--poll", str(poll),
        "--max-seconds", str(max_seconds), "--offline", "--quiet", "--announce",
    ]
    if crash is not None:
        cmd += ["--crash-at", str(crash), "--crash-mode", "wait"]
    return cmd


class WorkerProcess:
    """A worker subprocess whose stdout is scanned for crash-point markers."""

    def __init__(self, cmd: list[str], worker_id: str, *, log_path: Path | None = None) -> None:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(package_root()) + os.pathsep + env.get("PYTHONPATH", "")
        env["PYTHONIOENCODING"] = "utf-8"
        env["DURABLE_AGENT_OFFLINE"] = "1"
        self.worker_id = worker_id
        self.markers: queue.Queue[tuple[str, float]] = queue.Queue()
        self.started_at = time.time()
        self.ready_at: float | None = None
        self._log = open(log_path, "ab") if log_path is not None else None
        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=self._log if self._log is not None else subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            env=env,
        )
        threading.Thread(target=self._read, name=f"read-{worker_id}", daemon=True).start()

    def _read(self) -> None:
        assert self.proc.stdout is not None
        with self.proc.stdout:
            for line in self.proc.stdout:
                if line.startswith(READY_MARKER):
                    self.ready_at = time.time()
                elif line.startswith(MARKER):
                    self.markers.put((line[len(MARKER) :].strip(), time.time()))

    @property
    def alive(self) -> bool:
        return self.proc.poll() is None

    def wait_for_marker(self, timeout: float) -> str | None:
        """Block until the worker reports reaching its crash point (or it exits / times out)."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                return self.markers.get(timeout=0.05)[0]
            except queue.Empty:
                if not self.alive:
                    return None
        return None

    def kill(self) -> float:
        """Hard-kill the process; returns the wall-clock time just before the kill."""
        at = time.time()
        for _ in range(5):  # TerminateProcess is asynchronous; on a loaded machine the exit can lag
            if not self.alive:
                break
            self.proc.kill()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                continue
        if self._log is not None:
            self._log.close()
        return at
