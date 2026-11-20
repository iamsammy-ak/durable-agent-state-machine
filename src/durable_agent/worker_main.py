"""Lightweight worker process: no CLI framework or terminal-rendering imports, so it starts quickly.

    python -m durable_agent.worker_main --db .durable-agent/runs.db --id worker-1

The ``durable-agent worker`` command runs the same loop; supervisors (the demo and the chaos
benchmark) start this module because they start many short-lived workers.
"""

from __future__ import annotations

import argparse
import threading
from pathlib import Path

from .crash import CRASH_POINTS, READY_MARKER, CrashInjector, CrashMode, CrashPlan
from .worker import WorkerStats


def run_worker(
    db: Path,
    *,
    worker_id: str | None = None,
    lease_ttl: float = 5.0,
    poll: float = 0.2,
    max_seconds: float | None = None,
    crash_at: str = "",
    crash_mode: CrashMode = CrashMode.EXIT,
    checkpoint_every: int = 1,
    offline: bool = False,
    log: bool = True,
    announce: bool = False,
) -> WorkerStats:
    """Poll ``db`` for ready runs of the research workflow and drive them until stopped."""
    from .research import build_engine

    engine = build_engine(db, offline=True if offline else None, lease_ttl=lease_ttl,
                          checkpoint_every=checkpoint_every)
    crash = CrashInjector(CrashPlan.parse(crash_at), crash_mode) if crash_at else None
    worker = engine.worker(worker_id, poll_interval=poll, crash=crash, log=log)
    stop = threading.Event()
    if announce:
        print(f"{READY_MARKER} {worker.worker_id}", flush=True)
    try:
        return worker.run(stop=stop, max_seconds=max_seconds)
    except KeyboardInterrupt:
        stop.set()
        return worker.stats
    finally:
        worker.close()
        engine.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m durable_agent.worker_main", description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--id", dest="worker_id")
    parser.add_argument("--ttl", type=float, default=5.0)
    parser.add_argument("--poll", type=float, default=0.2)
    parser.add_argument("--max-seconds", type=float)
    parser.add_argument("--crash-at", default="", help=f"one of {', '.join(CRASH_POINTS)}, as point[@state][:nth]")
    parser.add_argument("--crash-mode", type=CrashMode, default=CrashMode.EXIT)
    parser.add_argument("--checkpoint-every", type=int, default=1)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--announce", action="store_true", help="print a ready marker for a supervisor")
    args = parser.parse_args(argv)
    run_worker(
        args.db,
        worker_id=args.worker_id,
        lease_ttl=args.ttl,
        poll=args.poll,
        max_seconds=args.max_seconds,
        crash_at=args.crash_at,
        crash_mode=args.crash_mode,
        checkpoint_every=args.checkpoint_every,
        offline=args.offline,
        log=not args.quiet,
        announce=args.announce,
    )


if __name__ == "__main__":
    main()
