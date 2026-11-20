# Chaos benchmark

Command: `PYTHONPATH=src python benchmarks/chaos_bench.py --runs 200 --crashes 200 --workers 8 --seed 7 --lease-ttl 1`

Environment: Python 3.13.2, SQLite 3.45.3, Windows 11, 12 logical CPUs. Offline: deterministic stand-in LLM and a synthetic corpus; the search tool fails 30% of attempts on purpose.

Setup: research-workflow runs are admitted two per worker (at least 200, more while kill plans remain), 8 worker processes that receive kill plans plus 1 standby worker(s) that are never killed, lease TTL 1s, seed 7. Kill plans: 75% at named crash points (seeded point and hit number), 25% at a random time after the worker is ready; a planned worker whose crash point never comes is killed after 6s. Approver decisions are seeded: approve 70%, edit 15%, reject 10%, none 5%.

| Metric | outbox (as designed) | direct call (baseline) |
|---|---|---|
| Runs | 756 | 926 |
| Worker processes killed | 200 | 196 |
| ... at a named crash point / random time / unfired plan | 136 / 54 / 10 | 122 / 54 / 20 |
| Kills that interrupted a leased run | 163 | 170 |
| Runs ending as their decision implies | 756/756 (100.0%) | 926/926 (100.0%) |
| **Runs published twice** | **0** | **10** |
| Approved runs never published | 0 | 0 |
| CMS content differing from the journal | 0 | 0 |
| Redeliveries absorbed by the idempotency key | 19 | n/a |
| Journal invariant violations | 0 | 0 |
| Completed steps executed again | 0 | 0 |
| Activity executions (StepStarted) | 3297 | 4037 |
| In-doubt attempts re-run (WorkerLost) | 89 | 94 |
| Retries exhausted | 0 | 0 |
| Recovery time p50 / p95 / max (s) | 1.0091 / 1.1823 / 2.0703 | 0.9886 / 1.0262 / 1.0791 |
| Replay on takeover p50 / max (ms) | 1.158 / 3.371 | 0.95 / 6.661 |
| Events / checkpoints written | 17996 / 3712 | 22019 / 4547 |
| Wall time (s) | 126.8 | 56.91 |

Failed attempts by type (outbox run): ActivityTimeout 130, RateLimited 108, WorkerLost 89, SearchUnavailable 64.

Attempts needed by the flaky `gather` step (outbox run): 1 attempt: 522 runs, 2 attempts: 162 runs, 3 attempts: 48 runs, 4 attempts: 15 runs, 5 attempts: 5 runs, 6 attempts: 3 runs, 8 attempts: 1 runs.

Kills by crash point (outbox run): `gate.before_signal` 21, `outbox.after_deliver` 17, `random` 64, `run.after_claim` 29, `step.after_commit` 18, `step.after_start` 23, `step.before_commit` 19, `timer.before_fire` 9.

Recovery time is measured from the kill to the `RunRecovered` event written by the worker that took over. It is bounded below by the remaining lease (heartbeats renew it every TTL/3) and includes the poll interval and, when every healthy worker is busy or still starting, the start-up of a replacement.
