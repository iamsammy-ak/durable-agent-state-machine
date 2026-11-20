# Persistence overhead

Command: `PYTHONPATH=src python benchmarks/overhead.py`

Environment: Python 3.13.2, SQLite 3.45.3, Windows 11, 12 logical CPUs. 7 repeats (configurations interleaved) of a 300-step loop whose state carries ~1 KB of text. Each step is two SQLite write transactions (StepStarted; StepCompleted + next StepScheduled + optional checkpoint).

| Configuration | median ms per step | best ms per step | median overhead vs in memory (ms) |
|---|---:|---:|---:|
| synchronous=NORMAL, checkpoint every 1 | 1.971 | 1.194 | 1.946 |
| synchronous=NORMAL, checkpoint every 10 | 1.282 | 0.885 | 1.258 |
| synchronous=NORMAL, no checkpoints | 1.563 | 0.869 | 1.538 |
| synchronous=FULL, checkpoint every 1 | 3.323 | 2.642 | 3.298 |
| in memory, no persistence | 0.025 | - | - |

| Journal length at the crash (events) | full replay, best (ms) | checkpoint + tail, best (ms) | tail events |
|---:|---:|---:|---:|
| 296 | 16.93 | 1.18 | 19 |
| 1536 | 105.90 | 0.94 | 19 |
| 6186 | 263.22 | 1.07 | 19 |

Timings come from one laptop that was also running other work, so treat them as orders of magnitude; the comparisons between rows are the point.
