"""Render the README's terminal screenshots: the demo, and the history of the run it produces.

    PYTHONPATH=src python scripts/render_assets.py

Writes assets/demo.svg and assets/history.svg (and leaves the demo database in .durable-agent/).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from rich.console import Console
from rich.terminal_theme import MONOKAI

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
os.chdir(ROOT)  # the demo prints its database path; keep it relative

from durable_agent.demo import DEMO_RUN_ID, run_demo  # noqa: E402
from durable_agent.render import history_table  # noqa: E402
from durable_agent.research import build_engine  # noqa: E402


def main() -> None:
    db = Path(".durable-agent/demo.db")
    with open(os.devnull, "w", encoding="utf-8") as sink:
        demo = Console(record=True, width=100, file=sink)
        if not run_demo(demo, db, lease_ttl=1.0):
            raise SystemExit("the demo reported a failure")
        demo.save_svg(str(ROOT / "assets" / "demo.svg"), title="python -m durable_agent demo", theme=MONOKAI)

        engine = build_engine(db, offline=True)
        timeline = Console(record=True, width=140, file=sink)
        timeline.print(history_table(engine.history(DEMO_RUN_ID)))
        timeline.save_svg(str(ROOT / "assets" / "history.svg"), title=f"durable-agent history {DEMO_RUN_ID}",
                          theme=MONOKAI)
        engine.close()
    for name in ("demo.svg", "history.svg"):  # drop the exporter's comment line from the SVG header
        svg = ROOT / "assets" / name
        lines = svg.read_text(encoding="utf-8").splitlines(keepends=True)
        svg.write_text("".join(line for line in lines if not line.lstrip().startswith("<!--")), encoding="utf-8")
    print("wrote assets/demo.svg and assets/history.svg")


if __name__ == "__main__":
    main()
