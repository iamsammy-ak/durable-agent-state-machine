"""A minimal durable workflow of your own: an expense claim with a retried check and a manager gate.

Run it twice to see durability: the first run stops at the gate; the second run finds the same
claim waiting, approves it and finishes. Delete ``.durable-agent/expenses.db`` to start over.

    PYTHONPATH=src python examples/approval_workflow.py
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel

from durable_agent import ActivityContext, Engine, RetryPolicy, RunStatus, Workflow
from durable_agent.render import history_table
from durable_agent.workflow import Signal


class Expense(BaseModel):
    employee: str
    amount_eur: float
    policy_ok: bool | None = None
    decision: str | None = None


class Decision(BaseModel):
    note: str = ""


wf: Workflow[Expense] = Workflow("expense", Expense, start="check_policy")


@wf.activity("check_policy", retry=RetryPolicy(max_attempts=3, initial_interval=0.1), timeout=5.0)
def check_policy(ctx: ActivityContext, claim: Expense) -> dict[str, object]:
    if ctx.attempt == 1:
        raise ConnectionError("policy service unavailable")  # a transient failure, retried with backoff
    return {"policy_ok": claim.amount_eur <= 500}


@wf.activity("pay")
def pay(ctx: ActivityContext, claim: Expense) -> dict[str, object]:
    # The side effect goes through the outbox, keyed by a stable idempotency key.
    ctx.outbox.send("payments", {"employee": claim.employee, "amount_eur": claim.amount_eur})
    return {}


def record(claim: Expense, signal: Signal) -> dict[str, object]:
    return {"decision": f"{signal.name} by {signal.sender}"}


wf.gate("manager", signals={"approve": Decision, "reject": Decision}, timeout=3 * 24 * 3600.0, apply=record)
wf.terminal("paid")
wf.terminal("declined")
wf.transition("check_policy", "pay", guard=lambda c: bool(c.policy_ok), label="within policy")
wf.transition("check_policy", "manager", label="needs a manager")
wf.transition("manager", "pay", on="approve")
wf.transition("manager", "declined", on="reject")
wf.transition("manager", "declined", on="timeout")
wf.transition("pay", "paid")


class PrintSink:
    def deliver(self, key: str, payload: dict[str, object]) -> None:
        print(f"payment sent once, key {key}: {payload}")


if __name__ == "__main__":
    engine = Engine(Path(".durable-agent/expenses.db"), [wf], sinks={"payments": PrintSink()}, lease_ttl=2.0)
    waiting = [r for r in engine.runs([RunStatus.WAITING]) if r.workflow == "expense"]
    if not waiting:
        run_id = engine.start("expense", {"employee": "Giulia", "amount_eur": 820.0})
        engine.drain()
        print(f"{run_id} is waiting for a manager. Run this script again to approve it.")
    else:
        run_id = waiting[0].run_id
        engine.signal(run_id, "approve", {"note": "conference travel"}, sender="manager@example.org")
        engine.drain()
        from rich.console import Console

        Console(width=120).print(history_table(engine.history(run_id), show_checkpoints=False))
