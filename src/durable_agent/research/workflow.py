"""The example workflow: a research-report agent with a human approval gate.

    plan -> gather -> draft -> approval gate -> publish -> done
                 \\-> insufficient_sources        |-> escalation gate (on timeout)
                                                 \\-> rejected

* ``gather`` calls a flaky search API (injected timeouts, 429s and 503s) under a retry policy.
* ``draft`` calls the language model (OpenRouter when a key is set, an offline stand-in otherwise).
* ``approval`` waits for a person to approve, edit or reject; if nobody answers in time the run
  escalates, and an unanswered escalation auto-rejects - nothing is published without approval.
* ``publish`` emits its side effect through the transactional outbox (or, as a benchmark baseline,
  calls the publishing API directly with a fresh request id per attempt).
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from ..context import ActivityContext
from ..engine import Engine
from ..llm import LLM
from ..retry import RetryPolicy
from ..workflow import Signal, Workflow
from .agent import ReportDraft, ResearchPlan, draft_messages, get_llm, plan_messages
from .tools import CmsSink, FailureMode, FlakySearch, Source


class ApprovePayload(BaseModel):
    note: str = ""


class EditPayload(BaseModel):
    title: str | None = Field(default=None, min_length=3)
    body: str | None = Field(default=None, min_length=20)
    note: str = ""

    @model_validator(mode="after")
    def _something_to_edit(self) -> EditPayload:
        if self.title is None and self.body is None:
            raise ValueError("an edit needs a new title or body")
        return self


class RejectPayload(BaseModel):
    reason: str = ""


class Review(BaseModel):
    decision: Literal["approve", "edit", "reject"]
    by: str
    note: str = ""


class ReportOptions(BaseModel):
    """Run settings (part of the input, so they are journaled with the run)."""

    min_sources: int = Field(default=3, ge=1)
    failure_rate: float = Field(default=0.0, ge=0.0, le=1.0, description="Injected search failure probability.")
    scripted_failures: list[FailureMode] = Field(default_factory=list, description="Failures for attempts 1..n.")
    fault_seed: int = 0
    approval_timeout: float | None = Field(default=24 * 3600.0, gt=0)
    escalation_timeout: float | None = Field(default=24 * 3600.0, gt=0)
    publish_mode: Literal["outbox", "direct"] = "outbox"
    search_latency: float = Field(default=0.0, ge=0.0, description="Simulated seconds per search call.")


class ReportState(BaseModel):
    topic: str = Field(min_length=3)
    options: ReportOptions = Field(default_factory=ReportOptions)
    questions: list[str] = Field(default_factory=list)
    sources: list[Source] = Field(default_factory=list)
    title: str | None = None
    body: str | None = None
    drafted_by: str | None = None
    review: Review | None = None
    publication_key: str | None = None


SIGNALS: dict[str, type[BaseModel]] = {"approve": ApprovePayload, "edit": EditPayload, "reject": RejectPayload}

research_workflow: Workflow[ReportState] = Workflow(
    "research_report", ReportState, start="plan", description="Research brief with human approval"
)


@research_workflow.activity(
    "plan", retry=RetryPolicy(max_attempts=4, initial_interval=0.5, max_interval=10.0), timeout=120.0
)
def plan(ctx: ActivityContext, state: ReportState) -> dict[str, Any]:
    """Language model: split the topic into research questions."""
    llm: LLM = ctx.resource("llm")
    result = llm.complete_json(plan_messages(state.topic), ResearchPlan)
    return {"questions": result.questions[:4]}


@research_workflow.activity(
    "gather",
    retry=RetryPolicy(max_attempts=10, initial_interval=0.1, max_interval=2.0, non_retryable=("InvalidQuery",)),
    timeout=0.5,
)
def gather(ctx: ActivityContext, state: ReportState) -> dict[str, Any]:
    """Flaky search API: one batched call for all questions."""
    tool: FlakySearch = ctx.resource("search")
    opts = state.options
    failure = tool.failure_for(
        seed=opts.fault_seed,
        key=ctx.idempotency_key,
        attempt=ctx.attempt,
        failure_rate=opts.failure_rate,
        script=opts.scripted_failures,
    )
    hits = tool.search_many(state.questions, failure=failure, cancelled=ctx.cancelled, latency=opts.search_latency)
    return {"sources": hits}


@research_workflow.activity(
    "draft", retry=RetryPolicy(max_attempts=4, initial_interval=1.0, max_interval=20.0), timeout=180.0
)
def draft(ctx: ActivityContext, state: ReportState) -> dict[str, Any]:
    """Language model: write the brief from the numbered sources."""
    llm: LLM = ctx.resource("llm")
    result = llm.complete_json(draft_messages(state.topic, state.questions, state.sources), ReportDraft)
    return {"title": result.title.strip(), "body": result.body.strip(), "drafted_by": llm.name}


@research_workflow.activity(
    "publish", retry=RetryPolicy(max_attempts=5, initial_interval=0.2, max_interval=5.0), timeout=30.0
)
def publish(ctx: ActivityContext, state: ReportState) -> dict[str, Any]:
    """Side effect: publish the approved brief to the CMS."""
    title, body, sources = state.title or "", state.body or "", [s.url for s in state.sources]
    if state.options.publish_mode == "direct":
        # Baseline for the benchmark: call the API from the activity with a new request id per
        # attempt, as naive agent code does. A crash after this call and before the step commits
        # makes the retry publish a second copy.
        cms: CmsSink = ctx.resource("cms")
        request_id = uuid.uuid4().hex
        cms.publish(request_id=request_id, run_id=ctx.run_id, title=title, body=body, sources=sources)
        return {"publication_key": request_id}
    payload = {"run_id": ctx.run_id, "title": title, "body": body, "sources": sources}
    return {"publication_key": ctx.outbox.send(CmsSink.TOPIC, payload)}


def apply_review(state: ReportState, signal: Signal) -> dict[str, Any]:
    """Gate handler: record who decided what; an edit also replaces the title and/or body."""
    payload = signal.payload
    note = getattr(payload, "note", "") or getattr(payload, "reason", "")
    review = Review.model_validate({"decision": signal.name, "by": signal.sender, "note": note})
    patch: dict[str, Any] = {"review": review}
    if isinstance(payload, EditPayload):
        if payload.title is not None:
            patch["title"] = payload.title
        if payload.body is not None:
            patch["body"] = payload.body
    return patch


def enough_sources(state: ReportState) -> bool:
    return len(state.sources) >= state.options.min_sources


research_workflow.gate(
    "approval",
    signals=SIGNALS,
    timeout=lambda s: s.options.approval_timeout,
    assignee="editor",
    apply=apply_review,
)
research_workflow.gate(
    "escalation",
    signals=SIGNALS,
    timeout=lambda s: s.options.escalation_timeout,
    assignee="managing-editor",
    apply=apply_review,
)
research_workflow.terminal("done")
research_workflow.terminal("rejected")
research_workflow.terminal("insufficient_sources", failed=True)

research_workflow.transition("plan", "gather")
research_workflow.transition("gather", "draft", guard=enough_sources, label="enough sources")
research_workflow.transition("gather", "insufficient_sources", label="too few sources")
research_workflow.transition("draft", "approval")
for _gate in ("approval", "escalation"):
    research_workflow.transition(_gate, "publish", on="approve")
    research_workflow.transition(_gate, "publish", on="edit")
    research_workflow.transition(_gate, "rejected", on="reject")
research_workflow.transition("approval", "escalation", on="timeout")
research_workflow.transition("escalation", "rejected", on="timeout", label="auto-reject")
research_workflow.transition("publish", "done")


def cms_path(db_path: str | Path) -> Path:
    """Where the stand-in CMS keeps its own database, next to the runtime database."""
    path = Path(db_path)
    return path.with_name(path.stem + ".cms.db")


def build_engine(
    db_path: str | Path,
    *,
    offline: bool | None = None,
    llm: LLM | None = None,
    lease_ttl: float = 5.0,
    checkpoint_every: int = 1,
    synchronous: str = "NORMAL",
) -> Engine:
    """An engine with the research workflow, its tools, the LLM and the CMS outbox sink."""
    cms = CmsSink(cms_path(db_path))
    return Engine(
        db_path,
        [research_workflow],
        sinks={CmsSink.TOPIC: cms},
        resources={"llm": llm or get_llm(offline), "search": FlakySearch(), "cms": cms},
        lease_ttl=lease_ttl,
        checkpoint_every=checkpoint_every,
        synchronous=synchronous,
    )
