from __future__ import annotations

from pathlib import Path

import pytest

from durable_agent import Engine, EventType, RunStatus, check_run
from durable_agent.llm import extract_json
from durable_agent.research import (
    CmsSink,
    FlakySearch,
    OfflineResearchLLM,
    ReportDraft,
    ResearchPlan,
    build_engine,
    cms_path,
    get_llm,
)
from durable_agent.research.agent import draft_messages, plan_messages
from durable_agent.research.corpus import TOPICS, search


def test_offline_stand_in_plans_and_drafts_deterministically() -> None:
    llm = OfflineResearchLLM()
    plan = llm.complete_json(plan_messages("solid-state batteries"), ResearchPlan)
    assert plan == llm.complete_json(plan_messages("solid-state batteries"), ResearchPlan)
    assert all("solid-state batteries" in q for q in plan.questions)
    hits = FlakySearch().search_many(plan.questions)
    draft = llm.complete_json(draft_messages("solid-state batteries", plan.questions, hits), ReportDraft)
    assert draft.title.startswith("Solid-state batteries")
    assert "[1]" in draft.body and hits[0].url in draft.body


def test_every_corpus_topic_finds_enough_sources_and_unknown_topics_find_none() -> None:
    llm = OfflineResearchLLM()
    for topic in TOPICS:
        questions = llm.complete_json(plan_messages(topic), ResearchPlan).questions
        assert len(FlakySearch().search_many(questions)) >= 3
    assert search("Background: what is meant by medieval tapestry dyes?") == []


def test_flaky_search_failures_are_deterministic() -> None:
    tool = FlakySearch()
    draws = [tool.failure_for(seed=1, key="r/gather#1", attempt=a, failure_rate=0.5, script=[]) for a in range(1, 40)]
    again = [tool.failure_for(seed=1, key="r/gather#1", attempt=a, failure_rate=0.5, script=[]) for a in range(1, 40)]
    assert draws == again and None in draws and any(d is not None for d in draws)
    assert tool.failure_for(seed=1, key="k", attempt=1, failure_rate=0.0, script=["invalid"]) == "invalid"
    assert tool.failure_for(seed=1, key="k", attempt=2, failure_rate=0.0, script=["invalid"]) is None
    assert tool.failure_for(seed=1, key="k", attempt=9, failure_rate=1.0, script=[]) is not None


def test_get_llm_falls_back_to_the_offline_stand_in_without_a_key() -> None:
    assert isinstance(get_llm(), OfflineResearchLLM)
    assert isinstance(get_llm(offline=True), OfflineResearchLLM)


def test_extract_json_handles_fences_prose_and_reasoning() -> None:
    assert extract_json('```json\n{"a": 1}\n```') == {"a": 1}
    assert extract_json('<think>hmm</think> Sure! {"b": [1, 2]} hope that helps') == {"b": [1, 2]}
    with pytest.raises(ValueError):
        extract_json("no json here")


def _engine(tmp_path: Path) -> Engine:
    return build_engine(tmp_path / "r.db", offline=True, lease_ttl=0.3)


def test_edit_then_publish_puts_the_edited_title_in_the_cms(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    run_id = engine.start("research_report", {"topic": "urban heat islands"})
    engine.drain()
    assert engine.run(run_id).waiting_on == "signal:approval"
    engine.signal(run_id, "edit", {"title": "Cooling cities: the evidence"}, sender="alice")
    engine.drain()
    state = engine.state(run_id)
    assert state.outcome == "done" and state.data["review"] == {"decision": "edit", "by": "alice", "note": ""}
    articles = CmsSink(cms_path(tmp_path / "r.db")).articles(run_id)
    assert [a.title for a in articles] == ["Cooling cities: the evidence"]
    assert articles[0].body == state.data["body"]
    assert check_run(engine.store, run_id) == []


def test_rejection_and_unanswered_escalation_never_publish(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    rejected = engine.start("research_report", {"topic": "solid-state batteries"})
    ignored = engine.start(
        "research_report",
        {"topic": "solid-state batteries", "options": {"approval_timeout": 0.2, "escalation_timeout": 0.2}},
    )
    engine.signal(rejected, "reject", {"reason": "off-brief"}, sender="bob")
    engine.drain()
    assert engine.run(rejected).outcome == "rejected"
    assert engine.run(ignored).outcome == "rejected"
    fired = [e.step for e in engine.history(ignored) if e.type is EventType.TIMER_FIRED]
    assert fired == ["approval#1", "escalation#1"]
    assert CmsSink(cms_path(tmp_path / "r.db")).articles() == []


def test_too_few_sources_ends_in_the_failure_state(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    run_id = engine.start("research_report", {"topic": "medieval tapestry dyes"})
    engine.drain()
    row = engine.run(run_id)
    assert row.status is RunStatus.FAILED and row.outcome == "insufficient_sources"


def test_scripted_search_failures_are_retried_then_succeed(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    run_id = engine.start(
        "research_report",
        {"topic": "durable execution for AI agents",
         "options": {"scripted_failures": ["rate_limit", "unavailable", "timeout"]}},
    )
    engine.drain()
    failed = [e.data["error_type"] for e in engine.history(run_id) if e.type is EventType.STEP_FAILED]
    assert failed == ["RateLimited", "SearchUnavailable", "ActivityTimeout"]
    assert engine.run(run_id).waiting_on == "signal:approval"


def test_an_invalid_query_is_not_retried(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    run_id = engine.start(
        "research_report", {"topic": "solid-state batteries", "options": {"scripted_failures": ["invalid"]}}
    )
    engine.drain()
    row = engine.run(run_id)
    assert row.status is RunStatus.FAILED and "InvalidQuery" in (row.error or "")
