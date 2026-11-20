from __future__ import annotations

import pytest
from helpers import Count, Ok, counter_workflow
from pydantic import BaseModel

from durable_agent import InvalidResult, TransitionError, Workflow, WorkflowDefinitionError
from durable_agent.research import research_workflow


def _noop(ctx: object, state: Count) -> None:
    return None


def _wf() -> Workflow[Count]:
    wf: Workflow[Count] = Workflow("t", Count, start="a")
    wf.add_activity("a", _noop)
    return wf


def test_example_workflows_are_valid() -> None:
    research_workflow.validate()
    counter_workflow(gate=True, gate_timeout=1.0, sleep=0.1, failure_state=True).validate()


@pytest.mark.parametrize(
    ("build", "message"),
    [
        (lambda wf: wf.transition("a", "nowhere"), "unknown state 'nowhere'"),
        (lambda wf: (wf.terminal("end"), wf.transition("a", "end"), wf.transition("end", "a")), "terminal state"),
        (lambda wf: (wf.terminal("end"), wf.transition("a", "end", on="failure")), "needs a transition for completion"),
        (lambda wf: (wf.terminal("end"), wf.transition("a", "end"), wf.terminal("orphan")), "unreachable"),
        (
            lambda wf: (wf.gate("g", signals={"go": Ok}), wf.terminal("end"), wf.transition("a", "g")),
            "no transition for signal 'go'",
        ),
        (
            lambda wf: (
                wf.gate("g", signals={"go": Ok}, timeout=5.0),
                wf.terminal("end"),
                wf.transition("a", "g"),
                wf.transition("g", "end", on="go"),
            ),
            "has a timeout but no 'timeout' transition",
        ),
        (
            lambda wf: (
                wf.gate("g", signals={"timeout": Ok}),
                wf.terminal("end"),
                wf.transition("a", "g"),
                wf.transition("g", "end", on="timeout"),
            ),
            "reserved trigger",
        ),
    ],
)
def test_validate_reports_structural_problems(build: object, message: str) -> None:
    wf = _wf()
    build(wf)  # type: ignore[operator]
    with pytest.raises(WorkflowDefinitionError, match=message):
        wf.validate()


def test_duplicate_and_malformed_state_names_are_rejected() -> None:
    wf = _wf()
    with pytest.raises(WorkflowDefinitionError, match="defined twice"):
        wf.terminal("a")
    with pytest.raises(WorkflowDefinitionError, match="invalid state name"):
        wf.terminal("bad#name")


def test_next_state_uses_the_first_transition_whose_guard_accepts() -> None:
    wf = counter_workflow()
    assert wf.next_state("tick", Count(n=1, target=3)) == "tick"
    assert wf.next_state("tick", Count(n=3, target=3)) == "done"
    with pytest.raises(TransitionError):
        wf.next_state("tick", Count(), on="approve")


def test_apply_result_returns_only_changed_fields() -> None:
    wf = counter_workflow()
    patch, model = wf.apply_result(Count(n=1, note="x"), {"n": 2, "note": "x"})
    assert patch == {"n": 2}
    assert model.n == 2
    assert wf.apply_result(Count(), None) == ({}, Count())
    patch, _ = wf.apply_result(Count(), Count(n=5))
    assert patch == {"n": 5}


@pytest.mark.parametrize("value", [{"unknown": 1}, {"n": "not a number"}, ["a", "list"]])
def test_apply_result_rejects_values_that_do_not_fit_the_state(value: object) -> None:
    with pytest.raises(InvalidResult):
        counter_workflow().apply_result(Count(), value)


def test_signal_models_are_collected_across_gates() -> None:
    assert set(research_workflow.signal_models) == {"approve", "edit", "reject"}

    class Other(BaseModel):
        x: int = 0

    wf = _wf()
    wf.gate("g1", signals={"go": Ok})
    wf.gate("g2", signals={"go": Other})
    wf.terminal("end")
    for t in (("a", "g1"), ("g1", "g2"), ("g2", "end")):
        wf.transition(*t, on=None if t[0] == "a" else "go")
    with pytest.raises(WorkflowDefinitionError, match="different payload models"):
        wf.validate()


def test_mermaid_diagram_is_generated_from_the_definition() -> None:
    text = research_workflow.to_mermaid()
    assert text.startswith("stateDiagram-v2")
    assert "[*] --> plan" in text
    assert "gather --> draft : enough sources" in text
    assert "approval --> escalation : timeout" in text
    assert "class approval,escalation gate" in text
    assert "insufficient_sources --> [*]" in text
