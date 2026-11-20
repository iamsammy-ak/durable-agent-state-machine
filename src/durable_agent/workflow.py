"""Typed workflow definitions: states, guarded transitions and a Pydantic state model.

A workflow is a finite state machine. Each state is one of:

* an **activity** - a function with side effects (tool call, LLM call), run with a retry policy;
* a **gate** - waits for a human signal (approve, edit, reject, ...) with an optional timeout;
* a **sleep** - a durable timer;
* a **terminal** state - the run ends there.

Transitions are evaluated in declaration order; the first whose ``on`` matches the trigger and
whose guard accepts the new state model wins. Triggers are ``None`` for a completed activity or
an elapsed sleep, a signal name at a gate, ``"timeout"`` for a gate timeout and ``"failure"``
when an activity exhausts its retries.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ValidationError

from .context import ActivityContext
from .errors import InvalidResult, TransitionError, WorkflowDefinitionError
from .retry import RetryPolicy

S = TypeVar("S", bound=BaseModel)
F = TypeVar("F", bound=Callable[..., Any])

ActivityFn = Callable[[ActivityContext, Any], Any]
Guard = Callable[[Any], bool]
Seconds = float | Callable[[Any], float | None] | None


@dataclass(frozen=True)
class Signal:
    """A signal as a gate handler sees it: name, validated payload and who sent it."""

    name: str
    payload: BaseModel
    sender: str


GateApply = Callable[[Any, Signal], Mapping[str, Any] | None]

RESERVED_TRIGGERS = frozenset({"timeout", "failure", "cancel"})


@dataclass(frozen=True)
class Transition:
    """An edge of the state machine."""

    source: str
    target: str
    on: str | None = None
    guard: Guard | None = None
    label: str | None = None

    def describe(self) -> str:
        parts: list[str] = []
        if self.on:
            parts.append(self.on)
        if self.label:
            parts.append(self.label)
        elif self.guard is not None:
            parts.append(getattr(self.guard, "__name__", "guard").replace("_", " "))
        return " · ".join(parts)


@dataclass(frozen=True)
class ActivityNode:
    name: str
    fn: ActivityFn
    retry: RetryPolicy
    timeout: float | None = None
    description: str = ""


@dataclass(frozen=True)
class GateNode:
    name: str
    signals: Mapping[str, type[BaseModel]]
    timeout: Seconds = None
    assignee: str | None = None
    apply: GateApply | None = None
    description: str = ""

    def timeout_for(self, model: BaseModel) -> float | None:
        value = self.timeout(model) if callable(self.timeout) else self.timeout
        return None if value is None else max(0.0, float(value))


@dataclass(frozen=True)
class SleepNode:
    name: str
    seconds: float | Callable[[Any], float]
    description: str = ""

    def seconds_for(self, model: BaseModel) -> float:
        value = self.seconds(model) if callable(self.seconds) else self.seconds
        return max(0.0, float(value))


@dataclass(frozen=True)
class TerminalNode:
    name: str
    failed: bool = False
    description: str = ""


Node = ActivityNode | GateNode | SleepNode | TerminalNode


@dataclass
class Workflow(Generic[S]):
    """A named, versioned state machine over the state model ``state``."""

    name: str
    state: type[S]
    start: str
    version: int = 1
    description: str = ""
    nodes: dict[str, Node] = field(default_factory=dict)
    transitions: list[Transition] = field(default_factory=list)

    # ------------------------------------------------------------------ building
    def _add(self, node: Node) -> None:
        if not node.name or any(ch in node.name for ch in "#/ "):
            raise WorkflowDefinitionError(f"invalid state name {node.name!r} (no '#', '/' or spaces)")
        if node.name in self.nodes:
            raise WorkflowDefinitionError(f"state {node.name!r} is defined twice")
        self.nodes[node.name] = node

    def activity(
        self, name: str, *, retry: RetryPolicy | None = None, timeout: float | None = None, description: str = ""
    ) -> Callable[[F], F]:
        """Decorator registering ``fn(ctx, state) -> patch`` as the activity of state ``name``."""

        def register(fn: F) -> F:
            self.add_activity(name, fn, retry=retry, timeout=timeout, description=description)
            return fn

        return register

    def add_activity(
        self,
        name: str,
        fn: ActivityFn,
        *,
        retry: RetryPolicy | None = None,
        timeout: float | None = None,
        description: str = "",
    ) -> None:
        if timeout is not None and timeout <= 0:
            raise WorkflowDefinitionError(f"{name}: timeout must be positive")
        self._add(ActivityNode(name, fn, retry or RetryPolicy(), timeout, description))

    def gate(
        self,
        name: str,
        *,
        signals: Mapping[str, type[BaseModel]],
        timeout: Seconds = None,
        assignee: str | None = None,
        apply: GateApply | None = None,
        description: str = "",
    ) -> None:
        """A state that waits for one of ``signals`` (name -> payload model)."""
        self._add(GateNode(name, dict(signals), timeout, assignee, apply, description))

    def sleep(self, name: str, *, seconds: float | Callable[[Any], float], description: str = "") -> None:
        """A durable timer state: the wake-up time is persisted when the state is entered."""
        self._add(SleepNode(name, seconds, description))

    def terminal(self, name: str, *, failed: bool = False, description: str = "") -> None:
        self._add(TerminalNode(name, failed, description))

    def transition(
        self, source: str, target: str, *, on: str | None = None, guard: Guard | None = None, label: str | None = None
    ) -> Workflow[S]:
        self.transitions.append(Transition(source, target, on, guard, label))
        return self

    # ------------------------------------------------------------------- queries
    def node(self, name: str) -> Node:
        try:
            return self.nodes[name]
        except KeyError:
            raise WorkflowDefinitionError(f"workflow {self.name!r} has no state {name!r}") from None

    def outgoing(self, source: str) -> list[Transition]:
        return [t for t in self.transitions if t.source == source]

    def parse_state(self, data: Mapping[str, Any]) -> S:
        return self.state.model_validate(dict(data))

    def has_trigger(self, source: str, on: str) -> bool:
        return any(t.on == on for t in self.outgoing(source))

    def next_state(self, source: str, model: BaseModel, *, on: str | None = None) -> str:
        """Target of the first transition from ``source`` with trigger ``on`` whose guard accepts ``model``."""
        for t in self.outgoing(source):
            if t.on == on and (t.guard is None or t.guard(model)):
                return t.target
        trigger = "completion" if on is None else repr(on)
        raise TransitionError(f"no transition from {source!r} on {trigger} accepts the current state")

    @property
    def signal_models(self) -> dict[str, type[BaseModel]]:
        """Every signal any gate accepts, with its payload model."""
        out: dict[str, type[BaseModel]] = {}
        for node in self.nodes.values():
            if isinstance(node, GateNode):
                out.update(node.signals)
        return out

    def apply_result(self, model: S, value: Any) -> tuple[dict[str, Any], S]:
        """Merge an activity's return value into ``model``.

        Returns the normalised patch (JSON-safe, only fields that changed) and the new model.
        Raises :class:`InvalidResult` if the value names unknown fields or fails validation.
        """
        if value is None:
            return {}, model
        if isinstance(value, BaseModel):
            value = value.model_dump(mode="json")
        if not isinstance(value, Mapping):
            raise InvalidResult(f"an activity must return a mapping, a model or None, not {type(value).__name__}")
        unknown = sorted(set(value) - set(self.state.model_fields))
        if unknown:
            raise InvalidResult(f"unknown state fields {unknown}")
        before = model.model_dump(mode="json")
        try:
            new_model = self.state.model_validate({**before, **dict(value)})
        except ValidationError as exc:
            raise InvalidResult(f"result does not fit {self.state.__name__}: {exc.errors()[0]['msg']}") from exc
        after = new_model.model_dump(mode="json")
        patch = {key: after[key] for key in value if before.get(key) != after[key]}
        return patch, new_model

    # ---------------------------------------------------------------- validation
    def validate(self) -> None:
        """Raise :class:`WorkflowDefinitionError` listing every structural problem."""
        problems: list[str] = []
        if self.start not in self.nodes:
            problems.append(f"start state {self.start!r} is not defined")
        for t in self.transitions:
            for end in (t.source, t.target):
                if end not in self.nodes:
                    problems.append(f"transition {t.source} -> {t.target} uses unknown state {end!r}")
        seen_models: dict[str, type[BaseModel]] = {}
        for name, node in self.nodes.items():
            triggers = {t.on for t in self.outgoing(name)}
            if isinstance(node, TerminalNode):
                if triggers:
                    problems.append(f"terminal state {name!r} has outgoing transitions")
                continue
            if not triggers:
                problems.append(f"state {name!r} has no outgoing transitions")
            if isinstance(node, ActivityNode):
                if None not in triggers:
                    problems.append(f"activity {name!r} needs a transition for completion (on=None)")
                bad = triggers - {None, "failure"}
                if bad:
                    problems.append(f"activity {name!r} has transitions on {sorted(map(str, bad))}")
            elif isinstance(node, GateNode):
                if not node.signals:
                    problems.append(f"gate {name!r} accepts no signals")
                for signal, payload_model in node.signals.items():
                    if signal in RESERVED_TRIGGERS:
                        problems.append(f"gate {name!r}: {signal!r} is a reserved trigger name")
                    if signal not in triggers:
                        problems.append(f"gate {name!r} has no transition for signal {signal!r}")
                    other = seen_models.setdefault(signal, payload_model)
                    if other is not payload_model:
                        problems.append(f"signal {signal!r} has different payload models in different gates")
                bad = triggers - set(node.signals) - {"timeout"}
                if bad:
                    problems.append(f"gate {name!r} has transitions on unknown triggers {sorted(map(str, bad))}")
                if node.timeout is not None and "timeout" not in triggers:
                    problems.append(f"gate {name!r} has a timeout but no 'timeout' transition")
                if node.timeout is None and "timeout" in triggers:
                    problems.append(f"gate {name!r} has a 'timeout' transition but no timeout")
            elif isinstance(node, SleepNode) and triggers != {None}:
                problems.append(f"sleep {name!r} may only have plain (on=None) transitions")
        if self.start in self.nodes:
            reachable, frontier = {self.start}, [self.start]
            while frontier:
                for t in self.outgoing(frontier.pop()):
                    if t.target not in reachable:
                        reachable.add(t.target)
                        frontier.append(t.target)
            for name in self.nodes:
                if name not in reachable:
                    problems.append(f"state {name!r} is unreachable from {self.start!r}")
        if problems:
            raise WorkflowDefinitionError(f"workflow {self.name!r}: " + "; ".join(problems))

    # ------------------------------------------------------------------ diagrams
    def to_mermaid(self) -> str:
        """A Mermaid ``stateDiagram-v2`` of the workflow, generated from the definition itself."""
        lines = ["stateDiagram-v2"]
        lines.append("    classDef gate fill:#fef3c7,stroke:#b45309,color:#1f2937")
        lines.append("    classDef timer fill:#e0f2fe,stroke:#0369a1,color:#1f2937")
        lines.append("    classDef failed fill:#fee2e2,stroke:#b91c1c,color:#1f2937")
        for name, node in self.nodes.items():
            if isinstance(node, ActivityNode):
                note = f"activity, up to {node.retry.max_attempts} attempts"
                if node.timeout is not None:
                    note += f", {node.timeout:g}s timeout"
            elif isinstance(node, GateNode):
                note = "human gate: " + " / ".join(node.signals)
            elif isinstance(node, SleepNode):
                note = "durable timer"
            else:
                note = "end (failed)" if node.failed else "end"
            lines.append(f"    {name} : {note}")
        lines.append(f"    [*] --> {self.start}")
        for t in self.transitions:
            label = t.describe()
            lines.append(f"    {t.source} --> {t.target}" + (f" : {label}" if label else ""))
        for name, node in self.nodes.items():
            if isinstance(node, TerminalNode):
                lines.append(f"    {name} --> [*]")
        groups = {
            "gate": [n for n, v in self.nodes.items() if isinstance(v, GateNode)],
            "timer": [n for n, v in self.nodes.items() if isinstance(v, SleepNode)],
            "failed": [n for n, v in self.nodes.items() if isinstance(v, TerminalNode) and v.failed],
        }
        for css, names in groups.items():
            if names:
                lines.append(f"    class {','.join(names)} {css}")
        return "\n".join(lines) + "\n"
