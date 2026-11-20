"""Prompts for the research agent and a deterministic offline stand-in for the language model."""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from typing import Any, TypeVar

from pydantic import BaseModel, Field

from ..config import load_settings
from ..llm import LLM, LLMError, Message, OpenRouterLLM
from .corpus import tokens
from .tools import Source

T = TypeVar("T", bound=BaseModel)


class ResearchPlan(BaseModel):
    questions: list[str] = Field(min_length=2, max_length=5)


class ReportDraft(BaseModel):
    title: str = Field(min_length=3, max_length=160)
    body: str = Field(min_length=20)


PLANNER_SYSTEM = (
    "You plan desk research. Break the topic into 3 or 4 specific, answerable research questions "
    "that together cover what it is, the evidence, the limitations and what is changing. "
    'Return JSON: {"questions": ["...", "..."]}.'
)
WRITER_SYSTEM = (
    "You write short evidence briefs. Use only the numbered sources you are given and cite them as [n]. "
    "Write 180-280 words of Markdown: one bold-led paragraph per research question, then a 'Sources' list. "
    "Do not invent facts, numbers or sources. "
    'Return JSON: {"title": "...", "body": "..."}.'
)


def plan_messages(topic: str) -> list[Message]:
    return [
        {"role": "system", "content": PLANNER_SYSTEM},
        {"role": "user", "content": json.dumps({"task": "plan", "topic": topic})},
    ]


def draft_messages(topic: str, questions: Sequence[str], sources: Sequence[Source]) -> list[Message]:
    numbered = [
        {"n": i, "title": s.title, "url": s.url, "snippet": s.snippet, "question": s.question}
        for i, s in enumerate(sources, start=1)
    ]
    payload = {"task": "draft", "topic": topic, "questions": list(questions), "sources": numbered}
    return [{"role": "system", "content": WRITER_SYSTEM}, {"role": "user", "content": json.dumps(payload)}]


def _first_sentence(text: str) -> str:
    match = re.match(r"(.+?[.!?])(\s|$)", text.strip())
    return match.group(1) if match else text.strip()


class OfflineResearchLLM:
    """A deterministic stand-in for a language model, used when no API key is configured.

    It reads the JSON task payload that the prompts carry and answers with templates (research
    plans) and extracts from the gathered sources (drafts). It does not reason or paraphrase; it
    exists so the workflow runs end to end offline, in tests and in the benchmarks.
    """

    name = "offline-stand-in"

    @staticmethod
    def _task(messages: Sequence[Message]) -> dict[str, Any]:
        for message in reversed(messages):
            if message.get("role") == "user":
                try:
                    value = json.loads(message["content"])
                except json.JSONDecodeError:
                    break
                if isinstance(value, dict):
                    return value
        raise LLMError("the offline stand-in only understands this agent's JSON task payloads")

    def complete(self, messages: list[Message]) -> str:
        task = self._task(messages)
        return self._draft(task).body if task.get("task") == "draft" else "\n".join(self._plan(task).questions)

    def complete_json(self, messages: list[Message], schema: type[T]) -> T:
        task = self._task(messages)
        result: BaseModel
        if task.get("task") == "plan":
            result = self._plan(task)
        elif task.get("task") == "draft":
            result = self._draft(task)
        else:
            raise LLMError(f"unknown task {task.get('task')!r}")
        return schema.model_validate(result.model_dump())

    @staticmethod
    def _plan(task: dict[str, Any]) -> ResearchPlan:
        topic = str(task["topic"]).strip().rstrip("?.")
        return ResearchPlan(
            questions=[
                f"Background: what is meant by {topic}?",
                f"Evidence: what do studies and field data say about {topic}?",
                f"Limits: what are the main limitations and risks of {topic}?",
                f"Outlook: what is changing next for {topic}?",
            ]
        )

    @staticmethod
    def _draft(task: dict[str, Any]) -> ReportDraft:
        topic = str(task["topic"]).strip().rstrip("?.")
        sources: list[dict[str, Any]] = task.get("sources", [])
        lines = [f"_Offline stand-in draft: extracts from {len(sources)} sources, no language model involved._", ""]
        used: set[int] = set()
        for question in task.get("questions", []):
            wanted = tokens(question)
            ranked = sorted(
                sources,
                key=lambda s: (s["n"] in used, -len(wanted & tokens(s["title"] + " " + s["snippet"])), s["n"]),
            )
            cited = ranked[:2]
            if cited:
                used.update(s["n"] for s in cited)
                text = " ".join(f"{_first_sentence(s['snippet'])} [{s['n']}]" for s in cited)
                lines += [f"**{question}** {text}", ""]
        lines.append("**Sources**")
        lines += [f"[{s['n']}] {s['title']} ({s['url']})" for s in sources]
        title = f"{topic[:1].upper()}{topic[1:]}: an evidence brief"
        return ReportDraft(title=title, body="\n".join(lines))


def get_llm(offline: bool | None = None) -> LLM:
    """OpenRouter when a key is configured (and offline mode is not forced), else the offline stand-in."""
    settings = load_settings()
    if offline or not settings.live:
        return OfflineResearchLLM()
    return OpenRouterLLM(settings)
