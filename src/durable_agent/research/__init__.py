"""Example: a research-report agent with a flaky search tool, an LLM draft step, a human approval
gate and a publish step whose side effect goes through the transactional outbox."""

from __future__ import annotations

from .agent import OfflineResearchLLM, ReportDraft, ResearchPlan, get_llm
from .corpus import DOCUMENTS, TOPICS
from .tools import CmsSink, FlakySearch, Source
from .workflow import (
    ApprovePayload,
    EditPayload,
    RejectPayload,
    ReportOptions,
    ReportState,
    Review,
    build_engine,
    cms_path,
    research_workflow,
)

__all__ = [
    "DOCUMENTS",
    "TOPICS",
    "ApprovePayload",
    "CmsSink",
    "EditPayload",
    "FlakySearch",
    "OfflineResearchLLM",
    "RejectPayload",
    "ReportDraft",
    "ReportOptions",
    "ReportState",
    "ResearchPlan",
    "Review",
    "Source",
    "build_engine",
    "cms_path",
    "get_llm",
    "research_workflow",
]
