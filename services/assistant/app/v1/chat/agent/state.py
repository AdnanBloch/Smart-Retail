"""The customer graph's typed state and its injected dependencies (E2).

Nodes read only the state and the dependencies passed in the run config, and
return the keys they change — so each is testable with a constructed state.
"""

from dataclasses import dataclass, field
from typing import Any, TypedDict

from app.core.clients.llm import LLMProvider, LLMRequest
from app.core.config import Settings
from app.v1.chat.agent.intents import Classification
from app.v1.prompts.repo import Template
from app.v1.search.repo import Hit, SearchFilters
from app.v1.search.service import SearchService


@dataclass
class Usage:
    prompt_tokens: int = 0
    completion_tokens: int = 0
    cost_by_model: dict[str, tuple[int, int]] = field(default_factory=dict)

    def add(self, model: str, prompt: int, completion: int) -> None:
        self.prompt_tokens += prompt
        self.completion_tokens += completion
        before = self.cost_by_model.get(model, (0, 0))
        self.cost_by_model[model] = (before[0] + prompt, before[1] + completion)


class TurnState(TypedDict, total=False):
    message: str
    filters: SearchFilters
    history: list[dict[str, str]]
    prior_sources: list[dict[str, Any]]
    memory: list[dict[str, str]]
    classification: Classification
    intent: str
    candidates: list[Hit]
    follow_up_sources: list[dict[str, Any]]
    sources: list[dict[str, Any]]
    retrieved: bool
    degraded: str | None
    request: LLMRequest | None
    canned: str | None
    usage: Usage


@dataclass
class AgentDeps:
    search: SearchService
    provider: LLMProvider
    settings: Settings
    chat_system: Template
    classifier: Template
    categories: list[str] = field(default_factory=list)
