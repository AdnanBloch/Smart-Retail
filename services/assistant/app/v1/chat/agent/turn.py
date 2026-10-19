"""One customer turn, end to end, as typed events (E3, E5, AST-D38).

Order on the wire: ``status`` (``retrieving`` when the classifier routes to
retrieval) → ``status`` ``generating`` → ``sources`` (always, before the first
token; empty when nothing was retrieved) → ``token``… → ``usage`` → ``done``.

Failure paths:

* provider fails **before** the first token → the retrieved products with a
  short non-AI message, ``finish_reason: error``, still ``usage`` and ``done``
  (G11 (0005));
* provider fails **after** the first token → ``event: error`` with a problem
  body, then the stream ends (the partial answer is persisted);
* client disconnect → the producer task is cancelled, which cancels the
  provider stream; the partial answer is persisted with ``finish_reason:
  error`` (AST-Q3) before the task ends.

Both ``/chat`` (SSE) and ``/chat/sync`` (collects the events) run this.
"""

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from app.core.clients.llm import LLMError, LLMProvider, LLMResult
from app.core.config import Settings
from app.core.enums import FinishReason
from app.core.sse import Emit, SseEvent, problem_event
from app.v1.chat.agent.context import fallback_answer
from app.v1.chat.agent.state import AgentDeps, TurnState, Usage
from app.v1.chat.enums import RETRIEVING, Intent
from app.v1.chat.repo import ConversationRepo, TurnRecord
from app.v1.generations.budget_repo import cost_of
from app.v1.prompts.repo import PromptRepo
from app.v1.search.clients.embeddings import EmbeddingProvider
from app.v1.search.hydration import Hydrator
from app.v1.search.repo import SearchFilters
from app.v1.search.service import SearchService
from smartretail_common.db import Database
from smartretail_common.errors import DependencyUnavailable

logger = logging.getLogger("assistant.chat")

TEMPLATE_MODEL = "template"
_WORDS_SPLIT = " "


@dataclass
class TurnInput:
    message: str
    conversation_id: uuid.UUID
    user_id: str | None
    session_id: str | None
    filters: SearchFilters
    history: list[dict[str, str]]
    prior_sources: list[dict[str, Any]]
    budget_scope: str
    correlation_id: str | None
    instance: str = "/api/v1/assistant/chat"


@dataclass
class TurnOutcome:
    message_id: uuid.UUID
    content: str = ""
    sources: list[dict[str, Any]] = field(default_factory=list)
    finish_reason: FinishReason = FinishReason.STOP
    usage: dict[str, Any] = field(default_factory=dict)
    persisted: bool = False


def _words(text: str) -> list[str]:
    parts = text.split(_WORDS_SPLIT)
    return [p + (_WORDS_SPLIT if i < len(parts) - 1 else "") for i, p in enumerate(parts)]


class ChatTurn:
    def __init__(
        self,
        db: Database,
        *,
        graph: Any,
        provider: LLMProvider,
        embeddings: EmbeddingProvider,
        hydrator: Hydrator,
        settings: Settings,
    ) -> None:
        self.db = db
        self.graph = graph
        self.provider = provider
        self.embeddings = embeddings
        self.hydrator = hydrator
        self.settings = settings

    async def _plan(self, turn: TurnInput, emit: Emit) -> TurnState:
        async with self.db.sessionmaker() as session:
            search = SearchService(session, self.embeddings, self.hydrator, self.settings)
            prompts = PromptRepo(session, self.settings.prompts_dir)
            deps = AgentDeps(
                search=search,
                provider=self.provider,
                settings=self.settings,
                chat_system=await prompts.system("chat"),
                classifier=await prompts.system("classifier"),
            )
            state: dict[str, Any] = {
                "message": turn.message,
                "filters": turn.filters,
                "history": turn.history,
                "prior_sources": turn.prior_sources,
                "usage": Usage(),
            }
            config = {"configurable": {"deps": deps}}
            async for update in self.graph.astream(state, config, stream_mode="updates"):
                for node, delta in update.items():
                    state.update(delta or {})
                    if (
                        node == "classify"
                        and Intent.parse(state["intent"], default=Intent.PRODUCT_SEARCH)
                        in RETRIEVING
                    ):
                        await emit(SseEvent("status", {"state": "retrieving"}))
            await session.commit()  # hydration may have removed stale products
            return state  # type: ignore[return-value]

    async def run(self, turn: TurnInput, emit: Emit) -> TurnOutcome:
        started = time.monotonic()
        outcome = TurnOutcome(message_id=uuid.uuid4())
        try:
            state = await self._plan(turn, emit)
        except Exception as exc:
            logger.exception("turn planning failed")
            await emit(self._problem(exc, turn.instance))
            outcome.finish_reason = FinishReason.ERROR
            return outcome

        usage: Usage = state["usage"]
        sources = state.get("sources") or []
        outcome.sources = sources
        intent = str(state.get("intent") or Intent.PRODUCT_SEARCH)
        await emit(SseEvent("status", {"state": "generating"}))
        await emit(
            SseEvent(
                "sources",
                {"products": sources, "degraded": state.get("degraded")},
            )
        )

        model = TEMPLATE_MODEL
        pieces: list[str] = []
        first_token_at: float | None = None
        finish = FinishReason.STOP
        mid_stream_error: Exception | None = None
        try:
            request = state.get("request")
            if request is None:  # templated refusal: no model call
                finish = FinishReason.REFUSED
                for word in _words(state.get("canned") or ""):
                    first_token_at = first_token_at or time.monotonic()
                    pieces.append(word)
                    await emit(SseEvent("token", {"text": word}))
            else:
                model = request.model
                try:
                    async for item in self.provider.stream(request):
                        if isinstance(item, LLMResult):
                            usage.add(item.model, item.input_tokens, item.output_tokens)
                            finish = item.finish_reason
                            model = item.model
                            continue
                        first_token_at = first_token_at or time.monotonic()
                        pieces.append(item)
                        await emit(SseEvent("token", {"text": item}))
                except (DependencyUnavailable, LLMError) as exc:
                    finish = FinishReason.ERROR
                    if first_token_at is None:  # G11: products, no prose
                        for word in _words(fallback_answer(sources)):
                            first_token_at = first_token_at or time.monotonic()
                            pieces.append(word)
                            await emit(SseEvent("token", {"text": word}))
                    else:
                        mid_stream_error = exc
        except asyncio.CancelledError:
            logger.info("client disconnected; provider stream cancelled")
            outcome.finish_reason = FinishReason.ERROR
            outcome.content = "".join(pieces)
            await self._persist(turn, outcome, intent, usage, model, started, first_token_at)
            raise

        outcome.content = "".join(pieces)
        outcome.finish_reason = finish
        await self._persist(turn, outcome, intent, usage, model, started, first_token_at)
        if mid_stream_error is not None:
            await emit(self._problem(mid_stream_error, turn.instance))
            return outcome
        await emit(SseEvent("usage", outcome.usage))
        done: dict[str, Any] = {
            "message_id": str(outcome.message_id),
            "conversation_id": str(turn.conversation_id),
            "finish_reason": str(outcome.finish_reason),
        }
        if turn.user_id is None:
            done["session_id"] = turn.session_id
        await emit(SseEvent("done", done))
        return outcome

    def _problem(self, exc: Exception, instance: str) -> SseEvent:
        if isinstance(exc, DependencyUnavailable):
            return problem_event(
                status=exc.status,
                type_slug=exc.type_slug,
                title=exc.title,
                detail=exc.detail,
                instance=instance,
            )
        return problem_event(
            status=500,
            type_slug="internal-error",
            title="Internal server error",
            detail=None,
            instance=instance,
        )

    async def _persist(
        self,
        turn: TurnInput,
        outcome: TurnOutcome,
        intent: str,
        usage: Usage,
        model: str,
        started: float,
        first_token_at: float | None,
    ) -> None:
        cost = sum(
            (cost_of(self.settings, m, p, c) for m, (p, c) in usage.cost_by_model.items()),
            Decimal(0),
        )
        latency = int((time.monotonic() - started) * 1000)
        ttft = int((first_token_at - started) * 1000) if first_token_at else None
        outcome.usage = {
            "prompt_tokens": usage.prompt_tokens,
            "completion_tokens": usage.completion_tokens,
            "model": model,
            "cost_usd": str(cost),
            "latency_ms": latency,
            "time_to_first_token_ms": ttft,
        }
        record = TurnRecord(
            conversation_id=turn.conversation_id,
            message_id=outcome.message_id,
            user_id=turn.user_id,
            session_id=turn.session_id,
            question=turn.message,
            answer=outcome.content,
            intent=intent,
            sources=outcome.sources,
            prompt_tokens=usage.prompt_tokens,
            completion_tokens=usage.completion_tokens,
            model=model,
            cost_usd=cost,
            latency_ms=latency,
            time_to_first_token_ms=ttft,
            finish_reason=outcome.finish_reason,
            budget_scope=turn.budget_scope,
            correlation_id=turn.correlation_id,
        )
        try:
            async with self.db.sessionmaker() as session:
                await ConversationRepo(session).persist_turn(record, self.settings)
            outcome.persisted = True
        except Exception:
            logger.exception("turn not persisted", extra={"message_id": str(outcome.message_id)})
