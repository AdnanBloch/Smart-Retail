"""Interactive merchant generation, streamed or synchronous (E12, §3.1.2.1 sequence).

Events: ``status`` ``generating`` → ``token``… → ``usage`` → ``done`` with
``generation_id``. The draft is persisted with its budget charge and the
``assistant.interaction`` outbox row in one transaction after the output
filter. A provider failure before the first token is an ``error`` event (or
``503`` for ``/generate/sync``); after it, an ``error`` event and nothing is
stored; a disconnect cancels the provider stream and stores nothing.
"""

import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from app.core.clients.llm import LLMError, LLMProvider, LLMResult
from app.core.config import Settings
from app.core.enums import FinishReason
from app.core.sse import Emit, SseEvent, problem_event
from app.v1.generations.repo import (
    GenerationParams,
    GenerationRecord,
    GenerationRepo,
    ProductSnapshot,
    build_request,
)
from app.v1.prompts.repo import PromptRepo
from smartretail_common.db import Database
from smartretail_common.errors import DependencyUnavailable

logger = logging.getLogger("assistant.generations")


@dataclass
class GenerationOutcome:
    generation_id: uuid.UUID
    content: str = ""
    usage: dict[str, Any] = field(default_factory=dict)
    finish_reason: FinishReason = FinishReason.STOP
    filtered: bool = False
    error: Exception | None = None


class Generator:
    def __init__(self, db: Database, provider: LLMProvider, settings: Settings) -> None:
        self.db = db
        self.provider = provider
        self.settings = settings

    async def run(
        self,
        snap: ProductSnapshot,
        params: GenerationParams,
        *,
        emit: Emit | None,
        correlation_id: str | None,
        instance: str,
    ) -> GenerationOutcome:
        started = time.monotonic()
        outcome = GenerationOutcome(generation_id=uuid.uuid4())
        async with self.db.sessionmaker() as session:
            request, prompt_version = await build_request(
                PromptRepo(session, self.settings.prompts_dir), self.settings, snap, params
            )
        if emit is not None:
            await emit(SseEvent("status", {"state": "generating"}))
        pieces: list[str] = []
        first_token_at: float | None = None
        result: LLMResult | None = None
        try:
            if emit is None:
                result = await self.provider.complete(request)
                pieces.append(result.text)
            else:
                async for item in self.provider.stream(request):
                    if isinstance(item, LLMResult):
                        result = item
                        continue
                    first_token_at = first_token_at or time.monotonic()
                    pieces.append(item)
                    await emit(SseEvent("token", {"text": item}))
        except (DependencyUnavailable, LLMError) as exc:
            outcome.error = exc
            outcome.finish_reason = FinishReason.ERROR
            if emit is None:
                raise
            status = exc.status if isinstance(exc, DependencyUnavailable) else 502
            slug = exc.type_slug if isinstance(exc, DependencyUnavailable) else "llm-error"
            await emit(
                problem_event(
                    status=status,
                    type_slug=slug,
                    title="Generation failed",
                    detail=str(exc),
                    instance=instance,
                )
            )
            return outcome
        assert result is not None
        latency = int((time.monotonic() - started) * 1000)
        ttft = int((first_token_at - started) * 1000) if first_token_at else None
        record = GenerationRecord(
            generation_id=outcome.generation_id,
            snapshot=snap,
            params=params,
            content="".join(pieces),
            prompt_tokens=result.input_tokens,
            completion_tokens=result.output_tokens,
            model=result.model,
            finish_reason=result.finish_reason,
            latency_ms=latency,
            time_to_first_token_ms=ttft,
            prompt_version=prompt_version,
            correlation_id=correlation_id,
        )
        async with self.db.sessionmaker() as session, session.begin():
            generation, filtered = await GenerationRepo(session, self.settings).insert(record)
            outcome.content = generation.content
            outcome.filtered = filtered
            outcome.usage = {
                "prompt_tokens": result.input_tokens,
                "completion_tokens": result.output_tokens,
                "model": result.model,
                "cost_usd": str(generation.cost_usd),
                "latency_ms": latency,
                "time_to_first_token_ms": ttft,
            }
        outcome.finish_reason = result.finish_reason
        if emit is not None:
            await emit(SseEvent("usage", outcome.usage))
            await emit(
                SseEvent(
                    "done",
                    {
                        "generation_id": str(outcome.generation_id),
                        "finish_reason": str(outcome.finish_reason),
                        "filtered": filtered,
                    },
                )
            )
        return outcome
