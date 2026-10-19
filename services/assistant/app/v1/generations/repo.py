"""GenerationRepo: ownership check, prompt building, drafts and ``applied`` (§3.1.1.2).

Ownership is checked against catalog with the merchant's own token before
anything is generated: catalog returns the merchant's own products (drafts
included) and other merchants' published ones, so the product's
``merchant_id`` must equal the token's merchant claim, else ``403`` (AST-D24).
A product the merchant cannot see at all is ``404``; catalog down is ``503``.
"""

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.clients.catalog import CatalogClient
from app.core.clients.llm import LLMRequest
from app.core.config import Settings
from app.core.enums import ContentType, FinishReason
from app.core.errors import GenerationNotFound, NotProductOwner, ProductNotFound
from app.core.events import emit_interaction, interaction_payload
from app.core.safety import filter_output
from app.v1.generations.budget_repo import BudgetRepo, cost_of
from app.v1.generations.models import Generation
from app.v1.prompts.repo import PromptRepo
from app.v1.search.clients.embeddings import EmbeddingProvider
from app.v1.search.renderer import category_path, document_from_catalog_product, summary
from app.v1.search.repo import SearchRepo
from smartretail_common.pagination import PageParams, Slice

DEFAULT_TONE = "friendly"
DEFAULT_MAX_LENGTH = 1200


@dataclass(frozen=True)
class GenerationParams:
    content_type: ContentType
    tone: str | None = None
    max_length: int | None = None
    keywords: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {"tone": self.tone, "max_length": self.max_length, "keywords": self.keywords}


@dataclass(frozen=True)
class ProductSnapshot:
    product_id: uuid.UUID
    merchant_id: uuid.UUID
    document: dict[str, Any]
    siblings: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "product_id": str(self.product_id),
            "merchant_id": str(self.merchant_id),
            "document": self.document,
            "siblings": self.siblings,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ProductSnapshot":
        return cls(
            uuid.UUID(data["product_id"]),
            uuid.UUID(data["merchant_id"]),
            data["document"],
            list(data.get("siblings") or []),
        )


async def owned_product(
    catalog: CatalogClient, product_id: uuid.UUID, merchant_id: str, token: str
) -> dict[str, Any]:
    product = await catalog.get_product(product_id, token=token)
    if product is None:
        raise ProductNotFound()
    if str(product.get("merchant_id")) != merchant_id:
        raise NotProductOwner()
    return product


async def snapshot(
    session: AsyncSession,
    embeddings: EmbeddingProvider,
    settings: Settings,
    product: dict[str, Any],
    content_type: ContentType,
) -> ProductSnapshot:
    """The facts a generation is grounded in; cross-sell adds the merchant's
    nearest indexed products in the same category (§3.1.1.2)."""
    document = document_from_catalog_product(product)
    product_id = uuid.UUID(str(product["id"]))
    merchant_id = uuid.UUID(str(product["merchant_id"]))
    siblings: list[str] = []
    if content_type == ContentType.CROSS_SELL:
        category = product.get("category_id")
        hits = await SearchRepo(session, embeddings, settings).similar(
            product_id,
            5,
            merchant_id=merchant_id,
            category_id=uuid.UUID(str(category)) if category else None,
        )
        siblings = [h.name for h in hits or []]
    return ProductSnapshot(product_id, merchant_id, document, siblings)


async def build_request(
    prompts: PromptRepo, settings: Settings, snap: ProductSnapshot, params: GenerationParams
) -> tuple[LLMRequest, int]:
    system = await prompts.system("merchant")
    template = await prompts.active(params.content_type)
    max_length = params.max_length or DEFAULT_MAX_LENGTH
    facts = summary(snap.document)
    body = template.render(
        tone=params.tone or DEFAULT_TONE,
        max_length=max_length,
        keywords=", ".join(params.keywords) or "none",
        product_facts=facts,
        siblings="\n".join(f"- {s}" for s in snap.siblings) or "(none)",
    )
    path = category_path(snap.document)
    request = LLMRequest(
        model=settings.llm_model,
        system=system.body,
        messages=[{"role": "user", "content": body}],
        max_tokens=settings.llm_max_tokens,
        purpose="generate",
        hints={
            "content_type": str(params.content_type),
            "tone": params.tone,
            "keywords": params.keywords,
            "max_length": max_length,
            "siblings": snap.siblings,
            "product": {
                "name": snap.document.get("name"),
                "summary": facts,
                "category": path[-1] if path else None,
            },
        },
    )
    return request, template.version


@dataclass(frozen=True)
class GenerationRecord:
    generation_id: uuid.UUID
    snapshot: ProductSnapshot
    params: GenerationParams
    content: str
    prompt_tokens: int
    completion_tokens: int
    model: str
    finish_reason: FinishReason
    latency_ms: int
    time_to_first_token_ms: int | None
    prompt_version: int
    correlation_id: str | None
    bulk_job_id: uuid.UUID | None = None


class GenerationRepo:
    def __init__(self, session: AsyncSession, settings: Settings) -> None:
        self.session = session
        self.settings = settings

    async def insert(self, record: GenerationRecord) -> tuple[Generation, bool]:
        """Generation row + budget charge + outbox row, in the caller's transaction.
        Returns the row and whether the output filter changed the content."""
        content, filtered = filter_output(record.content)
        cost: Decimal = cost_of(
            self.settings, record.model, record.prompt_tokens, record.completion_tokens
        )
        merchant = str(record.snapshot.merchant_id)
        generation = Generation(
            id=record.generation_id,
            merchant_id=record.snapshot.merchant_id,
            product_id=record.snapshot.product_id,
            content_type=record.params.content_type,
            parameters={**record.params.as_dict(), "prompt_version": record.prompt_version},
            content=content,
            prompt_tokens=record.prompt_tokens,
            completion_tokens=record.completion_tokens,
            cost_usd=cost,
            model=record.model,
            finish_reason=record.finish_reason,
            bulk_job_id=record.bulk_job_id,
        )
        self.session.add(generation)
        await BudgetRepo(self.session, self.settings).charge(
            f"user:{merchant}", record.prompt_tokens + record.completion_tokens, cost
        )
        emit_interaction(
            self.session,
            aggregate_type="generation",
            aggregate_id=record.generation_id,
            correlation_id=record.correlation_id,
            payload=interaction_payload(
                interaction_id=record.generation_id,
                kind="generate",
                generation_id=record.generation_id,
                user_id=merchant,
                merchant_id=merchant,
                content_type=str(record.params.content_type),
                finish_reason=record.finish_reason,
                answered=record.finish_reason in (FinishReason.STOP, FinishReason.LENGTH),
                source_product_ids=[str(record.snapshot.product_id)],
                model=record.model,
                input_tokens=record.prompt_tokens,
                output_tokens=record.completion_tokens,
                cost_usd=cost,
                latency_ms=record.latency_ms,
                time_to_first_token_ms=record.time_to_first_token_ms,
                occurred_at=datetime.now(UTC),
            ),
        )
        await self.session.flush()
        return generation, filtered

    async def owned(self, generation_id: uuid.UUID, merchant_id: str) -> Generation:
        row = await self.session.get(Generation, generation_id)
        if row is None or str(row.merchant_id) != merchant_id:
            raise GenerationNotFound()
        return row

    async def list_for(
        self,
        merchant_id: str,
        params: PageParams,
        *,
        product_id: uuid.UUID | None = None,
        content_type: ContentType | None = None,
    ) -> Slice[Generation]:
        conds = [Generation.merchant_id == uuid.UUID(merchant_id)]
        if product_id is not None:
            conds.append(Generation.product_id == product_id)
        if content_type is not None:
            conds.append(Generation.content_type == content_type)
        total = await self.session.scalar(
            select(func.count()).select_from(Generation).where(*conds)
        )
        rows = (
            await self.session.scalars(
                select(Generation)
                .where(*conds)
                .order_by(Generation.created_at.desc(), Generation.id)
                .limit(params.limit)
                .offset(params.offset)
            )
        ).all()
        return Slice(list(rows), int(total or 0), params)

    async def mark_applied(self, generation_id: uuid.UUID, merchant_id: str) -> None:
        row = await self.owned(generation_id, merchant_id)
        row.applied = True  # false → true only; a repeat is a no-op
        await self.session.commit()
