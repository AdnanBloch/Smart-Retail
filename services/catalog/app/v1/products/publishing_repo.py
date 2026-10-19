"""Publishing: accepting a publish and every write the workflow makes (RFC 0004 §3.1.1.4).

The API side (``accept``, ``status``) and the workflow side (``validate``,
``record_media``, ``build_search_document``, ``stock_payload``, ``complete``,
``fail``) live together because they share one guard: the product is
``publishing`` *and* ``publish_workflow_id`` names the caller. Activities are
thin adapters over these methods (B1); no rule lives in workflow code.

``complete`` is the only write that makes a product public, and it is one
transaction: status, ``published_at``, version, audit row and outbox row.
"""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import (
    MediaNotFound,
    ProductIncomplete,
    ProductNotFound,
    PublishAlreadyRunning,
    PublishStartFailed,
    PublishSuperseded,
)
from app.v1.categories.repo import CategoryRepo
from app.v1.media.enums import MediaProcessingStatus
from app.v1.media.models import ProductMedia
from app.v1.products import access, audit_repo, completeness, events, search_document
from app.v1.products.enums import ProductAuditAction, ProductStatus, PublishState, PublishStep
from app.v1.products.models import Product, ProductPublishFailure
from app.v1.products.repo import bump, load_detail, lock_product
from app.v1.products.state_machine import transition
from smartretail_common.auth import Principal
from smartretail_common.context import get_request_id

# A failure reason is free text from an exception; keep rows bounded.
MAX_REASON = 2000


class PublishStarter(Protocol):
    """The orchestrator, as the repo needs it. ``workflows/client.py`` beside it is Temporal's."""

    async def start(
        self, *, product_id: uuid.UUID, workflow_id: str, actor_id: str, correlation_id: str | None
    ) -> None: ...

    async def is_running(self, workflow_id: str) -> bool: ...

    async def current_step(self, workflow_id: str) -> dict[str, Any] | None: ...


@dataclass(frozen=True)
class PublishAccepted:
    product: Product
    workflow_id: str


@dataclass(frozen=True)
class PublishStatus:
    state: PublishState
    workflow_id: str | None = None
    step: str | None = None
    started_at: str | None = None
    failure: ProductPublishFailure | None = None


@dataclass(frozen=True)
class MediaSource:
    media_id: uuid.UUID
    product_id: uuid.UUID
    storage_key: str
    content_type: str | None


@dataclass(frozen=True)
class StockPayload:
    merchant_id: uuid.UUID
    variants: list[tuple[uuid.UUID, str]]


def workflow_id_for(product: Product) -> str:
    """CAT-7: deterministic, and unique per accept because each accept bumps ``version``."""
    return f"product-publish-{product.id}-v{product.version}"


def ensure_current(product: Product, workflow_id: str) -> None:
    if product.status != ProductStatus.PUBLISHING or product.publish_workflow_id != workflow_id:
        raise PublishSuperseded(
            f"The product is {product.status} with publish {product.publish_workflow_id}"
        )


class PublishingRepo:
    def __init__(self, session: AsyncSession, starter: PublishStarter | None = None) -> None:
        self.session = session
        self.starter = starter

    def _starter(self) -> PublishStarter:
        assert self.starter is not None, "the API side needs a PublishStarter"
        return self.starter

    # --- API side ------------------------------------------------------------

    async def accept(
        self, principal: Principal, product_id: uuid.UUID, version: int
    ) -> PublishAccepted:
        """T3/T4: commit ``publishing`` first, then start the workflow (CAT-9).

        Committing before starting means a workflow can never run against a
        product that is not ``publishing``; a start that fails sends the product
        back to ``draft`` with ``failed_step = start``.
        """
        product = await lock_product(self.session, product_id)
        access.ensure_can_manage(principal, product)
        access.ensure_version(product, version)
        if product.status == ProductStatus.PUBLISHING:
            running = product.publish_workflow_id or ""
            if running and await self._starter().is_running(running):
                raise PublishAlreadyRunning(running)
            # A stuck row (a start that failed twice): recover it as a fresh accept.
            self._record_failure(
                product, running, PublishStep.START, "workflow not running", principal.subject
            )
        transition(product, ProductStatus.PUBLISHING)
        bump(product)
        workflow_id = workflow_id_for(product)
        product.publish_workflow_id = workflow_id
        audit_repo.record(
            self.session,
            product_id=product.id,
            actor_id=principal.subject,
            action=ProductAuditAction.STATUS_CHANGED,
            changes={"status": {"to": ProductStatus.PUBLISHING.value}, "workflow_id": workflow_id},
        )
        await self.session.commit()

        try:
            await self._starter().start(
                product_id=product.id,
                workflow_id=workflow_id,
                actor_id=principal.subject,
                correlation_id=get_request_id(),
            )
        except Exception as exc:
            await self.fail(
                product.id,
                workflow_id,
                PublishStep.START,
                f"{type(exc).__name__}: {exc}",
                actor_id=principal.subject,
            )
            raise PublishStartFailed("The product is back in draft; retry the publish") from exc
        return PublishAccepted(await load_detail(self.session, product.id), workflow_id)

    async def status(self, principal: Principal, product_id: uuid.UUID) -> PublishStatus:
        product = await self.session.get(Product, product_id)
        if product is None:
            raise ProductNotFound()
        access.ensure_can_manage(principal, product)
        workflow_id = product.publish_workflow_id
        if product.status == ProductStatus.PUBLISHING and workflow_id:
            step: dict[str, Any] | None = None
            try:
                step = await self._starter().current_step(workflow_id)
            except Exception:  # Temporal down: report the row's state without a step
                step = None
            return PublishStatus(
                PublishState.RUNNING,
                workflow_id,
                step=(step or {}).get("step"),
                started_at=(step or {}).get("started_at"),
            )
        if product.status == ProductStatus.PUBLISHED:
            return PublishStatus(PublishState.PUBLISHED, workflow_id)
        if workflow_id:
            failure = await self.session.scalar(
                select(ProductPublishFailure)
                .where(
                    ProductPublishFailure.product_id == product_id,
                    ProductPublishFailure.workflow_id == workflow_id,
                )
                .order_by(ProductPublishFailure.created_at.desc())
                .limit(1)
            )
            if failure is not None:
                return PublishStatus(PublishState.FAILED, workflow_id, failure=failure)
        return PublishStatus(PublishState.IDLE, workflow_id)

    # --- workflow side -------------------------------------------------------

    async def validate(self, product_id: uuid.UUID, workflow_id: str) -> list[uuid.UUID]:
        """Step 1: read-only. Returns the media that still need processing."""
        product = await load_detail(self.session, product_id)
        ensure_current(product, workflow_id)
        missing = completeness.missing_fields(product)
        if missing:
            raise ProductIncomplete(missing)
        return [
            m.id
            for m in product.media
            if m.processing_status != MediaProcessingStatus.READY and m.storage_key
        ]

    async def media_source(self, media_id: uuid.UUID) -> MediaSource:
        media = await self.session.get(ProductMedia, media_id)
        if media is None or media.storage_key is None:
            raise MediaNotFound()
        return MediaSource(media.id, media.product_id, media.storage_key, media.content_type)

    async def record_media(
        self,
        media_id: uuid.UUID,
        workflow_id: str,
        *,
        derived: dict[str, str] | None,
        error: str | None,
        actor_id: str,
    ) -> None:
        """Step 2's result. Last-writer-wins on identical values, so a retry is harmless."""
        media = await self.session.get(ProductMedia, media_id)
        if media is None:
            raise MediaNotFound()
        product = await lock_product(self.session, media.product_id)
        ensure_current(product, workflow_id)
        await self.session.refresh(media, with_for_update=True)
        if derived is not None:
            media.processing_status = MediaProcessingStatus.READY
            media.derived = derived
        else:
            media.processing_status = MediaProcessingStatus.FAILED
        audit_repo.record(
            self.session,
            product_id=product.id,
            actor_id=actor_id,
            action=ProductAuditAction.MEDIA_PROCESSED,
            changes={
                "media_id": media.id,
                "processing_status": str(media.processing_status),
                "workflow_id": workflow_id,
                **({"error": error[:MAX_REASON]} if error else {}),
            },
        )
        await self.session.commit()

    async def build_search_document(self, product_id: uuid.UUID, workflow_id: str) -> datetime:
        """Step 3: persisted, never sent through Temporal history (B2). No version bump:
        the projection is invisible until step 5."""
        product = await lock_product(self.session, product_id)
        ensure_current(product, workflow_id)
        detail = await load_detail(self.session, product_id)
        built_at = datetime.now(UTC)
        detail.search_document = search_document.build(
            detail, category_path=await self._category_path(detail), built_at=built_at
        )
        await self.session.commit()
        return built_at

    async def stock_payload(self, product_id: uuid.UUID, workflow_id: str) -> StockPayload:
        """What step 4 sends to inventory: every active variant."""
        product = await load_detail(self.session, product_id)
        ensure_current(product, workflow_id)
        return StockPayload(
            product.merchant_id, [(v.id, v.sku) for v in product.variants if v.is_active]
        )

    async def complete(self, product_id: uuid.UUID, workflow_id: str, actor_id: str) -> Product:
        """Step 5 (T5): the one transaction that makes the product public.

        Already ``published`` by this workflow → a no-op success, so a retried
        step publishes exactly once.
        """
        product = await lock_product(self.session, product_id)
        if product.status == ProductStatus.PUBLISHED and product.publish_workflow_id == workflow_id:
            return product
        ensure_current(product, workflow_id)
        detail = await load_detail(self.session, product_id)
        missing = completeness.missing_fields(detail, require_ready_media=True)
        if missing:
            raise ProductIncomplete(missing)
        transition(detail, ProductStatus.PUBLISHED)
        detail.published_at = datetime.now(UTC)
        bump(detail)
        audit_repo.record(
            self.session,
            product_id=detail.id,
            actor_id=actor_id,
            action=ProductAuditAction.STATUS_CHANGED,
            changes={
                "status": {
                    "from": ProductStatus.PUBLISHING.value,
                    "to": ProductStatus.PUBLISHED.value,
                },
                "workflow_id": workflow_id,
            },
        )
        await self.session.flush()
        events.published(self.session, detail)
        await self.session.commit()
        return detail

    async def fail(
        self,
        product_id: uuid.UUID,
        workflow_id: str,
        failed_step: PublishStep,
        reason: str,
        *,
        actor_id: str,
    ) -> bool:
        """The compensation (T6): back to ``draft`` with a failure row. Media and the
        search document are left as they are: nothing before step 5 is visible.

        Returns ``False`` when there was nothing to do — already compensated, or
        superseded by a newer publish — so a retried compensation is a no-op.
        """
        product = await lock_product(self.session, product_id)
        if product.status != ProductStatus.PUBLISHING or product.publish_workflow_id != workflow_id:
            await self.session.rollback()
            return False
        self._record_failure(product, workflow_id, failed_step, reason, actor_id)
        await self.session.commit()
        return True

    def _record_failure(
        self,
        product: Product,
        workflow_id: str,
        failed_step: PublishStep,
        reason: str,
        actor_id: str,
    ) -> None:
        transition(product, ProductStatus.DRAFT)
        bump(product)
        self.session.add(
            ProductPublishFailure(
                product_id=product.id,
                workflow_id=workflow_id,
                failed_step=failed_step,
                reason=reason[:MAX_REASON],
            )
        )
        audit_repo.record(
            self.session,
            product_id=product.id,
            actor_id=actor_id,
            action=ProductAuditAction.PUBLISH_FAILED,
            changes={
                "status": {"from": ProductStatus.PUBLISHING.value, "to": ProductStatus.DRAFT.value},
                "workflow_id": workflow_id,
                "failed_step": failed_step.value,
                "reason": reason[:MAX_REASON],
            },
        )

    async def _category_path(self, product: Product) -> list[str]:
        if product.category_id is None:
            return []
        found = await CategoryRepo(self.session).get(product.category_id)
        return [c.name for c in found.path] + [found.category.name]
