"""Activities of ``ProductPublishingWorkflow``: thin adapters over ``PublishingRepo`` (B1).

Each opens its own session with catalog's role (R1, PLT-1), calls one repo
method, and returns a small serialisable result. A 4xx ``DomainError`` is a
business outcome and becomes a non-retryable ``ApplicationError`` (INV-5);
anything else is retried under the workflow's policy. Each is idempotent
under its ``(workflow_id, activity)`` pair (B3): re-running converges on the
same rows and objects.
"""

import uuid
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from temporalio import activity
from temporalio.exceptions import ApplicationError

from app.core.config import Settings
from app.v1.media.storage import MediaStorage
from app.v1.media.tasks.process import InlineMediaJobs, MediaJobFailed, MediaJobs
from app.v1.products.clients.inventory import InventoryClient
from app.v1.products.enums import PublishStep
from app.v1.products.publishing_repo import PublishingRepo
from app.v1.products.workflows import types as t
from smartretail_common.db import Database
from smartretail_common.temporal import as_activity_errors, business_failure

T = TypeVar("T")


class PublishingActivities:
    def __init__(
        self,
        db: Database,
        settings: Settings,
        storage: MediaStorage,
        inventory: InventoryClient,
        media_jobs: MediaJobs | None = None,
    ) -> None:
        self.db = db
        self.settings = settings
        self.storage = storage
        self.inventory = inventory
        # Inline unless the worker is given the Celery runner (0002-Q7, PLT-D43).
        self.media_jobs = media_jobs or InlineMediaJobs(storage, settings)

    def all(self) -> list[Callable[..., Any]]:
        return [
            self.validate_product,
            self.process_media,
            self.build_search_document,
            self.sync_downstream,
            self.mark_published,
            self.record_publish_failure,
        ]

    async def _with_repo(self, fn: Callable[[PublishingRepo], Awaitable[T]]) -> T:
        async def call() -> T:
            async with self.db.sessionmaker() as session:
                return await fn(PublishingRepo(session))

        return await as_activity_errors(call)

    @activity.defn(name=t.VALIDATE_PRODUCT)
    async def validate_product(self, inp: t.PublishInput) -> list[str]:
        ids = await self._with_repo(
            lambda repo: repo.validate(uuid.UUID(inp.product_id), inp.workflow_id)
        )
        return [str(i) for i in ids]

    @activity.defn(name=t.PROCESS_MEDIA)
    async def process_media(self, inp: t.MediaInput) -> None:
        """Derive the configured sizes — on Celery's ``catalog.media`` queue when the
        worker has the Celery runner, inline otherwise (``app.v1.media.tasks.process``)."""
        media_id = uuid.UUID(inp.media_id)
        source = await self._with_repo(lambda repo: repo.media_source(media_id))

        async def run() -> dict[str, Any]:
            try:
                return await self.media_jobs.run(
                    media_id=media_id,
                    product_id=source.product_id,
                    storage_key=source.storage_key,
                    heartbeat=activity.heartbeat,
                )
            except MediaJobFailed as exc:
                raise ApplicationError(str(exc), type="MediaJobFailed") from exc

        try:
            result = await as_activity_errors(run)
            if "undecodable" in result:
                await self._record_media(inp, None, result["undecodable"])
                raise business_failure("MediaUndecodable", result["undecodable"])
        except ApplicationError as exc:
            final = activity.info().attempt >= self.settings.publish_media_max_attempts
            if final and not exc.non_retryable:
                await self._record_media(inp, None, str(exc))
            raise
        await self._record_media(inp, result["keys"], None)

    async def _record_media(
        self, inp: t.MediaInput, derived: dict[str, str] | None, error: str | None
    ) -> None:
        await self._with_repo(
            lambda repo: repo.record_media(
                uuid.UUID(inp.media_id),
                inp.publish.workflow_id,
                derived=derived,
                error=error,
                actor_id=inp.publish.actor_id,
            )
        )

    @activity.defn(name=t.BUILD_SEARCH_DOCUMENT)
    async def build_search_document(self, inp: t.PublishInput) -> str:
        built_at = await self._with_repo(
            lambda repo: repo.build_search_document(uuid.UUID(inp.product_id), inp.workflow_id)
        )
        return built_at.isoformat()

    @activity.defn(name=t.SYNC_DOWNSTREAM)
    async def sync_downstream(self, inp: t.PublishInput) -> int:
        """Zero-quantity stock rows for every active variant; existing rows untouched."""
        product_id = uuid.UUID(inp.product_id)
        payload = await self._with_repo(
            lambda repo: repo.stock_payload(product_id, inp.workflow_id)
        )
        await self.inventory.initialise_stock(product_id, payload.merchant_id, payload.variants)
        return len(payload.variants)

    @activity.defn(name=t.MARK_PUBLISHED)
    async def mark_published(self, inp: t.PublishInput) -> int:
        product = await self._with_repo(
            lambda repo: repo.complete(uuid.UUID(inp.product_id), inp.workflow_id, inp.actor_id)
        )
        return product.version

    @activity.defn(name=t.RECORD_PUBLISH_FAILURE)
    async def record_publish_failure(self, inp: t.FailureInput) -> bool:
        step = PublishStep.parse(inp.failed_step, default=PublishStep.VALIDATE_PRODUCT)
        return await self._with_repo(
            lambda repo: repo.fail(
                uuid.UUID(inp.publish.product_id),
                inp.publish.workflow_id,
                step,
                inp.reason,
                actor_id=inp.publish.actor_id,
            )
        )
