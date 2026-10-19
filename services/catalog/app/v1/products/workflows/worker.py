"""``catalog-worker``: hosts ``ProductPublishingWorkflow`` on ``catalog-publishing``.

Run as ``python -m app.v1.products.workflows.worker`` from the catalog image. One worker
per owning service (decision PLT-D13a, answering RFC 0001 Q-PLT-1): the
activities run catalog's code with catalog's database role only (PLT-1), and
each service's ``app`` package stays importable on its own.
"""

import asyncio
import logging

import httpx
from temporalio.client import Client
from temporalio.worker import Worker

# Registers every ORM model: relationships name their targets by string, and the
# worker reaches only some models through its own imports.
import app.models  # noqa: F401
from app.core.config import Settings, get_settings
from app.core.db import create_database
from app.core.logger import init_logger
from app.v1.media.storage import MinioStorage
from app.v1.media.tasks.process import CeleryMediaJobs, MediaJobs
from app.v1.products.clients.inventory import InventoryClient
from app.v1.products.workflows.activities import PublishingActivities
from app.v1.products.workflows.publishing import ProductPublishingWorkflow
from smartretail_common.service_token import ServiceCredentials, ServiceTokenClient

logger = logging.getLogger("catalog.worker")

INVENTORY_SCOPE = "inventory:internal"


async def run(settings: Settings) -> None:
    db = create_database(settings)
    tokens = ServiceTokenClient(
        ServiceCredentials(
            settings.identity_token_url,
            settings.service_client_id,
            settings.service_client_secret,
            INVENTORY_SCOPE,
        )
    )
    async with httpx.AsyncClient(timeout=10) as http:
        activities = PublishingActivities(
            db,
            settings,
            MinioStorage(settings),
            InventoryClient(settings.inventory_base_url, tokens, http),
            media_jobs(settings),
        )
        client = await Client.connect(
            settings.temporal_address, namespace=settings.temporal_namespace
        )
        worker = Worker(
            client,
            task_queue=settings.publish_task_queue,
            workflows=[ProductPublishingWorkflow],
            activities=activities.all(),
        )
        logger.info("worker started", extra={"task_queue": settings.publish_task_queue})
        try:
            await worker.run()
        finally:
            await db.dispose()


def media_jobs(settings: Settings) -> MediaJobs | None:
    """The Celery runner when the events profile is up (CATALOG_MEDIA_VIA_CELERY)."""
    if not settings.catalog_media_via_celery:
        return None
    from app.core.celery_app import app as celery_app

    return CeleryMediaJobs(celery_app, timeout=settings.media_job_timeout_seconds)


def main() -> None:  # pragma: no cover - process entry point
    settings = get_settings()
    init_logger(settings)
    asyncio.run(run(settings))


if __name__ == "__main__":  # pragma: no cover
    main()
