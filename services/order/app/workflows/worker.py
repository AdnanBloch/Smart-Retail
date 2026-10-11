"""``order-worker``: hosts ``OrderWorkflow`` on ``order-lifecycle``.

Run as ``python -m app.workflows.worker`` from the order image. One worker per
owning service (PLT-D13a): order activities run order's code with order's
role only (PLT-1). The provider fakes live in this process, so their call log
is the worker's (B10).
"""

import asyncio
import logging

import httpx
from temporalio.client import Client
from temporalio.worker import Worker

# Registers every ORM model: relationships name their targets by string, and the
# worker reaches only some models through its own imports.
import app.models  # noqa: F401
from app.clients.inventory import InventoryClient
from app.core.config import Settings, get_settings
from app.core.db import create_database
from app.core.logger import init_logger
from app.providers.payment import FakePaymentProvider
from app.providers.shipping import FakeShippingProvider
from app.workflows.activities import OrderActivities
from app.workflows.order_workflow import OrderWorkflow
from smartretail_common.service_token import ServiceCredentials, ServiceTokenClient

logger = logging.getLogger("order.worker")

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
        activities = OrderActivities(
            db,
            settings,
            InventoryClient(settings.inventory_base_url, tokens, http),
            FakePaymentProvider(settings),
            FakeShippingProvider(settings),
        )
        client = await Client.connect(
            settings.temporal_address, namespace=settings.temporal_namespace
        )
        worker = Worker(
            client,
            task_queue=settings.order_task_queue,
            workflows=[OrderWorkflow],
            activities=activities.all(),
        )
        logger.info("worker started", extra={"task_queue": settings.order_task_queue})
        try:
            await worker.run()
        finally:
            await db.dispose()


def main() -> None:  # pragma: no cover - process entry point
    settings = get_settings()
    init_logger(settings)
    asyncio.run(run(settings))


if __name__ == "__main__":  # pragma: no cover
    main()
