"""``order-worker``: hosts ``OrderWorkflow`` on ``order-lifecycle``.

Run as ``python -m app.v1.orders.workflows.worker`` from the order image. One worker per
owning service (PLT-D13a): order activities run order's code with order's
role only (PLT-1). The provider fakes live in this process, so their call log
is the worker's (B10).
"""

import asyncio
import logging
from typing import Any

import httpx
from temporalio.client import Client
from temporalio.worker import Worker

# Registers every ORM model: relationships name their targets by string, and the
# worker reaches only some models through its own imports.
import app.models  # noqa: F401
from app.core.config import Settings, get_settings
from app.core.db import create_database
from app.core.logger import init_logger
from app.v1.orders.clients.inventory import InventoryClient
from app.v1.orders.providers.payment import FakePaymentProvider
from app.v1.orders.providers.payment_store import PostgresPaymentStore
from app.v1.orders.providers.shipping import FakeShippingProvider
from app.v1.orders.workflows.activities import OrderActivities
from app.v1.orders.workflows.order_workflow import OrderWorkflow
from smartretail_common.service_token import ServiceCredentials, ServiceTokenClient

logger = logging.getLogger("order.worker")

INVENTORY_SCOPE = "inventory:internal"


def worker_options(settings: Settings) -> dict[str, Any]:
    """ORD-D37: this worker's share of the `order-lifecycle` queue, set explicitly.

    A burst of orders waits on the task queue; each replica pulls at most this
    much at once. Add replicas (`--scale order-worker=N`) for more throughput;
    ORD-D36 makes that safe for payments.
    """
    options: dict[str, Any] = {
        "max_concurrent_activities": settings.order_worker_max_concurrent_activities,
        "max_concurrent_workflow_tasks": settings.order_worker_max_concurrent_workflow_tasks,
    }
    if settings.order_worker_max_activities_per_second is not None:
        options["max_activities_per_second"] = settings.order_worker_max_activities_per_second
    return options


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
            FakePaymentProvider(settings, PostgresPaymentStore(db)),
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
            **worker_options(settings),
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
