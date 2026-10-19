"""``smartretail.order.resume_unstarted_orders`` — Celery Beat, every minute (ORD-9).

Closes the one atomicity gap in checkout. The order is committed, then its
workflow is started: two systems, so a crash or a Temporal outage between them
leaves a ``pending`` order with no run id. This job finds such orders older
than 30 s and starts their workflow.

Safe to run twice and alongside a slow checkout: the workflow id is
``order-{order_id}`` with ``REJECT_DUPLICATE``, so a second start fails with
``WorkflowAlreadyStartedError``, which is success — the run id is read back
and recorded. The run id is only ever written where it is still null.
"""

import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Protocol

from sqlalchemy import select, update
from temporalio.exceptions import WorkflowAlreadyStartedError

from app.core.celery_app import app
from app.core.config import Settings, get_settings
from app.core.db import create_database
from app.v1.orders.enums import OrderStatus
from app.v1.orders.models import Order
from app.v1.orders.repo import workflow_id_for
from smartretail_common.celery_app import TASK_DEFAULTS, run_async
from smartretail_common.db import Database

logger = logging.getLogger("order.resume")

GRACE = timedelta(seconds=30)  # longer than any checkout's own start attempt
BATCH = 100


class StartableWorkflows(Protocol):
    async def start(
        self,
        *,
        order_id: uuid.UUID,
        workflow_id: str,
        correlation_id: str | None,
        payment_method: str,
    ) -> str: ...

    async def run_id(self, workflow_id: str) -> str: ...


async def resume_unstarted(
    db: Database, workflows: StartableWorkflows, *, now: datetime | None = None
) -> int:
    """Starts the workflow of every stranded order; returns how many it recorded."""
    cutoff = (now or datetime.now(UTC)) - GRACE
    async with db.sessionmaker() as session:
        stranded = (
            await session.execute(
                select(Order.id, Order.workflow_id, Order.payment_method)
                .where(
                    Order.status == OrderStatus.PENDING,
                    Order.workflow_run_id.is_(None),
                    Order.created_at < cutoff,
                )
                .order_by(Order.created_at)
                .limit(BATCH)
            )
        ).all()
    resumed = 0
    for order_id, workflow_id, payment_method in stranded:
        workflow_id = workflow_id or workflow_id_for(order_id)
        if payment_method is None:  # placed before payment_method was stored
            logger.warning(
                "stranded order has no payment method", extra={"order_id": str(order_id)}
            )
            continue
        try:
            try:
                run_id = await workflows.start(
                    order_id=order_id,
                    workflow_id=workflow_id,
                    correlation_id=f"resume:{order_id}",
                    payment_method=payment_method,
                )
            except WorkflowAlreadyStartedError:
                run_id = await workflows.run_id(workflow_id)
        except Exception:  # Temporal still down: the next tick tries again
            logger.warning("order workflow still not started", extra={"order_id": str(order_id)})
            continue
        async with db.sessionmaker() as session:
            await session.execute(
                update(Order)
                .where(Order.id == order_id, Order.workflow_run_id.is_(None))
                .values(workflow_run_id=run_id)
            )
            await session.commit()
        resumed += 1
        logger.info("order workflow resumed", extra={"order_id": str(order_id)})
    return resumed


async def resume(settings: Settings | None = None) -> int:  # pragma: no cover - wiring
    from app.v1.orders.workflows.client import TemporalOrderWorkflows
    from smartretail_common.temporal import TemporalClientFactory

    settings = settings or get_settings()
    db = create_database(settings)
    try:
        workflows = TemporalOrderWorkflows(
            TemporalClientFactory(settings.temporal_address, settings.temporal_namespace), settings
        )
        return await resume_unstarted(db, workflows)
    finally:
        await db.dispose()


@app.task(name="smartretail.order.resume_unstarted_orders", **TASK_DEFAULTS)
def resume_unstarted_orders() -> int:  # pragma: no cover - the Celery shell around `resume`
    return run_async(resume)
