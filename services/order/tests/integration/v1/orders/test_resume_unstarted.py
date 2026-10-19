"""``resume_unstarted_orders`` (ORD-9): the commit-then-start gap of checkout,
closed by a Beat job that starts stranded orders' workflows."""

import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import select, text
from temporalio.exceptions import WorkflowAlreadyStartedError

from app.core.celery_app import app as celery_app
from app.v1.orders.models import Order
from app.v1.orders.tasks.maintenance import resume_unstarted
from tests.integration.conftest import checkout_body, place

LATER = datetime.now(UTC) + timedelta(minutes=5)  # past the 30 s grace


async def stranded(client, callers, catalog, workflows, method: str = "tok_ok") -> uuid.UUID:
    workflows.fail_start = True
    product = catalog.add(callers.merchant_a_id)
    r = await place(client, callers.customer_a, checkout_body((product, 1), method=method))
    assert r.status_code == 202, r.text
    workflows.fail_start = False
    return uuid.UUID(r.json()["order_id"])


async def run_ids(app) -> dict[uuid.UUID, str | None]:
    async with app.state.db.sessionmaker() as session:
        rows = await session.execute(select(Order.id, Order.workflow_run_id))
        return {r.id: r.workflow_run_id for r in rows}


class AlreadyStarted:
    """Temporal says the workflow exists: a checkout's start did succeed, only
    its run id was never recorded."""

    def __init__(self) -> None:
        self.described: list[str] = []

    async def start(self, **_: object) -> str:
        raise WorkflowAlreadyStartedError("order-x", "OrderWorkflow")

    async def run_id(self, workflow_id: str) -> str:
        self.described.append(workflow_id)
        return "existing-run"


def test_the_task_runs_every_minute_on_the_order_queue() -> None:
    assert celery_app.conf.task_default_queue == "order.maintenance"
    [entry] = celery_app.conf.beat_schedule.values()
    assert entry["task"] == "smartretail.order.resume_unstarted_orders"
    assert entry["schedule"] == 60.0


async def test_a_stranded_order_is_started_with_its_stored_payment_method(
    app, client, callers, catalog, workflows
) -> None:
    order_id = await stranded(client, callers, catalog, workflows, method="tok_decline")
    assert await resume_unstarted(app.state.db, workflows, now=LATER) == 1
    [started] = workflows.started
    assert started == {
        "order_id": str(order_id),
        "workflow_id": f"order-{order_id}",
        "method": "tok_decline",
    }
    assert (await run_ids(app))[order_id] == "run-1"
    # Recorded, so the next tick leaves it alone.
    assert await resume_unstarted(app.state.db, workflows, now=LATER) == 0


async def test_a_young_order_is_left_to_its_own_checkout(app, client, callers, catalog, workflows):
    await stranded(client, callers, catalog, workflows)
    assert await resume_unstarted(app.state.db, workflows) == 0  # inside the 30 s grace
    assert workflows.started == []


async def test_an_already_started_workflow_is_success(app, client, callers, catalog, workflows):
    order_id = await stranded(client, callers, catalog, workflows)
    temporal = AlreadyStarted()
    assert await resume_unstarted(app.state.db, temporal, now=LATER) == 1
    assert temporal.described == [f"order-{order_id}"]
    assert (await run_ids(app))[order_id] == "existing-run"


async def test_temporal_still_down_leaves_the_order_for_the_next_tick(
    app, client, callers, catalog, workflows
) -> None:
    order_id = await stranded(client, callers, catalog, workflows)
    workflows.fail_start = True
    assert await resume_unstarted(app.state.db, workflows, now=LATER) == 0
    assert (await run_ids(app))[order_id] is None


async def test_an_order_without_a_stored_payment_method_is_skipped(
    app, client, callers, catalog, workflows
) -> None:
    order_id = await stranded(client, callers, catalog, workflows)
    async with app.state.db.engine.begin() as conn:
        await conn.execute(text("UPDATE orders SET payment_method = NULL"))
    assert await resume_unstarted(app.state.db, workflows, now=LATER) == 0
    assert (await run_ids(app))[order_id] is None
