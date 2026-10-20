"""``OrderRepo`` refusals not reached through the happy routes: an order that
does not exist, a signal Temporal will not take, a merchant with no orders."""

import uuid

import pytest

from app.core.errors import OrderNotFound
from app.v1.orders.enums import OrderStatus
from app.v1.orders.repo import Change, OrderRepo
from app.v1.orders.workflows import types as t
from app.v1.orders.workflows.activities import OrderActivities
from tests.integration.conftest import checkout_body, place


@pytest.mark.parametrize(
    "call",
    [
        lambda repo, oid: repo.advance(oid, Change("workflow", status=OrderStatus.FAILED)),
        lambda repo, oid: repo.for_workflow(oid),  # an activity on a deleted order
        lambda repo, oid: repo.get_any(oid),
    ],
    ids=["advance", "for_workflow", "get_any"],
)
async def test_an_unknown_order_is_not_found(app, call) -> None:
    async with app.state.db.sessionmaker() as s:
        with pytest.raises(OrderNotFound):
            await call(OrderRepo(s, app.state.settings), uuid.uuid4())


async def test_a_signal_temporal_refuses_is_503_and_changes_nothing(
    client, callers, catalog, workflows
) -> None:
    r = await place(
        client, callers.customer_a, checkout_body((catalog.add(callers.merchant_a_id), 1))
    )
    order_id = r.json()["order_id"]

    async def refuse(*args, **kwargs) -> None:
        raise ConnectionError("temporal unreachable")

    workflows.request_cancellation = refuse
    r = await client.post(f"/api/v1/orders/{order_id}/cancel", headers=callers.customer_a)
    assert r.status_code == 503 and r.json()["type"].endswith("/workflow-unavailable")
    detail = (await client.get(f"/api/v1/orders/{order_id}", headers=callers.customer_a)).json()
    assert detail["status"] == "pending" and len(detail["timeline"]) == 1


async def test_a_merchant_with_no_orders_gets_an_empty_page(client, callers, catalog) -> None:
    r = await place(
        client, callers.customer_a, checkout_body((catalog.add(callers.merchant_a_id), 1))
    )
    assert r.status_code == 202
    page = (await client.get("/api/v1/orders/merchant", headers=callers.merchant_b)).json()
    assert (page["items"], page["total"]) == ([], 0)


async def test_a_retried_status_activity_returns_the_status_in_effect(
    app, client, callers, catalog
) -> None:
    """The first attempt committed but its ack was lost: the retry is a no-op and
    must still answer, or the saga would compensate an order that succeeded."""
    r = await place(
        client, callers.customer_a, checkout_body((catalog.add(callers.merchant_a_id), 1))
    )
    activities = OrderActivities(app.state.db, app.state.settings, None, None, None)  # type: ignore[arg-type]
    step = t.StatusInput(r.json()["order_id"], "awaiting_payment")
    assert await activities.set_order_status(step) == "awaiting_payment"
    assert await activities.set_order_status(step) == "awaiting_payment"


async def test_a_no_op_advance_returns_a_loaded_order(app, client, callers, catalog) -> None:
    r = await place(
        client, callers.customer_a, checkout_body((catalog.add(callers.merchant_a_id), 1))
    )
    order_id = uuid.UUID(r.json()["order_id"])
    change = Change("workflow", status=OrderStatus.CANCELLED)
    async with app.state.db.sessionmaker() as s:
        await OrderRepo(s, app.state.settings).advance(order_id, change)
    async with app.state.db.sessionmaker() as s:
        again = await OrderRepo(s, app.state.settings).advance(order_id, change)
    # Read after the session closed, as the activity and the carrier route do.
    assert (again.status, again.version) == (OrderStatus.CANCELLED, 2)
