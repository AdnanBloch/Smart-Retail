"""``ShipmentRepo.apply_carrier_update`` (RFC 0005 §3.1.1.4, ORD-2): one
implementation for the admin simulation and the signed carrier webhook."""

import uuid

import pytest
from sqlalchemy import select

from app.core.errors import OrderNotFound
from app.models import OrderStatusHistory
from app.v1.orders.enums import OrderStatus, PaymentStatus, ShipmentStatus
from app.v1.orders.repo import Change, OrderRepo
from app.v1.orders.shipment_repo import ShipmentRepo
from app.v1.orders.state_machine import IllegalTransition
from tests.integration.conftest import checkout_body, outbox_rows, place

S, P, H = OrderStatus, PaymentStatus, ShipmentStatus
CARRIER = "carrier:acme"


async def _pending(client, callers, catalog) -> uuid.UUID:
    r = await place(
        client, callers.customer_a, checkout_body((catalog.add(callers.merchant_a_id), 1))
    )
    assert r.status_code == 202, r.text
    return uuid.UUID(r.json()["order_id"])


async def _confirmed(app, client, callers, catalog) -> uuid.UUID:
    order_id = await _pending(client, callers, catalog)
    async with app.state.db.sessionmaker() as s:
        repo = OrderRepo(s, app.state.settings)
        for change in (
            Change("workflow", status=S.AWAITING_PAYMENT, payment=P.PENDING),
            Change("workflow", payment=P.SUCCEEDED, payment_reference="pay_1"),
            Change("workflow", shipment=H.CREATED, shipment_reference="shp_1"),
            Change("workflow", status=S.CONFIRMED),
        ):
            await repo.advance(order_id, change)
    return order_id


async def _apply(app, order_id, status, **kw):
    async with app.state.db.sessionmaker() as s:
        return await ShipmentRepo(s, app.state.settings).apply_carrier_update(
            order_id, status, actor=CARRIER, **kw
        )


async def _timeline(app, order_id) -> list[tuple[str | None, str, str]]:
    async with app.state.db.sessionmaker() as s:
        rows = await s.scalars(
            select(OrderStatusHistory)
            .where(OrderStatusHistory.order_id == order_id)
            .order_by(OrderStatusHistory.id)
        )
        return [(r.from_status, r.to_status, r.actor) for r in rows]


async def test_each_carrier_status_and_its_effect_on_the_order(
    app, client, callers, catalog, database_url
) -> None:
    order_id = await _confirmed(app, client, callers, catalog)

    shipped = await _apply(app, order_id, H.SHIPPED, carrier_event_id="ev-1")
    assert shipped.applied and (shipped.order.status, shipped.order.shipment_status) == (
        S.SHIPPED,
        H.SHIPPED,
    )
    # in_transit moves only the shipment; the order stays shipped.
    moving = await _apply(
        app, order_id, H.IN_TRANSIT, tracking_number="TRK-2", carrier_event_id="ev-2"
    )
    assert (moving.order.status, moving.order.shipment_status) == (S.SHIPPED, H.IN_TRANSIT)
    assert moving.order.tracking_number == "TRK-2"
    delivered = await _apply(app, order_id, H.DELIVERED, carrier_event_id="ev-3")
    assert (delivered.order.status, delivered.order.shipment_status) == (S.DELIVERED, H.DELIVERED)

    assert (await _timeline(app, order_id))[-2:] == [
        ("confirmed", "shipped", CARRIER),
        ("shipped", "delivered", CARRIER),
    ]
    changes = await outbox_rows(database_url, "shipment.status_changed")
    assert [(c["from_status"], c["to_status"], c["carrier_event_id"]) for c in changes] == [
        ("created", "shipped", "ev-1"),
        ("shipped", "in_transit", "ev-2"),
        ("in_transit", "delivered", "ev-3"),
    ]
    assert changes[1]["tracking_number"] == "TRK-2"


async def test_a_repeated_status_is_a_no_op(app, client, callers, catalog, database_url) -> None:
    """A carrier redelivers webhooks: the second copy changes nothing."""
    order_id = await _confirmed(app, client, callers, catalog)
    first = await _apply(app, order_id, H.SHIPPED)
    again = await _apply(app, order_id, H.SHIPPED, tracking_number="IGNORED")
    assert first.applied and not again.applied
    assert again.order.version == first.order.version
    assert again.order.tracking_number is None
    assert len(await outbox_rows(database_url, "shipment.status_changed")) == 1


@pytest.mark.parametrize("status", [H.SHIPPED, H.DELIVERED])
async def test_an_unconfirmed_order_refuses_carrier_updates(
    app, client, callers, catalog, database_url, status
) -> None:
    order_id = await _pending(client, callers, catalog)
    with pytest.raises(IllegalTransition) as exc:
        await _apply(app, order_id, status)
    assert exc.value.extras == {"field": "shipment_status", "from": "none", "to": str(status)}
    assert await outbox_rows(database_url, "shipment.status_changed") == []


async def test_skipping_or_reversing_a_carrier_status_is_refused(app, client, callers, catalog):
    order_id = await _confirmed(app, client, callers, catalog)
    with pytest.raises(IllegalTransition):  # created → in_transit skips shipped
        await _apply(app, order_id, H.IN_TRANSIT)
    await _apply(app, order_id, H.SHIPPED)
    await _apply(app, order_id, H.DELIVERED)
    with pytest.raises(IllegalTransition):  # delivered is final
        await _apply(app, order_id, H.IN_TRANSIT)
    assert (await _apply(app, order_id, H.DELIVERED)).applied is False


async def test_an_unknown_order_is_not_found(app) -> None:
    with pytest.raises(OrderNotFound):
        await _apply(app, uuid.uuid4(), H.SHIPPED)


async def test_the_route_reports_an_unknown_order_as_404(client, callers) -> None:
    r = await client.post(
        f"/api/v1/orders/{uuid.uuid4()}/shipment/status",
        json={"status": "shipped"},
        headers=callers.admin,
    )
    assert r.status_code == 404 and r.json()["type"].endswith("/order-not-found")
