"""``OrderRepo.advance`` (I2, I3, I8, G8), reads and isolation, cancel, carrier
simulation and the workflow diagnostic (RFC 0005 §3.1.2)."""

import uuid

import pytest

from app.core.errors import VersionConflict
from app.orders.enums import (
    OrderFailureReason,
    OrderStatus,
    PaymentStatus,
    RefundOutcome,
    ShipmentStatus,
)
from app.orders.repo import Change, OrderRepo
from app.orders.state_machine import IllegalTransition
from tests.integration.conftest import checkout_body, outbox_rows, place

S, P, H = OrderStatus, PaymentStatus, ShipmentStatus


async def _order(client, callers, catalog, *, who="customer_a", lines=None) -> dict:
    a = catalog.add(callers.merchant_a_id, price="10.00")
    b = catalog.add(callers.merchant_b_id, price="5.00")
    body = checkout_body(*(lines or [(a, 1), (b, 2)]))
    r = await place(client, getattr(callers, who), body)
    assert r.status_code == 202, r.text
    return r.json()


async def _advance(app, order_id: str, **change) -> None:
    async with app.state.db.sessionmaker() as s:
        await OrderRepo(s, app.state.settings).advance(
            uuid.UUID(order_id), Change(actor="workflow", **change)
        )


async def _confirm(app, order_id: str) -> None:
    """What the saga's forward path writes, step by step."""
    await _advance(app, order_id, status=S.AWAITING_PAYMENT, payment=P.PENDING)
    await _advance(app, order_id, payment=P.SUCCEEDED, payment_reference="pay_1")
    await _advance(
        app, order_id, shipment=H.CREATED, shipment_reference="shp_1", tracking_number="TRK1"
    )
    await _advance(app, order_id, status=S.CONFIRMED)


async def test_forward_path_events_and_timeline(client, callers, catalog, app, database_url):
    order = await _order(client, callers, catalog)
    await _confirm(app, order["order_id"])
    # A retried step is an idempotent no-op: no new version, no new rows.
    await _advance(app, order["order_id"], status=S.CONFIRMED)

    detail = (await client.get(f"/api/v1/orders/{order['order_id']}", headers=callers.admin)).json()
    assert detail["status"] == "confirmed" and detail["confirmed_at"]
    assert detail["payment"] == {"status": "succeeded", "reference": "pay_1"}
    assert detail["shipment"] == {
        "status": "created",
        "reference": "shp_1",
        "tracking_number": "TRK1",
    }
    assert detail["version"] == 5
    assert [(t["from"], t["to"]) for t in detail["timeline"]] == [
        (None, "pending"),
        ("pending", "awaiting_payment"),
        ("awaiting_payment", "confirmed"),
    ]

    events = await outbox_rows(database_url)
    assert [e["type"] for e in events] == ["order.created", "payment.succeeded", "order.confirmed"]
    confirmed = events[2]
    assert confirmed["payment_reference"] == "pay_1" and confirmed["tracking_number"] == "TRK1"
    assert {i["product_name"] for i in confirmed["items"]}  # names travel on confirmed
    # Every later event of the order is caused by its order.created.
    assert events[1]["causation_id"] == confirmed["causation_id"] is not None


async def test_guards_and_rollback(client, callers, catalog, app, database_url) -> None:
    order = await _order(client, callers, catalog)
    oid = uuid.UUID(order["order_id"])
    async with app.state.db.sessionmaker() as s:
        repo = OrderRepo(s, app.state.settings)
        with pytest.raises(IllegalTransition):  # skipping payment entirely
            await repo.advance(oid, Change(actor="workflow", status=S.CONFIRMED))
        with pytest.raises(VersionConflict):
            await repo.advance(oid, Change(actor="x", status=S.CANCELLED, expected_version=9))
        await repo.advance(
            oid, Change(actor="workflow", status=S.AWAITING_PAYMENT, payment=P.PENDING)
        )
        with pytest.raises(IllegalTransition):  # the confirm guard: not charged, not shipped
            await repo.advance(oid, Change(actor="workflow", status=S.CONFIRMED))
        with pytest.raises(IllegalTransition):
            await repo.advance(oid, Change(actor="workflow", shipment=H.DELIVERED))
    # G8: a refused change leaves no outbox row (only order.created exists).
    assert [e["type"] for e in await outbox_rows(database_url)] == ["order.created"]


async def test_failure_paths_write_their_events(client, callers, catalog, app, database_url):
    declined = await _order(client, callers, catalog)
    await _advance(app, declined["order_id"], status=S.AWAITING_PAYMENT, payment=P.PENDING)
    await _advance(
        app,
        declined["order_id"],
        status=S.FAILED,
        payment=P.FAILED,
        failure_reason=OrderFailureReason.PAYMENT_DECLINED,
        context={"stage": "charging"},
    )

    refunded = await _order(client, callers, catalog)
    oid = uuid.UUID(refunded["order_id"])
    await _advance(app, refunded["order_id"], status=S.AWAITING_PAYMENT, payment=P.PENDING)
    await _advance(app, refunded["order_id"], payment=P.SUCCEEDED, payment_reference="pay")
    await _advance(
        app,
        refunded["order_id"],
        status=S.REFUNDING,
        failure_reason=OrderFailureReason.FULFILMENT_FAILED,
    )
    async with app.state.db.sessionmaker() as s:
        repo = OrderRepo(s, app.state.settings)
        await repo.record_refund(oid, RefundOutcome.REFUNDED, None)
        await repo.record_refund(oid, RefundOutcome.REFUNDED, None)  # no-op
        await repo.mark_notified(oid)
        await repo.mark_notified(oid)
    await _advance(app, refunded["order_id"], status=S.FAILED)

    cancelled = await _order(client, callers, catalog)
    await _advance(
        app,
        cancelled["order_id"],
        status=S.CANCELLED,
        reason="changed my mind",
        context={"stage": "reserving"},
    )

    failed = [e for e in await outbox_rows(database_url, "order.failed")]
    assert [(e["failure_reason"], e["payment_status"]) for e in failed] == [
        ("PAYMENT_DECLINED", "failed"),
        ("FULFILMENT_FAILED", "refunded"),
    ]
    assert failed[0]["failed_step"] == "charging"
    [payment_failed] = await outbox_rows(database_url, "payment.failed")
    assert payment_failed["reason"] == "PAYMENT_DECLINED"
    [cancel] = await outbox_rows(database_url, "order.cancelled")
    assert (cancel["reason"], cancel["stage"]) == ("changed my mind", "reserving")
    detail = (
        await client.get(f"/api/v1/orders/{refunded['order_id']}", headers=callers.admin)
    ).json()
    assert detail["status"] == "failed" and detail["failure_reason"] == "FULFILMENT_FAILED"
    assert detail["notified_at"] is not None
    assert [t["to"] for t in detail["timeline"]][-2:] == ["refunding", "failed"]


async def test_nothing_captured_refund_records_failed_payment(client, callers, catalog, app):
    order = await _order(client, callers, catalog)
    oid = uuid.UUID(order["order_id"])
    await _advance(app, order["order_id"], status=S.AWAITING_PAYMENT, payment=P.PENDING)
    async with app.state.db.sessionmaker() as s:
        updated = await OrderRepo(s, app.state.settings).record_refund(
            oid, RefundOutcome.NOTHING_CAPTURED, None
        )
    assert updated.payment_status == P.FAILED


async def test_isolation(client, callers, catalog) -> None:
    mine = await _order(client, callers, catalog)
    theirs = await _order(client, callers, catalog, who="customer_b")
    url = "/api/v1/orders"

    own = (await client.get(url, headers=callers.customer_a)).json()
    assert [o["order_id"] for o in own["items"]] == [mine["order_id"]]
    # Another customer's order is 404, which does not confirm it exists.
    assert (
        await client.get(f"{url}/{theirs['order_id']}", headers=callers.customer_a)
    ).status_code == 404
    assert (
        await client.get(f"{url}/{mine['order_id']}", headers=callers.merchant_a)
    ).status_code == 403
    assert (await client.get(url, headers=callers.merchant_a)).status_code == 403

    everyone = (await client.get(url, headers=callers.admin)).json()
    assert everyone["total"] == 2
    one = (
        await client.get(f"{url}?customer_id={callers.customer_b_id}", headers=callers.admin)
    ).json()
    assert [o["order_id"] for o in one["items"]] == [theirs["order_id"]]
    pending = (
        await client.get(
            f"{url}?status=pending&from=2000-01-01T00:00:00Z&to=2999-01-01T00:00:00Z",
            headers=callers.admin,
        )
    ).json()
    assert pending["total"] == 2

    merchant = (await client.get(f"{url}/merchant", headers=callers.merchant_a)).json()
    assert merchant["total"] == 2
    for row in merchant["items"]:
        assert {line["merchant_id"] for line in row["lines"]} == {callers.merchant_a_id}
        assert row["merchant_subtotal"] == "10.00"
        assert "total" not in row and "shipping_address" not in row
    assert (await client.get(f"{url}/merchant", headers=callers.customer_a)).status_code == 403
    assert (await client.get(f"{url}/merchant", headers=callers.admin)).status_code == 403


async def test_cancel_requests(client, callers, catalog, app, workflows) -> None:
    order = await _order(client, callers, catalog)
    url = f"/api/v1/orders/{order['order_id']}/cancel"

    workflows.stage = "charging"
    r = await client.post(url, json={"reason": "too slow"}, headers=callers.customer_a)
    assert r.status_code == 202 and r.json() == {
        "order_id": order["order_id"],
        "status": "pending",
        "cancellation": "requested",
    }
    assert workflows.cancellations == [
        {
            "workflow_id": f"order-{order['order_id']}",
            "actor": f"customer:{callers.customer_a_id}",
            "reason": "too slow",
        }
    ]

    workflows.stage = "committing"
    r = await client.post(url, headers=callers.admin)
    assert r.status_code == 409 and r.json()["type"].endswith("/past-cancellable-point")
    workflows.stage = None
    r = await client.post(url, headers=callers.customer_a)
    assert r.status_code == 503 and r.json()["type"].endswith("/workflow-unavailable")
    assert (await client.post(url, headers=callers.customer_b)).status_code == 404

    await _advance(app, order["order_id"], status=S.CANCELLED)
    r = await client.post(url, headers=callers.customer_a)
    assert r.status_code == 409 and r.json()["type"].endswith("/order-terminal")


async def test_carrier_simulation(client, callers, catalog, app, database_url) -> None:
    order = await _order(
        client,
        callers,
        catalog,
    )
    url = f"/api/v1/orders/{order['order_id']}/shipment/status"
    r = await client.post(url, json={"status": "shipped"}, headers=callers.admin)
    assert r.status_code == 409  # not confirmed yet

    await _confirm(app, order["order_id"])
    r = await client.post(url, json={"status": "shipped"}, headers=callers.admin)
    assert r.status_code == 200 and (r.json()["status"], r.json()["applied"]) == ("shipped", True)
    r = await client.post(url, json={"status": "shipped"}, headers=callers.admin)
    assert r.json()["applied"] is False  # repeating the current status is a no-op
    r = await client.post(
        url, json={"status": "in_transit", "tracking_number": "TRK9"}, headers=callers.admin
    )
    assert r.json()["status"] == "shipped" and r.json()["shipment"]["tracking_number"] == "TRK9"
    r = await client.post(url, json={"status": "delivered"}, headers=callers.admin)
    assert r.json()["status"] == "delivered"
    r = await client.post(url, json={"status": "shipped"}, headers=callers.admin)
    assert r.status_code == 409
    assert (
        await client.post(url, json={"status": "shipped"}, headers=callers.customer_a)
    ).status_code == 403

    changes = await outbox_rows(database_url, "shipment.status_changed")
    assert [(c["from_status"], c["to_status"]) for c in changes] == [
        ("created", "shipped"),
        ("shipped", "in_transit"),
        ("in_transit", "delivered"),
    ]
    assert {c["client_id"] for c in changes} == {None}  # carrier-driven: no origin client


async def test_workflow_diagnostic(client, callers, catalog, workflows) -> None:
    order = await _order(client, callers, catalog)
    url = f"/api/v1/orders/{order['order_id']}/workflow"
    r = await client.get(url, headers=callers.admin)
    assert r.status_code == 200
    body = r.json()
    assert body["workflow_id"] == f"order-{order['order_id']}" and body["stage"] == "charging"
    assert body["_links"]["temporal_ui"].endswith(body["workflow_id"])
    assert (await client.get(url, headers=callers.customer_a)).status_code == 403
    workflows.stage = None
    assert (await client.get(url, headers=callers.admin)).status_code == 503
    missing = await client.get(f"/api/v1/orders/{uuid.uuid4()}/workflow", headers=callers.admin)
    assert missing.status_code == 404


async def test_health(client) -> None:
    assert (await client.get("/health/ready")).json()["status"] == "ready"
