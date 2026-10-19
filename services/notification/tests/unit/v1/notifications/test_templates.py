"""Every template renders from the producers' real payload fields (§3.1.1.2)."""

import pytest

from app.v1.notifications.enums import NotificationType
from app.v1.notifications.templates import RENDERERS, render
from tests.integration.conftest import (
    cancelled,
    confirmed,
    failed,
    registered,
    shipment,
    stockout,
)

U = "6f1c1a52-6b3e-4f0e-9a51-1d2a3b4c5d6e"


def test_every_type_has_a_template() -> None:
    assert set(RENDERERS) == set(NotificationType)


def test_welcome() -> None:
    assert render(NotificationType.WELCOME, registered(U, "merchant")) == (
        "Welcome to SmartRetail",
        "Hi Ada Lovelace, your merchant account is ready.",
    )
    assert render(NotificationType.WELCOME, {**registered(U), "full_name": " "})[1] == (
        "Your customer account is ready."
    )


def test_order_confirmed_with_and_without_tracking() -> None:
    title, body = render(NotificationType.ORDER_CONFIRMED, confirmed(U, "SR-9"))
    assert (title, body) == ("Order confirmed", "Order SR-9 for 42.00 USD is confirmed.")
    payload = {**confirmed(U, "SR-9"), "tracking_number": "TRK9"}
    assert render(NotificationType.ORDER_CONFIRMED, payload)[1].endswith("Tracking number: TRK9.")


@pytest.mark.parametrize(
    ("reason", "payment_status", "body"),
    [
        (
            "PAYMENT_DECLINED",
            "failed",
            "Order SR-1002 failed: the payment was declined. You have not been charged.",
        ),
        (
            "INSUFFICIENT_STOCK",
            "none",
            "Order SR-1002 failed: an item is out of stock. You have not been charged.",
        ),
        (
            "FULFILMENT_FAILED",
            "refunded",
            "Order SR-1002 failed: the shipment could not be created."
            " Your payment has been refunded.",
        ),
        ("SOMETHING_NEW", "succeeded", "Order SR-1002 failed: SOMETHING_NEW."),
        (None, "pending", "Order SR-1002 failed: an unexpected error. You have not been charged."),
    ],
)
def test_order_failed(reason, payment_status, body) -> None:
    payload = {**failed(U, reason), "payment_status": payment_status}
    assert render(NotificationType.ORDER_FAILED, payload) == ("Order could not be completed", body)


@pytest.mark.parametrize(
    ("by", "payment", "body"),
    [
        (
            "customer",
            "refunded",
            "Order SR-1003 was cancelled at your request. Your payment has been refunded.",
        ),
        ("merchant", "none", "Order SR-1003 was cancelled by the merchant."),
        ("admin", "none", "Order SR-1003 was cancelled by an administrator."),
        ("system", "none", "Order SR-1003 was cancelled automatically."),
        (None, "none", "Order SR-1003 was cancelled."),
        ("robot", "none", "Order SR-1003 was cancelled."),
    ],
)
def test_order_cancelled(by, payment, body) -> None:
    payload = {**cancelled(U), "cancelled_by": by, "payment_status": payment}
    assert render(NotificationType.ORDER_CANCELLED, payload) == ("Order cancelled", body)


@pytest.mark.parametrize(
    ("status", "title", "body"),
    [
        ("shipped", "Your order has shipped", "Order SR-1004 is now shipped (tracking TRK1)."),
        (
            "in_transit",
            "Your order is on its way",
            "Order SR-1004 is now in transit (tracking TRK1).",
        ),
        (
            "delivered",
            "Your order has been delivered",
            "Order SR-1004 is now delivered (tracking TRK1).",
        ),
        ("lost", "Shipment update", "Order SR-1004 is now lost (tracking TRK1)."),
    ],
)
def test_shipment_update(status, title, body) -> None:
    assert render(NotificationType.SHIPMENT_UPDATE, shipment(U, status)) == (title, body)
    no_tracking = {**shipment(U, status), "tracking_number": None}
    assert "(tracking" not in render(NotificationType.SHIPMENT_UPDATE, no_tracking)[1]


def test_stockout_by_trigger() -> None:
    assert render(NotificationType.STOCKOUT, stockout(U)) == (
        "Out of stock",
        "TEE-RED-M is out of stock.",
    )
    low = {**stockout(U), "trigger": "low_stock", "available": 3}
    assert render(NotificationType.STOCKOUT, low) == (
        "Running low on stock",
        "TEE-RED-M is running low: 3 left (threshold 5).",
    )
    refused = {**stockout(U), "trigger": "reservation_refused", "available": 1, "requested": 4}
    assert render(NotificationType.STOCKOUT, refused)[1] == (
        "TEE-RED-M could not cover an order: 4 requested, 1 available."
    )
    refused_unknown = {**refused, "requested": None}
    assert render(NotificationType.STOCKOUT, refused_unknown)[1] == (
        "TEE-RED-M could not cover an order: 1 available."
    )


@pytest.mark.parametrize(
    ("kind", "payload", "missing"),
    [
        (NotificationType.WELCOME, registered(U), "role"),
        (NotificationType.ORDER_CONFIRMED, confirmed(U), "order_number"),
        (NotificationType.ORDER_FAILED, failed(U), "payment_status"),
        (NotificationType.ORDER_CANCELLED, cancelled(U), "cancelled_by"),
        (NotificationType.SHIPMENT_UPDATE, shipment(U, "shipped"), "to_status"),
        (NotificationType.STOCKOUT, stockout(U), "sku"),
    ],
)
def test_a_missing_required_field_is_poison(kind, payload, missing) -> None:
    del payload[missing]
    with pytest.raises(KeyError):
        render(kind, payload)
