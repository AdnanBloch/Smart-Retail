"""``order_failed`` ← ``order.failed``.

``failure_reason`` is order's ``OrderFailureReason`` code (RFC 0005 ORD-5);
an unknown code renders as itself. ``payment_status`` decides the money
sentence: nothing captured → "not charged", refunded → "refunded".
"""

from collections.abc import Mapping
from typing import Any

TITLE = "Order could not be completed"

REASONS: dict[str, str] = {
    "INSUFFICIENT_STOCK": "an item is out of stock",
    "INVENTORY_UNAVAILABLE": "stock could not be checked",
    "PAYMENT_DECLINED": "the payment was declined",
    "PAYMENT_TIMEOUT": "the payment timed out",
    "RESERVATION_EXPIRED": "the reservation expired",
    "COMMIT_FAILED": "stock could not be confirmed",
    "FULFILMENT_FAILED": "the shipment could not be created",
    "REFUND_FAILED": "the refund could not be completed",
    "OPERATOR_TERMINATED": "it was stopped by an operator",
}

NOT_CHARGED = frozenset({"none", "pending", "failed"})


def reason_text(code: str | None) -> str:
    if not code:
        return "an unexpected error"
    return REASONS.get(code, code)


def render(payload: Mapping[str, Any]) -> tuple[str, str]:
    body = f"Order {payload['order_number']} failed: {reason_text(payload['failure_reason'])}."
    payment_status = payload["payment_status"]
    if payment_status in NOT_CHARGED:
        body += " You have not been charged."
    elif payment_status == "refunded":
        body += " Your payment has been refunded."
    return TITLE, body
