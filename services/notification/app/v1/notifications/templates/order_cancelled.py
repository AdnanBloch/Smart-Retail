"""``order_cancelled`` ← ``order.cancelled``."""

from collections.abc import Mapping
from typing import Any

TITLE = "Order cancelled"

BY_WHOM: dict[str, str] = {
    "customer": " at your request",
    "merchant": " by the merchant",
    "admin": " by an administrator",
    "system": " automatically",
}


def render(payload: Mapping[str, Any]) -> tuple[str, str]:
    by_whom = BY_WHOM.get(payload["cancelled_by"] or "", "")
    body = f"Order {payload['order_number']} was cancelled{by_whom}."
    if payload["payment_status"] == "refunded":
        body += " Your payment has been refunded."
    return TITLE, body
