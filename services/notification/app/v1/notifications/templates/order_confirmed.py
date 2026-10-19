"""``order_confirmed`` ← ``order.confirmed``."""

from collections.abc import Mapping
from typing import Any

TITLE = "Order confirmed"


def render(payload: Mapping[str, Any]) -> tuple[str, str]:
    amount = f"{payload['total']} {payload['currency']}"
    body = f"Order {payload['order_number']} for {amount} is confirmed."
    tracking = payload.get("tracking_number")
    if tracking:
        body += f" Tracking number: {tracking}."
    return TITLE, body
