"""``shipment_update`` ← ``shipment.status_changed`` (shipped, in_transit, delivered)."""

from collections.abc import Mapping
from typing import Any

TITLES: dict[str, str] = {
    "shipped": "Your order has shipped",
    "in_transit": "Your order is on its way",
    "delivered": "Your order has been delivered",
}
STATUS_TEXT: dict[str, str] = {"in_transit": "in transit"}


def render(payload: Mapping[str, Any]) -> tuple[str, str]:
    status = str(payload["to_status"])
    title = TITLES.get(status, "Shipment update")
    tracking = payload.get("tracking_number")
    carrier_ref = f" (tracking {tracking})" if tracking else ""
    body = f"Order {payload['order_number']} is now {STATUS_TEXT.get(status, status)}{carrier_ref}."
    return title, body
