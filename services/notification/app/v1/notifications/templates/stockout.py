"""``stockout`` ← ``inventory.stockout``, to the merchant (INV-7 ``trigger``)."""

from collections.abc import Mapping
from typing import Any


def render(payload: Mapping[str, Any]) -> tuple[str, str]:
    sku, available, trigger = payload["sku"], payload["available"], payload["trigger"]
    if trigger == "low_stock":
        return (
            "Running low on stock",
            f"{sku} is running low: {available} left "
            f"(threshold {payload['low_stock_threshold']}).",
        )
    if trigger == "reservation_refused":
        requested = payload.get("requested")
        wanted = f"{requested} requested, " if requested is not None else ""
        return "Out of stock", f"{sku} could not cover an order: {wanted}{available} available."
    return "Out of stock", f"{sku} is out of stock."
