"""Per-type templates (RFC 0007 §3.1.1.2, NTF-D6): plain format functions, no engine.

Each module's ``render(payload)`` returns ``(title, body)`` from the source
event's payload. A missing required field raises ``KeyError``, which makes the
event poison (C6); unknown codes render as themselves and never fail.
"""

from collections.abc import Callable, Mapping
from typing import Any

from app.v1.notifications.enums import NotificationType
from app.v1.notifications.templates import (
    order_cancelled,
    order_confirmed,
    order_failed,
    shipment_update,
    stockout,
    welcome,
)

Renderer = Callable[[Mapping[str, Any]], tuple[str, str]]

RENDERERS: dict[NotificationType, Renderer] = {
    NotificationType.WELCOME: welcome.render,
    NotificationType.ORDER_CONFIRMED: order_confirmed.render,
    NotificationType.ORDER_FAILED: order_failed.render,
    NotificationType.ORDER_CANCELLED: order_cancelled.render,
    NotificationType.SHIPMENT_UPDATE: shipment_update.render,
    NotificationType.STOCKOUT: stockout.render,
}


def render(kind: NotificationType, payload: Mapping[str, Any]) -> tuple[str, str]:
    return RENDERERS[kind](payload)
