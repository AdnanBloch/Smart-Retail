"""Pure rules: event → recipe, scope derivation, dispatch backoff. No I/O.

RFC 0007 §3.1.1.2. The event → notification table is a per-type constant in
code (NTF-3): preferences are deferred, so this is where they would plug in.
Payload field names are the producers' real Avro schemas (NTF-8 as amended,
NTF-D33).
"""

import random
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from app.v1.notifications.enums import (
    NotificationChannel,
    NotificationDeliveryScope,
    NotificationType,
)

IN_APP = NotificationChannel.IN_APP
EMAIL = NotificationChannel.EMAIL

# Only these channels go through an adapter and a Celery task (NTF-4).
DISPATCHED_CHANNELS = frozenset({NotificationChannel.EMAIL, NotificationChannel.SMS})

# shipment.status_changed: only customer-visible transitions notify (NTF-7).
NOTIFYING_SHIPMENT_STATUSES = frozenset({"shipped", "in_transit", "delivered"})


@dataclass(frozen=True)
class Recipe:
    """Who is told what, on which channels."""

    user_id: uuid.UUID
    type: NotificationType
    channels: tuple[NotificationChannel, ...]


# event type → (payload field naming the recipient, type, channels)
_TABLE: dict[str, tuple[str, NotificationType, tuple[NotificationChannel, ...]]] = {
    "identity.user.registered": ("user_id", NotificationType.WELCOME, (IN_APP, EMAIL)),
    "order.confirmed": ("customer_id", NotificationType.ORDER_CONFIRMED, (IN_APP, EMAIL)),
    "order.failed": ("customer_id", NotificationType.ORDER_FAILED, (IN_APP, EMAIL)),
    "order.cancelled": ("customer_id", NotificationType.ORDER_CANCELLED, (IN_APP, EMAIL)),
    "shipment.status_changed": ("customer_id", NotificationType.SHIPMENT_UPDATE, (IN_APP, EMAIL)),
    "inventory.stockout": ("merchant_id", NotificationType.STOCKOUT, (IN_APP,)),
}


def resolve(event_type: str, payload: Mapping[str, Any]) -> Recipe | None:
    """The recipe for an event, or ``None`` when it produces no notification
    (``payment.*``, a non-notifying shipment status, an unknown type).

    A missing recipient field raises ``KeyError`` and a malformed one
    ``ValueError``: both are poison (C6) — notifying the wrong person, or
    nobody, is worse than not notifying.
    """
    row = _TABLE.get(event_type)
    if row is None:
        return None
    if (
        event_type == "shipment.status_changed"
        and payload["to_status"] not in NOTIFYING_SHIPMENT_STATUSES
    ):
        return None
    field, kind, channels = row
    return Recipe(user_id=uuid.UUID(str(payload[field])), type=kind, channels=channels)


DEFAULT_SCOPE: dict[NotificationType, NotificationDeliveryScope] = {
    NotificationType.ORDER_CONFIRMED: NotificationDeliveryScope.ORIGIN_FIRST,
    NotificationType.ORDER_FAILED: NotificationDeliveryScope.ORIGIN_FIRST,
    NotificationType.ORDER_CANCELLED: NotificationDeliveryScope.ALL_DEVICES,
    NotificationType.SHIPMENT_UPDATE: NotificationDeliveryScope.ALL_DEVICES,
    NotificationType.STOCKOUT: NotificationDeliveryScope.ALL_DEVICES,
    NotificationType.WELCOME: NotificationDeliveryScope.ORIGIN_ONLY,
}


def derive_scope(kind: NotificationType, origin_client_id: str | None) -> NotificationDeliveryScope:
    """I2: no origin → ``all_devices``, with no exception for narrower types (C21).

    Stored on every row so the rule has one implementation; frames are not yet
    filtered by it (scoped delivery is deferred, NTF-D26).
    """
    if origin_client_id is None:
        return NotificationDeliveryScope.ALL_DEVICES
    return DEFAULT_SCOPE.get(kind, NotificationDeliveryScope.ALL_DEVICES)


def backoff_seconds(
    attempt: int,
    *,
    base: float,
    cap: float,
    rand: Callable[[float, float], float] = random.uniform,
) -> float:
    """NTF-5: exponential (``base * 2**(n - 1)``), capped, with full jitter."""
    ceiling = min(cap, base * 2 ** max(attempt - 1, 0))
    return rand(0.0, ceiling)
