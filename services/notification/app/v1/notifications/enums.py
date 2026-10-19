"""Notification vocabularies (RFC 0007 §3.1.1.2, S4). The only place these values live (R8).

``stream/`` imports ``NotificationChannel`` and ``NotificationDeliveryScope``
from here; they are not redefined there.
"""

from smartretail_common.enums import DomainEnum


class NotificationType(DomainEnum):
    """``passthrough`` on read — displayed, not branched on; the vocabulary most likely to grow."""

    ORDER_CONFIRMED = "order_confirmed"
    ORDER_FAILED = "order_failed"
    ORDER_CANCELLED = "order_cancelled"
    SHIPMENT_UPDATE = "shipment_update"
    STOCKOUT = "stockout"
    WELCOME = "welcome"


class NotificationChannel(DomainEnum):
    """``passthrough`` on read. ``push`` has no registered adapter (NTF-D22)."""

    IN_APP = "in_app"
    EMAIL = "email"
    SMS = "sms"
    PUSH = "push"


class DeliveryStatus(DomainEnum):
    """``delivery_attempts.status``; ``fallback`` → ``pending`` (a state machine reads it)."""

    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"


class NotificationDeliveryScope(DomainEnum):
    """``fallback`` → ``all_devices``: scope narrows delivery, so the default must not (C21)."""

    ORIGIN_ONLY = "origin_only"
    ORIGIN_FIRST = "origin_first"
    ALL_DEVICES = "all_devices"
