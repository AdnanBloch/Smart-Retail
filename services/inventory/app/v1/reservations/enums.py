"""Reservation vocabularies (RFC 0006 §3.1.1.2). The only place these values live (R8)."""

from enum import StrEnum

from smartretail_common.enums import DomainEnum


class ReservationStatus(DomainEnum):
    HELD = "held"
    COMMITTED = "committed"
    RELEASED = "released"
    EXPIRED = "expired"


class ReleaseReason(StrEnum):
    """``inventory.released.reason`` (INV-7)."""

    COMPENSATION = "compensation"
    ORDER_CANCELLED = "order_cancelled"
    EXPIRED = "expired"
