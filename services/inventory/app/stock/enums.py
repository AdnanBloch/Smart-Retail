"""Stock vocabularies (RFC 0006 §3.1.1.2). The only place these values live (R8)."""

from enum import StrEnum

from smartretail_common.enums import DomainEnum


class StockMovementReason(DomainEnum):
    """Ledger reasons; ``passthrough`` on read — a vocabulary expected to grow."""

    INITIAL = "initial"
    MANUAL_ADJUSTMENT = "manual_adjustment"
    RESERVE = "reserve"
    RELEASE = "release"
    COMMIT = "commit"
    EXPIRE = "expire"
    RESTOCK = "restock"
    RETURN = "return"
    COMPENSATION = "compensation"


class AdjustReason(StrEnum):
    """The reasons a merchant may give to ``adjust``; a subset of the ledger's."""

    RESTOCK = "restock"
    RETURN = "return"
    MANUAL_ADJUSTMENT = "manual_adjustment"


class StockoutTrigger(StrEnum):
    """INV-7: ``inventory.stockout.trigger``. Payload only, never a column."""

    DEPLETED = "depleted"
    LOW_STOCK = "low_stock"
    RESERVATION_REFUSED = "reservation_refused"


class ReferenceType(StrEnum):
    """``stock_movements.reference_type``: what caused a movement."""

    PRODUCT = "product"
    ORDER = "order"
    RESERVATION = "reservation"
    ADJUSTMENT = "adjustment"


class InventoryEvent(StrEnum):
    """Event types written to the outbox (RFC 0000 §3.1.2.3)."""

    RESERVED = "inventory.reserved"
    RELEASED = "inventory.released"
    STOCKOUT = "inventory.stockout"
