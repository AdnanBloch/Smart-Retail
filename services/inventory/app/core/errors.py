"""inventory problem types (RFC 0006 §3.1.2.1).

Business outcomes are 4xx and never retried by a workflow; contention and
infrastructure are 503 and retried (INV-5).
"""

from typing import Any

from smartretail_common.errors import Conflict, DependencyUnavailable, NotFound, Unprocessable


class StockItemNotFound(NotFound):
    type_slug = "stock-item-not-found"
    title = "No stock row for this variant"


class ReservationNotFound(NotFound):
    type_slug = "reservation-not-found"
    title = "Reservation not found"


class InsufficientStock(Conflict):
    type_slug = "insufficient-stock"
    title = "Not enough stock"

    def __init__(self, shortages: list[dict[str, Any]]) -> None:
        super().__init__(
            "At least one line cannot be reserved",
            extras={
                "violations": [
                    {**s, "field": "lines", "code": "insufficient", "message": "not enough stock"}
                    for s in shortages
                ]
            },
        )


class UnknownVariant(Unprocessable):
    type_slug = "unknown-variant"
    title = "Unknown variant"

    def __init__(self, variant_ids: list[str]) -> None:
        super().__init__(
            "No stock row exists for these variants",
            violations=[
                {"field": "lines", "message": f"unknown variant {v}", "code": "unknown"}
                for v in variant_ids
            ],
        )


class ReservationConflict(Conflict):
    type_slug = "reservation-conflict"
    title = "This order already holds different reservations"


class ReservationNotHeld(Conflict):
    type_slug = "reservation-not-held"
    title = "The reservation is no longer held"


class ReservationCommitted(Conflict):
    type_slug = "reservation-committed"
    title = "A committed reservation cannot be released"


class ReservationNotCommitted(Conflict):
    type_slug = "reservation-not-committed"
    title = "Only a committed reservation can be reversed"


class StockBelowReserved(Conflict):
    type_slug = "stock-below-reserved"
    title = "On-hand stock cannot drop below what is reserved"


class VersionMismatch(Conflict):
    type_slug = "version-mismatch"
    title = "The stock row was modified by someone else"

    def __init__(self, current_version: int) -> None:
        super().__init__(
            f"Current version is {current_version}; reload and retry",
            extras={"current_version": current_version},
        )


class StockConstraintViolated(Conflict):
    """23514: the oversell CHECK caught a write the repo should have refused."""

    type_slug = "stock-constraint-violated"
    title = "The change would make stock negative"


class InventoryContended(DependencyUnavailable):
    type_slug = "inventory-contended"
    title = "Stock rows are busy; retry shortly"

    def __init__(self) -> None:
        super().__init__("Lock wait exhausted after bounded retries", retry_after=1)
