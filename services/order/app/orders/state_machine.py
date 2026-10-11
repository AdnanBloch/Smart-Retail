"""Order, payment and shipment state machines (RFC 0005 §3.1.1.3).

Three ``text`` columns (S4); the database checks the label, never the edge, so
the edge is checked here. ``transition()`` is the only code that assigns
``status``, ``payment_status`` or ``shipment_status`` (I2) — a review check.
14 of the 81 ordered order-status pairs are legal; the other 67 raise.
"""

from collections.abc import Mapping
from typing import TypeVar

from app.orders.enums import OrderStatus as S
from app.orders.enums import PaymentStatus as P
from app.orders.enums import ShipmentStatus as H
from smartretail_common.enums import DomainEnum
from smartretail_common.errors import Conflict

ORDER_TRANSITIONS: Mapping[S, frozenset[S]] = {
    S.PENDING: frozenset({S.AWAITING_PAYMENT, S.FAILED, S.CANCELLED}),
    # REFUNDING: ORD-1, a commit or shipment failure after a successful charge.
    S.AWAITING_PAYMENT: frozenset({S.CONFIRMED, S.FAILED, S.CANCELLED, S.REFUNDING}),
    S.CONFIRMED: frozenset({S.SHIPPED, S.REFUNDING}),
    S.SHIPPED: frozenset({S.DELIVERED, S.REFUNDING}),
    S.DELIVERED: frozenset({S.REFUNDING}),
    S.REFUNDING: frozenset({S.REFUNDED, S.FAILED}),
    S.CANCELLED: frozenset(),  # terminal
    S.FAILED: frozenset(),  # terminal
    S.REFUNDED: frozenset(),  # terminal
}

# ORD-2.
PAYMENT_TRANSITIONS: Mapping[P, frozenset[P]] = {
    P.NONE: frozenset({P.PENDING}),
    P.PENDING: frozenset({P.SUCCEEDED, P.FAILED, P.REFUNDED}),
    P.SUCCEEDED: frozenset({P.REFUNDED}),
    P.FAILED: frozenset(),
    P.REFUNDED: frozenset(),
}

SHIPMENT_TRANSITIONS: Mapping[H, frozenset[H]] = {
    H.NONE: frozenset({H.CREATED}),
    H.CREATED: frozenset({H.SHIPPED}),
    H.SHIPPED: frozenset({H.IN_TRANSIT, H.DELIVERED}),
    H.IN_TRANSIT: frozenset({H.DELIVERED}),
    H.DELIVERED: frozenset(),
}

TERMINAL_ORDER_STATES = frozenset(s for s, t in ORDER_TRANSITIONS.items() if not t)

E = TypeVar("E", bound=DomainEnum)


class IllegalTransition(Conflict):
    type_slug = "illegal-transition"
    title = "Illegal status transition"

    def __init__(self, field: str, current: object, target: object) -> None:
        super().__init__(
            f"Cannot move {field} from {current} to {target}",
            extras={"field": field, "from": str(current), "to": str(target)},
        )


def allowed(table: Mapping[E, frozenset[E]], current: E | str, target: E) -> bool:
    # A raw value that is not a member has no edges (it was read as a fallback).
    return any(state == current and target in targets for state, targets in table.items())


def transition(table: Mapping[E, frozenset[E]], field: str, current: E | str, target: E) -> E:
    """Returns ``target`` if ``current → target`` is an edge of ``table``; raises otherwise."""
    if not allowed(table, current, target):
        raise IllegalTransition(field, current, target)
    return target
