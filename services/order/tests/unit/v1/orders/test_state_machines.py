"""ORDER_TRANSITIONS: 14 legal edges of 81 pairs, 67 refused (RFC 0005 §3.1.1.3)."""

import itertools
from decimal import Decimal

import pytest

from app.v1.orders import utils
from app.v1.orders.enums import OrderStatus as S
from app.v1.orders.enums import PaymentStatus as P
from app.v1.orders.enums import ShipmentStatus as H
from app.v1.orders.state_machine import (
    ORDER_TRANSITIONS,
    PAYMENT_TRANSITIONS,
    SHIPMENT_TRANSITIONS,
    TERMINAL_ORDER_STATES,
    IllegalTransition,
    allowed,
    transition,
)

LEGAL = {(a, b) for a, targets in ORDER_TRANSITIONS.items() for b in targets}
ILLEGAL = [p for p in itertools.product(S, S) if p not in LEGAL]


def test_counts_and_totality() -> None:
    assert set(ORDER_TRANSITIONS) == set(S)
    assert len(LEGAL) == 14 and len(ILLEGAL) == 67
    assert {S.CANCELLED, S.FAILED, S.REFUNDED} == TERMINAL_ORDER_STATES
    # ORD-1: the post-charge failure path is reachable.
    assert (S.AWAITING_PAYMENT, S.REFUNDING) in LEGAL and (S.REFUNDING, S.FAILED) in LEGAL


@pytest.mark.parametrize(("current", "target"), sorted(LEGAL))
def test_legal(current, target) -> None:
    assert transition(ORDER_TRANSITIONS, "status", current, target) == target


@pytest.mark.parametrize(("current", "target"), ILLEGAL)
def test_illegal(current, target) -> None:
    with pytest.raises(IllegalTransition) as err:
        transition(ORDER_TRANSITIONS, "status", current, target)
    assert err.value.status == 409 and err.value.extras["from"] == current.value


def test_sub_state_tables() -> None:
    payment = {(a, b) for a, t in PAYMENT_TRANSITIONS.items() for b in t}
    assert payment == {
        (P.NONE, P.PENDING),
        (P.PENDING, P.SUCCEEDED),
        (P.PENDING, P.FAILED),
        (P.PENDING, P.REFUNDED),
        (P.SUCCEEDED, P.REFUNDED),
    }
    shipment = {(a, b) for a, t in SHIPMENT_TRANSITIONS.items() for b in t}
    assert shipment == {
        (H.NONE, H.CREATED),
        (H.CREATED, H.SHIPPED),
        (H.SHIPPED, H.IN_TRANSIT),
        (H.SHIPPED, H.DELIVERED),
        (H.IN_TRANSIT, H.DELIVERED),
    }
    assert not allowed(ORDER_TRANSITIONS, "from-a-newer-build", S.CONFIRMED)


def test_money_is_exact() -> None:
    assert utils.line_total(Decimal("19.99"), 3) == Decimal("59.97")
    assert utils.money(Decimal("0.125")) == Decimal("0.12")  # ROUND_HALF_EVEN
    assert utils.money(Decimal("0.135")) == Decimal("0.14")
    subtotal, total = utils.totals([Decimal("10.10"), Decimal("0.20")], Decimal("4.99"))
    assert (subtotal, total) == (Decimal("10.30"), Decimal("15.29"))
    assert utils.totals([], Decimal("0")) == (Decimal("0.00"), Decimal("0.00"))
