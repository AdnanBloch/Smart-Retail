"""Money (S2, ORD-4): ``Decimal`` only, quantised to cents with ``ROUND_HALF_EVEN``.

Stored totals are computed here once, from the snapshot, and never recomputed
from live catalog data (I4).
"""

from collections.abc import Iterable
from decimal import ROUND_HALF_EVEN, Decimal

CENT = Decimal("0.01")


def money(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_EVEN)


def line_total(unit_price: Decimal, quantity: int) -> Decimal:
    return money(unit_price * quantity)


def totals(line_totals: Iterable[Decimal], shipping_fee: Decimal) -> tuple[Decimal, Decimal]:
    """Returns ``(subtotal, total)``; a stored total always equals its lines plus the fee."""
    subtotal = money(sum(line_totals, Decimal("0")))
    return subtotal, money(subtotal + shipping_fee)
