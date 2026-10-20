"""Merchant slugs and status transitions (RFC 0003 §3.1.1.3)."""

import pytest

from app.core.enums import MerchantStatus as S
from app.v1.merchants.utils import base_slug, can_transition, next_free_slug


def test_slugs() -> None:
    assert base_slug("Café Déjà Vu!") == "cafe-deja-vu"
    assert base_slug("!!!") == "merchant"
    assert next_free_slug("shop", set()) == "shop"
    assert next_free_slug("shop", {"shop", "shop-2"}) == "shop-3"


def test_a_gap_in_the_suffixes_is_not_reused() -> None:
    # Only the next number after the base is probed: "shop-4" is taken by
    # someone else, "shop-3" is free.
    assert next_free_slug("shop", {"shop", "shop-2", "shop-4"}) == "shop-3"
    assert base_slug("x" * 200) == "x" * 80


@pytest.mark.parametrize(
    ("current", "target", "allowed"),
    [
        (S.PENDING, S.APPROVED, True),
        (S.PENDING, S.SUSPENDED, True),
        (S.APPROVED, S.SUSPENDED, True),
        (S.SUSPENDED, S.APPROVED, True),
        (S.SUSPENDED, S.PENDING, True),
        (S.APPROVED, S.PENDING, False),  # approval is never undone to pending
        (S.PENDING, S.PENDING, False),  # not a transition
        (S.APPROVED, S.APPROVED, False),
        ("archived", S.APPROVED, False),  # an unknown stored value moves nowhere
    ],
)
def test_merchant_status_transitions(current, target, allowed: bool) -> None:
    assert can_transition(current, target) is allowed
