"""Ownership decisions (RFC 0005 §3.1.2.2): always from the verified token's
claims, never from a request field. Customer sees own, admin sees all, a
merchant never reads a whole order (its lines come from ``/merchant``)."""

import uuid

import pytest

from app.v1.orders import access
from app.v1.orders.models import Order
from smartretail_common.auth import Principal

OWNER = uuid.uuid4()


def principal(role: str | None, subject: uuid.UUID | None = None, **kw) -> Principal:
    return Principal(
        subject=str(subject or uuid.uuid4()),
        role=role,
        token_type=kw.pop("token_type", "access"),
        merchant_id=kw.pop("merchant_id", None),
        merchant_status=None,
        session_id=None,
        expires_at=0,
        key_id=None,
        claims={},
    )


def order() -> Order:
    return Order(id=uuid.uuid4(), customer_id=OWNER)


@pytest.mark.parametrize(
    ("who", "visible"),
    [
        (principal("customer", OWNER), True),
        (principal("customer"), False),  # another customer
        (principal("admin"), True),
        # A merchant whose subject happens to equal the customer id still cannot see it.
        (principal("merchant", OWNER, merchant_id=str(OWNER)), False),
        (principal(None, OWNER), False),
    ],
)
def test_can_view(who, visible) -> None:
    assert access.can_view(who, order()) is visible


def test_caller_merchant_id_only_for_merchants_with_a_merchant_claim() -> None:
    merchant = uuid.uuid4()
    assert access.caller_merchant_id(principal("merchant", merchant_id=str(merchant))) == merchant
    assert access.caller_merchant_id(principal("merchant")) is None  # no claim, no lines
    assert access.caller_merchant_id(principal("customer", merchant_id=str(merchant))) is None
    assert access.caller_merchant_id(principal("admin", merchant_id=str(merchant))) is None


def test_actor_names_the_kind_and_subject() -> None:
    assert access.actor(principal("admin", OWNER)) == f"admin:{OWNER}"
    assert access.actor(principal("customer", OWNER)) == f"customer:{OWNER}"
    assert access.is_admin(principal("admin")) and not access.is_admin(principal("customer"))
