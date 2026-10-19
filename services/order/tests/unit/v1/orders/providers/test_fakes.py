"""The provider fakes: every saga branch reachable by configuration (B10)."""

import uuid
from decimal import Decimal

import pytest

from app.core.config import Settings
from app.v1.orders.enums import RefundOutcome
from app.v1.orders.providers.errors import AddressRejected, PaymentDeclined, ProviderTransientError
from app.v1.orders.providers.payment import FakePaymentProvider
from app.v1.orders.providers.shipping import FakeShippingProvider

SETTINGS = Settings(database_url="x", fake_provider_timeout_seconds=0)


async def _charge(fake: FakePaymentProvider, order: uuid.UUID, method: str):
    return await fake.charge(
        idempotency_key=f"order-{order}:charge",
        order_id=order,
        amount=Decimal("10.00"),
        currency="USD",
        payment_method=method,
    )


async def _refund(fake: FakePaymentProvider, order: uuid.UUID):
    return await fake.refund(
        idempotency_key=f"order-{order}:refund",
        charge_idempotency_key=f"order-{order}:charge",
        amount=Decimal("10.00"),
        currency="USD",
    )


async def test_charges_dedupe_by_key_and_refunds_follow() -> None:
    fake = FakePaymentProvider(SETTINGS)
    order = uuid.uuid4()
    first = await _charge(fake, order, "tok_ok")
    assert (await _charge(fake, order, "tok_ok")) == first and first.reference.startswith("pay_")
    assert fake.charge_count(order) == 1
    refund = await _refund(fake, order)
    assert refund.outcome == RefundOutcome.REFUNDED and await _refund(fake, order) == refund
    nothing = await _refund(fake, uuid.uuid4())
    assert nothing.outcome == RefundOutcome.NOTHING_CAPTURED


async def test_failure_tokens() -> None:
    fake = FakePaymentProvider(SETTINGS)
    with pytest.raises(PaymentDeclined):
        await _charge(fake, uuid.uuid4(), "tok_decline")
    with pytest.raises(ProviderTransientError):
        await _charge(fake, uuid.uuid4(), "tok_timeout")
    flaky = uuid.uuid4()
    for _ in range(2):
        with pytest.raises(ProviderTransientError):
            await _charge(fake, flaky, "tok_flaky_2")
    assert (await _charge(fake, flaky, "tok_flaky_2")).reference
    stuck = uuid.uuid4()
    await _charge(fake, stuck, "tok_refund_fail")
    with pytest.raises(ProviderTransientError):
        await _refund(fake, stuck)


async def test_shipping_fake() -> None:
    fake = FakeShippingProvider(SETTINGS)
    kwargs = {"order_number": "SR-1", "lines": []}
    ok = await fake.create_shipment(idempotency_key="k", address={"postal_code": "1"}, **kwargs)
    again = await fake.create_shipment(idempotency_key="k", address={"postal_code": "1"}, **kwargs)
    assert ok == again and ok.tracking_number.startswith("TRK")
    with pytest.raises(AddressRejected):
        await fake.create_shipment(idempotency_key="r", address={"postal_code": "00000"}, **kwargs)
    with pytest.raises(ProviderTransientError):
        await fake.create_shipment(idempotency_key="t", address={"postal_code": "99999"}, **kwargs)
