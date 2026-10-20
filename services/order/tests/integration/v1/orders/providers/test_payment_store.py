"""``PostgresPaymentStore``: exactly one charge across worker replicas and
restarts (ORD-D36, mentor review 2026-10-20 §7)."""

import asyncio
import uuid
from decimal import Decimal

import pytest

from app.v1.orders.enums import RefundOutcome
from app.v1.orders.providers.errors import ProviderTransientError
from app.v1.orders.providers.payment import FakePaymentProvider
from app.v1.orders.providers.payment_store import PostgresPaymentStore


@pytest.fixture
def replicas(app, settings) -> list[FakePaymentProvider]:
    """Two workers: two providers, two stores, one database."""
    settings.fake_provider_timeout_seconds = 0
    return [FakePaymentProvider(settings, PostgresPaymentStore(app.state.db)) for _ in range(2)]


def charge(provider: FakePaymentProvider, order: uuid.UUID, method: str = "tok_ok"):
    return provider.charge(
        idempotency_key=f"order-{order}:charge",
        order_id=order,
        amount=Decimal("10.00"),
        currency="USD",
        payment_method=method,
    )


def refund(provider: FakePaymentProvider, order: uuid.UUID):
    return provider.refund(
        idempotency_key=f"order-{order}:refund",
        charge_idempotency_key=f"order-{order}:charge",
        amount=Decimal("10.00"),
        currency="USD",
    )


async def test_two_replicas_charging_at_once_charge_once(replicas) -> None:
    for _ in range(10):
        order = uuid.uuid4()
        results = await asyncio.gather(*(charge(p, order) for p in replicas * 3))
        assert len({r.reference for r in results}) == 1
        assert await replicas[0].charge_count(order) == 1


async def test_a_restarted_worker_sees_the_earlier_charge(app, settings, replicas) -> None:
    order = uuid.uuid4()
    first = await charge(replicas[0], order)
    restarted = FakePaymentProvider(settings, PostgresPaymentStore(app.state.db))
    assert await charge(restarted, order) == first
    assert restarted.calls == [("charge", f"order-{order}:charge")]
    assert await restarted.charge_count(order) == 1


async def test_flaky_attempts_are_counted_across_replicas(replicas) -> None:
    order = uuid.uuid4()
    with pytest.raises(ProviderTransientError):
        await charge(replicas[0], order, "tok_flaky_2")
    with pytest.raises(ProviderTransientError):
        await charge(replicas[1], order, "tok_flaky_2")
    assert (await charge(replicas[0], order, "tok_flaky_2")).reference.startswith("pay_")


async def test_refunds_dedupe_and_follow_the_stored_charge(replicas) -> None:
    order = uuid.uuid4()
    await charge(replicas[0], order)
    results = await asyncio.gather(refund(replicas[0], order), refund(replicas[1], order))
    assert {r.outcome for r in results} == {RefundOutcome.REFUNDED}
    assert len({r.reference for r in results}) == 1


async def test_refund_of_an_uncaptured_charge_is_nothing_captured(replicas) -> None:
    never = uuid.uuid4()
    assert (await refund(replicas[0], never)).outcome == RefundOutcome.NOTHING_CAPTURED
    timed_out = uuid.uuid4()
    with pytest.raises(ProviderTransientError):
        await charge(replicas[0], timed_out, "tok_timeout")
    assert (await refund(replicas[1], timed_out)).outcome == RefundOutcome.NOTHING_CAPTURED


async def test_a_refund_the_provider_rejects_keeps_failing(replicas) -> None:
    order = uuid.uuid4()
    await charge(replicas[0], order, "tok_refund_fail")
    for provider in replicas:
        with pytest.raises(ProviderTransientError):
            await refund(provider, order)


async def test_record_without_attempt_is_a_programming_error(app) -> None:
    store = PostgresPaymentStore(app.state.db)
    with pytest.raises(AssertionError):
        await store.record("never-attempted", outcome="charged", reference=None)
    assert await store.get("never-attempted") is None
