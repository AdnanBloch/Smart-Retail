"""``InMemoryPaymentStore`` has the store contract the Postgres one has (ORD-D36)."""

import uuid

from app.v1.orders.enums import ChargeOutcome, PaymentAttemptKind
from app.v1.orders.providers.payment_store import InMemoryPaymentStore


async def test_attempts_count_and_the_first_record_wins() -> None:
    store = InMemoryPaymentStore()
    order = uuid.uuid4()
    first = await store.attempt("k", kind=PaymentAttemptKind.CHARGE, order_id=order)
    second = await store.attempt("k", kind=PaymentAttemptKind.CHARGE, order_id=order)
    assert (first.attempts, second.attempts) == (1, 2) and second.outcome is None

    won = await store.record("k", outcome=ChargeOutcome.CHARGED, reference="pay_a")
    lost = await store.record("k", outcome=ChargeOutcome.CHARGED, reference="pay_b")
    assert won.reference == lost.reference == "pay_a"
    assert (await store.get("k")).outcome == ChargeOutcome.CHARGED
    assert await store.get("missing") is None


async def test_charge_count_counts_only_successful_charges_of_the_order() -> None:
    store = InMemoryPaymentStore()
    order, other = uuid.uuid4(), uuid.uuid4()
    await store.attempt("a", kind=PaymentAttemptKind.CHARGE, order_id=order)
    await store.record("a", outcome=ChargeOutcome.CHARGED, reference="pay_a")
    await store.attempt("b", kind=PaymentAttemptKind.CHARGE, order_id=order)  # never succeeded
    await store.attempt("c", kind=PaymentAttemptKind.CHARGE, order_id=other)
    await store.record("c", outcome=ChargeOutcome.CHARGED, reference="pay_c")
    await store.attempt("r", kind=PaymentAttemptKind.REFUND)
    await store.record("r", outcome="refunded", reference="rf")
    assert await store.charge_count(order) == 1
    assert await store.charge_count(uuid.uuid4()) == 0
