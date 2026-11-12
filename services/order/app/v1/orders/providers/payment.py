"""``PaymentProvider`` and its deterministic fake (A9, B10).

No card data exists anywhere: ``payment_method`` is an opaque provider token.
The fake's behaviour is a pure function of ``(token, attempt number for that
idempotency key)`` and it dedupes by idempotency key, so a retried charge
returns the first result — **exactly one charge** (B3). The dedupe state lives
in a ``PaymentStore`` shared by every worker replica (ORD-D36). Within this project
the fake is the design, not a placeholder.
"""

import asyncio
import hashlib
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Protocol

from app.core.config import Settings
from app.v1.orders.enums import ChargeOutcome, PaymentAttemptKind, RefundOutcome
from app.v1.orders.providers.errors import PaymentDeclined, ProviderTransientError
from app.v1.orders.providers.payment_store import InMemoryPaymentStore, PaymentStore


@dataclass(frozen=True)
class ChargeResult:
    reference: str


@dataclass(frozen=True)
class RefundResult:
    outcome: RefundOutcome
    reference: str | None = None


class PaymentProvider(Protocol):
    async def charge(
        self,
        *,
        idempotency_key: str,
        order_id: uuid.UUID,
        amount: Decimal,
        currency: str,
        payment_method: str,
    ) -> ChargeResult: ...

    async def refund(
        self,
        *,
        idempotency_key: str,
        charge_idempotency_key: str,
        amount: Decimal,
        currency: str,
    ) -> RefundResult: ...


def _ref(prefix: str, key: str) -> str:
    return f"{prefix}_{hashlib.sha256(key.encode()).hexdigest()[:16]}"


@dataclass
class FakePaymentProvider:
    """Dedupes by idempotency key through a ``PaymentStore`` (ORD-D36): the
    worker passes ``PostgresPaymentStore``, so replicas and restarts share it.
    ``calls`` is this process's call log only (B10)."""

    settings: Settings
    store: PaymentStore = field(default_factory=InMemoryPaymentStore)
    calls: list[tuple[str, str]] = field(default_factory=list)

    async def charge(
        self,
        *,
        idempotency_key: str,
        order_id: uuid.UUID,
        amount: Decimal,
        currency: str,
        payment_method: str,
    ) -> ChargeResult:
        self.calls.append(("charge", idempotency_key))
        stored = await self.store.attempt(
            idempotency_key,
            kind=PaymentAttemptKind.CHARGE,
            order_id=order_id,
            payment_method=payment_method,
        )
        if stored.outcome == ChargeOutcome.CHARGED:
            return ChargeResult(str(stored.reference))
        s = self.settings
        if payment_method == s.fake_payment_decline_token:
            raise PaymentDeclined("card declined")
        if payment_method == s.fake_payment_timeout_token:
            # Longer than the activity's start_to_close: an ambiguous timeout.
            await asyncio.sleep(s.fake_provider_timeout_seconds)
            raise ProviderTransientError("payment provider timed out")
        if payment_method == s.fake_payment_flaky_token and stored.attempts <= 2:
            raise ProviderTransientError("payment provider hiccup")
        final = await self.store.record(
            idempotency_key, outcome=ChargeOutcome.CHARGED, reference=_ref("pay", idempotency_key)
        )
        return ChargeResult(str(final.reference))

    async def refund(
        self,
        *,
        idempotency_key: str,
        charge_idempotency_key: str,
        amount: Decimal,
        currency: str,
    ) -> RefundResult:
        self.calls.append(("refund", idempotency_key))
        stored = await self.store.attempt(idempotency_key, kind=PaymentAttemptKind.REFUND)
        if stored.outcome is not None:
            return RefundResult(RefundOutcome(stored.outcome), stored.reference)
        charge = await self.store.get(charge_idempotency_key)
        if charge is None or charge.outcome != ChargeOutcome.CHARGED:
            outcome, reference = RefundOutcome.NOTHING_CAPTURED, None
        elif charge.payment_method == self.settings.fake_payment_refund_fail_token:
            raise ProviderTransientError("refund rejected by provider")
        else:
            outcome, reference = RefundOutcome.REFUNDED, _ref("rf", idempotency_key)
        final = await self.store.record(idempotency_key, outcome=outcome, reference=reference)
        return RefundResult(RefundOutcome(str(final.outcome)), final.reference)

    async def charge_count(self, order_id: uuid.UUID) -> int:
        """Successful charges for an order — the "exactly one charge" check."""
        return await self.store.charge_count(order_id)
