"""``PaymentProvider`` and its deterministic fake (A9, B10).

No card data exists anywhere: ``payment_method`` is an opaque provider token.
The fake's behaviour is a pure function of ``(token, attempt number for that
idempotency key)`` and it dedupes by idempotency key, so a retried charge
returns the first result — **exactly one charge** (B3). Within this project
the fake is the design, not a placeholder.
"""

import asyncio
import hashlib
import uuid
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Protocol

from app.core.config import Settings
from app.orders.enums import RefundOutcome
from app.providers.errors import PaymentDeclined, ProviderTransientError


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
    settings: Settings
    calls: list[tuple[str, str]] = field(default_factory=list)
    attempts: Counter[str] = field(default_factory=Counter)
    charges: dict[str, ChargeResult] = field(default_factory=dict)
    methods: dict[str, str] = field(default_factory=dict)
    refunds: dict[str, RefundResult] = field(default_factory=dict)

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
        self.attempts[idempotency_key] += 1
        if idempotency_key in self.charges:
            return self.charges[idempotency_key]
        s = self.settings
        if payment_method == s.fake_payment_decline_token:
            raise PaymentDeclined("card declined")
        if payment_method == s.fake_payment_timeout_token:
            # Longer than the activity's start_to_close: an ambiguous timeout.
            await asyncio.sleep(s.fake_provider_timeout_seconds)
            raise ProviderTransientError("payment provider timed out")
        if payment_method == s.fake_payment_flaky_token and self.attempts[idempotency_key] <= 2:
            raise ProviderTransientError("payment provider hiccup")
        result = ChargeResult(_ref("pay", idempotency_key))
        self.charges[idempotency_key] = result
        self.methods[idempotency_key] = payment_method
        return result

    async def refund(
        self,
        *,
        idempotency_key: str,
        charge_idempotency_key: str,
        amount: Decimal,
        currency: str,
    ) -> RefundResult:
        self.calls.append(("refund", idempotency_key))
        if idempotency_key in self.refunds:
            return self.refunds[idempotency_key]
        if charge_idempotency_key not in self.charges:
            result = RefundResult(RefundOutcome.NOTHING_CAPTURED)
        elif self.methods[charge_idempotency_key] == self.settings.fake_payment_refund_fail_token:
            raise ProviderTransientError("refund rejected by provider")
        else:
            result = RefundResult(RefundOutcome.REFUNDED, _ref("rf", idempotency_key))
        self.refunds[idempotency_key] = result
        return result

    def charge_count(self, order_id: uuid.UUID) -> int:
        """Successful charges for an order — the "exactly one charge" check."""
        return sum(1 for key in self.charges if key.startswith(f"order-{order_id}:"))
