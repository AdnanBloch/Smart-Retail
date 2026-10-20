"""Where ``FakePaymentProvider`` keeps its dedupe state (ORD-D36).

A real provider dedupes by idempotency key on its own side, so every caller
sees one charge. The fake models that side, so its state must be shared by
every ``order-worker`` replica and survive a restart: ``PostgresPaymentStore``
keeps it in ``payment_attempts``. ``InMemoryPaymentStore`` has the same
semantics for unit tests.

Both follow two rules:

- ``attempt`` counts one more call for a key and returns any stored result;
- ``record`` stores a result only if none is stored yet, and returns whichever
  result is stored afterwards. The first writer wins, so two replicas racing
  on one key both return the same charge.
"""

import uuid
from dataclasses import dataclass
from typing import Protocol

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert

from app.v1.orders.enums import ChargeOutcome, PaymentAttemptKind
from app.v1.orders.models import PaymentAttempt
from smartretail_common.db import Database


@dataclass(frozen=True)
class StoredAttempt:
    attempts: int
    outcome: str | None = None
    reference: str | None = None
    payment_method: str | None = None


class PaymentStore(Protocol):
    async def attempt(
        self,
        key: str,
        *,
        kind: PaymentAttemptKind,
        order_id: uuid.UUID | None = None,
        payment_method: str | None = None,
    ) -> StoredAttempt: ...

    async def record(self, key: str, *, outcome: str, reference: str | None) -> StoredAttempt: ...

    async def get(self, key: str) -> StoredAttempt | None: ...

    async def charge_count(self, order_id: uuid.UUID) -> int: ...


@dataclass
class _Row:
    kind: PaymentAttemptKind
    order_id: uuid.UUID | None
    payment_method: str | None
    attempts: int = 0
    outcome: str | None = None
    reference: str | None = None

    def view(self) -> StoredAttempt:
        return StoredAttempt(self.attempts, self.outcome, self.reference, self.payment_method)


class InMemoryPaymentStore:
    """One process only; for unit tests."""

    def __init__(self) -> None:
        self.rows: dict[str, _Row] = {}

    async def attempt(
        self,
        key: str,
        *,
        kind: PaymentAttemptKind,
        order_id: uuid.UUID | None = None,
        payment_method: str | None = None,
    ) -> StoredAttempt:
        row = self.rows.setdefault(key, _Row(kind, order_id, payment_method))
        row.attempts += 1
        return row.view()

    async def record(self, key: str, *, outcome: str, reference: str | None) -> StoredAttempt:
        row = self.rows[key]
        if row.outcome is None:
            row.outcome, row.reference = outcome, reference
        return row.view()

    async def get(self, key: str) -> StoredAttempt | None:
        row = self.rows.get(key)
        return row.view() if row is not None else None

    async def charge_count(self, order_id: uuid.UUID) -> int:
        return sum(
            1
            for row in self.rows.values()
            if row.order_id == order_id
            and row.kind == PaymentAttemptKind.CHARGE
            and row.outcome == ChargeOutcome.CHARGED
        )


def _view(row: PaymentAttempt) -> StoredAttempt:
    return StoredAttempt(row.attempts, row.outcome, row.reference, row.payment_method)


class PostgresPaymentStore:
    """Shared by every worker replica. Each call is its own short transaction."""

    def __init__(self, db: Database) -> None:
        self.db = db

    async def attempt(
        self,
        key: str,
        *,
        kind: PaymentAttemptKind,
        order_id: uuid.UUID | None = None,
        payment_method: str | None = None,
    ) -> StoredAttempt:
        stmt = (
            insert(PaymentAttempt)
            .values(
                idempotency_key=key,
                kind=kind,
                order_id=order_id,
                payment_method=payment_method,
                attempts=1,
            )
            .on_conflict_do_update(
                index_elements=[PaymentAttempt.idempotency_key],
                set_={"attempts": PaymentAttempt.attempts + 1, "updated_at": func.now()},
            )
            .returning(PaymentAttempt)
        )
        async with self.db.sessionmaker() as session, session.begin():
            return _view((await session.execute(stmt)).scalar_one())

    async def record(self, key: str, *, outcome: str, reference: str | None) -> StoredAttempt:
        stmt = (
            update(PaymentAttempt)
            .where(PaymentAttempt.idempotency_key == key, PaymentAttempt.outcome.is_(None))
            .values(outcome=outcome, reference=reference, updated_at=func.now())
            .returning(PaymentAttempt)
        )
        async with self.db.sessionmaker() as session, session.begin():
            row = (await session.execute(stmt)).scalar_one_or_none()
            if row is None:  # another replica recorded first: return its result
                row = await session.get(PaymentAttempt, key, populate_existing=True)
            assert row is not None, f"record() without attempt() for {key}"
            return _view(row)

    async def get(self, key: str) -> StoredAttempt | None:
        async with self.db.sessionmaker() as session:
            row = await session.get(PaymentAttempt, key)
            return _view(row) if row is not None else None

    async def charge_count(self, order_id: uuid.UUID) -> int:
        async with self.db.sessionmaker() as session:
            count = await session.scalar(
                select(func.count())
                .select_from(PaymentAttempt)
                .where(
                    PaymentAttempt.order_id == order_id,
                    PaymentAttempt.kind == PaymentAttemptKind.CHARGE,
                    PaymentAttempt.outcome == ChargeOutcome.CHARGED,
                )
            )
            return int(count or 0)
