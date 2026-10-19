"""Lock discipline for every quantity change (RFC 0006 INV-1, INV-2, INV-3).

* Lock order: ``stock_items`` rows first, ascending ``variant_id``, then
  ``reservations`` rows. A total order on acquisition makes deadlock cycles
  impossible; the bounded retry makes that claim safe rather than load-bearing.
* ``SET LOCAL lock_timeout`` bounds every wait; ``statement_timeout`` is the
  outer bound.
* On 55P03 / 40P01 / 40001 the whole transaction is retried with jittered
  exponential backoff; exhausted → ``503 inventory-contended`` (retryable).
* 23514 (the oversell ``CHECK``) maps to 409.

``locked`` is the variant for a caller that owns the transaction — the Kafka
consumer, whose ``processed_events`` claim must commit with the effect. It
neither commits nor retries: contention raises ``InventoryContended`` (a
``DependencyUnavailable``), and the consumer base rolls the whole transaction
back and retries it after a pause, without counting an attempt.
"""

import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.errors import InventoryContended, StockConstraintViolated

T = TypeVar("T")

RETRYABLE_SQLSTATES = frozenset({"55P03", "40P01", "40001"})
CHECK_VIOLATION = "23514"


def sqlstate(exc: DBAPIError) -> str | None:
    return getattr(exc.orig, "sqlstate", None) or getattr(exc.orig, "pgcode", None)


async def set_timeouts(session: AsyncSession, settings: Settings) -> None:
    # Integers from validated settings, so interpolation is safe here.
    await session.execute(
        text(f"SET LOCAL lock_timeout = {int(settings.inventory_lock_timeout_ms)}")
    )
    await session.execute(
        text(f"SET LOCAL statement_timeout = {int(settings.inventory_statement_timeout_ms)}")
    )


async def in_locked_transaction(
    session: AsyncSession, settings: Settings, work: Callable[[], Awaitable[T]]
) -> T:
    """Runs ``work`` in one transaction under the timeouts, retrying on contention."""
    attempts = max(1, settings.inventory_lock_retry_attempts)
    for attempt in range(1, attempts + 1):
        try:
            await set_timeouts(session, settings)
            result = await work()
            await session.commit()
            return result
        except DBAPIError as exc:
            await session.rollback()
            state = sqlstate(exc)
            if state == CHECK_VIOLATION:
                raise StockConstraintViolated() from exc
            if state not in RETRYABLE_SQLSTATES:
                raise
            if attempt == attempts:
                raise InventoryContended() from exc
            base = settings.inventory_lock_retry_base_ms / 1000
            await asyncio.sleep(base * 2 ** (attempt - 1) * (1 + random.random()))  # noqa: S311
        except BaseException:
            await session.rollback()
            raise
    raise AssertionError("unreachable")  # pragma: no cover


async def locked(session: AsyncSession, settings: Settings, work: Callable[[], Awaitable[T]]) -> T:
    """Runs ``work`` under the timeouts inside the caller's transaction; never commits."""
    try:
        await set_timeouts(session, settings)
        return await work()
    except DBAPIError as exc:
        state = sqlstate(exc)
        if state == CHECK_VIOLATION:
            raise StockConstraintViolated() from exc
        if state in RETRYABLE_SQLSTATES:
            raise InventoryContended() from exc
        raise
