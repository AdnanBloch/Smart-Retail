"""Bucket keys and period composition (ANL-3). Pure: no I/O."""

from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Any

from bson.decimal128 import Decimal128

from app.v1.metrics.enums import Granularity, MetricKey

MONEY_QUANTUM = Decimal("0.01")
RATE_QUANTUM = Decimal("0.0001")


def bucket_date(occurred_at: datetime) -> str:
    """The UTC calendar date of ``occurred_at`` — never processing time (ANL-3)."""
    if occurred_at.tzinfo is None:
        occurred_at = occurred_at.replace(tzinfo=UTC)
    return occurred_at.astimezone(UTC).date().isoformat()


def bucket_id(day: str, metric: str, merchant_id: str | None) -> dict[str, Any]:
    """``daily_metrics._id``. Built only here so the key order is always
    ``date, metric, merchant_id``: an embedded ``_id`` matches by exact field
    order, and a second order would silently create a second document."""
    return {"date": day, "metric": str(metric), "merchant_id": merchant_id}


def failure_id(day: str, source: str, reason: str) -> dict[str, Any]:
    """``failure_stats._id``, same single-builder rule as ``bucket_id``."""
    return {"date": day, "source": str(source), "reason": reason}


def orders_metric(currency: str) -> str:
    return f"{MetricKey.ORDERS_PREFIX}{currency.upper()}"


def period_of(day: str, granularity: Granularity) -> str:
    """``2026-09-28`` → day ``2026-09-28``, ISO week ``2026-W40``, month ``2026-09``."""
    if granularity is Granularity.DAY:
        return day
    parsed = date.fromisoformat(day)
    if granularity is Granularity.WEEK:
        year, week, _ = parsed.isocalendar()
        return f"{year}-W{week:02d}"
    return f"{parsed.year}-{parsed.month:02d}"


def days_between(start: date, end: date) -> list[str]:
    """Every UTC date in ``[start, end)``."""
    return [(start + timedelta(days=i)).isoformat() for i in range((end - start).days)]


def to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal(0)
    if isinstance(value, Decimal128):
        return value.to_decimal()
    return Decimal(str(value))


def dec128(value: Decimal | str) -> Decimal128:
    """Money into BSON. Never a float (S2)."""
    return Decimal128(value if isinstance(value, Decimal) else Decimal(value))


def money(value: Decimal) -> str:
    return str(value.quantize(MONEY_QUANTUM, rounding=ROUND_HALF_EVEN))


def rate(numerator: int | Decimal, denominator: int | Decimal) -> str | None:
    """A ratio as a 4-decimal string; ``None`` when the denominator is zero."""
    if not denominator:
        return None
    return str((Decimal(numerator) / Decimal(denominator)).quantize(RATE_QUANTUM, ROUND_HALF_EVEN))
