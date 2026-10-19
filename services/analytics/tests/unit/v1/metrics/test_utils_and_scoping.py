import uuid
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from bson.decimal128 import Decimal128

from app.core.config import Settings
from app.core.errors import InvalidWindow, MerchantScopeMismatch
from app.v1.metrics.enums import Granularity
from app.v1.metrics.scoping import Scope, resolve, resolve_window
from app.v1.metrics.utils import (
    bucket_date,
    days_between,
    dec128,
    money,
    orders_metric,
    period_of,
    rate,
    to_decimal,
)
from smartretail_common.auth import Principal


def principal(role: str, subject: str | None = None) -> Principal:
    subject = subject or str(uuid.uuid4())
    return Principal(
        subject=subject,
        role=role,
        token_type="user",
        merchant_id=subject if role == "merchant" else None,
        merchant_status=None,
        session_id=None,
        expires_at=0,
        key_id=None,
        claims={},
    )


def test_bucket_date_is_the_utc_date_of_occurred_at() -> None:
    plus5 = timezone(timedelta(hours=5))
    assert bucket_date(datetime(2026, 10, 15, 2, 0, tzinfo=plus5)) == "2026-10-14"
    assert bucket_date(datetime(2026, 10, 15, 2, 0)) == "2026-10-15"  # naive is UTC


def test_week_composes_monday_to_sunday_and_month_by_calendar() -> None:
    monday, sunday, next_monday = "2026-10-12", "2026-10-18", "2026-10-19"
    assert period_of(monday, Granularity.WEEK) == period_of(sunday, Granularity.WEEK) == "2026-W42"
    assert period_of(next_monday, Granularity.WEEK) == "2026-W43"
    assert period_of("2026-10-31", Granularity.MONTH) == "2026-10"
    assert period_of("2026-10-31", Granularity.DAY) == "2026-10-31"


def test_money_rate_and_decimals() -> None:
    assert rate(2, 3) == "0.6667"
    assert rate(2, 40) == "0.0500"
    assert rate(1, 0) is None
    assert money(Decimal("150")) == "150.00"
    assert to_decimal(Decimal128("1.50")) == Decimal("1.5")
    assert to_decimal(None) == 0
    assert dec128("2.10") == Decimal128("2.10")
    assert orders_metric("usd") == "orders.USD"
    assert days_between(date(2026, 10, 1), date(2026, 10, 3)) == ["2026-10-01", "2026-10-02"]


def test_a_merchant_is_always_scoped_to_its_own_id() -> None:
    me = principal("merchant")
    assert resolve(me, None) == Scope(me.subject)
    assert resolve(me, uuid.UUID(me.subject)) == Scope(me.subject)
    with pytest.raises(MerchantScopeMismatch):
        resolve(me, uuid.uuid4())


def test_an_admin_gets_platform_scope_or_the_merchant_asked_for() -> None:
    admin = principal("admin")
    assert resolve(admin, None).is_platform
    other = uuid.uuid4()
    assert resolve(admin, other) == Scope(str(other))


def test_window_defaults_and_bounds() -> None:
    today = date(2026, 10, 15)
    w = resolve_window(None, None, now=today)
    assert (w.start, w.end) == (date(2026, 9, 16), date(2026, 10, 16))
    assert w.start_at == datetime(2026, 9, 16, tzinfo=UTC)
    assert w.end_at == datetime(2026, 10, 16, tzinfo=UTC)
    with pytest.raises(InvalidWindow):
        resolve_window(date(2026, 10, 2), date(2026, 10, 1))
    with pytest.raises(InvalidWindow):
        resolve_window(date(2025, 1, 1), date(2026, 10, 1))


def test_settings_reject_a_lookback_outside_raw_events() -> None:
    with pytest.raises(ValueError, match="RECONCILE_LOOKBACK_DAYS"):
        Settings(reconcile_lookback_days=7, raw_events_ttl_days=7)
    with pytest.raises(ValueError, match="PROCESSED_EVENTS_TTL_DAYS"):
        Settings(processed_events_ttl_days=1)
