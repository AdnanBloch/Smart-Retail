"""Merchant scope and reporting window, resolved from the verified token and the
query (RFC 0008 §3.1.2.1-§3.1.2.2, ANL-D21).

Every read in ``repo.py`` takes a ``Scope`` as a mandatory argument, so a
merchant query without a scope cannot be constructed; the scope becomes a
match on ``merchant_id`` inside the pipeline, never a filter on the response.
"""

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from uuid import UUID

from app.core.errors import InvalidWindow, MerchantScopeMismatch
from app.v1.metrics.enums import UserRole
from smartretail_common.auth import Principal

DEFAULT_WINDOW_DAYS = 30
MAX_WINDOW_DAYS = 366


@dataclass(frozen=True)
class Scope:
    """``merchant_id=None`` is the platform scope (admin only)."""

    merchant_id: str | None

    @property
    def is_platform(self) -> bool:
        return self.merchant_id is None


def resolve(principal: Principal, merchant_id: UUID | None) -> Scope:
    if principal.role == UserRole.MERCHANT:
        own = str(principal.merchant_id or principal.subject)
        # A different merchant_id is an error, never a silent override.
        if merchant_id is not None and str(merchant_id) != own.lower():
            raise MerchantScopeMismatch()
        return Scope(own)
    # admin (the route's role guard admits nobody else)
    return Scope(str(merchant_id) if merchant_id is not None else None)


@dataclass(frozen=True)
class Window:
    """``[start, end)`` in UTC dates."""

    start: date
    end: date

    @property
    def start_at(self) -> datetime:
        return datetime.combine(self.start, datetime.min.time(), tzinfo=UTC)

    @property
    def end_at(self) -> datetime:
        return datetime.combine(self.end, datetime.min.time(), tzinfo=UTC)


def today() -> date:
    return datetime.now(UTC).date()


def resolve_window(start: date | None, end: date | None, *, now: date | None = None) -> Window:
    """``from`` inclusive, ``to`` exclusive; default the last 30 days including today."""
    end = end or (now or today()) + timedelta(days=1)
    start = start or end - timedelta(days=DEFAULT_WINDOW_DAYS)
    if start >= end:
        raise InvalidWindow("`from` must be before `to`")
    if (end - start).days > MAX_WINDOW_DAYS:
        raise InvalidWindow(f"the window may span at most {MAX_WINDOW_DAYS} days")
    return Window(start, end)
