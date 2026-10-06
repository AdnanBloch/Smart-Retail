"""Merchant status transitions and slugs (RFC 0003 §3.1.1.3).

Milestone 1 only drives the role-change edges; the admin status endpoint that
uses the full table arrives in Milestone 3.

Slugs are lower-case, ASCII-folded and hyphenated, with ``-2``, ``-3`` on
collision.
"""

import re

from slugify import slugify
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import MerchantStatus as S
from app.merchants.models import MerchantProfile

MERCHANT_TRANSITIONS: dict[S, frozenset[S]] = {
    S.PENDING: frozenset({S.APPROVED, S.SUSPENDED}),
    S.APPROVED: frozenset({S.SUSPENDED}),
    S.SUSPENDED: frozenset({S.APPROVED, S.PENDING}),
}


def can_transition(current: S | str, target: S) -> bool:
    return isinstance(current, S) and target in MERCHANT_TRANSITIONS[current]


def base_slug(name: str) -> str:
    return slugify(name, max_length=80) or "merchant"


def next_free_slug(base: str, taken: set[str]) -> str:
    if base not in taken:
        return base
    n = 2
    while f"{base}-{n}" in taken:
        n += 1
    return f"{base}-{n}"


async def unique_slug(session: AsyncSession, name: str) -> str:
    base = base_slug(name)
    pattern = re.compile(rf"^{re.escape(base)}(-\d+)?$")
    rows = await session.scalars(
        select(MerchantProfile.slug).where(MerchantProfile.slug.like(f"{base}%"))
    )
    return next_free_slug(base, {s for s in rows if pattern.match(s)})
