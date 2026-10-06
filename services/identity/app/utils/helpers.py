"""Small helpers shared by the repos and the seed command.

Password policy (RFC 0003 IDN-2): 12-128 characters after NFKC, not the email.
Merchant slugs: lower-case, ASCII-folded, hyphenated; ``-2``, ``-3`` on collision.
"""

import re

from slugify import slugify
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.configs.env import Settings
from app.infrastructure.security import normalise_password
from app.models import MerchantProfile
from smartretail_common.errors import Unprocessable


def check_password_policy(settings: Settings, password: str, email: str, *, field: str) -> None:
    normalised = normalise_password(password)
    problems = []
    if len(normalised) < settings.password_min_length:
        problems.append(f"must be at least {settings.password_min_length} characters")
    if len(normalised) > settings.password_max_length:
        problems.append(f"must be at most {settings.password_max_length} characters")
    if normalised.casefold() == email.casefold():
        problems.append("must not equal the email address")
    if problems:
        raise Unprocessable(
            "Password does not meet the policy",
            violations=[
                {"field": field, "message": m, "code": "password_policy"} for m in problems
            ],
        )


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
