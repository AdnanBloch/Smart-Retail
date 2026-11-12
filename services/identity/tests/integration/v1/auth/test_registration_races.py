"""Concurrent registrations (RFC 0003 §3.1.4.1, I5): the database's unique
constraints, not the application's pre-check, decide who wins a race."""

import asyncio

import pytest
from sqlalchemy import func, select

from app.models import MerchantProfile, OutboxEvent, User
from tests.integration.conftest import PASSWORD


def body(email: str, **extra: str) -> dict[str, str]:
    return {"email": email, "password": PASSWORD, "full_name": "Racer", **extra}


async def test_concurrent_registrations_of_one_email_create_exactly_one_user(app, client) -> None:
    # Same address, different case: citext makes them one identity.
    emails = ["race@test.local", "RACE@test.local", "Race@Test.Local", "race@TEST.local"] * 2
    results = await asyncio.gather(
        *(client.post("/api/v1/auth/register", json=body(e)) for e in emails)
    )
    statuses = sorted(r.status_code for r in results)
    assert statuses == [201] + [409] * (len(emails) - 1), [r.text for r in results]
    for r in results:
        if r.status_code == 409:
            assert r.json()["type"].endswith("/email-taken")
    async with app.state.db.sessionmaker() as s:
        assert await s.scalar(select(func.count()).select_from(User)) == 1
        # The losers' outbox rows rolled back with them (A10).
        assert await s.scalar(select(func.count()).select_from(OutboxEvent)) == 1


@pytest.mark.parametrize(
    ("business_name", "base"),
    [
        ("Corner Shop", "corner-shop"),
        # The slug contains "email": the lost race must not be reported as a taken email.
        ("Email Experts", "email-experts"),
    ],
)
async def test_concurrent_merchants_with_one_business_name_all_register(
    app, client, business_name: str, base: str
) -> None:
    emails = [f"shop{i}@test.local" for i in range(4)]
    results = await asyncio.gather(
        *(
            client.post(
                "/api/v1/auth/register",
                json=body(e, role="merchant", business_name=business_name),
            )
            for e in emails
        )
    )
    assert [r.status_code for r in results] == [201] * 4, [r.text for r in results]
    async with app.state.db.sessionmaker() as s:
        slugs = sorted(await s.scalars(select(MerchantProfile.slug)))
        events = await s.scalar(select(func.count()).select_from(OutboxEvent))
    assert slugs == [base, f"{base}-2", f"{base}-3", f"{base}-4"]
    assert events == 4  # one registered event per user, none from a retried attempt
