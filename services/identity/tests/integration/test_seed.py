"""``make seed`` (RFC 0003 T15) against the test database: what it creates,
that it is idempotent, and that it never overwrites an existing admin's password."""

import pytest
from sqlalchemy import func, select

from app.core.enums import MerchantStatus, UserRole
from app.core.security import PasswordHasher
from app.models import MerchantProfile, User
from app.seed import DEMO_CUSTOMERS, DEMO_MERCHANTS, seed
from tests.integration.conftest import login


@pytest.fixture
def seed_settings(settings):
    settings.seed_admin_email = "root@test.local"
    settings.seed_admin_password = "admin password 123"
    settings.seed_demo_password = "demo password 123"
    return settings


async def counts(app) -> dict[str, int]:
    async with app.state.db.sessionmaker() as s:
        users = await s.scalar(select(func.count()).select_from(User))
        profiles = await s.scalar(select(func.count()).select_from(MerchantProfile))
    return {"users": int(users or 0), "profiles": int(profiles or 0)}


async def test_seed_creates_an_admin_and_approved_demo_accounts(app, client, seed_settings) -> None:
    await seed(seed_settings)
    assert await counts(app) == {"users": 5, "profiles": 2}
    async with app.state.db.sessionmaker() as s:
        admin = await s.scalar(select(User).where(User.email == "root@test.local"))
        assert admin is not None and admin.role == UserRole.ADMIN and admin.email_verified
        profiles = list(await s.scalars(select(MerchantProfile).order_by(MerchantProfile.slug)))
    assert [(p.business_name, p.slug, p.status) for p in profiles] == [
        ("Contoso Home", "contoso-home", MerchantStatus.APPROVED),
        ("Northwind Outfitters", "northwind-outfitters", MerchantStatus.APPROVED),
    ]
    # Every seeded account can log in through the API.
    await login(client, "root@test.local", "admin password 123")
    for email, *_ in DEMO_MERCHANTS + DEMO_CUSTOMERS:
        await login(client, email, "demo password 123")


async def test_seed_is_idempotent_and_keeps_an_existing_admin_password(
    app, client, seed_settings
) -> None:
    await seed(seed_settings)
    seed_settings.seed_admin_password = "a different password"
    await seed(seed_settings)
    assert await counts(app) == {"users": 5, "profiles": 2}
    await login(client, "root@test.local", "admin password 123")


async def test_demo_accounts_default_to_the_admin_password(app, client, seed_settings) -> None:
    seed_settings.seed_demo_password = None
    await seed(seed_settings)
    await login(client, DEMO_CUSTOMERS[0][0], "admin password 123")


async def test_seed_refuses_to_run_without_an_admin_password(app, seed_settings) -> None:
    seed_settings.seed_admin_password = None
    with pytest.raises(SystemExit, match="SEED_ADMIN_PASSWORD"):
        await seed(seed_settings)
    assert await counts(app) == {"users": 0, "profiles": 0}


async def test_seeded_hashes_use_the_configured_parameters(app, seed_settings) -> None:
    await seed(seed_settings)
    async with app.state.db.sessionmaker() as s:
        hashes = list(await s.scalars(select(User.password_hash)))
    hasher = PasswordHasher(seed_settings)
    assert hashes and not any(hasher.needs_rehash(h) for h in hashes)
