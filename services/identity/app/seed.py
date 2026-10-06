"""Seed accounts (RFC 0003 T15). Idempotent: run ``make seed`` any number of times.

Creates an admin from SEED_ADMIN_EMAIL / SEED_ADMIN_PASSWORD (an existing
admin's password is never overwritten), two approved merchants and two
customers. Writes directly to the database.
"""

import asyncio
import logging

from sqlalchemy import select

from app.core.config import Settings
from app.core.enums import MerchantStatus, UserRole
from app.core.security import PasswordHasher
from app.merchants.utils import unique_slug
from app.models import MerchantProfile, User
from smartretail_common.db import Database
from smartretail_common.logging import configure_logging

logger = logging.getLogger("identity.seed")

DEMO_MERCHANTS = [
    ("merchant1@smartretail.local", "Mona Merchant", "Northwind Outfitters"),
    ("merchant2@smartretail.local", "Omar Merchant", "Contoso Home"),
]
DEMO_CUSTOMERS = [
    ("customer1@smartretail.local", "Cara Customer"),
    ("customer2@smartretail.local", "Chen Customer"),
]


async def seed(settings: Settings) -> None:
    if not settings.seed_admin_password:
        raise SystemExit("SEED_ADMIN_PASSWORD is required")
    demo_password = settings.seed_demo_password or settings.seed_admin_password
    hasher = PasswordHasher(settings)
    db = Database(settings.database_url)
    try:
        async with db.sessionmaker() as session:

            async def ensure_user(email: str, name: str, role: UserRole, password: str) -> User:
                user = await session.scalar(select(User).where(User.email == email))
                if user is None:
                    user = User(
                        email=email,
                        full_name=name,
                        role=role,
                        password_hash=await hasher.hash(password),
                        email_verified=True,
                    )
                    session.add(user)
                    await session.flush()
                    logger.info("seeded user", extra={"role": str(role)})
                return user

            await ensure_user(
                settings.seed_admin_email,
                "Platform Admin",
                UserRole.ADMIN,
                settings.seed_admin_password,
            )
            for email, name in DEMO_CUSTOMERS:
                await ensure_user(email, name, UserRole.CUSTOMER, demo_password)
            for email, name, business in DEMO_MERCHANTS:
                user = await ensure_user(email, name, UserRole.MERCHANT, demo_password)
                if await session.get(MerchantProfile, user.id) is None:
                    session.add(
                        MerchantProfile(
                            user_id=user.id,
                            business_name=business,
                            slug=await unique_slug(session, business),
                            status=MerchantStatus.APPROVED,
                        )
                    )
                    await session.flush()
            await session.commit()
    finally:
        await db.dispose()


def main() -> None:
    settings = Settings()
    configure_logging("identity-seed", settings.log_level)
    asyncio.run(seed(settings))
    logger.info("identity seed complete")


if __name__ == "__main__":
    main()
