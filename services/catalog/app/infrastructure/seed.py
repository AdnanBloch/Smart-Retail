"""Seed the category tree. Idempotent: existing slugs are left untouched.

Products are created through the API (and, from Milestone 2, published
through the workflow), so they are not seeded here.
"""

import asyncio
import logging

from sqlalchemy import select

from app.configs.env import Settings
from app.models import Category
from smartretail_common.db import Database
from smartretail_common.logging import configure_logging

logger = logging.getLogger("catalog.seed")

CATEGORIES: list[tuple[str, str, str | None]] = [
    ("Apparel", "apparel", None),
    ("Men", "apparel-men", "apparel"),
    ("Women", "apparel-women", "apparel"),
    ("Electronics", "electronics", None),
    ("Phones", "electronics-phones", "electronics"),
    ("Audio", "electronics-audio", "electronics"),
    ("Home & Kitchen", "home-kitchen", None),
    ("Cookware", "home-kitchen-cookware", "home-kitchen"),
]


async def seed(settings: Settings) -> None:
    db = Database(settings.database_url)
    try:
        async with db.sessionmaker() as session:
            by_slug = {c.slug: c for c in await session.scalars(select(Category))}
            for name, slug, parent_slug in CATEGORIES:
                if slug in by_slug:
                    continue
                parent = by_slug.get(parent_slug) if parent_slug else None
                category = Category(name=name, slug=slug, parent_id=parent.id if parent else None)
                session.add(category)
                await session.flush()
                by_slug[slug] = category
                logger.info("seeded category", extra={"slug": slug})
            await session.commit()
    finally:
        await db.dispose()


def main() -> None:
    settings = Settings()
    configure_logging("catalog-seed", settings.log_level)
    asyncio.run(seed(settings))
    logger.info("catalog seed complete")


if __name__ == "__main__":
    main()
