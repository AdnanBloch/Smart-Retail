"""Seeds ``prompt_templates`` from ``ops/prompts/``: ``python -m app.seed`` (``make seed``).

Idempotent: an unchanged source inserts nothing; a changed one becomes the
next active version (AST-D26).
"""

import asyncio

from app.core.config import Settings, get_settings
from app.core.db import create_database
from app.v1.prompts.repo import seed_templates


async def seed(settings: Settings) -> int:
    db = create_database(settings)
    try:
        async with db.sessionmaker() as session, session.begin():
            return await seed_templates(session, settings.prompts_dir)
    finally:
        await db.dispose()


def main() -> None:  # pragma: no cover - CLI wiring
    count = asyncio.run(seed(get_settings()))
    print(f"prompt templates: {count} new version(s)")


if __name__ == "__main__":  # pragma: no cover
    main()
