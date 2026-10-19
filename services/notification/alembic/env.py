"""Alembic environment (RFC 0000 S14). Migrations run only via ``make migrate``."""

import asyncio
import os

from alembic import context
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from app.models import Base
from smartretail_common.db_types import render_item

config = context.config
target_metadata = Base.metadata

# notification produces no events, so it has no outbox (RFC 0007 §3.1.2.3).
# ``smartretail_common.migrations`` imports the outbox model to share its DDL,
# which registers the table on the shared metadata; leave it out here.
NOT_OURS = frozenset({"outbox"})


def _include_object(obj: object, name: str | None, type_: str, *_: object) -> bool:
    if type_ == "table":
        return name not in NOT_OURS
    table = getattr(obj, "table", None)
    return getattr(table, "name", None) not in NOT_OURS


def _url() -> str:
    # Read directly so migrations don't need the service's runtime settings.
    return os.environ["DATABASE_URL"]


def _configure(**kwargs: object) -> None:
    context.configure(
        target_metadata=target_metadata,
        render_item=render_item,
        include_object=_include_object,
        compare_type=True,
        **kwargs,  # type: ignore[arg-type]
    )


def run_migrations_offline() -> None:
    _configure(url=_url(), literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def _run_sync(connection: Connection) -> None:
    _configure(connection=connection)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    engine = create_async_engine(_url(), poolclass=NullPool)
    async with engine.connect() as connection:
        await connection.run_sync(_run_sync)
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
