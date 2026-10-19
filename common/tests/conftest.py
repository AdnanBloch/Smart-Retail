"""A scratch database holding only the shared tables (``outbox``,
``processed_events``) for the event-pipeline tests. Skipped without ``make test-db``."""

import asyncio

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from smartretail_common.db import Base
from smartretail_common.events.dedupe import ProcessedEvent
from smartretail_common.outbox import OutboxEvent
from smartretail_common.testing import test_admin_url

NAME = "common_events_test"


@pytest.fixture(scope="session")
def events_db_url() -> str:
    admin = test_admin_url()

    async def recreate() -> bool:
        try:
            conn = await asyncpg.connect(admin, timeout=3)
        except (OSError, asyncpg.PostgresError):
            return False
        try:
            await conn.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1", NAME
            )
            await conn.execute(f'DROP DATABASE IF EXISTS "{NAME}"')
            await conn.execute(f'CREATE DATABASE "{NAME}"')
        finally:
            await conn.close()
        base, _, _ = admin.rpartition("/")
        url = f"{base.replace('postgresql://', 'postgresql+asyncpg://', 1)}/{NAME}"
        engine = create_async_engine(url)
        async with engine.begin() as c:
            await c.run_sync(
                lambda sync: Base.metadata.create_all(
                    sync, tables=[OutboxEvent.__table__, ProcessedEvent.__table__]
                )
            )
        await engine.dispose()
        return True

    if not asyncio.run(recreate()):
        pytest.skip("test Postgres not reachable; run `make test-db`")
    base, _, _ = admin.rpartition("/")
    return f"{base.replace('postgresql://', 'postgresql+asyncpg://', 1)}/{NAME}"


@pytest.fixture
async def events_engine(events_db_url):
    engine = create_async_engine(events_db_url, pool_size=5)
    yield engine
    async with engine.begin() as conn:
        await conn.exec_driver_sql("TRUNCATE outbox, processed_events RESTART IDENTITY")
    await engine.dispose()
