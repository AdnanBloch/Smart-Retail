"""The shared-table DDL helpers produce exactly the tables the ORM models
describe, and round-trip (upgrade then downgrade). Plus the X-Client-Id rule."""

import asyncpg
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, inspect

from smartretail_common import migrations
from smartretail_common.client_id import valid_client_id
from smartretail_common.testing import test_admin_url as admin_url

NAME = "common_ddl_test"


@pytest.fixture
async def sync_url():
    admin = admin_url()
    try:
        conn = await asyncpg.connect(admin, timeout=3)
    except (OSError, asyncpg.PostgresError):
        pytest.skip("test Postgres not reachable; run `make test-db`")
    try:
        await conn.execute(f'DROP DATABASE IF EXISTS "{NAME}"')
        await conn.execute(f'CREATE DATABASE "{NAME}"')
    finally:
        await conn.close()
    base, _, _ = admin.rpartition("/")
    return f"{base.replace('postgresql://', 'postgresql+psycopg2://', 1)}/{NAME}"


def test_upgrade_creates_and_downgrade_drops_the_shared_tables(sync_url) -> None:
    pytest.importorskip("psycopg2")
    engine = create_engine(sync_url)
    with engine.begin() as conn:
        op = Operations(MigrationContext.configure(conn))
        migrations.create_outbox(op)
        migrations.create_processed_events(op)
    inspector = inspect(engine)
    assert {"outbox", "processed_events"} <= set(inspector.get_table_names())
    assert "ix_outbox_unpublished" in {i["name"] for i in inspector.get_indexes("outbox")}
    pk = inspector.get_pk_constraint("processed_events")["constrained_columns"]
    assert pk == ["consumer_group", "event_id"]
    with engine.begin() as conn:
        op = Operations(MigrationContext.configure(conn))
        migrations.drop_outbox(op)
        migrations.drop_processed_events(op)
    assert not {"outbox", "processed_events"} & set(inspect(engine).get_table_names())
    engine.dispose()


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("web-tab-0001", "web-tab-0001"),
        ("A_b-12345678", "A_b-12345678"),
        ("short", None),
        ("bad id!", None),
        ("x" * 65, None),
        (None, None),
    ],
)
def test_client_id_shape(value, expected) -> None:
    assert valid_client_id(value) == expected
