"""RFC 0000 S1/S3/S4/S15 against the migrated database, plus S14 and NTF-1."""

import os
import subprocess
import sys

import asyncpg

from tests.integration.conftest import SERVICE_DIR


async def test_conventions(database_url) -> None:
    conn = await asyncpg.connect(database_url.replace("postgresql+asyncpg://", "postgresql://", 1))
    try:
        assert await conn.fetchval("SELECT count(*) FROM pg_type WHERE typtype = 'e'") == 0
        naive = await conn.fetch(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND data_type = 'timestamp without time zone'"
        )
        assert naive == []
        fks = await conn.fetch(
            "SELECT k.column_name FROM information_schema.key_column_usage k "
            "JOIN information_schema.table_constraints t USING (constraint_name) "
            "WHERE t.constraint_type = 'FOREIGN KEY'"
        )
        assert [r["column_name"] for r in fks] == ["notification_id"]  # the only FK (R2)
        tables = {
            r["tablename"]
            for r in await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        }
        assert tables == {
            "alembic_version",
            "notifications",
            "delivery_attempts",
            "push_subscriptions",
            "stream_cursors",
            "processed_events",
        }  # no outbox: notification produces nothing
        unique = await conn.fetchval(
            "SELECT indexdef FROM pg_indexes "
            "WHERE indexname = 'uq_notifications_event_user_channel'"
        )
        assert "UNIQUE" in unique and "payload ->> 'event_id'" in unique
        comments = await conn.fetch(
            "SELECT c.table_name, c.column_name, "
            "col_description(format('%s', c.table_name)::regclass, c.ordinal_position) AS comment "
            "FROM information_schema.columns c WHERE c.table_schema = 'public' "
            "AND c.column_name IN ('type', 'channel', 'delivery_scope', 'status')"
        )
        assert len(comments) == 5
        assert all(r["comment"].startswith("DomainEnum ") for r in comments)
        identity = await conn.fetchval(
            "SELECT is_identity FROM information_schema.columns "
            "WHERE table_name = 'notifications' AND column_name = 'seq'"
        )
        assert identity == "YES"
    finally:
        await conn.close()


def test_head_base_head_and_no_pending_diff(database_url) -> None:
    for args in (("downgrade", "base"), ("upgrade", "head"), ("check",)):
        result = subprocess.run(  # noqa: S603
            [sys.executable, "-m", "alembic", *args],
            cwd=SERVICE_DIR,
            env={**os.environ, "DATABASE_URL": database_url},
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
