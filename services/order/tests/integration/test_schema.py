"""RFC 0000 S1/S3/S4/S15 against the migrated database, plus S14."""

import os
import subprocess
import sys

import asyncpg

from tests.integration.conftest import SERVICE_DIR


async def test_conventions(database_url) -> None:
    conn = await asyncpg.connect(database_url.replace("postgresql+asyncpg://", "postgresql://", 1))
    try:
        assert await conn.fetchval("SELECT count(*) FROM pg_type WHERE typtype = 'e'") == 0
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM pg_class WHERE relkind = 'S' AND relname = 'order_number_seq'"
            )
            == 1
        )
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
        assert [r["column_name"] for r in fks] == ["order_id"]  # the only FK (R2)
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
