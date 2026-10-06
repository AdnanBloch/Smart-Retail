"""RFC 0000 S1/S3/S4/S15 checks against the migrated database, plus S14."""

import os
import subprocess
import sys

import asyncpg

from tests.integration.conftest import SERVICE_DIR


def _pg(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


async def test_conventions(database_url) -> None:
    conn = await asyncpg.connect(_pg(database_url))
    try:
        enums = await conn.fetchval("SELECT count(*) FROM pg_type WHERE typtype = 'e'")
        assert enums == 0, "S4: no native PostgreSQL enums"

        pk_types = dict(
            await conn.fetch(
                """
                SELECT c.table_name, c.data_type
                FROM information_schema.table_constraints t
                JOIN information_schema.key_column_usage k USING (constraint_name, table_name)
                JOIN information_schema.columns c USING (table_name, column_name)
                WHERE t.constraint_type = 'PRIMARY KEY' AND t.table_schema = 'public'
                  AND t.table_name <> 'alembic_version'
                """
            )
        )
        assert pk_types.pop("product_audit_log") == "bigint"
        assert set(pk_types.values()) == {"uuid"}, pk_types

        naive = await conn.fetch(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND data_type = 'timestamp without time zone'"
        )
        assert naive == [], "S3: timestamptz only"

        fk_to_merchant = await conn.fetchval(
            """
            SELECT count(*) FROM information_schema.key_column_usage k
            JOIN information_schema.table_constraints t USING (constraint_name)
            WHERE t.constraint_type = 'FOREIGN KEY' AND k.column_name = 'merchant_id'
            """
        )
        assert fk_to_merchant == 0, "S15: merchant_id is a claim, not a FK"
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
