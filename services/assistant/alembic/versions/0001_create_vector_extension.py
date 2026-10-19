"""create_vector_extension: pgvector in assistant_db only (RFC 0009 §3.1.3, migration 1).

Needs a role allowed to create the extension. Locally the init script creates
it as the superuser first (``IF NOT EXISTS`` makes this a no-op then); the
test server runs migrations as the superuser.

Revision ID: 0001_create_vector_extension
Revises:
Create Date: 2026-10-15
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0001_create_vector_extension"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")


def downgrade() -> None:
    op.execute("DROP EXTENSION IF EXISTS vector")
