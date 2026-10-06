"""enum column comments point at app/core/enums.py

The entity-first layout (mentor review 2026-10-06) moved the vocabularies
shared by users and merchants from app/models/ to app/core/; the column
comments that name the file follow it.

Revision ID: 0005_enum_comments_core
Revises: 0004_enum_comment_paths
Create Date: 2026-10-06
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005_enum_comments_core"
down_revision: str | None = "0004_enum_comment_paths"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_COLUMNS = [
    ("users", "role", "UserRole"),
    ("merchant_profiles", "status", "MerchantStatus"),
]


def _comment(enum: str, folder: str) -> str:
    return f"DomainEnum {enum} — see services/identity/app/{folder}/enums.py"


def _apply(old: str, new: str) -> None:
    for table, column, enum in _COLUMNS:
        op.alter_column(
            table,
            column,
            existing_type=sa.Text(),
            existing_nullable=False,
            comment=_comment(enum, new),
            existing_comment=_comment(enum, old),
        )


def upgrade() -> None:
    _apply("models", "core")


def downgrade() -> None:
    _apply("core", "models")
