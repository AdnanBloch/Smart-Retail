"""enum column comments point at app/models/enums.py

The service layout moved the enums from app/domain/ to app/models/; the
column comments that name the file follow it.

Revision ID: 0004_enum_comment_paths
Revises: 0003_refresh_sessions
Create Date: 2026-10-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004_enum_comment_paths"
down_revision: str | None = "0003_refresh_sessions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_COLUMNS = [
    ("users", "role", "UserRole"),
    ("merchant_profiles", "status", "MerchantStatus"),
]


def _comment(enum: str, folder: str) -> str:
    return f"DomainEnum {enum} — see services/identity/app/{folder}/enums.py"


def upgrade() -> None:
    for table, column, enum in _COLUMNS:
        op.alter_column(
            table,
            column,
            existing_type=sa.Text(),
            existing_nullable=False,
            comment=_comment(enum, "models"),
            existing_comment=_comment(enum, "domain"),
        )


def downgrade() -> None:
    for table, column, enum in _COLUMNS:
        op.alter_column(
            table,
            column,
            existing_type=sa.Text(),
            existing_nullable=False,
            comment=_comment(enum, "domain"),
            existing_comment=_comment(enum, "models"),
        )
