"""enum column comments point at app/models/enums.py

The service layout moved the enums from app/domain/ to app/models/ and the
state machine to app/utils/; the column comments that name the files follow.

Revision ID: 0002_enum_comment_paths
Revises: 0001_initial
Create Date: 2026-10-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_enum_comment_paths"
down_revision: str | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _status(enums: str, machine: str) -> str:
    return (
        f"DomainEnum ProductStatus — see services/catalog/app/{enums}/enums.py; "
        f"transitions in app/{machine}/state_machine.py"
    )


def _action(enums: str) -> str:
    return f"DomainEnum ProductAuditAction — see services/catalog/app/{enums}/enums.py"


def _apply(old: tuple[str, str], new: tuple[str, str]) -> None:
    op.alter_column(
        "products",
        "status",
        existing_type=sa.Text(),
        existing_nullable=False,
        comment=_status(*new),
        existing_comment=_status(*old),
    )
    op.alter_column(
        "product_audit_log",
        "action",
        existing_type=sa.Text(),
        existing_nullable=False,
        comment=_action(new[0]),
        existing_comment=_action(old[0]),
    )


def upgrade() -> None:
    _apply(("domain", "domain"), ("models", "utils"))


def downgrade() -> None:
    _apply(("models", "utils"), ("domain", "domain"))
