"""enum column comments point at app/products/

The entity-first layout (mentor review 2026-10-06) moved the product
vocabularies to app/products/enums.py and the transition table to
app/products/state_machine.py; the column comments that name them follow.

Revision ID: 0003_enum_comments_products
Revises: 0002_enum_comment_paths
Create Date: 2026-10-06
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_enum_comments_products"
down_revision: str | None = "0002_enum_comment_paths"
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
    _apply(("models", "utils"), ("products", "products"))


def downgrade() -> None:
    _apply(("products", "products"), ("models", "utils"))
