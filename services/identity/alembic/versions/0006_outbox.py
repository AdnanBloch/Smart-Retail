"""outbox: identity becomes an event producer (RFC 0003 IDN-T4, RFC 0000 §3.1.3.2).

``identity.user.registered`` is written in the registration transaction; the
outbox-relay role is granted its rights here (PLT-3).

Revision ID: 0006_outbox
Revises: 0005_enum_comments_core
"""

from collections.abc import Sequence

from alembic import op

from smartretail_common.migrations import create_outbox, drop_outbox

revision: str = "0006_outbox"
down_revision: str | None = "0005_enum_comments_core"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    create_outbox(op)


def downgrade() -> None:
    drop_outbox(op)
