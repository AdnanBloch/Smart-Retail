"""stream_cursors: the server-side SSE replay floor, one row per (user, device) (C22).

Revision ID: 0002_stream_cursors
Revises: 0001_initial
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_stream_cursors"
down_revision: str | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "stream_cursors",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("client_id", sa.Text(), nullable=False),
        sa.Column("cursor_seq", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "last_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_stream_cursors")),
        sa.UniqueConstraint(
            "user_id", "client_id", name=op.f("uq_stream_cursors_user_id_client_id")
        ),
    )
    op.create_index("ix_stream_cursors_last_seen_at", "stream_cursors", ["last_seen_at"])


def downgrade() -> None:
    op.drop_index("ix_stream_cursors_last_seen_at", table_name="stream_cursors")
    op.drop_table("stream_cursors")
