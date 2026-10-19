"""Grant outbox-relay its access to ``outbox`` (RFC 0001 PLT-3).

The relay connects as ``outbox_relay`` (created by the init script) and may
read, mark published and delete outbox rows: nothing else in this database.

Revision ID: 0003_outbox_relay_grant
Revises: 0002_idempotency_state_comment
"""

from collections.abc import Sequence

from alembic import op

from smartretail_common.outbox import RELAY_GRANT_SQL, RELAY_REVOKE_SQL

revision: str = "0003_outbox_relay_grant"
down_revision: str | None = "0002_idempotency_state_comment"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(RELAY_GRANT_SQL)


def downgrade() -> None:
    op.execute(RELAY_REVOKE_SQL)
