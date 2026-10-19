"""Transactional outbox (RFC 0000 §3.1.3.2, A10, B11).

One identical ``outbox`` table in every producing service. A row is written
with :func:`write_outbox` inside the transaction that makes the state change,
so a rolled-back change leaves no row and no committed change lacks one.
``outbox-relay`` (``smartretail_common.events.relay``) is the only reader and
the only writer of ``published_at``.

Importing this module registers the table on the shared ``Base.metadata``,
so only services that import it (directly or via their ``app/models.py``)
carry the table in their migrations.
"""

import uuid
from datetime import datetime
from typing import Any

from pydantic_core import to_jsonable_python
from sqlalchemy import BigInteger, DateTime, Index, Text, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from smartretail_common.context import get_request_id
from smartretail_common.db import Base, CreatedAt
from smartretail_common.events.serde import validate_payload


class OutboxEvent(CreatedAt, Base):
    __tablename__ = "outbox"
    __table_args__ = (
        # The relay's only query: unpublished rows in id order.
        Index(
            "ix_outbox_unpublished",
            "id",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    # bigserial: the S1 exception for a high-volume, never-shared log.
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, unique=True)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    aggregate_type: Mapped[str] = mapped_column(Text, nullable=False)
    aggregate_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    correlation_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    causation_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    # The X-Client-Id of the originating request, or null (C21).
    client_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


RELAY_ROLE = "outbox_relay"

# PLT-3: the relay's role reads the backlog, marks rows published and deletes
# old published rows, and nothing else; column-level UPDATE is enough for
# SELECT … FOR UPDATE. Granted by each producer's migration because the init
# script runs before the table exists. Conditional, so a database without the
# role (a test server) still migrates.
RELAY_GRANT_SQL = """
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'outbox_relay') THEN
    GRANT SELECT, DELETE ON outbox TO outbox_relay;
    GRANT UPDATE (published_at) ON outbox TO outbox_relay;
  END IF;
END $$;
"""

RELAY_REVOKE_SQL = """
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'outbox_relay') THEN
    REVOKE ALL ON outbox FROM outbox_relay;
  END IF;
END $$;
"""


def write_outbox(
    session: AsyncSession,
    *,
    event_type: str,
    aggregate_type: str,
    aggregate_id: uuid.UUID,
    payload: dict[str, Any],
    correlation_id: str | None = None,
    causation_id: str | None = None,
    client_id: str | None = None,
) -> uuid.UUID:
    """Adds an outbox row to ``session``'s current transaction; returns its event id.

    The caller owns the transaction: nothing is flushed or committed here.
    ``correlation_id`` defaults to the request id of the current request.

    The payload is validated against the topic's Avro schema here, at write
    time (PLT-4): a payload the relay could not serialise fails inside the
    business transaction, where it rolls back with the state change, instead
    of becoming a poison row. An event type outside the catalogue is refused
    the same way (``UnknownEvent``).
    """
    stored = to_jsonable_python(payload)
    validate_payload(event_type, stored)
    event_id = uuid.uuid4()
    session.add(
        OutboxEvent(
            event_id=event_id,
            event_type=event_type,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            payload=stored,
            correlation_id=correlation_id or get_request_id(),
            causation_id=causation_id,
            client_id=client_id,
        )
    )
    return event_id
