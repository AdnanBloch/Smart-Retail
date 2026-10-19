"""``processed_events``: the consumer half of at-least-once (RFC 0000 §3.1.3.2, A11, C4).

The claim is an ``INSERT … ON CONFLICT DO NOTHING`` in the same transaction
as the consumer's effect. Both commit or neither does, so "the effect
happened" and "the event is recorded as handled" can never disagree: a
redelivered event finds its row and is skipped, and an event whose effect
rolled back left no row and is handled again.

Importing this module registers the table on the shared ``Base.metadata``,
so only consuming services (that import it from ``app/models.py``) carry it.
"""

import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any, Protocol

from sqlalchemy import DateTime, Index, Text, delete, func, select, tuple_
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Mapped, mapped_column

from smartretail_common.db import Base
from smartretail_common.events.envelope import Envelope

RETENTION_DAYS = 30  # PLT-17


class ProcessedEvent(Base):
    __tablename__ = "processed_events"
    __table_args__ = (Index("ix_processed_events_processed_at", "processed_at"),)

    consumer_group: Mapped[str] = mapped_column(Text, primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


Handler = Callable[[Envelope, Any], Awaitable[None]]


class DedupeStore(Protocol):
    """Where a consumer group records the events it has handled, and the
    transaction its handler runs in (RFC 0001 §3.1.1.1: "analytics supplies a
    Mongo store"). ``run`` claims the event and runs the handler atomically and
    returns ``"handled"`` or ``"duplicate"`` — or ``"stale"`` for a store that
    can tell an event is older than its dedupe memory."""

    async def run(self, group: str, envelope: Envelope, handler: Handler) -> str: ...


class PostgresDedupeStore:
    """The default: ``processed_events`` in the consumer's own Postgres, the
    claim and the effect in one transaction; the handler receives the session."""

    def __init__(self, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
        self.sessionmaker = sessionmaker

    async def run(self, group: str, envelope: Envelope, handler: Handler) -> str:
        async with self.sessionmaker() as session, session.begin():
            if not await claim(session, group, envelope):
                return "duplicate"
            await handler(envelope, session)
        return "handled"


async def claim(session: AsyncSession, group: str, envelope: Envelope) -> bool:
    """True if this group has not handled ``envelope`` before. Never commits."""
    claimed = await session.scalar(
        pg_insert(ProcessedEvent)
        .values(consumer_group=group, event_id=envelope.event_id, event_type=envelope.event_type)
        .on_conflict_do_nothing(index_elements=["consumer_group", "event_id"])
        .returning(ProcessedEvent.event_id)
    )
    return claimed is not None


async def seen(session: AsyncSession, group: str, event_id: uuid.UUID) -> bool:
    found = await session.scalar(
        select(ProcessedEvent.event_id).where(
            ProcessedEvent.consumer_group == group, ProcessedEvent.event_id == event_id
        )
    )
    return found is not None


async def sweep(session: AsyncSession, *, days: int = RETENTION_DAYS, batch: int = 5000) -> int:
    """Deletes rows older than ``days`` in batches; commits each batch (PLT-17)."""
    removed = 0
    while True:
        cutoff = func.now() - func.make_interval(0, 0, 0, days)
        doomed = (
            select(ProcessedEvent.consumer_group, ProcessedEvent.event_id)
            .where(ProcessedEvent.processed_at < cutoff)
            .limit(batch)
        )
        result = await session.execute(
            delete(ProcessedEvent).where(
                tuple_(ProcessedEvent.consumer_group, ProcessedEvent.event_id).in_(doomed)
            )
        )
        await session.commit()
        count = int(result.rowcount or 0)
        removed += count
        if count < batch:
            return removed
