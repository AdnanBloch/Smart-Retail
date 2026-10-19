"""Event → recipient → type → channels → scope → rows (RFC 0007 §3.1.1.4, the consumer).

Runs inside the consumer base's transaction, after its ``processed_events``
claim (C4). The insert is ``ON CONFLICT DO NOTHING`` against the NTF-1 unique
index, so even with the dedupe table bypassed (a truncate, a replay tool) an
event can never produce a second row per ``(event, user, channel)`` (I1).
"""

import json
import logging
import uuid
from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.v1.notifications.models import Notification
from app.v1.notifications.templates import render
from app.v1.notifications.utils import derive_scope, resolve
from smartretail_common.client_id import valid_client_id
from smartretail_common.events.envelope import Envelope

logger = logging.getLogger("notification.recording")


def envelope_document(envelope: Envelope) -> dict[str, Any]:
    """The source envelope as ``payload`` stores it (S8): JSON, ``event_id`` a string."""
    document = {
        "event_id": str(envelope.event_id),
        "event_type": envelope.event_type,
        "event_version": envelope.event_version,
        "occurred_at": envelope.occurred_at.isoformat(),
        "producer": envelope.producer,
        "correlation_id": envelope.correlation_id,
        "causation_id": envelope.causation_id,
        "actor_id": envelope.actor_id,
        "client_id": envelope.client_id,
        "payload": envelope.payload,
    }
    loaded: dict[str, Any] = json.loads(json.dumps(document, default=str))
    return loaded


class RecordingRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def record(self, envelope: Envelope) -> list[uuid.UUID]:
        """Inserts this event's rows; returns the ids actually inserted."""
        recipe = resolve(envelope.event_type, envelope.payload)
        if recipe is None:
            return []
        # The envelope's client_id is a hint, validated for shape only (C21).
        origin = valid_client_id(envelope.client_id)
        scope = derive_scope(recipe.type, origin)
        title, body = render(recipe.type, envelope.payload)
        document = envelope_document(envelope)
        rows = [
            {
                "user_id": recipe.user_id,
                "type": recipe.type,
                "channel": channel,
                "title": title,
                "body": body,
                "payload": document,
                "correlation_id": envelope.correlation_id,
                "origin_client_id": origin,
                "delivery_scope": scope,
            }
            for channel in recipe.channels
        ]
        inserted = list(
            await self.session.scalars(
                pg_insert(Notification)
                .values(rows)
                .on_conflict_do_nothing()
                .returning(Notification.id)
            )
        )
        logger.info(
            "notifications recorded",
            extra={
                "event_id": str(envelope.event_id),
                "event_type": envelope.event_type,
                "user_id": str(recipe.user_id),
                "notification_type": str(recipe.type),
                "inserted": len(inserted),
            },
        )
        return inserted
