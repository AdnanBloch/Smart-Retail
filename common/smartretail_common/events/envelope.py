"""The envelope every event carries (RFC 0000 §3.1.2.4)."""

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from smartretail_common.events import registry


@dataclass(frozen=True)
class Envelope:
    event_id: uuid.UUID
    event_type: str
    event_version: int
    occurred_at: datetime
    producer: str
    correlation_id: str
    payload: dict[str, Any] = field(default_factory=dict)
    causation_id: str | None = None
    actor_id: str | None = None
    client_id: str | None = None
    traceparent: str | None = None

    @classmethod
    def from_outbox(cls, row: Any, *, producer: str) -> "Envelope":
        """``row`` is an ``outbox`` row (ORM object or mapping-like with attributes).

        ``event_version`` and ``producer`` are not outbox columns: the relay
        supplies them from the catalogue and the source database. A row
        without a correlation id (written outside a request) is correlated to
        itself, since the envelope field is required.
        """
        return cls(
            event_id=row.event_id,
            event_type=row.event_type,
            event_version=registry.catalogue().spec(row.event_type).version,
            occurred_at=row.created_at,
            producer=producer,
            correlation_id=row.correlation_id or str(row.event_id),
            causation_id=row.causation_id,
            client_id=row.client_id,
            payload=row.payload,
        )

    def to_record(self) -> dict[str, Any]:
        occurred = self.occurred_at
        if occurred.tzinfo is None:
            occurred = occurred.replace(tzinfo=UTC)
        return {
            "event_id": str(self.event_id),
            "event_type": self.event_type,
            "event_version": self.event_version,
            "occurred_at": occurred,
            "producer": self.producer,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "actor_id": self.actor_id,
            "client_id": self.client_id,
            "traceparent": self.traceparent,
            "payload": self.payload,
        }

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> "Envelope":
        event_id = record["event_id"]
        return cls(
            event_id=event_id if isinstance(event_id, uuid.UUID) else uuid.UUID(event_id),
            event_type=record["event_type"],
            event_version=record["event_version"],
            occurred_at=record["occurred_at"],
            producer=record["producer"],
            correlation_id=record["correlation_id"],
            causation_id=record.get("causation_id"),
            actor_id=record.get("actor_id"),
            client_id=record.get("client_id"),
            traceparent=record.get("traceparent"),
            payload=record["payload"],
        )
