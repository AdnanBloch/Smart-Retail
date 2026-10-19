"""The Kafka producer (RFC 0001 §3.1.1.6 *Producer and consumer configuration*).

Every setting that makes publishing safe is fixed here, not per service:

* ``enable.idempotence`` + ``acks=all``: a write is acknowledged once it is
  replicated, and the producer's own internal retries are deduplicated by the
  broker, so they can neither duplicate nor reorder within a partition.
* ``max.in.flight.requests.per.connection=5``: the most that idempotence
  still keeps in order.
* Keys are the aggregate id (UTF-8), so one aggregate is one partition (C3).
"""

from collections.abc import Callable
from typing import Any, Protocol

PRODUCER_CONFIG: dict[str, Any] = {
    "enable.idempotence": True,
    "acks": "all",
    "max.in.flight.requests.per.connection": 5,
    "linger.ms": 5,
    "compression.type": "lz4",
}

DeliveryCallback = Callable[[Any, Any], None]


class Publisher(Protocol):
    """The slice of ``confluent_kafka.Producer`` the relay and the DLQ use."""

    def produce(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        headers: list[tuple[str, bytes]] | None = None,
        on_delivery: DeliveryCallback | None = None,
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


def kafka_producer(bootstrap_servers: str, client_id: str) -> Publisher:  # pragma: no cover
    """A real producer. Imported lazily: ``confluent-kafka`` is the ``events`` extra."""
    from confluent_kafka import Producer

    producer: Publisher = Producer(
        {**PRODUCER_CONFIG, "bootstrap.servers": bootstrap_servers, "client.id": client_id}
    )
    return producer
