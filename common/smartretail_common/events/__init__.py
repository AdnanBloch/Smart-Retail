"""The event pipeline (RFC 0001 §3.1.1.1, §3.1.1.2, §3.1.1.6).

* ``registry``   — the event catalogue and its Avro schemas, loaded from data.
* ``serde``      — Confluent wire format over fastavro; write-time validation.
* ``schema_registry`` — the Schema Registry client (HTTP, and in-memory for tests).
* ``envelope``   — the envelope every message carries (RFC 0000 §3.1.2.4).
* ``producer``   — the idempotent Kafka producer behind a ``Publisher`` protocol.
* ``relay``      — ``outbox-relay``: outbox rows → Kafka.
* ``dedupe``     — ``processed_events`` and the claim that makes consumers idempotent.
* ``consumer``   — the ``EventConsumer`` base: dedupe, retry, DLQ, manual commit.

``confluent-kafka`` is the ``events`` extra; only ``producer`` and the Kafka
adapters in ``consumer`` import it, so services that only write outbox rows
need nothing beyond ``fastavro``.
"""
