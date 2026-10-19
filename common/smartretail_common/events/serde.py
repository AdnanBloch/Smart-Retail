"""Avro encoding of envelopes, and write-time payload validation (RFC 0001 PLT-4).

Wire format is Confluent's: one magic byte ``0``, the 4-byte big-endian schema
id, then the Avro binary body. Writing it here (over ``fastavro``) instead of
through ``confluent_kafka.schema_registry`` keeps the outbox writer free of
the Kafka client and makes the codec testable without a registry.

Payload fields marked ``"sr.json": true`` in a schema hold free-form JSON
(variant ``options``, ``search_document``): they travel as a JSON string and
are decoded back into objects, so consumers see the same shape the producer
wrote.
"""

import io
import json
import struct
from typing import Any

import fastavro
from fastavro.validation import ValidationError, validate

from smartretail_common.events import registry
from smartretail_common.events.envelope import Envelope
from smartretail_common.events.schema_registry import SchemaRegistry

MAGIC = 0
HEADER = struct.Struct(">bI")
JSON_MARK = "sr.json"


class SerializationError(ValueError):
    """A payload that does not match its topic's schema, or a malformed message."""


# --- the free-form JSON fields ------------------------------------------------


def _convert(value: Any, schema: Any, *, encode: bool) -> Any:
    """Walks ``value`` alongside its raw schema, (de)serialising ``sr.json`` fields."""
    if isinstance(schema, list):  # union: convert under the branch that fits
        if value is None:
            return None
        branches = [s for s in schema if s != "null"]
        return _convert(value, branches[0], encode=encode) if len(branches) == 1 else value
    if isinstance(schema, dict):
        kind = schema.get("type")
        if kind == "record" and isinstance(value, dict):
            out = dict(value)
            for field in schema["fields"]:
                name = field["name"]
                if name not in out:
                    continue
                if field.get(JSON_MARK) and out[name] is not None:
                    out[name] = (
                        json.dumps(out[name], sort_keys=True) if encode else json.loads(out[name])
                    )
                else:
                    out[name] = _convert(out[name], field["type"], encode=encode)
            return out
        if kind == "array" and isinstance(value, list):
            return [_convert(item, schema["items"], encode=encode) for item in value]
    return value


def to_avro_payload(event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    converted: dict[str, Any] = _convert(payload, registry.payload_schema(event_type), encode=True)
    return converted


def from_avro_payload(event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    converted: dict[str, Any] = _convert(payload, registry.payload_schema(event_type), encode=False)
    return converted


# --- validation -----------------------------------------------------------------


def validate_payload(event_type: str, payload: dict[str, Any]) -> None:
    """Raises ``SerializationError`` unless ``payload`` (JSON-safe, as stored in
    the outbox) encodes under ``event_type``'s schema. ``UnknownEvent`` for a
    type with no schema."""
    payload_schema = fastavro.parse_schema(registry.payload_schema(event_type))
    try:
        validate(to_avro_payload(event_type, payload), payload_schema, raise_errors=True)
    except ValidationError as exc:
        raise SerializationError(f"{event_type}: {exc}") from exc


# --- encode / decode ------------------------------------------------------------


class AvroCodec:
    def __init__(self, schemas: SchemaRegistry) -> None:
        self.schemas = schemas
        self._writers: dict[int, Any] = {}

    async def encode(self, envelope: Envelope) -> bytes:
        spec = registry.catalogue().spec(envelope.event_type)
        schema_id = await self.schemas.lookup(
            spec.subject, registry.message_schema_str(envelope.event_type)
        )
        record = envelope.to_record()
        record["payload"] = to_avro_payload(envelope.event_type, envelope.payload)
        body = io.BytesIO()
        body.write(HEADER.pack(MAGIC, schema_id))
        try:
            fastavro.schemaless_writer(body, registry.parsed_schema(envelope.event_type), record)
        except (ValueError, TypeError, KeyError, AttributeError) as exc:
            raise SerializationError(f"{envelope.event_type}: {exc}") from exc
        return body.getvalue()

    async def decode(self, topic: str, value: bytes | None) -> Envelope:
        """Reads with the writer's schema resolved against ours (Avro schema
        resolution), so an older or newer compatible writer is read correctly."""
        if value is None or len(value) < HEADER.size:
            raise SerializationError(f"{topic}: empty or truncated message")
        magic, schema_id = HEADER.unpack_from(value)
        if magic != MAGIC:
            raise SerializationError(f"{topic}: unknown magic byte {magic}")
        spec = registry.catalogue().by_topic(topic)
        writer = self._writers.get(schema_id)
        if writer is None:
            writer = fastavro.parse_schema(json.loads(await self.schemas.schema_by_id(schema_id)))
            self._writers[schema_id] = writer
        try:
            record = fastavro.schemaless_reader(
                io.BytesIO(value[HEADER.size :]), writer, registry.parsed_schema(spec.event_type)
            )
        except Exception as exc:  # fastavro raises a variety of errors on bad bytes
            raise SerializationError(f"{topic}: {exc}") from exc
        assert isinstance(record, dict)
        record["payload"] = from_avro_payload(spec.event_type, record["payload"])
        return Envelope.from_record(record)
