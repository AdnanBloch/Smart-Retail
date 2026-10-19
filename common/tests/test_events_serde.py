"""The catalogue, the Avro codec and write-time validation (PLT-4, PLT-26)."""

import json
import uuid
from datetime import UTC, datetime

import pytest

from smartretail_common.events import registry, serde
from smartretail_common.events.envelope import Envelope
from smartretail_common.events.registry import UnknownEvent
from smartretail_common.events.schema_registry import (
    InMemorySchemaRegistry,
    SchemaNotRegistered,
)
from smartretail_common.events.serde import AvroCodec, SerializationError

PUBLISHED = "catalog.product.published"


def product_state() -> dict[str, object]:
    return {
        "product_id": str(uuid.uuid4()),
        "merchant_id": str(uuid.uuid4()),
        "version": 3,
        "sku": "TSHIRT",
        "name": "T-shirt",
        "status": "published",
        "category_id": None,
        "base_price": "19.99",
        "currency": "USD",
        "published_at": "2026-10-14T10:00:00Z",
        "variants": [
            {
                "variant_id": str(uuid.uuid4()),
                "sku": "TSHIRT-M",
                "options": {"size": "M", "colour": ["red", "blue"]},
                "price_override": None,
                "is_active": True,
            }
        ],
        "search_document": {"name": "T-shirt", "category": None, "attributes": {"cotton": 100}},
        "changed_fields": [],
    }


def envelope(event_type: str = PUBLISHED, payload: dict[str, object] | None = None) -> Envelope:
    return Envelope(
        event_id=uuid.uuid4(),
        event_type=event_type,
        event_version=1,
        occurred_at=datetime(2026, 10, 14, 10, 0, tzinfo=UTC),
        producer="catalog",
        correlation_id="req-1",
        payload=payload if payload is not None else product_state(),
        client_id="web",
    )


def registered_codec(*event_types: str) -> AvroCodec:
    schemas = InMemorySchemaRegistry()
    for event_type in event_types:
        spec = registry.catalogue().spec(event_type)
        schemas.register_now(spec.subject, registry.message_schema_str(event_type))
    return AvroCodec(schemas)


# --- the catalogue ---------------------------------------------------------------


def test_catalogue_has_sixteen_topics_keyed_by_aggregate() -> None:
    cat = registry.catalogue()
    assert len(cat.events) == 16  # RFC 0000 §3.1.2.3: fourteen rows, two of them pairs
    assert cat.topic("order.cancelled") == "smartretail.order.cancelled"
    assert cat.spec("inventory.stockout").key == "variant_id"
    assert cat.by_topic("smartretail.catalog.product.published").cleanup == "compact"
    assert cat.spec(PUBLISHED).dlq_topic == "smartretail.catalog.product.published.dlq"


def test_every_schema_file_parses_and_names_are_unique() -> None:
    names = set()
    for spec in registry.catalogue().events:
        if spec.schema_file is None:
            assert not registry.has_schema(spec.event_type)
            continue
        registry.parsed_schema(spec.event_type)
        names.add(registry.message_schema(spec.event_type)["name"])
    assert len(names) == 15  # every event with a producer today


def test_unknown_events_and_schemaless_events_are_refused() -> None:
    with pytest.raises(UnknownEvent):
        registry.catalogue().spec("nope.nothing")
    with pytest.raises(UnknownEvent):
        registry.catalogue().by_topic("smartretail.nope")
    with pytest.raises(UnknownEvent):
        registry.message_schema("identity.merchant.status_changed")  # no producer yet
    assert registry.has_schema("nope.nothing") is False


# --- validation ------------------------------------------------------------------


def test_a_matching_payload_validates() -> None:
    serde.validate_payload(PUBLISHED, product_state())


@pytest.mark.parametrize(
    "change",
    [
        {"version": "3"},  # a string where Avro wants a long
        {"base_price": 19.99},  # money is a string (S2), never a float
        {"variants": [{"variant_id": "v"}]},  # required variant fields missing
    ],
)
def test_a_mismatched_payload_is_refused(change: dict[str, object]) -> None:
    with pytest.raises(SerializationError):
        serde.validate_payload(PUBLISHED, {**product_state(), **change})


# --- encode / decode -------------------------------------------------------------


async def test_round_trip_restores_the_envelope_and_free_form_json() -> None:
    codec = registered_codec(PUBLISHED)
    original = envelope()
    value = await codec.encode(original)
    assert value[0] == 0 and int.from_bytes(value[1:5], "big") == 1  # Confluent wire format
    decoded = await codec.decode("smartretail.catalog.product.published", value)
    assert decoded == original
    variant = decoded.payload["variants"][0]  # type: ignore[index]
    assert variant["options"] == {"size": "M", "colour": ["red", "blue"]}


async def test_a_null_free_form_field_stays_null() -> None:
    codec = registered_codec(PUBLISHED)
    payload = {**product_state(), "search_document": None}
    decoded = await codec.decode(
        "smartretail.catalog.product.published", await codec.encode(envelope(payload=payload))
    )
    assert decoded.payload["search_document"] is None


async def test_a_naive_timestamp_is_read_as_utc() -> None:
    codec = registered_codec(PUBLISHED)
    naive = Envelope(**{**envelope().__dict__, "occurred_at": datetime(2026, 10, 14, 10, 0)})
    decoded = await codec.decode("smartretail.catalog.product.published", await codec.encode(naive))
    assert decoded.occurred_at == datetime(2026, 10, 14, 10, 0, tzinfo=UTC)


async def test_an_unregistered_schema_cannot_be_produced() -> None:
    # auto.register.schemas=false: only `make topics` registers.
    with pytest.raises(SchemaNotRegistered):
        await AvroCodec(InMemorySchemaRegistry()).encode(envelope())


async def test_an_unencodable_payload_is_a_serialization_error() -> None:
    codec = registered_codec(PUBLISHED)
    with pytest.raises(SerializationError):
        await codec.encode(envelope(payload={**product_state(), "version": "three"}))


async def test_an_older_writer_schema_is_resolved_against_ours() -> None:
    # BACKWARD compatibility in practice: a v1 writer without client_id/traceparent.
    schemas = InMemorySchemaRegistry()
    old = registry.message_schema(PUBLISHED)
    old["fields"] = [f for f in old["fields"] if f["name"] not in {"client_id", "traceparent"}]
    old_id = schemas.register_now("old", json.dumps(old))
    schemas.register_now(
        registry.catalogue().spec(PUBLISHED).subject, registry.message_schema_str(PUBLISHED)
    )
    import io

    import fastavro

    record = envelope().to_record()
    record["payload"] = serde.to_avro_payload(PUBLISHED, record["payload"])
    body = io.BytesIO()
    body.write(serde.HEADER.pack(0, old_id))
    fastavro.schemaless_writer(body, fastavro.parse_schema(old), record)
    decoded = await AvroCodec(schemas).decode(
        "smartretail.catalog.product.published", body.getvalue()
    )
    assert decoded.client_id is None and decoded.traceparent is None


@pytest.mark.parametrize("value", [None, b"", b"\x00\x00", b"\x07\x00\x00\x00\x01xyz"])
async def test_malformed_messages_are_serialization_errors(value: bytes | None) -> None:
    with pytest.raises(SerializationError):
        await registered_codec(PUBLISHED).decode("smartretail.catalog.product.published", value)


async def test_garbage_after_a_valid_header_is_a_serialization_error() -> None:
    with pytest.raises(SerializationError):
        await registered_codec(PUBLISHED).decode(
            "smartretail.catalog.product.published", b"\x00\x00\x00\x00\x01\xff\xff\xff"
        )
