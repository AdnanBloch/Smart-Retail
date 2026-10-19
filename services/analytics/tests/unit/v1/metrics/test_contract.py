"""ANL-10: every payload field a formula reads exists in the producer's
registered Avro schema, and every test payload validates against it."""

from typing import Any

import pytest

from app.v1.metrics.consumers import subscribed_event_types
from app.v1.metrics.rollups import FIELDS_READ, LIVE
from smartretail_common.events import registry
from smartretail_common.events.serde import validate_payload
from tests import factories as f


def _fields(schema: Any) -> dict[str, Any]:
    return {field["name"]: field["type"] for field in schema["fields"]}


def _resolve(schema: Any, path: str) -> bool:
    """``items[].merchant_id`` / ``merchant_ids[]`` against a record schema."""
    node: Any = schema
    for part in path.split("."):
        array = part.endswith("[]")
        name = part.removesuffix("[]")
        if not isinstance(node, dict) or node.get("type") != "record":
            return False
        fields = _fields(node)
        if name not in fields:
            return False
        node = fields[name]
        if isinstance(node, list):  # nullable union
            node = next(t for t in node if t != "null")
        if array:
            if not isinstance(node, dict) or node.get("type") != "array":
                return False
            node = node["items"]
    return True


@pytest.mark.parametrize("event_type", sorted(FIELDS_READ))
def test_every_field_read_exists_in_the_registered_schema(event_type) -> None:
    schema = registry.payload_schema(event_type)
    missing = [p for p in FIELDS_READ[event_type] if not _resolve(schema, p)]
    assert missing == [], f"{event_type} schema lacks {missing}"


def test_every_live_formula_declares_its_fields() -> None:
    assert set(LIVE) <= set(FIELDS_READ)


def test_resolver_rejects_absent_paths() -> None:
    schema = registry.payload_schema("order.confirmed")
    assert not _resolve(schema, "lines[].merchant_id")
    assert not _resolve(schema, "currency[]")
    assert not _resolve(schema, "currency.code")


def test_the_consumer_subscribes_to_every_topic_with_a_schema() -> None:
    subscribed = set(subscribed_event_types())
    with_schema = {
        s.event_type for s in registry.catalogue().events if registry.has_schema(s.event_type)
    }
    assert subscribed == with_schema
    assert "identity.merchant.status_changed" not in subscribed  # no schema yet
    assert {"identity.user.registered", "assistant.interaction"} <= subscribed


@pytest.mark.parametrize(
    ("event_type", "payload"),
    [
        ("identity.user.registered", f.user_registered("merchant")),
        ("catalog.product.published", f.product_published()),
        ("catalog.product.unpublished", f.product_unpublished()),
        ("order.created", f.order_created([f.item(f.uid(), f.uid(), 2, "3.50")])),
        ("order.confirmed", f.order_confirmed([f.item(f.uid(), f.uid(), 2, "3.50")])),
        ("order.failed", f.order_failed([f.uid()])),
        ("payment.failed", f.payment_failed()),
        ("inventory.stockout", f.stockout(f.uid(), f.uid(), f.uid())),
        ("assistant.interaction", f.assistant()),
        ("shipment.status_changed", f.shipment()),
    ],
)
def test_test_payloads_match_the_producer_schemas(event_type, payload) -> None:
    validate_payload(event_type, payload)
