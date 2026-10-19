"""``metrics.rollups``: for each topic a fixed envelope produces the exact
operations (RFC 0008 §7 Unit)."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from bson.decimal128 import Decimal128
from bson.int64 import Int64

from app.v1.metrics.rollups import (
    LIVE,
    PROJECTIONS,
    Op,
    RollupContext,
    apply,
    apply_shipments,
    doc_key,
    fold,
)
from app.v1.metrics.utils import bucket_id, failure_id
from tests import factories as f

CTX = RollupContext(reporting_currency="USD")
AT = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)
DAY = "2026-10-14"
STAMP = {"$currentDate": {"updated_at": True}}


def ops_for(event_type: str, payload: dict, at: datetime = AT) -> list[Op]:
    return apply(f.envelope(event_type, payload, occurred_at=at), CTX)


def counter_op(name: str, by: int) -> Op:
    return Op("counters", {"_id": name}, {"$inc": {"value": Int64(by)}, **STAMP})


@pytest.mark.parametrize(
    ("role", "expected"),
    [("customer", "total_customers"), ("merchant", "total_merchants"), ("admin", None)],
)
def test_user_registered_counts_by_role(role, expected) -> None:
    ops = ops_for("identity.user.registered", f.user_registered(role))
    assert ops == ([counter_op(expected, 1)] if expected else [])


def test_products_published_and_unpublished_move_total_products() -> None:
    assert ops_for("catalog.product.published", f.product_published()) == [
        counter_op("total_products", 1)
    ]
    assert ops_for("catalog.product.unpublished", f.product_unpublished()) == [
        counter_op("total_products", -1)
    ]


def test_order_created_fans_out_to_platform_and_each_distinct_merchant() -> None:
    a, b = f.uid(), f.uid()
    items = [
        f.item(f.uid(), a, 1, "5.00"),
        f.item(f.uid(), a, 1, "5.00"),
        f.item(f.uid(), b, 1, "1"),
    ]
    ops = ops_for("order.created", f.order_created(items, currency="EUR"))
    assert [op.filter["_id"] for op in ops] == [
        bucket_id(DAY, "orders.EUR", None),
        bucket_id(DAY, "orders.EUR", a),
        bucket_id(DAY, "orders.EUR", b),
    ]
    assert all(op.update["$inc"] == {"orders_created": Int64(1)} for op in ops)
    assert all(op.update["$set"] == {"currency": "EUR"} for op in ops)
    assert all(op.upsert and op.collection == "daily_metrics" for op in ops)


def test_order_confirmed_two_merchants_and_two_variants_of_one_product() -> None:
    a, b = f.uid(), f.uid()
    shoe, hat = f.uid(), f.uid()
    items = [
        f.item(shoe, a, 2, "30.00", name="Trail Runner"),
        f.item(shoe, a, 1, "30.00", name="Trail Runner"),  # second variant, same product
        f.item(hat, b, 1, "15.50", name="Hat"),
    ]
    payload = f.order_confirmed(items, shipping="5.00")
    ops = ops_for("order.confirmed", payload)
    folded = fold(ops)

    assert folded.get("counters", "orders_total")["value"] == 1
    assert folded.get("counters", "revenue_total") == {
        "_id": "revenue_total",
        "value": Decimal("110.50"),
        "currency": "USD",
    }
    platform = folded.get("daily_metrics", bucket_id(DAY, "orders.USD", None))
    # Platform revenue is the order total, shipping included.
    assert platform["revenue"] == Decimal("110.50") and platform["orders_confirmed"] == 1
    # Merchant revenue is the sum of its own line totals; shipping is nobody's.
    assert folded.get("daily_metrics", bucket_id(DAY, "orders.USD", a))["revenue"] == Decimal("90")
    assert folded.get("daily_metrics", bucket_id(DAY, "orders.USD", b))["revenue"] == Decimal(
        "15.50"
    )
    product = folded.get("product_stats", shoe)
    assert product["quantity_sold"] == 3
    assert product["order_count"] == 1  # once per product per order
    assert product["revenue"] == Decimal("90.00")
    assert product["name"] == "Trail Runner" and product["merchant_id"] == a
    assert product["last_sold_at"] == AT
    # Money is Decimal128 in every operation, never a float (S2).
    stats_op = next(
        op for op in ops if op.collection == "product_stats" and op.filter["_id"] == shoe
    )
    assert isinstance(stats_op.update["$inc"]["revenue"], Decimal128)
    assert stats_op.update["$max"] == {"last_sold_at": AT}


def test_order_confirmed_in_another_currency_skips_all_time_revenue() -> None:
    product = f.uid()
    ops = ops_for(
        "order.confirmed", f.order_confirmed([f.item(product, f.uid(), 2, "4")], currency="EUR")
    )
    folded = fold(ops)
    assert folded.get("counters", "revenue_total") is None
    assert folded.get("counters", "orders_total")["value"] == 1
    assert folded.get("daily_metrics", bucket_id(DAY, "orders.EUR", None))["revenue"] == 8
    stats = folded.get("product_stats", product)
    assert stats["quantity_sold"] == 2 and "revenue" not in stats


def test_order_confirmed_without_a_product_name_keeps_the_snapshot() -> None:
    product = f.uid()
    ops = ops_for(
        "order.confirmed", f.order_confirmed([f.item(product, f.uid(), 1, "1", name=None)])
    )
    stats = next(op for op in ops if op.collection == "product_stats")
    assert "name" not in stats.update["$set"]


def test_order_failed_counts_buckets_and_a_failure_with_unknown_for_null_reason() -> None:
    a = f.uid()
    ops = ops_for("order.failed", f.order_failed([a, a], reason=None))
    folded = fold(ops)
    assert folded.get("daily_metrics", bucket_id(DAY, "orders.USD", None))["orders_failed"] == 1
    assert folded.get("daily_metrics", bucket_id(DAY, "orders.USD", a))["orders_failed"] == 1
    assert len(folded.of("daily_metrics")) == 2  # a repeated merchant counts once
    assert folded.get("failure_stats", failure_id(DAY, "order", "unknown"))["count"] == 1


def test_payment_failed_counts_by_reason() -> None:
    folded = fold(ops_for("payment.failed", f.payment_failed("card_declined")))
    assert folded.get("failure_stats", failure_id(DAY, "payment", "card_declined"))["count"] == 1
    folded = fold(ops_for("payment.failed", f.payment_failed(None)))
    assert folded.get("failure_stats", failure_id(DAY, "payment", "unknown"))["count"] == 1


@pytest.mark.parametrize(
    ("trigger", "counts"),
    [("depleted", True), ("reservation_refused", True), ("low_stock", False)],
)
def test_stockout_triggers(trigger, counts) -> None:
    variant, product, merchant = f.uid(), f.uid(), f.uid()
    folded = fold(ops_for("inventory.stockout", f.stockout(variant, product, merchant, trigger)))
    for scope in (None, merchant):
        doc = folded.get("daily_metrics", bucket_id(DAY, "stockouts", scope))
        assert doc["triggers"] == {trigger: 1}  # every trigger is recorded
        if counts:
            assert doc["stockouts"] == 1
            assert doc["variants"] == {variant: 1}
            assert doc["variant_products"] == {variant: product}
        else:  # a low-stock warning is not a refused customer
            assert "stockouts" not in doc and "variants" not in doc


@pytest.mark.parametrize(
    ("kind", "answered", "finish", "asked", "answers", "refusals"),
    [
        ("chat", True, "stop", 1, 1, 0),
        ("chat", False, "refused", 1, 0, 1),
        ("chat", True, "refused", 1, 0, 1),  # a refusal never counts as an answer
        ("chat", False, "length", 1, 0, 0),
        ("chat", False, "error", 1, 0, 0),
        ("chat", False, "budget_exceeded", 1, 0, 0),
        ("generation", True, "stop", 0, 0, 0),  # usage only
    ],
)
def test_assistant_classification(kind, answered, finish, asked, answers, refusals) -> None:
    merchant = f.uid() if kind == "generation" else None
    payload = f.assistant(kind, answered=answered, finish_reason=finish, merchant_id=merchant)
    folded = fold(ops_for("assistant.interaction", payload))

    def count(name: str) -> int:
        doc = folded.get("counters", name)
        return doc["value"] if doc else 0

    assert count("assistant_interactions") == 1
    assert count("assistant_questions_asked") == asked
    assert count("assistant_questions_answered") == answers
    assert count("assistant_refusals") == refusals
    daily = folded.get("daily_metrics", bucket_id(DAY, "assistant", None))
    assert daily["assistant_messages"] == 1
    assert daily["assistant_tokens"] == 150
    assert daily["assistant_cost"] == Decimal("0.0015")
    assert daily["assistant_latency_ms_total"] == 1200
    assert daily["assistant_answered"] == answers
    merchant_docs = [d for d in folded.of("daily_metrics") if d["_id"]["merchant_id"]]
    assert [d["_id"]["merchant_id"] for d in merchant_docs] == ([merchant] if merchant else [])


@pytest.mark.parametrize(
    "event_type",
    [
        "catalog.product.updated",
        "order.cancelled",
        "payment.succeeded",
        "shipment.status_changed",
        "inventory.reserved",
        "inventory.released",
        "identity.merchant.status_changed",
    ],
)
def test_raw_only_topics_yield_no_operation(event_type) -> None:
    assert apply(f.envelope(event_type, {}), CTX) == []


def test_bucket_boundary_lands_in_different_documents() -> None:
    late = datetime(2026, 10, 14, 23, 59, 59, 999000, tzinfo=UTC)
    early = datetime(2026, 10, 15, 0, 0, tzinfo=UTC)
    payload = f.order_created([f.item(f.uid(), f.uid(), 1, "1")])
    [late_op, *_] = ops_for("order.created", payload, late)
    [early_op, *_] = ops_for("order.created", payload, early)
    assert late_op.filter["_id"]["date"] == "2026-10-14"
    assert early_op.filter["_id"]["date"] == "2026-10-15"


def test_bucket_ids_have_a_stable_key_order() -> None:
    ops = ops_for("order.created", f.order_created([f.item(f.uid(), f.uid(), 1, "1")]))
    for op in ops:
        assert list(op.filter["_id"]) == ["date", "metric", "merchant_id"]


def test_shipments_projection_is_backfill_only() -> None:
    env = f.envelope("shipment.status_changed", f.shipment("delivered"), occurred_at=AT)
    assert apply(env, CTX) == []
    folded = fold(apply_shipments(env, CTX))
    doc = folded.get("daily_metrics", bucket_id(DAY, "shipments", None))
    assert doc["transitions"] == 1 and doc["statuses"] == {"delivered": 1}
    assert apply_shipments(f.envelope("order.created", {}), CTX) == []
    assert PROJECTIONS["shipments"] is apply_shipments


def test_fold_is_additive_and_max_never_moves_backwards() -> None:
    product = f.uid()
    newer = datetime(2026, 10, 14, 13, tzinfo=UTC)
    first = ops_for("order.confirmed", f.order_confirmed([f.item(product, f.uid(), 1, "2")]), newer)
    second = ops_for("order.confirmed", f.order_confirmed([f.item(product, f.uid(), 1, "3")]), AT)
    folded = fold(first + second)
    stats = folded.get("product_stats", product)
    assert stats["quantity_sold"] == 2 and stats["order_count"] == 2
    assert stats["revenue"] == Decimal(5)
    assert stats["last_sold_at"] == newer
    assert doc_key("product_stats", product) in folded.docs


def test_odd_keys_are_made_safe_for_documents() -> None:
    payload = f.stockout("v.1", f.uid(), f.uid(), trigger="$weird.trigger")
    folded = fold(ops_for("inventory.stockout", payload))
    doc = folded.get("daily_metrics", bucket_id(DAY, "stockouts", None))
    assert doc["triggers"] == {"weird_trigger": 1}


def test_every_live_formula_is_covered_here() -> None:
    assert set(LIVE) == {
        "identity.user.registered",
        "catalog.product.published",
        "catalog.product.unpublished",
        "order.created",
        "order.confirmed",
        "order.failed",
        "payment.failed",
        "inventory.stockout",
        "assistant.interaction",
    }
