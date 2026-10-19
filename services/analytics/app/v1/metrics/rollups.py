"""Every metric formula on the write side: envelope → Mongo update operations.

Pure functions (RFC 0008 §3.1.1.1): ``apply`` returns *operations*, never
writes, so the live consumer, the reconciliation job and the backfill all use
the same formula — one formula, three callers, one set of tests. ``fold``
applies operations in memory, which is how reconciliation recomputes a day
and how tests state the expected documents.

The per-topic effects are RFC 0008 §3.1.2.4, corrected to the payloads the
producers actually emit (ANL-D31): order lines are ``items[]``, ``order.failed``
carries ``merchant_ids[]``, reasons are nullable (→ ``"unknown"``), and only
``depleted``/``reservation_refused`` stockout triggers count as stockouts.
"""

import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from bson.int64 import Int64

from app.v1.metrics.enums import (
    UNKNOWN_REASON,
    Collection,
    CounterName,
    FailureSource,
    FinishReason,
    InteractionKind,
    MetricKey,
    StockoutTrigger,
    UserRole,
)
from app.v1.metrics.utils import (
    bucket_date,
    bucket_id,
    dec128,
    failure_id,
    orders_metric,
    to_decimal,
)
from smartretail_common.events.envelope import Envelope

COUNTING_TRIGGERS = frozenset({StockoutTrigger.DEPLETED, StockoutTrigger.RESERVATION_REFUSED})

# ANL-10: the payload fields each formula reads. A contract test checks every
# path exists in the producer's registered Avro schema.
FIELDS_READ: dict[str, tuple[str, ...]] = {
    "identity.user.registered": ("role",),
    "catalog.product.published": ("product_id",),
    "catalog.product.unpublished": ("product_id",),
    "order.created": ("currency", "items[].merchant_id"),
    "order.confirmed": (
        "currency",
        "total",
        "items[].product_id",
        "items[].merchant_id",
        "items[].product_name",
        "items[].quantity",
        "items[].line_total",
    ),
    "order.failed": ("currency", "failure_reason", "merchant_ids[]"),
    "payment.failed": ("reason",),
    "inventory.stockout": ("variant_id", "product_id", "merchant_id", "trigger"),
    "assistant.interaction": (
        "kind",
        "answered",
        "finish_reason",
        "merchant_id",
        "input_tokens",
        "output_tokens",
        "cost_usd",
        "latency_ms",
    ),
    "shipment.status_changed": ("to_status",),  # the `shipments` backfill projection
}


@dataclass(frozen=True)
class RollupContext:
    reporting_currency: str = "USD"


@dataclass(frozen=True)
class Op:
    """One ``update_one`` against ``analytics_db``."""

    collection: str
    filter: dict[str, Any]
    update: dict[str, Any]
    upsert: bool = True


Projection = Callable[[Envelope, RollupContext], list[Op]]


# --- op builders --------------------------------------------------------------


def _stamp(update: dict[str, Any]) -> dict[str, Any]:
    # Server-side clock: keeps the functions pure and replay-stable.
    return {**update, "$currentDate": {"updated_at": True}}


def _field_key(raw: str) -> str:
    """A payload value used as a sub-document key (variant id, trigger, status)."""
    return raw.replace(".", "_").lstrip("$") or UNKNOWN_REASON


def counter(name: CounterName, by: int | Decimal, *, currency: str | None = None) -> Op:
    value: Any = dec128(by) if isinstance(by, Decimal) else Int64(by)
    update: dict[str, Any] = {"$inc": {"value": value}}
    if currency is not None:
        update["$set"] = {"currency": currency}
    return Op(Collection.COUNTERS, {"_id": str(name)}, _stamp(update))


def bucket(
    day: str,
    metric: str,
    merchant_id: str | None,
    inc: dict[str, Any],
    set_: dict[str, Any] | None = None,
) -> Op:
    update: dict[str, Any] = {"$inc": inc}
    if set_:
        update["$set"] = set_
    return Op(
        Collection.DAILY_METRICS, {"_id": bucket_id(day, metric, merchant_id)}, _stamp(update)
    )


def failure(day: str, source: FailureSource, reason: str | None) -> Op:
    return Op(
        Collection.FAILURE_STATS,
        {"_id": failure_id(day, str(source), reason or UNKNOWN_REASON)},
        _stamp({"$inc": {"count": Int64(1)}}),
    )


def _one(n: int = 1) -> Int64:
    return Int64(n)


# --- per-topic formulas -----------------------------------------------------------


def _user_registered(env: Envelope, ctx: RollupContext) -> list[Op]:
    role = env.payload.get("role")
    if role == UserRole.CUSTOMER:
        return [counter(CounterName.TOTAL_CUSTOMERS, 1)]
    if role == UserRole.MERCHANT:
        return [counter(CounterName.TOTAL_MERCHANTS, 1)]
    return []  # admin (or a future role): no counter


def _product_published(env: Envelope, ctx: RollupContext) -> list[Op]:
    return [counter(CounterName.TOTAL_PRODUCTS, 1)]


def _product_unpublished(env: Envelope, ctx: RollupContext) -> list[Op]:
    return [counter(CounterName.TOTAL_PRODUCTS, -1)]


def _merchants(items: Iterable[dict[str, Any]]) -> list[str]:
    """Distinct merchants of an order, in first-seen order (stable op lists)."""
    return list(dict.fromkeys(str(i["merchant_id"]) for i in items))


def _order_created(env: Envelope, ctx: RollupContext) -> list[Op]:
    p = env.payload
    day, metric, currency = (
        bucket_date(env.occurred_at),
        orders_metric(p["currency"]),
        p["currency"],
    )
    ops = [bucket(day, metric, None, {"orders_created": _one()}, {"currency": currency})]
    ops += [
        bucket(day, metric, m, {"orders_created": _one()}, {"currency": currency})
        for m in _merchants(p.get("items") or [])
    ]
    return ops


def _order_confirmed(env: Envelope, ctx: RollupContext) -> list[Op]:
    p = env.payload
    day, currency = bucket_date(env.occurred_at), p["currency"]
    metric = orders_metric(currency)
    total = to_decimal(p["total"])
    items = p.get("items") or []
    reporting = currency.upper() == ctx.reporting_currency.upper()

    ops = [counter(CounterName.ORDERS_TOTAL, 1)]
    if reporting:  # all-time revenue in the reporting currency only (ANL-4)
        ops.append(counter(CounterName.REVENUE_TOTAL, total, currency=ctx.reporting_currency))
    # Platform revenue is the order total, shipping included.
    ops.append(
        bucket(
            day,
            metric,
            None,
            {"orders_confirmed": _one(), "revenue": dec128(total)},
            {"currency": currency},
        )
    )
    # Merchant revenue is the sum of that merchant's line totals (ANL-D24).
    for m in _merchants(items):
        line_sum = sum(
            (to_decimal(i["line_total"]) for i in items if i["merchant_id"] == m), Decimal(0)
        )
        ops.append(
            bucket(
                day,
                metric,
                m,
                {"orders_confirmed": _one(), "revenue": dec128(line_sum)},
                {"currency": currency},
            )
        )
    # Popularity: one document per product; order_count once per product per order.
    by_product: dict[str, dict[str, Any]] = {}
    for item in items:
        agg = by_product.setdefault(
            str(item["product_id"]),
            {"merchant_id": item["merchant_id"], "name": None, "qty": 0, "rev": Decimal(0)},
        )
        agg["qty"] += int(item["quantity"])
        agg["rev"] += to_decimal(item["line_total"])
        agg["name"] = item.get("product_name") or agg["name"]
    for product_id, agg in by_product.items():
        inc: dict[str, Any] = {"quantity_sold": Int64(agg["qty"]), "order_count": _one()}
        set_: dict[str, Any] = {"merchant_id": agg["merchant_id"]}
        if agg["name"]:
            set_["name"] = agg["name"]  # snapshot from the order line (A14)
        if reporting:
            inc["revenue"] = dec128(agg["rev"])
            set_["currency"] = ctx.reporting_currency
        ops.append(
            Op(
                Collection.PRODUCT_STATS,
                {"_id": product_id},
                _stamp({"$inc": inc, "$set": set_, "$max": {"last_sold_at": env.occurred_at}}),
            )
        )
    return ops


def _order_failed(env: Envelope, ctx: RollupContext) -> list[Op]:
    p = env.payload
    day, metric = bucket_date(env.occurred_at), orders_metric(p["currency"])
    set_ = {"currency": p["currency"]}
    ops = [bucket(day, metric, None, {"orders_failed": _one()}, set_)]
    ops += [
        bucket(day, metric, m, {"orders_failed": _one()}, set_)
        for m in dict.fromkeys(str(m) for m in p.get("merchant_ids") or [])
    ]
    # A workflow failure: the OrderWorkflow ended in compensation.
    ops.append(failure(day, FailureSource.ORDER, p.get("failure_reason")))
    return ops


def _payment_failed(env: Envelope, ctx: RollupContext) -> list[Op]:
    return [failure(bucket_date(env.occurred_at), FailureSource.PAYMENT, env.payload.get("reason"))]


def _stockout(env: Envelope, ctx: RollupContext) -> list[Op]:
    p = env.payload
    day = bucket_date(env.occurred_at)
    trigger = str(p.get("trigger") or UNKNOWN_REASON)
    inc: dict[str, Any] = {f"triggers.{_field_key(trigger)}": _one()}
    set_: dict[str, Any] | None = None
    if trigger in COUNTING_TRIGGERS:
        variant = _field_key(str(p["variant_id"]))
        inc |= {"stockouts": _one(), f"variants.{variant}": _one()}
        set_ = {f"variant_products.{variant}": p["product_id"]}
    return [
        bucket(day, MetricKey.STOCKOUTS, None, inc, set_),
        bucket(day, MetricKey.STOCKOUTS, str(p["merchant_id"]), dict(inc), set_),
    ]


def _assistant(env: Envelope, ctx: RollupContext) -> list[Op]:
    """First version (ANL-D37): usage, asked / answered / refused, tokens, cost
    and a latency sum. Conversion and p95 latency are deferred."""
    p = env.payload
    day = bucket_date(env.occurred_at)
    chat = p.get("kind") == InteractionKind.CHAT
    refused = p.get("finish_reason") == FinishReason.REFUSED
    answered = chat and bool(p.get("answered")) and not refused

    ops = [counter(CounterName.ASSISTANT_INTERACTIONS, 1)]
    if chat:
        ops.append(counter(CounterName.ASSISTANT_QUESTIONS_ASKED, 1))
    if answered:
        ops.append(counter(CounterName.ASSISTANT_QUESTIONS_ANSWERED, 1))
    if refused:
        ops.append(counter(CounterName.ASSISTANT_REFUSALS, 1))

    inc: dict[str, Any] = {
        "assistant_messages": _one(),
        "assistant_questions_asked": _one(1 if chat else 0),
        "assistant_answered": _one(1 if answered else 0),
        "assistant_refusals": _one(1 if refused else 0),
        "assistant_tokens": Int64(
            int(p.get("input_tokens") or 0) + int(p.get("output_tokens") or 0)
        ),
        "assistant_cost": dec128(to_decimal(p.get("cost_usd") or "0")),
        "assistant_latency_ms_total": Int64(int(p.get("latency_ms") or 0)),
    }
    ops.append(bucket(day, MetricKey.ASSISTANT, None, inc))
    if p.get("kind") == InteractionKind.GENERATION and p.get("merchant_id"):
        ops.append(bucket(day, MetricKey.ASSISTANT, str(p["merchant_id"]), dict(inc)))
    return ops


LIVE: dict[str, Projection] = {
    "identity.user.registered": _user_registered,
    "catalog.product.published": _product_published,
    "catalog.product.unpublished": _product_unpublished,
    "order.created": _order_created,
    "order.confirmed": _order_confirmed,
    "order.failed": _order_failed,
    "payment.failed": _payment_failed,
    "inventory.stockout": _stockout,
    "assistant.interaction": _assistant,
}


def apply(envelope: Envelope, ctx: RollupContext) -> list[Op]:
    """The live consumer's projection. Topics without a formula (product
    updated, cancellations, payments succeeded, shipments, reservations) are
    kept in ``raw_events`` only and yield no operation."""
    formula = LIVE.get(envelope.event_type)
    return formula(envelope, ctx) if formula else []


# --- backfill-only projections (Q4, ANL-G9) ---------------------------------------


def apply_shipments(envelope: Envelope, ctx: RollupContext) -> list[Op]:
    """Shipment status transitions per day — the projection that proves the
    ``raw_events`` backfill path. Not in the live consumer (ANL-D40)."""
    if envelope.event_type != "shipment.status_changed":
        return []
    status = _field_key(str(envelope.payload.get("to_status") or UNKNOWN_REASON))
    return [
        bucket(
            bucket_date(envelope.occurred_at),
            MetricKey.SHIPMENTS,
            None,
            {"transitions": _one(), f"statuses.{status}": _one()},
        )
    ]


PROJECTIONS: dict[str, Projection] = {"shipments": apply_shipments}


# --- in-memory application ------------------------------------------------------------


def doc_key(collection: str, _id: Any) -> str:
    return f"{collection}|{json.dumps(_id, default=str)}"


@dataclass
class Folded:
    """Documents produced by applying operations to an empty store."""

    docs: dict[str, dict[str, Any]] = field(default_factory=dict)
    collections: dict[str, str] = field(default_factory=dict)

    def get(self, collection: str, _id: Any) -> dict[str, Any] | None:
        return self.docs.get(doc_key(collection, _id))

    def of(self, collection: str) -> list[dict[str, Any]]:
        return [d for k, d in self.docs.items() if self.collections[k] == collection]


def _number(value: Any) -> int | Decimal:
    if isinstance(value, int):
        return int(value)
    return to_decimal(value)


def _path_get(doc: dict[str, Any], path: str) -> Any:
    node: Any = doc
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def _path_set(doc: dict[str, Any], path: str, value: Any) -> None:
    *parents, last = path.split(".")
    node = doc
    for part in parents:
        node = node.setdefault(part, {})
    node[last] = value


def fold(ops: Iterable[Op], into: Folded | None = None) -> Folded:
    """Applies ``$inc``/``$set``/``$max`` upserts in memory. Numbers come out as
    ``int`` and ``Decimal``; ``updated_at`` is not modelled."""
    out = into or Folded()
    for op in ops:
        _id = op.filter["_id"]
        key = doc_key(op.collection, _id)
        doc = out.docs.setdefault(key, {"_id": _id})
        out.collections[key] = op.collection
        for path, value in op.update.get("$inc", {}).items():
            current = _path_get(doc, path)
            _path_set(doc, path, (_number(current) if current is not None else 0) + _number(value))
        for path, value in op.update.get("$set", {}).items():
            _path_set(doc, path, value)
        for path, value in op.update.get("$max", {}).items():
            current = _path_get(doc, path)
            if current is None or (isinstance(value, datetime) and value > current):
                _path_set(doc, path, value)
    return out
