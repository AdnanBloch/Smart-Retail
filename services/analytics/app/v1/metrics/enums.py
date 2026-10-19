"""analytics vocabularies (RFC 0008 §3.1.1.2). Values are written into documents
and validated as query parameters."""

from enum import StrEnum


class Collection(StrEnum):
    COUNTERS = "counters"
    DAILY_METRICS = "daily_metrics"
    PRODUCT_STATS = "product_stats"
    FAILURE_STATS = "failure_stats"
    RAW_EVENTS = "raw_events"
    PROCESSED_EVENTS = "processed_events"


class CounterName(StrEnum):
    TOTAL_CUSTOMERS = "total_customers"
    TOTAL_MERCHANTS = "total_merchants"
    TOTAL_PRODUCTS = "total_products"
    ORDERS_TOTAL = "orders_total"
    REVENUE_TOTAL = "revenue_total"
    ASSISTANT_INTERACTIONS = "assistant_interactions"
    ASSISTANT_QUESTIONS_ASKED = "assistant_questions_asked"
    ASSISTANT_QUESTIONS_ANSWERED = "assistant_questions_answered"
    ASSISTANT_REFUSALS = "assistant_refusals"


class MetricKey(StrEnum):
    """``daily_metrics._id.metric``. Commerce documents are ``orders.<ISO4217>``
    (ANL-4), built by ``utils.orders_metric``."""

    ORDERS_PREFIX = "orders."
    STOCKOUTS = "stockouts"
    ASSISTANT = "assistant"
    SHIPMENTS = "shipments"  # backfill-only projection (ANL-D40)


class Granularity(StrEnum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"


class PopularRankBy(StrEnum):
    QUANTITY = "quantity"
    REVENUE = "revenue"


class FailureSource(StrEnum):
    ORDER = "order"
    PAYMENT = "payment"
    DLQ = "dlq"
    WORKFLOW = "workflow"  # reserved: no event exists yet (Q-ANL-5)


class FailureGroupBy(StrEnum):
    REASON = "reason"
    SOURCE = "source"


class StockoutTrigger(StrEnum):
    """``inventory.stockout.trigger``. Only the first two are a customer being
    refused stock; ``low_stock`` is a warning (ANL-D32)."""

    DEPLETED = "depleted"
    RESERVATION_REFUSED = "reservation_refused"
    LOW_STOCK = "low_stock"


class InteractionKind(StrEnum):
    CHAT = "chat"
    GENERATION = "generation"


class FinishReason(StrEnum):
    STOP = "stop"
    REFUSED = "refused"


class UserRole(StrEnum):
    CUSTOMER = "customer"
    MERCHANT = "merchant"
    ADMIN = "admin"


UNKNOWN_REASON = "unknown"
