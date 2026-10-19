"""``MetricsRepo``: the read side of every metric definition (RFC 0008 §3.1.1.2).

Windows are composed on read from daily buckets (C9); rankings are computed
on read. Every merchant-scopable read takes a ``Scope`` and matches on it in
the query. Money leaves as strings; ratios as 4-decimal strings.
"""

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from bson.decimal128 import Decimal128

from app.core.config import Settings
from app.core.db import Mongo, mongo_errors
from app.core.errors import InvalidWindow
from app.v1.metrics.enums import (
    Collection,
    CounterName,
    FailureGroupBy,
    Granularity,
    MetricKey,
    PopularRankBy,
)
from app.v1.metrics.models import collection
from app.v1.metrics.scoping import Scope, Window, today
from app.v1.metrics.utils import money, orders_metric, period_of, rate, to_decimal

ORDERS_REGEX = {"$regex": "^orders[.]"}


def _int(value: Any) -> int:
    return int(value or 0)


@dataclass
class MetricsRepo:
    mongo: Mongo
    settings: Settings

    # --- shared ------------------------------------------------------------------

    async def as_of(self) -> datetime | None:
        """``occurred_at`` of the newest archived (hence applied) event."""
        async with mongo_errors():
            doc = await collection(self.mongo, Collection.RAW_EVENTS).find_one(
                {}, {"occurred_at": 1}, sort=[("occurred_at", -1)]
            )
        return doc["occurred_at"] if doc else None

    async def _buckets(self, scope: Scope, window: Window, metric: Any) -> list[dict[str, Any]]:
        query = {
            "_id.merchant_id": scope.merchant_id,
            "_id.metric": metric,
            "_id.date": {"$gte": window.start.isoformat(), "$lt": window.end.isoformat()},
        }
        async with mongo_errors():
            cursor = collection(self.mongo, Collection.DAILY_METRICS).find(query)
            return [doc async for doc in cursor]

    # --- overview ------------------------------------------------------------------

    async def overview(self) -> dict[str, Any]:
        async with mongo_errors():
            docs = {d["_id"]: d async for d in collection(self.mongo, Collection.COUNTERS).find({})}
        out: dict[str, Any] = {
            name.value: _int(docs.get(name.value, {}).get("value"))
            for name in CounterName
            if name is not CounterName.REVENUE_TOTAL
        }
        revenue = docs.get(CounterName.REVENUE_TOTAL.value, {})
        out[CounterName.REVENUE_TOTAL.value] = {
            "amount": money(to_decimal(revenue.get("value"))),
            "currency": revenue.get("currency") or self.settings.analytics_reporting_currency,
        }
        return out

    # --- orders over time ----------------------------------------------------------------

    async def orders(
        self, scope: Scope, window: Window, granularity: Granularity
    ) -> list[dict[str, Any]]:
        totals: dict[str, dict[str, int]] = defaultdict(
            lambda: {"orders_created": 0, "orders_confirmed": 0, "orders_failed": 0}
        )
        for doc in await self._buckets(scope, window, ORDERS_REGEX):
            row = totals[period_of(doc["_id"]["date"], granularity)]
            for key in row:
                row[key] += _int(doc.get(key))
        return [
            {
                "period": period,
                **row,
                "confirmation_rate": rate(row["orders_confirmed"], row["orders_created"]),
            }
            for period, row in sorted(totals.items())
        ]

    # --- revenue and AOV -------------------------------------------------------------------

    async def revenue(
        self, scope: Scope, window: Window, granularity: Granularity, currency: str | None
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        metric = orders_metric(currency) if currency else ORDERS_REGEX
        series: dict[tuple[str, str], list[Any]] = defaultdict(lambda: [Decimal(0), 0])
        window_totals: dict[str, list[Any]] = defaultdict(lambda: [Decimal(0), 0])
        for doc in await self._buckets(scope, window, metric):
            cur = doc.get("currency") or doc["_id"]["metric"].removeprefix(MetricKey.ORDERS_PREFIX)
            revenue, confirmed = to_decimal(doc.get("revenue")), _int(doc.get("orders_confirmed"))
            for acc in (
                series[(period_of(doc["_id"]["date"], granularity), cur)],
                window_totals[cur],
            ):
                acc[0] += revenue
                acc[1] += confirmed

        def row(cur: str, revenue: Decimal, confirmed: int) -> dict[str, Any]:
            # AOV over a window is Σ revenue ÷ Σ confirmed, never a mean of daily means.
            aov = money(revenue / confirmed) if confirmed else None
            return {
                "currency": cur,
                "revenue": money(revenue),
                "orders_confirmed": confirmed,
                "average_order_value": aov,
            }

        return (
            [{"period": p, **row(c, *v)} for (p, c), v in sorted(series.items())],
            [row(c, *v) for c, v in sorted(window_totals.items())],
        )

    # --- popularity ----------------------------------------------------------------------------

    def raw_window_floor(self) -> date:
        return today() - timedelta(days=self.settings.analytics_raw_window_max_days - 1)

    async def popular(
        self, scope: Scope, rank_by: PopularRankBy, limit: int, window: Window | None
    ) -> list[dict[str, Any]]:
        if window is None:
            return await self._popular_all_time(scope, rank_by, limit)
        if window.start < self.raw_window_floor():
            raise InvalidWindow(
                "a popularity window must start within the last "
                f"{self.settings.analytics_raw_window_max_days} days (raw_events retention); "
                "omit from/to for the all-time ranking"
            )
        return await self._popular_window(scope, rank_by, limit, window)

    async def _popular_all_time(
        self, scope: Scope, rank_by: PopularRankBy, limit: int
    ) -> list[dict[str, Any]]:
        key = "quantity_sold" if rank_by is PopularRankBy.QUANTITY else "revenue"
        query = {} if scope.is_platform else {"merchant_id": scope.merchant_id}
        async with mongo_errors():
            cursor = (
                collection(self.mongo, Collection.PRODUCT_STATS)
                .find(query)
                .sort([(key, -1), ("_id", 1)])
                .limit(limit)
            )
            docs = [d async for d in cursor]
        return [self._product(d) for d in docs]

    async def _popular_window(
        self, scope: Scope, rank_by: PopularRankBy, limit: int, window: Window
    ) -> list[dict[str, Any]]:
        reporting = self.settings.analytics_reporting_currency
        key = "quantity_sold" if rank_by is PopularRankBy.QUANTITY else "revenue"
        pipeline: list[dict[str, Any]] = [
            {
                "$match": {
                    "event_type": "order.confirmed",
                    "occurred_at": {"$gte": window.start_at, "$lt": window.end_at},
                }
            },
            {"$unwind": "$payload.items"},
        ]
        if not scope.is_platform:
            pipeline.append({"$match": {"payload.items.merchant_id": scope.merchant_id}})
        pipeline += [
            {
                "$group": {
                    "_id": "$payload.items.product_id",
                    "merchant_id": {"$max": "$payload.items.merchant_id"},
                    "name": {"$max": "$payload.items.product_name"},
                    "quantity_sold": {"$sum": "$payload.items.quantity"},
                    "revenue": {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$payload.currency", reporting]},
                                {"$toDecimal": "$payload.items.line_total"},
                                Decimal128("0"),
                            ]
                        }
                    },
                    "orders": {"$addToSet": "$payload.order_id"},
                    "last_sold_at": {"$max": "$occurred_at"},
                }
            },
            {"$addFields": {"order_count": {"$size": "$orders"}, "currency": reporting}},
            {"$sort": {key: -1, "_id": 1}},
            {"$limit": limit},
        ]
        async with mongo_errors():
            cursor = await collection(self.mongo, Collection.RAW_EVENTS).aggregate(pipeline)
            docs = [d async for d in cursor]
        return [self._product(d) for d in docs]

    def _product(self, doc: dict[str, Any]) -> dict[str, Any]:
        return {
            "product_id": doc["_id"],
            "name": doc.get("name"),
            "merchant_id": doc.get("merchant_id"),
            "quantity_sold": _int(doc.get("quantity_sold")),
            "revenue": {
                "amount": money(to_decimal(doc.get("revenue"))),
                "currency": doc.get("currency") or self.settings.analytics_reporting_currency,
            },
            "order_count": _int(doc.get("order_count")),
            "last_sold_at": doc.get("last_sold_at"),
        }

    # --- stockout rate ---------------------------------------------------------------------------

    async def stockouts(
        self, scope: Scope, window: Window, granularity: Granularity, limit: int
    ) -> dict[str, Any]:
        per: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        variants: dict[str, int] = defaultdict(int)
        products: dict[str, str] = {}
        for doc in await self._buckets(scope, window, MetricKey.STOCKOUTS.value):
            per[period_of(doc["_id"]["date"], granularity)][0] += _int(doc.get("stockouts"))
            for variant, n in (doc.get("variants") or {}).items():
                variants[variant] += _int(n)
            products.update(doc.get("variant_products") or {})
        # Denominator: order attempts, i.e. orders_created incl. later failures (Q10).
        for doc in await self._buckets(scope, window, ORDERS_REGEX):
            per[period_of(doc["_id"]["date"], granularity)][1] += _int(doc.get("orders_created"))
        stockouts = sum(v[0] for v in per.values())
        attempts = sum(v[1] for v in per.values())
        top = sorted(variants.items(), key=lambda kv: (-kv[1], kv[0]))[:limit]
        return {
            "series": [
                {"period": p, "stockouts": s, "order_attempts": a, "stockout_rate": rate(s, a)}
                for p, (s, a) in sorted(per.items())
            ],
            "window": {
                "stockouts": stockouts,
                "order_attempts": attempts,
                "stockout_rate": rate(stockouts, attempts),
            },
            "top_variants": [
                {"variant_id": v, "product_id": products.get(v), "stockouts": n} for v, n in top
            ],
        }

    # --- failures ---------------------------------------------------------------

    async def failures(
        self, window: Window, group_by: FailureGroupBy | None
    ) -> list[dict[str, Any]]:
        query = {"_id.date": {"$gte": window.start.isoformat(), "$lt": window.end.isoformat()}}
        counts: dict[tuple[str | None, str | None], int] = defaultdict(int)
        async with mongo_errors():
            async for doc in collection(self.mongo, Collection.FAILURE_STATS).find(query):
                source = None if group_by is FailureGroupBy.REASON else doc["_id"]["source"]
                reason = None if group_by is FailureGroupBy.SOURCE else doc["_id"]["reason"]
                counts[(source, reason)] += _int(doc.get("count"))
        items = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0][0] or "", kv[0][1] or ""))
        out: list[dict[str, Any]] = []
        for (source, reason), n in items:
            item: dict[str, Any] = {"count": n}
            if group_by is not FailureGroupBy.REASON:
                item["source"] = source
            if group_by is not FailureGroupBy.SOURCE:
                item["reason"] = reason
            out.append(item)
        return out
