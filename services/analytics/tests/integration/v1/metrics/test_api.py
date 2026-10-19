"""``/api/v1/analytics/*``: one test per Part A metric with the exact numbers of
RFC 0008 §7, merchant isolation (ANL-G7), the role matrix and 503 on a Mongo
outage. Events go in through the real consumer."""

from datetime import UTC, datetime, timedelta

import pytest

from app.v1.metrics.consumers.dlq_observer import DlqObserver
from tests import factories as f
from tests.integration.conftest import Msg, Source, deliver

BASE = "/api/v1/analytics"


def today() -> str:
    return datetime.now(UTC).date().isoformat()


async def get(client, path: str, headers, **params):
    return await client.get(f"{BASE}{path}", headers=headers, params=params)


async def test_overview_counts_customers_merchants_products(client, callers, consumer) -> None:
    await deliver(
        consumer,
        *[f.envelope("identity.user.registered", f.user_registered("customer")) for _ in range(3)],
        *[f.envelope("identity.user.registered", f.user_registered("merchant")) for _ in range(2)],
        f.envelope("identity.user.registered", f.user_registered("admin")),
        f.envelope("catalog.product.published", f.product_published()),
        f.envelope("catalog.product.published", f.product_published()),
        f.envelope("catalog.product.unpublished", f.product_unpublished()),
        f.envelope("order.confirmed", f.order_confirmed([f.item(f.uid(), f.uid(), 1, "412.50")])),
        f.envelope("assistant.interaction", f.assistant()),
    )
    r = await get(client, "/overview", callers.admin)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total_customers"] == 3
    assert body["total_merchants"] == 2
    assert body["total_products"] == 1
    assert body["orders_total"] == 1
    assert body["revenue_total"] == {"amount": "412.50", "currency": "USD"}
    assert body["assistant_interactions"] == 1
    assert body["assistant_questions_answered"] == 1
    assert body["as_of"] is not None


async def test_an_empty_store_answers_zeroes(client, callers) -> None:
    body = (await get(client, "/overview", callers.admin)).json()
    assert body["as_of"] is None and body["total_customers"] == 0
    assert body["revenue_total"] == {"amount": "0.00", "currency": "USD"}


async def test_orders_over_time_and_confirmation_rate(client, callers, consumer) -> None:
    items = [f.item(f.uid(), f.uid(), 1, "10")]
    await deliver(
        consumer,
        *[f.envelope("order.created", f.order_created(items)) for _ in range(3)],
        *[f.envelope("order.confirmed", f.order_confirmed(items)) for _ in range(2)],
        f.envelope("order.failed", f.order_failed([items[0]["merchant_id"]])),
    )
    r = await get(client, "/orders", callers.admin)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["scope"] == {"merchant_id": None} and body["granularity"] == "day"
    assert body["series"] == [
        {
            "period": today(),
            "orders_created": 3,
            "orders_confirmed": 2,
            "orders_failed": 1,
            "confirmation_rate": "0.6667",
        }
    ]


async def test_daily_buckets_compose_into_weeks(client, callers, consumer) -> None:
    monday = datetime(2026, 10, 12, 10, tzinfo=UTC)
    items = [f.item(f.uid(), f.uid(), 1, "10")]
    await deliver(
        consumer,
        f.envelope("order.created", f.order_created(items), occurred_at=monday),
        f.envelope("order.created", f.order_created(items), occurred_at=monday + timedelta(days=6)),
        f.envelope("order.created", f.order_created(items), occurred_at=monday + timedelta(days=7)),
    )
    params = {"from": "2026-10-01", "to": "2026-11-01", "granularity": "week"}
    body = (await get(client, "/orders", callers.admin, **params)).json()
    assert [(p["period"], p["orders_created"]) for p in body["series"]] == [
        ("2026-W42", 2),
        ("2026-W43", 1),
    ]
    assert body["series"][0]["confirmation_rate"] == "0.0000"
    month = (
        await get(client, "/orders", callers.admin, **{**params, "granularity": "month"})
    ).json()
    assert [(p["period"], p["orders_created"]) for p in month["series"]] == [("2026-10", 3)]


async def test_revenue_and_average_order_value(client, callers, consumer) -> None:
    m = f.uid()
    await deliver(
        consumer,
        f.envelope("order.confirmed", f.order_confirmed([f.item(f.uid(), m, 1, "100")])),
        f.envelope("order.confirmed", f.order_confirmed([f.item(f.uid(), m, 2, "100")])),
        f.envelope(
            "order.confirmed", f.order_confirmed([f.item(f.uid(), m, 1, "7")], currency="EUR")
        ),
    )
    body = (await get(client, "/revenue", callers.admin)).json()
    assert body["basis"] == "order_total"
    usd = next(w for w in body["window"] if w["currency"] == "USD")
    assert usd == {
        "currency": "USD",
        "revenue": "300.00",
        "orders_confirmed": 2,
        "average_order_value": "150.00",
    }
    eur = next(w for w in body["window"] if w["currency"] == "EUR")
    assert eur["revenue"] == "7.00"  # never mixed with USD (ANL-4)
    assert {(p["period"], p["currency"]) for p in body["series"]} == {
        (today(), "USD"),
        (today(), "EUR"),
    }
    only_eur = (await get(client, "/revenue", callers.admin, currency="EUR")).json()
    assert [w["currency"] for w in only_eur["window"]] == ["EUR"]
    bad = await get(client, "/revenue", callers.admin, currency="usd")
    assert bad.status_code == 422


async def test_popular_products_by_quantity_and_revenue(client, callers, consumer) -> None:
    m = f.uid()
    a, b = f.uid(), f.uid()
    await deliver(
        consumer,
        f.envelope("order.confirmed", f.order_confirmed([f.item(a, m, 5, "10", name="A")])),
        f.envelope("order.confirmed", f.order_confirmed([f.item(b, m, 3, "40", name="B")])),
    )
    by_qty = (await get(client, "/products/popular", callers.admin)).json()
    assert by_qty["window"] is None and by_qty["rank_by"] == "quantity"
    assert [(i["name"], i["quantity_sold"]) for i in by_qty["items"]] == [("A", 5), ("B", 3)]
    assert by_qty["items"][0]["revenue"] == {"amount": "50.00", "currency": "USD"}
    assert by_qty["items"][0]["order_count"] == 1 and by_qty["items"][0]["last_sold_at"]
    by_rev = (await get(client, "/products/popular", callers.admin, rank_by="revenue")).json()
    assert [i["name"] for i in by_rev["items"]] == ["B", "A"]
    one = (await get(client, "/products/popular", callers.admin, limit=1)).json()
    assert len(one["items"]) == 1

    # A recent window is aggregated from raw_events with the same answer.
    start = (datetime.now(UTC).date() - timedelta(days=2)).isoformat()
    windowed = (
        await get(client, "/products/popular", callers.admin, rank_by="revenue", **{"from": start})
    ).json()
    assert windowed["window"]["start"] == start
    assert [(i["name"], i["quantity_sold"], i["revenue"]["amount"]) for i in windowed["items"]] == [
        ("B", 3, "120.00"),
        ("A", 5, "50.00"),
    ]
    assert windowed["items"][0]["order_count"] == 1


async def test_a_popularity_window_beyond_raw_events_is_422(client, callers) -> None:
    start = (datetime.now(UTC).date() - timedelta(days=10)).isoformat()
    r = await get(client, "/products/popular", callers.admin, **{"from": start})
    assert r.status_code == 422
    assert "raw_events retention" in r.json()["detail"]


async def test_stockout_rate_and_offending_variants(client, callers, consumer) -> None:
    m, p = f.uid(), f.uid()
    v1, v2 = f.uid(), f.uid()
    items = [f.item(p, m, 1, "1")]
    await deliver(
        consumer,
        *[f.envelope("order.created", f.order_created(items)) for _ in range(40)],
        f.envelope("inventory.stockout", f.stockout(v1, p, m, "depleted")),
        f.envelope("inventory.stockout", f.stockout(v2, p, m, "reservation_refused")),
        f.envelope("inventory.stockout", f.stockout(v1, p, m, "low_stock")),  # a warning only
    )
    body = (await get(client, "/inventory/stockouts", callers.admin)).json()
    assert body["window"] == {"stockouts": 2, "order_attempts": 40, "stockout_rate": "0.0500"}
    assert body["series"] == [
        {"period": today(), "stockouts": 2, "order_attempts": 40, "stockout_rate": "0.0500"}
    ]
    assert sorted(
        (v["variant_id"], v["product_id"], v["stockouts"]) for v in body["top_variants"]
    ) == sorted([(v1, p, 1), (v2, p, 1)])
    mine = (await get(client, "/inventory/stockouts", callers.admin, merchant_id=m, limit=1)).json()
    assert mine["window"]["stockout_rate"] == "0.0500" and len(mine["top_variants"]) == 1


async def test_failures_grouped_by_source_and_reason(client, callers, consumer, mongo) -> None:
    m = f.uid()
    await deliver(
        consumer,
        f.envelope("order.failed", f.order_failed([m], "payment_declined")),
        f.envelope("order.failed", f.order_failed([m], "payment_declined")),
        f.envelope("order.failed", f.order_failed([m], "insufficient_stock")),
        f.envelope("payment.failed", f.payment_failed(None)),
    )
    dlq = Msg("smartretail.order.confirmed.dlq", b"x", [("dlq.error.class", b"SchemaError")])
    await DlqObserver(mongo).process(dlq, Source())
    body = (await get(client, "/failures", callers.admin)).json()
    assert body["items"] == [
        {"source": "order", "reason": "payment_declined", "count": 2},
        {"source": "dlq", "reason": "smartretail.order.confirmed:SchemaError", "count": 1},
        {"source": "order", "reason": "insufficient_stock", "count": 1},
        {"source": "payment", "reason": "unknown", "count": 1},
    ]
    by_source = (await get(client, "/failures", callers.admin, group_by="source")).json()
    assert [(i["source"], i["count"]) for i in by_source["items"]] == [
        ("order", 3),
        ("dlq", 1),
        ("payment", 1),
    ]
    assert all(i["reason"] is None for i in by_source["items"])
    by_reason = (await get(client, "/failures", callers.admin, group_by="reason")).json()
    assert by_reason["items"][0] == {"source": None, "reason": "payment_declined", "count": 2}


async def test_a_merchant_sees_only_its_own_figures(client, callers, consumer) -> None:
    """ANL-G7: merchant A's reads exclude merchant B's data entirely."""
    a, b = callers.merchant_a_id, callers.merchant_b_id
    pa, pb = f.uid(), f.uid()
    mixed = [f.item(pa, a, 1, "10"), f.item(pb, b, 2, "50")]
    await deliver(
        consumer,
        f.envelope("order.created", f.order_created(mixed)),
        f.envelope("order.confirmed", f.order_confirmed(mixed, shipping="5")),
        f.envelope("order.created", f.order_created([f.item(pb, b, 1, "50")])),
        f.envelope("inventory.stockout", f.stockout(f.uid(), pb, b)),
    )
    rev = (await get(client, "/revenue", callers.merchant_a)).json()
    assert rev["scope"] == {"merchant_id": a} and rev["basis"] == "merchant_line_total"
    assert rev["window"] == [
        {
            "currency": "USD",
            "revenue": "10.00",
            "orders_confirmed": 1,
            "average_order_value": "10.00",
        }
    ]
    orders = (await get(client, "/orders", callers.merchant_a)).json()
    assert orders["series"][0]["orders_created"] == 1  # B's second order is invisible
    popular = (await get(client, "/products/popular", callers.merchant_a)).json()
    assert [i["product_id"] for i in popular["items"]] == [pa]
    start = (datetime.now(UTC).date() - timedelta(days=1)).isoformat()
    windowed = (
        await get(client, "/products/popular", callers.merchant_a, **{"from": start})
    ).json()
    assert [i["product_id"] for i in windowed["items"]] == [pa]
    stock = (await get(client, "/inventory/stockouts", callers.merchant_a)).json()
    assert stock["window"]["stockouts"] == 0 and stock["top_variants"] == []

    # The platform total for the admin includes shipping; B's scope is B's lines.
    platform = (await get(client, "/revenue", callers.admin)).json()
    assert platform["window"][0]["revenue"] == "115.00"
    as_b = (await get(client, "/revenue", callers.admin, merchant_id=b)).json()
    assert as_b["window"][0]["revenue"] == "100.00"
    # Asking for its own id explicitly is fine.
    own = await get(client, "/revenue", callers.merchant_a, merchant_id=a)
    assert own.status_code == 200


@pytest.mark.parametrize(
    "path", ["/orders", "/revenue", "/products/popular", "/inventory/stockouts"]
)
async def test_a_merchant_naming_another_merchant_gets_422(client, callers, path) -> None:
    r = await get(client, path, callers.merchant_a, merchant_id=callers.merchant_b_id)
    assert r.status_code == 422
    assert r.json()["violations"][0]["field"] == "merchant_id"


@pytest.mark.parametrize(
    ("path", "merchant_ok"),
    [
        ("/overview", False),
        ("/failures", False),
        ("/orders", True),
        ("/revenue", True),
        ("/products/popular", True),
        ("/inventory/stockouts", True),
    ],
)
async def test_role_matrix(client, callers, path, merchant_ok) -> None:
    assert (await get(client, path, callers.customer)).status_code == 403
    assert (await get(client, path, callers.merchant_a)).status_code == (
        200 if merchant_ok else 403
    )
    assert (await get(client, path, callers.admin)).status_code == 200
    assert (await client.get(f"{BASE}{path}")).status_code == 401


async def test_invalid_windows_are_422(client, callers) -> None:
    r = await get(client, "/orders", callers.admin, **{"from": "2026-10-10", "to": "2026-10-01"})
    assert r.status_code == 422
    r = await get(client, "/orders", callers.admin, **{"from": "2024-01-01", "to": "2026-10-01"})
    assert r.status_code == 422
    r = await get(client, "/orders", callers.admin, granularity="hour")
    assert r.status_code == 422


async def test_mongo_down_is_503(client, callers, app, dead_mongo) -> None:
    app.state.mongo = dead_mongo
    for path in ("/overview", "/orders", "/failures", "/products/popular"):
        r = await get(client, path, callers.admin)
        assert r.status_code == 503, path
        assert r.json()["type"].endswith("analytics-store-unavailable")
        assert r.headers["Retry-After"] == "5"


async def test_health_and_readiness(client, app, mongo, dead_mongo) -> None:
    assert (await client.get("/health")).json() == {"status": "ok"}
    ready = await client.get("/health/ready")
    assert ready.status_code == 200 and ready.json()["checks"] == {"mongo": "ok"}


async def test_readiness_fails_when_mongo_is_down(settings, keypair, dead_mongo) -> None:
    from httpx import ASGITransport, AsyncClient

    from main import create_application
    from smartretail_common.auth import JwtVerifier

    verifier = JwtVerifier(issuer="i", audience="a", static_keys={None: keypair.public_key})
    application = create_application(settings, verifier=verifier, mongo=dead_mongo)
    async with AsyncClient(transport=ASGITransport(app=application), base_url="http://a") as c:
        r = await c.get("/health/ready")
    assert r.status_code == 503 and r.json()["checks"] == {"mongo": "failed"}


async def test_lifespan_prepares_the_store(settings, keypair, mongo) -> None:
    from main import create_application
    from smartretail_common.auth import JwtVerifier

    verifier = JwtVerifier(issuer="i", audience="a", static_keys={None: keypair.public_key})
    application = create_application(settings, verifier=verifier)
    async with application.router.lifespan_context(application):
        names = await application.state.mongo.db.raw_events.index_information()
        assert "occurred_at_ttl" in names


async def test_lifespan_refuses_without_a_key(settings, mongo) -> None:
    from main import create_application
    from smartretail_common.auth import JwtVerifier

    verifier = JwtVerifier(issuer="i", audience="a", jwks_url="http://127.0.0.1:1/jwks")
    application = create_application(settings, verifier=verifier, mongo=mongo)
    with pytest.raises(RuntimeError, match="no JWT verification key"):
        async with application.router.lifespan_context(application):
            pass
