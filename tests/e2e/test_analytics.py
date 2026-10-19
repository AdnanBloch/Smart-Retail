"""analytics end to end through the gateway (RFC 0008): every Part A metric
moves when the events that feed it happen, and merchants see only their own.

Needs the events profile: ``make up migrate seed``.
"""

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest

from tests.e2e.test_events import compose
from tests.e2e.test_workflows import ok, order_body, published_product, settle, wait


@pytest.fixture(scope="module", autouse=True)
def events_profile():
    if "analytics-consumer" not in compose("ps", "--services", "--status", "running"):
        pytest.skip("the analytics consumer is not running")


def window() -> dict[str, str]:
    today = datetime.now(UTC).date()
    return {"from": str(today - timedelta(days=1)), "to": str(today + timedelta(days=1))}


def revenue(client, headers) -> Decimal:
    body = ok(client.get("/api/v1/analytics/revenue", params=window(), headers=headers))
    return sum((Decimal(w["revenue"]) for w in body["window"]), Decimal(0))


def overview(client, actors) -> dict:
    return ok(client.get("/api/v1/analytics/overview", headers=actors["admin"]))


def test_a_confirmed_order_moves_revenue_orders_and_popularity(client: httpx.Client, actors):
    merchant = actors["merchant"]
    before_rev = revenue(client, merchant)
    before_total = overview(client, actors)["orders_total"]
    product, _ = published_product(client, actors, stock=5)
    r = client.post(
        "/api/v1/orders",
        json=order_body(product, 2),
        headers={**actors["customer"], "Idempotency-Key": str(uuid.uuid4())},
    )
    assert settle(client, actors, ok(r, 202)["order_id"])["status"] == "confirmed"

    wait(lambda: revenue(client, merchant), lambda v: v > before_rev, seconds=60)
    wait(lambda: overview(client, actors)["orders_total"], lambda n: n > before_total)
    popular = ok(
        client.get(
            "/api/v1/analytics/products/popular",
            params={"rank_by": "quantity", "limit": 100},
            headers=merchant,
        )
    )
    [mine] = [p for p in popular["items"] if p["product_id"] == product["id"]]
    assert mine["quantity_sold"] == 2


def test_a_declined_payment_shows_in_failures_by_reason(client: httpx.Client, actors) -> None:
    product, _ = published_product(client, actors, stock=1)
    r = client.post(
        "/api/v1/orders",
        json=order_body(product, 1, method="tok_decline"),
        headers={**actors["customer"], "Idempotency-Key": str(uuid.uuid4())},
    )
    assert settle(client, actors, ok(r, 202)["order_id"])["status"] == "failed"

    def declined() -> int:
        body = ok(
            client.get("/api/v1/analytics/failures", params=window(), headers=actors["admin"])
        )
        return sum(
            f["count"] for f in body["items"] if (f.get("reason") or "") == "PAYMENT_DECLINED"
        )

    wait(declined, lambda n: n >= 1, seconds=60)


def test_merchants_are_scoped_and_admin_routes_are_closed(client: httpx.Client, actors) -> None:
    merchant = actors["merchant"]
    assert client.get("/api/v1/analytics/overview", headers=merchant).status_code == 403
    assert client.get("/api/v1/analytics/failures", headers=merchant).status_code == 403
    assert client.get("/api/v1/analytics/revenue", headers=actors["customer"]).status_code == 403
    other = str(uuid.uuid4())
    r = client.get(
        "/api/v1/analytics/revenue", params={**window(), "merchant_id": other}, headers=merchant
    )
    assert r.status_code == 422  # a merchant cannot ask about another merchant
    body = ok(client.get("/api/v1/analytics/orders", params=window(), headers=merchant))
    assert body["scope"]["merchant_id"] is not None


def test_stockout_rate_counts_a_sell_out(client: httpx.Client, actors) -> None:
    product, _ = published_product(client, actors, stock=1)
    r = client.post(
        "/api/v1/orders",
        json=order_body(product, 1),
        headers={**actors["customer"], "Idempotency-Key": str(uuid.uuid4())},
    )
    assert settle(client, actors, ok(r, 202)["order_id"])["status"] == "confirmed"

    def stockouts() -> int:
        body = ok(
            client.get(
                "/api/v1/analytics/inventory/stockouts",
                params=window(),
                headers=actors["merchant"],
            )
        )
        return int(body["window"]["stockouts"])

    wait(stockouts, lambda n: n >= 1, seconds=60)
