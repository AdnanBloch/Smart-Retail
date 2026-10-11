"""Milestone 2 end to end through the gateway: publish → order → confirm, the
failure paths, oversell under concurrency (G6), and a worker outage (G5).

Needs the whole stack, migrated and seeded: ``make up migrate seed``.
"""

import asyncio
import shutil
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest

from tests.e2e.test_smoke import ADMIN_EMAIL, ADMIN_PASSWORD, BASE_URL, login

REPO = Path(__file__).resolve().parents[2]
PASSWORD = "e2e workflow password"
SETTLED = {"confirmed", "failed", "cancelled", "shipped", "delivered", "refunded"}


@pytest.fixture(scope="module")
def client():
    with httpx.Client(base_url=BASE_URL, timeout=15) as c:
        try:
            ready = c.get("/health/ready").json()
        except httpx.HTTPError:
            pytest.skip(f"gateway not reachable at {BASE_URL}; run `make up migrate seed`")
        if ready["informational"]["upstreams"].get("order") != "ok":
            pytest.skip("the workflow profile is not running")
        yield c


@pytest.fixture(scope="module")
def actors(client: httpx.Client) -> dict[str, dict[str, str]]:
    run = uuid.uuid4().hex[:8]
    out: dict[str, dict[str, str]] = {"admin": login(client, ADMIN_EMAIL, ADMIN_PASSWORD)}
    for role in ("merchant", "customer"):
        email = f"wf-{role}-{run}@smartretail.local"
        r = client.post(
            "/api/v1/auth/register",
            json={"email": email, "password": PASSWORD, "full_name": f"WF {role}", "role": role},
        )
        assert r.status_code == 201, r.text
        out[role] = login(client, email, PASSWORD)
    r = client.post(
        "/api/v1/categories",
        json={"name": f"WF {run}", "slug": f"wf-{run}"},
        headers=out["admin"],
    )
    assert r.status_code == 201, r.text
    out["category"] = {"id": r.json()["id"]}
    return out


def ok(r: httpx.Response, status: int = 200) -> Any:
    assert r.status_code == status, f"{r.request.method} {r.request.url.path}: {r.text}"
    return r.json() if r.content else None


def wait(fn, done, *, seconds: float = 60) -> Any:
    deadline = time.monotonic() + seconds
    while True:
        value = fn()
        if done(value):
            return value
        assert time.monotonic() < deadline, f"timed out; last value: {value}"
        time.sleep(0.3)


def make_product(
    client: httpx.Client, actors, *, media: bool = True, price: str = "25.00"
) -> dict[str, Any]:
    merchant = actors["merchant"]
    sku = f"WF-{uuid.uuid4().hex[:8]}"
    product = ok(
        client.post(
            "/api/v1/products",
            json={
                "sku": sku,
                "name": "Workflow Boot",
                "description": "Published by the e2e suite.",
                "category_id": actors["category"]["id"],
                "base_price": price,
            },
            headers=merchant,
        ),
        201,
    )
    version = ok(
        client.post(
            f"/api/v1/products/{product['id']}/variants",
            json={"sku": f"{sku}-M", "options": {"size": "M"}, "version": product["version"]},
            headers=merchant,
        ),
        201,
    )["version"]
    if media:
        version = ok(
            client.post(
                f"/api/v1/products/{product['id']}/media",
                json={"url": "https://cdn.example.com/boot.jpg", "version": version},
                headers=merchant,
            ),
            201,
        )["version"]
    return ok(client.get(f"/api/v1/products/{product['id']}", headers=merchant))


def publish(client: httpx.Client, actors, product) -> dict[str, Any]:
    merchant = actors["merchant"]
    accepted = ok(
        client.post(
            f"/api/v1/products/{product['id']}/status",
            json={"status": "published", "version": product["version"]},
            headers=merchant,
        ),
        202,
    )
    assert accepted["status"] == "publishing"
    return wait(
        lambda: ok(client.get(accepted["_links"]["publish_status"], headers=merchant)),
        lambda s: s["state"] in ("published", "failed"),
    )


def published_product(client, actors, *, stock: int) -> tuple[dict[str, Any], str]:
    product = make_product(client, actors)
    assert publish(client, actors, product)["state"] == "published"
    variant = product["variants"][0]["id"]
    # sync_downstream created the stock row; the merchant sets the quantity.
    assert ok(client.get(f"/api/v1/inventory/{variant}"))["on_hand"] == 0
    ok(
        client.put(
            f"/api/v1/inventory/{variant}", json={"on_hand": stock}, headers=actors["merchant"]
        )
    )
    return product, variant


def order_body(product, quantity: int = 1, *, method: str = "tok_ok", postcode="10001"):
    return {
        "items": [
            {
                "product_id": product["id"],
                "variant_id": product["variants"][0]["id"],
                "quantity": quantity,
            }
        ],
        "shipping_address": {
            "name": "E2E Customer",
            "line1": "1 Test Road",
            "city": "Springfield",
            "postal_code": postcode,
            "country": "US",
        },
        "payment_method": method,
    }


def settle(client: httpx.Client, actors, order_id: str, seconds: float = 60) -> dict[str, Any]:
    return wait(
        lambda: ok(client.get(f"/api/v1/orders/{order_id}", headers=actors["customer"])),
        lambda o: o["status"] in SETTLED,
        seconds=seconds,
    )


def test_publish_then_order_to_confirmation(client: httpx.Client, actors) -> None:
    product, variant = published_product(client, actors, stock=10)
    public = ok(client.get(f"/api/v1/products/{product['id']}"))
    assert public["status"] == "published" and public["published_at"]

    key = str(uuid.uuid4())
    headers = {**actors["customer"], "Idempotency-Key": key}
    first = client.post("/api/v1/orders", json=order_body(product, 2), headers=headers)
    assert first.status_code == 202, first.text
    replay = client.post("/api/v1/orders", json=order_body(product, 2), headers=headers)
    assert replay.status_code == 202 and replay.json() == first.json()
    assert replay.headers["idempotency-replayed"] == "true"

    order = settle(client, actors, first.json()["order_id"])
    assert order["status"] == "confirmed", order
    assert order["total"] == "50.00" and order["payment"]["status"] == "succeeded"
    assert order["shipment"]["tracking_number"]
    assert [t["to"] for t in order["timeline"]] == ["pending", "awaiting_payment", "confirmed"]
    assert ok(client.get(f"/api/v1/inventory/{variant}")) == {
        "variant_id": variant,
        "on_hand": 8,
        "reserved": 0,
        "available": 8,
        "in_stock": True,
    }

    merchant_view = ok(client.get("/api/v1/orders/merchant", headers=actors["merchant"]))
    mine = next(o for o in merchant_view["items"] if o["order_id"] == order["order_id"])
    assert mine["merchant_subtotal"] == "50.00"

    workflow = ok(
        client.get(f"/api/v1/orders/{order['order_id']}/workflow", headers=actors["admin"])
    )
    assert workflow["execution_status"] == "COMPLETED"

    # The carrier takes it from here.
    url = f"/api/v1/orders/{order['order_id']}/shipment/status"
    for status in ("shipped", "delivered"):
        ok(client.post(url, json={"status": status}, headers=actors["admin"]))
    assert (
        ok(client.get(f"/api/v1/orders/{order['order_id']}", headers=actors["customer"]))["status"]
        == "delivered"
    )


def test_declined_payment_restores_stock(client: httpx.Client, actors) -> None:
    """G3: a failed payment leaves available stock exactly as it began."""
    product, variant = published_product(client, actors, stock=5)
    r = client.post(
        "/api/v1/orders",
        json=order_body(product, 3, method="tok_decline"),
        headers={**actors["customer"], "Idempotency-Key": str(uuid.uuid4())},
    )
    order = settle(client, actors, r.json()["order_id"])
    assert (order["status"], order["failure_reason"]) == ("failed", "PAYMENT_DECLINED")
    assert ok(client.get(f"/api/v1/inventory/{variant}"))["available"] == 5


def test_undeliverable_address_refunds_and_reverses(client: httpx.Client, actors) -> None:
    product, variant = published_product(client, actors, stock=5)
    r = client.post(
        "/api/v1/orders",
        json=order_body(product, 2, postcode="00000"),
        headers={**actors["customer"], "Idempotency-Key": str(uuid.uuid4())},
    )
    order = settle(client, actors, r.json()["order_id"])
    assert (order["status"], order["failure_reason"]) == ("failed", "FULFILMENT_FAILED")
    assert order["payment"]["status"] == "refunded"
    assert ok(client.get(f"/api/v1/inventory/{variant}"))["available"] == 5


def test_incomplete_product_fails_to_publish(client: httpx.Client, actors) -> None:
    """G1: a failed publish leaves the product in draft with the reason recorded."""
    product = make_product(client, actors, media=False)
    status = publish(client, actors, product)
    assert (status["state"], status["failed_step"]) == ("failed", "validate_product")
    assert "media" in status["failure_reason"]
    detail = ok(client.get(f"/api/v1/products/{product['id']}", headers=actors["merchant"]))
    assert detail["status"] == "draft"
    assert client.get(f"/api/v1/products/{product['id']}").status_code == 404


def test_fifty_orders_for_ten_units_confirm_exactly_ten(client: httpx.Client, actors) -> None:
    """G6: oversell is impossible under concurrency."""
    product, variant = published_product(client, actors, stock=10)
    body = order_body(product, 1)

    async def burst() -> list[httpx.Response]:
        async with httpx.AsyncClient(base_url=BASE_URL, timeout=30) as ac:
            return await asyncio.gather(
                *(
                    ac.post(
                        "/api/v1/orders",
                        json=body,
                        headers={**actors["customer"], "Idempotency-Key": str(uuid.uuid4())},
                    )
                    for _ in range(50)
                )
            )

    accepted = asyncio.run(burst())
    assert [r.status_code for r in accepted] == [202] * 50
    orders = [settle(client, actors, r.json()["order_id"], seconds=120) for r in accepted]
    outcomes = [(o["status"], o["failure_reason"]) for o in orders]
    assert outcomes.count(("confirmed", None)) == 10, outcomes
    assert outcomes.count(("failed", "INSUFFICIENT_STOCK")) == 40, outcomes
    assert ok(client.get(f"/api/v1/inventory/{variant}")) == {
        "variant_id": variant,
        "on_hand": 0,
        "reserved": 0,
        "available": 0,
        "in_stock": False,
    }


def _compose(*args: str) -> None:
    subprocess.run(  # noqa: S603
        ["docker", "compose", "--profile", "core", "--profile", "workflow", *args],  # noqa: S607
        cwd=REPO,
        check=True,
        capture_output=True,
    )


@pytest.mark.skipif(shutil.which("docker") is None, reason="needs the docker CLI")
def test_orders_wait_out_a_worker_outage(client: httpx.Client, actors) -> None:
    """G5 at the stack level: with order-worker down, accepted orders queue on the
    task queue; when it returns they complete. (The mid-activity kill is the
    integration test test_worker_killed_mid_order_resumes.)"""
    product, _ = published_product(client, actors, stock=3)
    _compose("stop", "order-worker")
    try:
        r = client.post(
            "/api/v1/orders",
            json=order_body(product, 1),
            headers={**actors["customer"], "Idempotency-Key": str(uuid.uuid4())},
        )
        assert r.status_code == 202  # checkout does not depend on the worker
        time.sleep(2)
        assert (
            ok(client.get(f"/api/v1/orders/{r.json()['order_id']}", headers=actors["customer"]))[
                "status"
            ]
            == "pending"
        )
    finally:
        _compose("start", "order-worker")
    assert settle(client, actors, r.json()["order_id"], seconds=90)["status"] == "confirmed"
