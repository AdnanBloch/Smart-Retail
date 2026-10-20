"""Mentor review 2026-10-20 end to end: bursts, races and scale-out through the
real stack.

* one customer flooding checkout is throttled at the edge (GW-7), others are not;
* a cancel racing the saga ends in exactly one consistent state;
* after concurrent traffic the inventory ledger reconciles (INV-D23, INV-D32);
* two order-worker replicas still charge every order exactly once (ORD-D36).

Needs the whole stack with the events profile: ``make up migrate seed``.
"""

import asyncio
import os
import shutil
import uuid

import httpx
import pytest

from tests.e2e.test_events import compose, sql
from tests.e2e.test_smoke import BASE_URL
from tests.e2e.test_workflows import (
    new_customer,
    ok,
    order_body,
    published_product,
    settle,
    wait,
)

pytestmark = pytest.mark.skipif(shutil.which("docker") is None, reason="needs the docker CLI")

ORDER_LIMIT = int(os.environ.get("RL_ORDER_CREATE_PER_USER_PER_MIN", "30"))


@pytest.fixture(scope="module", autouse=True)
def events_profile():
    if "inventory-celery" not in compose("ps", "--services", "--status", "running"):
        pytest.skip("the events profile is not running")


def place_many(body: dict, buyers: list[dict[str, str]]) -> list[httpx.Response]:
    async def burst() -> list[httpx.Response]:
        async with httpx.AsyncClient(base_url=BASE_URL, timeout=30) as ac:
            return await asyncio.gather(
                *(
                    ac.post(
                        "/api/v1/orders",
                        json=body,
                        headers={**buyer, "Idempotency-Key": str(uuid.uuid4())},
                    )
                    for buyer in buyers
                )
            )

    return asyncio.run(burst())


def test_one_customer_flooding_checkout_is_throttled(client: httpx.Client, actors) -> None:
    # Out of stock, so every accepted order fails fast and cheaply.
    product, _ = published_product(client, actors, stock=0)
    flooder, bystander = new_customer(client), new_customer(client)
    results = place_many(order_body(product, 1), [flooder] * (ORDER_LIMIT + 5))
    statuses = sorted(r.status_code for r in results)
    assert statuses == [202] * ORDER_LIMIT + [429] * 5, statuses
    limited = next(r for r in results if r.status_code == 429)
    assert limited.json()["scope"] == "order_create"
    assert int(limited.headers["retry-after"]) > 0
    # Another customer is unaffected by the flooder's window.
    [other] = place_many(order_body(product, 1), [bystander])
    assert other.status_code == 202
    assert int(other.headers["ratelimit-remaining"]) == ORDER_LIMIT - 1


def test_cancel_racing_the_saga_ends_consistent(client: httpx.Client, actors) -> None:
    product, variant = published_product(client, actors, stock=5)
    buyer = new_customer(client)
    for _ in range(5):
        r = client.post(
            "/api/v1/orders",
            json=order_body(product, 1),
            headers={**buyer, "Idempotency-Key": str(uuid.uuid4())},
        )
        order_id = ok(r, 202)["order_id"]
        cancel = client.post(
            f"/api/v1/orders/{order_id}/cancel", json={"reason": "race"}, headers=buyer
        )
        assert cancel.status_code in (202, 409), cancel.text
        final = settle(client, {"customer": buyer}, order_id, seconds=90)["status"]
        if cancel.status_code == 409:  # refused: already past the pivot
            assert final == "confirmed", final
        else:
            # `202 requested`, not `cancelled`: a signal that lands after the
            # pivot is ignored and the order confirms (ORD-Q4). Either end is
            # whole; nothing is half-cancelled.
            assert final in ("cancelled", "confirmed"), final
    stock = ok(client.get(f"/api/v1/inventory/{variant}"))
    assert stock["reserved"] == 0
    confirmed = 5 - stock["on_hand"]
    assert 0 <= confirmed <= 5


def test_the_ledger_reconciles_after_concurrent_traffic(client: httpx.Client, actors) -> None:
    product, variant = published_product(client, actors, stock=4)
    buyers = [new_customer(client) for _ in range(3)]
    results = place_many(order_body(product, 1), buyers * 3)
    assert [r.status_code for r in results] == [202] * 9
    for buyer, r in zip(buyers * 3, results, strict=True):
        settle(client, {"customer": buyer}, r.json()["order_id"], seconds=120)
    assert ok(client.get(f"/api/v1/inventory/{variant}"))["on_hand"] == 0
    drift = compose(
        "exec",
        "-T",
        "inventory-celery",
        "python",
        "-c",
        "import asyncio; from app.v1.stock.tasks.maintenance import run; print(asyncio.run(run()))",
    ).strip()
    assert drift.splitlines()[-1] == "0"


def test_two_worker_replicas_charge_each_order_once(client: httpx.Client, actors) -> None:
    product, variant = published_product(client, actors, stock=10)
    buyers = [new_customer(client) for _ in range(2)]
    compose("up", "-d", "--no-recreate", "--scale", "order-worker=2", "order-worker")
    try:
        wait(
            lambda: compose("ps", "--status", "running", "-q", "order-worker").split(),
            lambda ids: len(ids) == 2,
            seconds=60,
        )
        results = place_many(order_body(product, 1), buyers * 5)
        assert [r.status_code for r in results] == [202] * 10
        order_ids = [r.json()["order_id"] for r in results]
        for buyer, oid in zip(buyers * 5, order_ids, strict=True):
            assert settle(client, {"customer": buyer}, oid, seconds=120)["status"] == "confirmed"
    finally:
        compose("up", "-d", "--no-recreate", "--scale", "order-worker=1", "order-worker")
    for oid in order_ids:
        charges = sql(
            "order_db",
            "SELECT count(*) FROM payment_attempts WHERE order_id = :'oid' "
            "AND kind = 'charge' AND outcome = 'charged'",
            oid=oid,
        )
        assert charges == "1", (oid, charges)
    assert ok(client.get(f"/api/v1/inventory/{variant}"))["on_hand"] == 0
