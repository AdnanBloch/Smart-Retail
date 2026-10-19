"""notification end to end through the gateway (RFC 0007): events from identity
and order reach the customer's list and, live, their SSE stream.

Needs the events profile: ``make up migrate seed``.
"""

import json
import threading
import time
import uuid

import httpx
import pytest

from tests.e2e.test_events import compose
from tests.e2e.test_smoke import BASE_URL, login
from tests.e2e.test_workflows import PASSWORD, ok, order_body, published_product, settle, wait


@pytest.fixture(scope="module", autouse=True)
def events_profile():
    if "notification" not in compose("ps", "--services", "--status", "running"):
        pytest.skip("the notification service is not running")


def notifications(client: httpx.Client, headers, **params) -> list[dict]:
    return ok(client.get("/api/v1/notifications", params=params, headers=headers))["items"]


class Stream:
    """An SSE stream through the gateway, read on a thread into ``frames``."""

    def __init__(self, ticket: str) -> None:
        self.frames: list[tuple[str, dict]] = []
        self.opened = threading.Event()
        self._stop = False
        self._thread = threading.Thread(target=self._read, args=(ticket,), daemon=True)
        self._thread.start()
        assert self.opened.wait(10), "stream did not open"

    def _read(self, ticket: str) -> None:
        with (
            httpx.Client(base_url=BASE_URL, timeout=httpx.Timeout(10, read=60)) as c,
            c.stream("GET", "/api/v1/notifications/stream", params={"ticket": ticket}) as r,
        ):
            assert r.status_code == 200, r.read()
            assert r.headers["content-type"].startswith("text/event-stream")
            self.opened.set()
            event = None
            for line in r.iter_lines():
                if self._stop:
                    return
                if line.startswith("event: "):
                    event = line[7:]
                elif line.startswith("data: ") and event:
                    self.frames.append((event, json.loads(line[6:])))
                    event = None

    def of(self, kind: str) -> list[dict]:
        return [data for event, data in self.frames if event == kind]

    def close(self) -> None:
        self._stop = True


def test_registration_produces_a_welcome_notification(client: httpx.Client) -> None:
    email = f"ntf-{uuid.uuid4().hex[:8]}@smartretail.local"
    r = client.post(
        "/api/v1/auth/register",
        json={"email": email, "password": PASSWORD, "full_name": "Nina Notify"},
        headers={"X-Client-Id": "e2e-device-0001"},
    )
    assert r.status_code == 201, r.text
    me = login(client, email, PASSWORD)
    items = wait(
        lambda: notifications(client, me),
        lambda xs: any(n["type"] == "welcome" for n in xs),
        seconds=30,
    )
    assert any("Nina" in n["title"] + n["body"] for n in items)


def test_an_order_confirmation_arrives_live_over_sse(client: httpx.Client, actors) -> None:
    customer = actors["customer"]
    ticket = ok(client.post("/api/v1/notifications/stream-ticket", headers=customer))["ticket"]
    stream = Stream(ticket)
    try:
        assert wait(lambda: stream.of("hello"), bool, seconds=10)
        product, _ = published_product(client, actors, stock=2)
        r = client.post(
            "/api/v1/orders",
            json=order_body(product, 1),
            headers={**customer, "Idempotency-Key": str(uuid.uuid4())},
        )
        order = ok(r, 202)
        assert settle(client, actors, order["order_id"])["status"] == "confirmed"
        started = time.monotonic()
        frames = wait(
            lambda: [f for f in stream.of("notification") if f["type"] == "order_confirmed"],
            bool,
            seconds=30,
        )
        assert order["order_number"] in frames[0]["title"] + frames[0]["body"]
        assert time.monotonic() - started < 30
    finally:
        stream.close()
    # The same notification is in the list, and marking it read moves the count.
    [listed] = [
        n
        for n in notifications(client, customer, unread_only=True)
        if n["type"] == "order_confirmed" and n["id"] == frames[0]["id"]
    ]
    before = ok(client.get("/api/v1/notifications/unread-count", headers=customer))["count"]
    ok(client.post(f"/api/v1/notifications/{listed['id']}/read", headers=customer))
    after = ok(client.get("/api/v1/notifications/unread-count", headers=customer))["count"]
    assert after == before - 1


def test_a_declined_payment_notifies_once(client: httpx.Client, actors) -> None:
    product, _ = published_product(client, actors, stock=1)
    r = client.post(
        "/api/v1/orders",
        json=order_body(product, 1, method="tok_decline"),
        headers={**actors["customer"], "Idempotency-Key": str(uuid.uuid4())},
    )
    order = ok(r, 202)
    assert settle(client, actors, order["order_id"])["status"] == "failed"
    failed = wait(
        lambda: [
            n
            for n in notifications(client, actors["customer"], type="order_failed")
            if order["order_number"] in n["title"] + n["body"]
        ],
        bool,
        seconds=30,
    )
    assert len(failed) == 1  # NTF-1: one notification per event, however often it is delivered


def test_selling_out_notifies_the_merchant(client: httpx.Client, actors) -> None:
    product, _ = published_product(client, actors, stock=1)
    r = client.post(
        "/api/v1/orders",
        json=order_body(product, 1),
        headers={**actors["customer"], "Idempotency-Key": str(uuid.uuid4())},
    )
    assert settle(client, actors, ok(r, 202)["order_id"])["status"] == "confirmed"
    sku = product["variants"][0]["sku"]
    wait(
        lambda: [
            n
            for n in notifications(client, actors["merchant"], type="stockout")
            if sku in n["title"] + n["body"]
        ],
        bool,
        seconds=30,
    )
