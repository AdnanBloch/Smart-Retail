"""assistant end to end through the gateway (RFC 0009): a published product
becomes searchable via the indexer, chat streams sources before tokens, and
merchant generation works — all with the key-less default providers.

Needs the ai profile: ``make up migrate seed``.
"""

import json
import uuid

import httpx
import pytest

from tests.e2e.test_events import compose
from tests.e2e.test_smoke import BASE_URL
from tests.e2e.test_workflows import ok, published_product, wait


@pytest.fixture(scope="module", autouse=True)
def ai_profile():
    if "indexer-worker" not in compose("ps", "--services", "--status", "running"):
        pytest.skip("the ai profile is not running")


def search(client: httpx.Client, query: str) -> list[dict]:
    """By SKU in the tests: every e2e product shares a name, the SKU is unique and
    is part of every chunk's text, so the keyword leg pins the exact product."""
    body = ok(client.post("/api/v1/search/semantic", json={"query": query, "limit": 20}))
    return body["items"]


def sse_events(response: httpx.Response) -> list[tuple[str, dict]]:
    events, event = [], None
    for line in response.iter_lines():
        if line.startswith("event: "):
            event = line[7:]
        elif line.startswith("data: ") and event:
            events.append((event, json.loads(line[6:])))
            event = None
    return events


def test_publish_indexes_and_unpublish_removes(client: httpx.Client, actors) -> None:
    product, _ = published_product(client, actors, stock=3)
    pid = product["id"]
    hits = wait(
        lambda: [h for h in search(client, product["sku"]) if h["product_id"] == pid],
        bool,
        seconds=60,
    )
    assert hits[0]["hydrated"] is True and hits[0]["availability"] == "in_stock"

    detail = ok(client.get(f"/api/v1/products/{pid}", headers=actors["merchant"]))
    ok(
        client.post(
            f"/api/v1/products/{pid}/status",
            json={"status": "draft", "version": detail["version"]},
            headers=actors["merchant"],
        )
    )
    wait(
        lambda: [h for h in search(client, product["sku"]) if h["product_id"] == pid],
        lambda xs: not xs,
        seconds=60,
    )


def test_chat_streams_sources_before_tokens(client: httpx.Client, actors) -> None:
    product, _ = published_product(client, actors, stock=3)
    wait(
        lambda: [h for h in search(client, product["sku"]) if h["product_id"] == product["id"]],
        bool,
        seconds=60,
    )
    with (
        httpx.Client(base_url=BASE_URL, timeout=httpx.Timeout(10, read=120)) as c,
        c.stream(
            "POST",
            "/api/v1/assistant/chat",
            json={"message": f"Do you have {product['sku']}?", "session_id": uuid.uuid4().hex},
        ) as r,
    ):
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/event-stream")
        events = sse_events(r)
    kinds = [k for k, _ in events]
    assert "sources" in kinds and "token" in kinds and kinds[-1] == "done"
    assert kinds.index("sources") < kinds.index("token")


def test_chat_sync_and_a_follow_up_keep_the_conversation(client: httpx.Client, actors) -> None:
    session = uuid.uuid4().hex
    first = ok(
        client.post(
            "/api/v1/assistant/chat/sync",
            json={"message": "show me boots", "session_id": session},
        )
    )
    second = ok(
        client.post(
            "/api/v1/assistant/chat/sync",
            json={
                "message": "which of those is cheapest?",
                "session_id": session,
                "conversation_id": first["conversation_id"],
            },
        )
    )
    assert second["conversation_id"] == first["conversation_id"]
    assert second["finish_reason"] in {"stop", "length"}


def test_merchant_generation_and_bulk(client: httpx.Client, actors) -> None:
    product, _ = published_product(client, actors, stock=1)
    merchant = actors["merchant"]
    gen = ok(
        client.post(
            "/api/v1/assistant/generate/sync",
            json={"product_id": product["id"], "content_type": "description"},
            headers=merchant,
        )
    )
    assert gen["content"]
    key = str(uuid.uuid4())
    body = {"product_ids": [product["id"]], "content_type": "seo_title"}
    job = ok(
        client.post(
            "/api/v1/assistant/generate/bulk",
            json=body,
            headers={**merchant, "Idempotency-Key": key},
        ),
        202,
    )
    replay = ok(
        client.post(
            "/api/v1/assistant/generate/bulk",
            json=body,
            headers={**merchant, "Idempotency-Key": key},
        ),
        202,
    )
    assert replay["job_id"] == job["job_id"]
    done = wait(
        lambda: ok(
            client.get(f"/api/v1/assistant/generate/jobs/{job['job_id']}", headers=merchant)
        ),
        lambda j: j["status"] in {"completed", "failed", "partially_failed", "cancelled"},
        seconds=90,
    )
    assert done["status"] == "completed", done
