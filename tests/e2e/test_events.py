"""Milestone 3 end to end: the event path through real Kafka, and the Celery
recovery job through a real Temporal outage (mentor review 2026-10-13).

* outbox → outbox-relay → Kafka → inventory-consumer → ``processed_events``
* ``order.cancelled`` → the inventory consumer
* checkout while Temporal is down → ``resume_unstarted_orders`` (Celery Beat)
  starts the workflow when it is back, and the order confirms.

Needs the whole stack with the events profile: ``make up migrate seed``.
"""

import shutil
import subprocess
import uuid

import httpx
import pytest

from tests.e2e.test_workflows import (
    REPO,
    ok,
    order_body,
    published_product,
    settle,
    wait,
)

pytestmark = pytest.mark.skipif(shutil.which("docker") is None, reason="needs the docker CLI")

COMPOSE = ["docker", "compose", "--profile", "core", "--profile", "workflow", "--profile", "events"]


def compose(*args: str, stdin: str | None = None) -> str:
    result = subprocess.run(  # noqa: S603
        [*COMPOSE, *args], cwd=REPO, check=True, capture_output=True, text=True, input=stdin
    )
    return result.stdout


def sql(database: str, query: str, **params: str) -> str:
    """One value from ``query``; ``:'name'`` placeholders are psql variables, so
    no id is ever formatted into the SQL text."""
    variables = [arg for k, v in params.items() for arg in ("-v", f"{k}={v}")]
    return compose(
        "exec",
        "-T",
        "postgres",
        "psql",
        "-U",
        "postgres",
        "-d",
        database,
        "-tA",
        *variables,
        "-f",
        "-",
        stdin=query,
    ).strip()


@pytest.fixture(scope="module", autouse=True)
def events_profile():
    if "inventory-consumer" not in compose("ps", "--services", "--status", "running"):
        pytest.skip("the events profile is not running")


def test_a_published_product_travels_outbox_relay_kafka_consumer(client: httpx.Client, actors):
    product, _ = published_product(client, actors, stock=1)
    pid = product["id"]
    # The relay published the catalog's outbox row (and marked it only on ack)...
    wait(
        lambda: sql(
            "catalog_db",
            "SELECT count(*) FROM outbox WHERE event_type = 'catalog.product.published' "
            "AND aggregate_id = :'pid' AND published_at IS NOT NULL",
            pid=pid,
        ),
        lambda n: n == "1",
        seconds=30,
    )
    event_id = sql(
        "catalog_db",
        "SELECT event_id FROM outbox WHERE aggregate_id = :'pid' "
        "AND event_type = 'catalog.product.published'",
        pid=pid,
    )
    # ...and the inventory consumer handled exactly that event, once.
    wait(
        lambda: sql(
            "inventory_db",
            "SELECT count(*) FROM processed_events WHERE event_id = :'eid' "
            "AND consumer_group = 'inventory'",
            eid=event_id,
        ),
        lambda n: n == "1",
        seconds=30,
    )
    # The stock row the workflow created was left alone (idempotent by state).
    variant = product["variants"][0]["id"]
    assert ok(client.get(f"/api/v1/inventory/{variant}"))["on_hand"] == 1


def test_a_cancelled_order_reaches_the_inventory_consumer(client: httpx.Client, actors):
    product, variant = published_product(client, actors, stock=2)
    # tok_flaky_2: the charge fails twice before succeeding, holding the saga in
    # `charging` (a cancellable stage) long enough to cancel it deterministically.
    r = client.post(
        "/api/v1/orders",
        json=order_body(product, 1, method="tok_flaky_2"),
        headers={**actors["customer"], "Idempotency-Key": str(uuid.uuid4())},
    )
    order_id = ok(r, 202)["order_id"]
    wait(
        lambda: ok(client.get(f"/api/v1/orders/{order_id}/workflow", headers=actors["admin"])),
        lambda w: w["stage"] == "charging",
        seconds=30,
    )
    ok(
        client.post(
            f"/api/v1/orders/{order_id}/cancel",
            json={"reason": "changed my mind"},
            headers=actors["customer"],
        ),
        202,
    )
    assert settle(client, actors, order_id, seconds=90)["status"] == "cancelled"
    event_id = sql(
        "order_db",
        "SELECT event_id FROM outbox WHERE aggregate_id = :'oid' "
        "AND event_type = 'order.cancelled'",
        oid=order_id,
    )
    assert event_id, "the saga wrote order.cancelled to the order outbox"
    wait(
        lambda: sql(
            "inventory_db",
            "SELECT count(*) FROM processed_events WHERE event_id = :'eid'",
            eid=event_id,
        ),
        lambda n: n == "1",
        seconds=30,
    )
    assert ok(client.get(f"/api/v1/inventory/{variant}"))["available"] == 2


def test_an_order_accepted_while_temporal_is_down_is_resumed_by_celery(
    client: httpx.Client,
    actors,
):
    product, _ = published_product(client, actors, stock=1)
    compose("stop", "temporal")
    try:
        r = client.post(
            "/api/v1/orders",
            json=order_body(product, 1),
            headers={**actors["customer"], "Idempotency-Key": str(uuid.uuid4())},
        )
        order_id = ok(r, 202)["order_id"]  # acceptance does not depend on Temporal (ORD-9)
        assert (
            sql(
                "order_db",
                "SELECT workflow_run_id IS NULL FROM orders WHERE id = :'oid'",
                oid=order_id,
            )
            == "t"
        )
    finally:
        compose("up", "-d", "--wait", "temporal")
    # Beat runs resume_unstarted_orders every minute; the order then completes.
    assert settle(client, actors, order_id, seconds=240)["status"] == "confirmed"
    assert (
        sql(
            "order_db",
            "SELECT workflow_run_id IS NOT NULL FROM orders WHERE id = :'oid'",
            oid=order_id,
        )
        == "t"
    )


def test_an_uploaded_image_is_processed_on_the_celery_media_queue(client: httpx.Client, actors):
    """0002-Q7 / PLT-D43: the publishing activity hands process_media to catalog-celery
    (queue catalog.media) and waits for the result over RabbitMQ."""
    import io

    from PIL import Image

    from tests.e2e.test_workflows import make_product, publish

    product = make_product(client, actors, media=False)
    buffer = io.BytesIO()
    Image.new("RGB", (800, 600), "teal").save(buffer, format="PNG")
    upload = ok(
        client.post(
            f"/api/v1/products/{product['id']}/media/upload",
            data={"version": str(product["version"])},
            files={"file": ("teal.png", buffer.getvalue(), "image/png")},
            headers=actors["merchant"],
        ),
        201,
    )
    assert (
        publish(client, actors, {**product, "version": upload["version"]})["state"] == "published"
    )
    detail = ok(client.get(f"/api/v1/products/{product['id']}", headers=actors["merchant"]))
    [media] = [m for m in detail["media"] if m["id"] == upload["id"]]
    assert media["processing_status"] == "ready" and set(media["derived"]) >= {"thumb", "medium"}
    logs = subprocess.run(  # noqa: S603
        ["docker", "logs", "smartretail-catalog-celery-1"],  # noqa: S607
        capture_output=True,
        text=True,
        check=True,
    )
    assert "smartretail.catalog.process_media" in logs.stdout + logs.stderr
