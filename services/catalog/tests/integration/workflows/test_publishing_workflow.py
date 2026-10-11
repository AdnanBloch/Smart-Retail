"""``ProductPublishingWorkflow`` end to end: real workflow, real activities, real
Postgres, Temporal's time-skipping test server (RFC 0004 §7, G1, G7, G9).

Every failure point leaves the product in ``draft`` with a recorded reason —
never ``published``, never half-published.
"""

import asyncio
import io
import uuid

import pytest
from PIL import Image
from sqlalchemy import select
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from app.core.config import Settings
from app.models import ProductMedia, ProductPublishFailure
from app.products.publishing_repo import PublishingRepo
from app.workflows.activities import PublishingActivities
from app.workflows.client import TemporalPublishStarter
from app.workflows.publishing import ProductPublishingWorkflow
from smartretail_common.temporal import TemporalClientFactory
from tests.integration.conftest import complete_product, outbox_rows, publish

QUEUE = "catalog-publishing"


class FakeInventory:
    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.error: Exception | None = None
        self.gate: asyncio.Event | None = None

    async def initialise_stock(self, product_id, merchant_id, variants) -> None:
        self.calls.append((product_id, merchant_id, variants))
        if self.gate is not None:
            await self.gate.wait()
        if self.error is not None:
            raise self.error


@pytest.fixture
async def env():
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except Exception as exc:  # pragma: no cover - offline machines
        pytest.skip(f"Temporal test server unavailable: {exc}")
    yield environment
    await environment.shutdown()


class RecordingStarter(TemporalPublishStarter):
    """Keeps each started handle: only handles returned by the time-skipping
    client's own ``start_workflow`` skip timers while awaited."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.handles: dict[str, object] = {}

    async def start(self, **kwargs) -> None:
        self.handles[kwargs["workflow_id"]] = await self.start_handle(**kwargs)


@pytest.fixture
def starter(env, database_url):
    factory = TemporalClientFactory("unused")
    factory.set(env.client)
    return RecordingStarter(factory, Settings(database_url=database_url))


@pytest.fixture
def inventory() -> FakeInventory:
    return FakeInventory()


@pytest.fixture
async def worker(env, app, storage, inventory):
    activities = PublishingActivities(app.state.db, app.state.settings, storage, inventory)
    async with Worker(
        env.client,
        task_queue=QUEUE,
        workflows=[ProductPublishingWorkflow],
        activities=activities.all(),
    ) as running:
        yield running


def png() -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (1600, 900), "blue").save(buffer, format="PNG")
    return buffer.getvalue()


async def outcome_of(handle) -> dict:
    """``result()``, tolerating the test server ending a history long-poll early."""
    for _ in range(30):
        try:
            return await handle.result()
        except RuntimeError as exc:
            if "No completion event found" not in str(exc):
                raise
    raise AssertionError("workflow did not complete")


async def _run_publish(client, callers, starter, product) -> tuple[dict, dict]:
    accepted = await publish(client, callers.merchant_a, product)
    assert accepted.status_code == 202, accepted.text
    outcome = await outcome_of(starter.handles[accepted.json()["workflow_id"]])
    detail = await client.get(f"/api/v1/products/{product['id']}", headers=callers.admin)
    return outcome, detail.json()


async def _failure(app, product_id: str) -> ProductPublishFailure:
    async with app.state.db.sessionmaker() as s:
        [row] = (
            await s.scalars(
                select(ProductPublishFailure).where(
                    ProductPublishFailure.product_id == uuid.UUID(product_id)
                )
            )
        ).all()
    return row


async def test_happy_path_publishes_with_processed_media(
    client, callers, env, starter, worker, app, storage, inventory, database_url
) -> None:
    product = await complete_product(client, callers)
    r = await client.post(
        f"/api/v1/products/{product['id']}/media/upload",
        data={"version": str(product["version"])},
        files={"file": ("big.png", png(), "image/png")},
        headers=callers.merchant_a,
    )
    assert r.status_code == 201, r.text
    upload = r.json()

    outcome, detail = await _run_publish(
        client, callers, starter, {**product, "version": upload["version"]}
    )
    assert outcome == {"status": "published", "failed_step": None, "reason": None}
    assert detail["status"] == "published" and detail["published_at"]

    media = next(m for m in detail["media"] if m["id"] == upload["id"])
    assert media["processing_status"] == "ready"
    assert set(media["derived"]) == {"thumb", "medium", "large"}
    with Image.open(io.BytesIO(storage.objects[media["derived"]["medium"]][0])) as derived:
        assert derived.format == "WEBP" and max(derived.size) == 600

    [(pid, merchant, variants)] = inventory.calls
    assert str(pid) == product["id"] and str(merchant) == callers.merchant_a_id
    assert [sku for _, sku in variants] == ["PUB-1-M"]
    [event] = await outbox_rows(database_url, "catalog.product.published")
    assert event["product_id"] == product["id"]
    status = await client.get(
        f"/api/v1/products/{product['id']}/publish-status", headers=callers.merchant_a
    )
    assert status.json()["state"] == "published"


async def test_incomplete_product_fails_validation(
    client, callers, env, starter, worker, app
) -> None:
    product = (
        await client.post(
            "/api/v1/products",
            json={"sku": "BARE", "name": "Bare", "base_price": "5"},
            headers=callers.merchant_a,
        )
    ).json()
    outcome, detail = await _run_publish(client, callers, starter, product)
    assert outcome["status"] == "failed" and outcome["failed_step"] == "validate_product"
    assert "description" in outcome["reason"]
    assert detail["status"] == "draft"
    assert (await _failure(app, product["id"])).failed_step == "validate_product"


@pytest.mark.parametrize(
    ("error", "attempts"),
    [
        (ApplicationError("inventory down", type="DependencyUnavailable"), 8),
        (ApplicationError("bad variants", type="validation", non_retryable=True), 1),
    ],
)
async def test_inventory_failure_compensates(
    client, callers, env, starter, worker, app, inventory, database_url, error, attempts
) -> None:
    inventory.error = error
    product = await complete_product(client, callers)
    outcome, detail = await _run_publish(client, callers, starter, product)
    assert (outcome["status"], outcome["failed_step"]) == ("failed", "sync_downstream")
    assert len(inventory.calls) == attempts  # CAT-8: 8 attempts covers an inventory restart
    assert detail["status"] == "draft"
    assert (await _failure(app, product["id"])).reason.endswith(str(error.message))
    assert await outbox_rows(database_url, "catalog.product.published") == []


async def test_undecodable_media_fails_without_retry(
    client, callers, env, starter, worker, app, storage
) -> None:
    product = await complete_product(client, callers)
    r = await client.post(
        f"/api/v1/products/{product['id']}/media/upload",
        data={"version": str(product["version"])},
        # Passes the magic-byte check, but is not a decodable PNG.
        files={"file": ("x.png", b"\x89PNG\r\n\x1a\n" + b"garbage" * 10, "image/png")},
        headers=callers.merchant_a,
    )
    upload = r.json()
    outcome, detail = await _run_publish(
        client, callers, starter, {**product, "version": upload["version"]}
    )
    assert (outcome["status"], outcome["failed_step"]) == ("failed", "process_media")
    assert detail["status"] == "draft"
    async with app.state.db.sessionmaker() as s:
        media = await s.get(ProductMedia, uuid.UUID(upload["id"]))
        assert media.processing_status == "failed"


async def test_storage_outage_marks_media_failed_on_the_last_attempt(
    client, callers, env, starter, worker, app, storage
) -> None:
    product = await complete_product(client, callers)
    r = await client.post(
        f"/api/v1/products/{product['id']}/media/upload",
        data={"version": str(product["version"])},
        files={"file": ("a.png", png(), "image/png")},
        headers=callers.merchant_a,
    )
    upload = r.json()
    storage.fail = True
    outcome, _ = await _run_publish(
        client, callers, starter, {**product, "version": upload["version"]}
    )
    assert outcome["failed_step"] == "process_media"
    async with app.state.db.sessionmaker() as s:
        media = await s.get(ProductMedia, uuid.UUID(upload["id"]))
        assert media.processing_status == "failed"


@pytest.mark.parametrize(
    ("method", "step"),
    [("build_search_document", "build_search_document"), ("complete", "mark_published")],
)
async def test_late_step_failures_never_leave_the_product_public(
    client, callers, env, starter, worker, app, monkeypatch, database_url, method, step
) -> None:
    async def broken(self, *args, **kwargs):
        raise RuntimeError(f"{method} exploded")

    monkeypatch.setattr(PublishingRepo, method, broken)
    product = await complete_product(client, callers)
    outcome, detail = await _run_publish(client, callers, starter, product)
    assert (outcome["status"], outcome["failed_step"]) == ("failed", step)
    assert detail["status"] == "draft"
    assert (await client.get(f"/api/v1/products/{product['id']}")).status_code == 404
    assert await outbox_rows(database_url, "catalog.product.published") == []


async def test_cancellation_compensates(
    client, callers, env, starter, worker, app, inventory
) -> None:
    inventory.gate = asyncio.Event()
    product = await complete_product(client, callers)
    accepted = (await publish(client, callers.merchant_a, product)).json()
    handle = starter.handles[accepted["workflow_id"]]
    for _ in range(200):  # wait until the workflow is inside sync_downstream
        if inventory.calls:
            break
        await asyncio.sleep(0.05)
    step = await handle.query("current_step")
    assert step["step"] == "sync_downstream" and step["started_at"]

    await handle.cancel()
    with pytest.raises(WorkflowFailureError):
        await handle.result()
    inventory.gate.set()
    detail = await client.get(f"/api/v1/products/{product['id']}", headers=callers.admin)
    assert detail.json()["status"] == "draft"
    failure = await _failure(app, product["id"])
    assert (failure.failed_step, failure.reason) == ("sync_downstream", "cancelled")


async def test_history_replays_deterministically(client, callers, env, starter, worker) -> None:
    """PLT-15: the workflow code replays its own history without nondeterminism."""
    product = await complete_product(client, callers)
    accepted = (await publish(client, callers.merchant_a, product)).json()
    handle = starter.handles[accepted["workflow_id"]]
    await outcome_of(handle)
    history = await handle.fetch_history()
    await Replayer(workflows=[ProductPublishingWorkflow]).replay_workflow(history)

    # The starter's describe/query paths, as publish-status and the accept guard use them.
    assert await starter.is_running(accepted["workflow_id"]) is False
    assert await starter.is_running("product-publish-never-started") is False
    assert (await starter.current_step(accepted["workflow_id"]))["step"] == "done"
