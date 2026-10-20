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
from app.v1.products.publishing_repo import PublishingRepo
from app.v1.products.workflows.activities import PublishingActivities
from app.v1.products.workflows.client import TemporalPublishStarter
from app.v1.products.workflows.publishing import ProductPublishingWorkflow
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


# --- process_media on Celery (0002-Q7) ---------------------------------------------


class EagerCelery:
    """Stands in for the catalog Celery app: ``send_task`` records the call and
    ``get()`` runs the real job body on the worker's thread, as catalog-celery would."""

    def __init__(self, storage, settings) -> None:
        self.storage, self.settings = storage, settings
        self.sent: list[list[str]] = []

    def send_task(self, name, args):
        from app.v1.media.tasks.process import derive_and_store

        self.sent.append(args)
        media_id, key, product_id = args
        storage, settings = self.storage, self.settings

        class Pending:
            def get(self, timeout, propagate):
                return asyncio.run(
                    derive_and_store(
                        storage,
                        settings,
                        media_id=uuid.UUID(media_id),
                        product_id=uuid.UUID(product_id),
                        storage_key=key,
                    )
                )

        return Pending()


@pytest.fixture
async def celery_worker(env, app, storage, inventory):
    from app.v1.media.tasks.process import CeleryMediaJobs

    celery = EagerCelery(storage, app.state.settings)
    activities = PublishingActivities(
        app.state.db, app.state.settings, storage, inventory, CeleryMediaJobs(celery, timeout=30)
    )
    async with Worker(
        env.client,
        task_queue=QUEUE,
        workflows=[ProductPublishingWorkflow],
        activities=activities.all(),
    ):
        yield celery


async def test_media_processed_through_celery_publishes(
    client, callers, env, starter, celery_worker, app, storage
) -> None:
    product = await complete_product(client, callers)
    r = await client.post(
        f"/api/v1/products/{product['id']}/media/upload",
        data={"version": str(product["version"])},
        files={"file": ("big.png", png(), "image/png")},
        headers=callers.merchant_a,
    )
    upload = r.json()
    outcome, detail = await _run_publish(
        client, callers, starter, {**product, "version": upload["version"]}
    )
    assert outcome["status"] == "published"
    assert [args[0] for args in celery_worker.sent] == [upload["id"]]
    media = next(m for m in detail["media"] if m["id"] == upload["id"])
    assert media["processing_status"] == "ready" and set(media["derived"]) == {
        "thumb",
        "medium",
        "large",
    }


async def test_undecodable_media_through_celery_fails_without_retry(
    client, callers, env, starter, celery_worker, app
) -> None:
    product = await complete_product(client, callers)
    r = await client.post(
        f"/api/v1/products/{product['id']}/media/upload",
        data={"version": str(product["version"])},
        files={"file": ("x.png", b"\x89PNG\r\n\x1a\n" + b"garbage" * 10, "image/png")},
        headers=callers.merchant_a,
    )
    upload = r.json()
    outcome, detail = await _run_publish(
        client, callers, starter, {**product, "version": upload["version"]}
    )
    assert (outcome["status"], outcome["failed_step"]) == ("failed", "process_media")
    assert len(celery_worker.sent) == 1  # a business failure: not retried
    async with app.state.db.sessionmaker() as s:
        media = await s.get(ProductMedia, uuid.UUID(upload["id"]))
        assert media.processing_status == "failed"


class LostCelery:
    """A broker that accepts the task but never answers: ``get`` times out."""

    def __init__(self) -> None:
        self.sent: list[list[str]] = []

    def send_task(self, name, args):
        self.sent.append(args)

        class Pending:
            def get(self, timeout, propagate):
                raise TimeoutError("no reply from catalog.media")

        return Pending()


async def test_lost_celery_job_is_retried_then_fails_the_media(
    client, callers, env, starter, app, storage, inventory
) -> None:
    """MediaJobFailed is infrastructure, so retried up to publish_media_max_attempts;
    the last attempt marks the media failed and the publish compensates."""
    from app.v1.media.tasks.process import CeleryMediaJobs

    celery = LostCelery()
    activities = PublishingActivities(
        app.state.db, app.state.settings, storage, inventory, CeleryMediaJobs(celery, timeout=1)
    )
    async with Worker(
        env.client,
        task_queue=QUEUE,
        workflows=[ProductPublishingWorkflow],
        activities=activities.all(),
    ):
        product = await complete_product(client, callers)
        r = await client.post(
            f"/api/v1/products/{product['id']}/media/upload",
            data={"version": str(product["version"])},
            files={"file": ("a.png", png(), "image/png")},
            headers=callers.merchant_a,
        )
        upload = r.json()
        outcome, detail = await _run_publish(
            client, callers, starter, {**product, "version": upload["version"]}
        )
    assert (outcome["status"], outcome["failed_step"]) == ("failed", "process_media")
    assert "MediaJobFailed" in outcome["reason"] and "no reply" in outcome["reason"]
    assert len(celery.sent) == app.state.settings.publish_media_max_attempts
    assert detail["status"] == "draft" and inventory.calls == []
    async with app.state.db.sessionmaker() as s:
        media = await s.get(ProductMedia, uuid.UUID(upload["id"]))
        assert media.processing_status == "failed"


# --- races and duplicates through real Temporal (CAT-7) -----------------------


async def test_concurrent_publishes_run_one_workflow_and_publish_once(
    client, callers, env, starter, worker, inventory, database_url
) -> None:
    product = await complete_product(client, callers)
    responses = await asyncio.gather(
        *(publish(client, callers.merchant_a, product) for _ in range(4))
    )
    accepted = [r for r in responses if r.status_code == 202]
    assert len(accepted) == 1, [r.text for r in responses]
    assert sorted(r.status_code for r in responses) == [202, 409, 409, 409]
    assert list(starter.handles) == [accepted[0].json()["workflow_id"]]

    outcome = await outcome_of(starter.handles[accepted[0].json()["workflow_id"]])
    assert outcome["status"] == "published"
    assert len(inventory.calls) == 1
    assert len(await outbox_rows(database_url, "catalog.product.published")) == 1


async def test_a_workflow_id_is_never_started_twice(client, callers, env, starter, worker) -> None:
    """REJECT_DUPLICATE: the second line of defence behind the status guard holds
    even after the first run has finished."""
    from temporalio.exceptions import WorkflowAlreadyStartedError

    from app.v1.products.workflows.client import TemporalPublishStarter

    product = await complete_product(client, callers)
    accepted = (await publish(client, callers.merchant_a, product)).json()
    await outcome_of(starter.handles[accepted["workflow_id"]])
    with pytest.raises(WorkflowAlreadyStartedError):
        await TemporalPublishStarter.start(
            starter,
            product_id=product["id"],
            workflow_id=accepted["workflow_id"],
            actor_id=callers.merchant_a_id,
            correlation_id=None,
        )


async def test_republish_after_a_failed_run_starts_a_fresh_workflow(
    client, callers, env, starter, worker, inventory
) -> None:
    """A failed publish leaves the product in draft; the merchant's retry gets a new
    workflow id (new version) and succeeds once the dependency is back."""
    inventory.error = ApplicationError("bad variants", type="validation", non_retryable=True)
    product = await complete_product(client, callers)
    first, detail = await _run_publish(client, callers, starter, product)
    assert first["status"] == "failed"

    inventory.error = None
    second, detail = await _run_publish(client, callers, starter, detail)
    assert second["status"] == "published" and detail["status"] == "published"
    assert len(starter.handles) == 2 and len(set(starter.handles)) == 2
    status = (
        await client.get(
            f"/api/v1/products/{product['id']}/publish-status", headers=callers.merchant_a
        )
    ).json()
    assert status["state"] == "published"


async def test_cancel_before_the_first_activity_still_compensates(
    client, callers, env, starter, app, storage, inventory
) -> None:
    """The cancel arrives with the workflow's first task, before any activity is
    scheduled (the ``asyncio.CancelledError`` path, not ``ActivityError``): the
    product must still go back to draft with a recorded reason."""
    product = await complete_product(client, callers)
    accepted = (await publish(client, callers.merchant_a, product)).json()
    handle = starter.handles[accepted["workflow_id"]]
    await handle.cancel()  # no worker yet: start and cancel land in one activation

    activities = PublishingActivities(app.state.db, app.state.settings, storage, inventory)
    async with Worker(
        env.client,
        task_queue=QUEUE,
        workflows=[ProductPublishingWorkflow],
        activities=activities.all(),
    ):
        with pytest.raises(WorkflowFailureError):
            await handle.result()
    detail = await client.get(f"/api/v1/products/{product['id']}", headers=callers.admin)
    assert detail.json()["status"] == "draft"
    failure = await _failure(app, product["id"])
    assert (failure.failed_step, failure.reason) == ("validate_product", "cancelled")
    assert inventory.calls == []
