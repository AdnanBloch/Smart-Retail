"""The publish contract at the API edge (RFC 0004 §3.1.2.1, CAT-7, CAT-9, G9) and
every write the workflow makes through ``PublishingRepo`` (T5, T6)."""

import uuid

import pytest

from app.core.errors import ProductIncomplete, PublishSuperseded
from app.media.enums import MediaProcessingStatus
from app.models import ProductMedia, ProductPublishFailure
from app.products.enums import PublishStep
from app.products.publishing_repo import PublishingRepo
from tests.integration.conftest import (
    complete_product,
    create_product,
    force_published,
    outbox_rows,
    publish,
)


async def test_publish_is_accepted_with_202(client, callers, starter) -> None:
    product = await complete_product(client, callers)
    r = await publish(client, callers.merchant_a, product)
    assert r.status_code == 202, r.text
    body = r.json()
    expected_id = f"product-publish-{product['id']}-v{product['version'] + 1}"
    assert body["workflow_id"] == expected_id and body["status"] == "publishing"
    assert body["_links"]["publish_status"] == f"/api/v1/products/{product['id']}/publish-status"
    assert r.headers["location"] == body["_links"]["publish_status"]
    assert starter.started == [
        {"product_id": product["id"], "workflow_id": expected_id, "actor_id": callers.merchant_a_id}
    ]

    detail = (await client.get(f"/api/v1/products/{product['id']}", headers=callers.admin)).json()
    assert detail["status"] == "publishing" and detail["version"] == body["version"]
    # Not public while publishing.
    assert (await client.get(f"/api/v1/products/{product['id']}")).status_code == 404

    status = await client.get(
        f"/api/v1/products/{product['id']}/publish-status", headers=callers.merchant_a
    )
    assert status.json() == {
        "state": "running",
        "workflow_id": expected_id,
        "step": "validate_product",
        "started_at": "2026-10-11T00:00:00",
        "failed_step": None,
        "failure_reason": None,
        "failed_at": None,
    }


async def test_only_the_workflow_writes_while_publishing(client, callers) -> None:
    product = await complete_product(client, callers)
    version = (await publish(client, callers.merchant_a, product)).json()["version"]
    base = f"/api/v1/products/{product['id']}"
    writes = [
        client.patch(base, json={"name": "x", "version": version}, headers=callers.merchant_a),
        client.post(
            f"{base}/status", json={"status": "draft", "version": version}, headers=callers.admin
        ),
        client.delete(f"{base}?version={version}", headers=callers.merchant_a),
        client.post(
            f"{base}/variants",
            json={"sku": "Z", "options": {}, "version": version},
            headers=callers.merchant_a,
        ),
    ]
    for call in writes:
        r = await call
        assert r.status_code == 409 and r.json()["type"].endswith("/product-publishing"), r.text


async def test_second_publish_while_running_is_409(client, callers, starter) -> None:
    product = await complete_product(client, callers)
    accepted = (await publish(client, callers.merchant_a, product)).json()
    r = await publish(client, callers.merchant_a, {**product, "version": accepted["version"]})
    assert r.status_code == 409 and r.json()["type"].endswith("/publish-already-running")
    assert r.json()["workflow_id"] == accepted["workflow_id"]

    # A stuck row — Temporal no longer knows the workflow — is recovered as a fresh accept.
    starter.running.clear()
    r = await publish(client, callers.merchant_a, {**product, "version": accepted["version"]})
    assert r.status_code == 202 and r.json()["workflow_id"] != accepted["workflow_id"]


async def test_start_failure_returns_503_and_draft(client, callers, starter) -> None:
    """CAT-9: commit first, then start; a failed start compensates with failed_step=start."""
    product = await complete_product(client, callers)
    starter.fail_start = True
    r = await publish(client, callers.merchant_a, product)
    assert r.status_code == 503 and r.json()["type"].endswith("/publish-start-failed")
    detail = (await client.get(f"/api/v1/products/{product['id']}", headers=callers.admin)).json()
    assert detail["status"] == "draft"

    status = (
        await client.get(
            f"/api/v1/products/{product['id']}/publish-status", headers=callers.merchant_a
        )
    ).json()
    assert status["state"] == "failed" and status["failed_step"] == "start"
    assert "temporal unreachable" in status["failure_reason"]
    history = await client.get(
        f"/api/v1/products/{product['id']}/history", headers=callers.merchant_a
    )
    assert history.json()["items"][0]["action"] == "publish_failed"


async def test_publish_status_states_and_access(client, callers, app, starter) -> None:
    product = await create_product(client, callers.merchant_a)
    url = f"/api/v1/products/{product['id']}/publish-status"
    assert (await client.get(url, headers=callers.merchant_a)).json()["state"] == "idle"
    assert (await client.get(url, headers=callers.merchant_b)).status_code == 404
    assert (await client.get(url, headers=callers.customer)).status_code == 403
    assert (
        await client.get(f"/api/v1/products/{uuid.uuid4()}/publish-status", headers=callers.admin)
    ).status_code == 404

    full = await complete_product(client, callers, sku="S2")
    await publish(client, callers.merchant_a, full)
    starter.step = None  # Temporal down: the row's state, without a step
    running = (
        await client.get(f"/api/v1/products/{full['id']}/publish-status", headers=callers.admin)
    ).json()
    assert running["state"] == "running" and running["step"] is None

    await force_published(app, product["id"])
    assert (await client.get(url, headers=callers.admin)).json()["state"] == "published"


async def test_publish_needs_ownership_and_version(client, callers) -> None:
    product = await complete_product(client, callers)
    assert (await publish(client, callers.merchant_b, product)).status_code == 404
    r = await publish(client, callers.merchant_a, {**product, "version": 99})
    assert r.status_code == 409 and r.json()["type"].endswith("/version-conflict")


# --- the workflow's writes ----------------------------------------------------


async def _accepted(client, callers) -> tuple[dict, str]:
    product = await complete_product(client, callers)
    body = (await publish(client, callers.merchant_a, product)).json()
    return product, body["workflow_id"]


async def test_complete_publishes_exactly_once(client, callers, app, database_url) -> None:
    product, workflow_id = await _accepted(client, callers)
    pid = uuid.UUID(product["id"])
    async with app.state.db.sessionmaker() as s:
        repo = PublishingRepo(s)
        assert await repo.validate(pid, workflow_id) == []
        await repo.build_search_document(pid, workflow_id)
        payload = await repo.stock_payload(pid, workflow_id)
        assert payload.merchant_id == uuid.UUID(callers.merchant_a_id)
        assert [sku for _, sku in payload.variants] == ["PUB-1-M"]
        first = await repo.complete(pid, workflow_id, callers.merchant_a_id)
        again = await repo.complete(pid, workflow_id, callers.merchant_a_id)
        assert first.version == again.version

    public = await client.get(f"/api/v1/products/{product['id']}")
    assert public.status_code == 200 and public.json()["status"] == "published"
    assert public.json()["published_at"] is not None
    [event] = await outbox_rows(database_url, "catalog.product.published")
    assert event["product_id"] == product["id"] and event["status"] == "published"
    assert event["search_document"]["name"] == "Trail Shoe"
    assert event["search_document"]["category"]["path"][-1].startswith("C-")
    assert [v["sku"] for v in event["variants"]] == ["PUB-1-M"]


async def test_fail_compensates_once_and_superseded_writes_are_refused(client, callers, app):
    product, workflow_id = await _accepted(client, callers)
    pid = uuid.UUID(product["id"])
    async with app.state.db.sessionmaker() as s:
        repo = PublishingRepo(s)
        with pytest.raises(PublishSuperseded):
            await repo.validate(pid, "product-publish-someone-else")
        assert await repo.fail(
            pid,
            workflow_id,
            PublishStep.SYNC_DOWNSTREAM,
            "inventory down",
            actor_id=callers.merchant_a_id,
        )
        assert not await repo.fail(
            pid, workflow_id, PublishStep.SYNC_DOWNSTREAM, "again", actor_id=callers.merchant_a_id
        )
        with pytest.raises(PublishSuperseded):
            await repo.complete(pid, workflow_id, callers.merchant_a_id)
        failures = (
            await s.execute(
                ProductPublishFailure.__table__.select().where(
                    ProductPublishFailure.product_id == pid
                )
            )
        ).all()
    assert len(failures) == 1
    status = (
        await client.get(
            f"/api/v1/products/{product['id']}/publish-status", headers=callers.merchant_a
        )
    ).json()
    assert (status["state"], status["failed_step"]) == ("failed", "sync_downstream")
    assert (await client.get(f"/api/v1/products/{product['id']}")).status_code == 404


async def test_validate_reports_what_is_missing(client, callers, app) -> None:
    product = await create_product(client, callers.merchant_a)
    accepted = (await publish(client, callers.merchant_a, product)).json()
    async with app.state.db.sessionmaker() as s:
        with pytest.raises(ProductIncomplete) as err:
            await PublishingRepo(s).validate(uuid.UUID(product["id"]), accepted["workflow_id"])
    assert err.value.extras["missing"] == ["description", "category_id", "variants", "media"]


async def test_media_results_are_recorded(client, callers, app, storage) -> None:
    product = await complete_product(client, callers)
    r = await client.post(
        f"/api/v1/products/{product['id']}/media/upload",
        data={"version": str(product["version"])},
        files={"file": ("a.png", b"\x89PNG\r\n\x1a\n" + b"0" * 32, "image/png")},
        headers=callers.merchant_a,
    )
    assert r.status_code == 201, r.text
    media_id = r.json()["id"]
    accepted = (
        await publish(client, callers.merchant_a, {**product, "version": r.json()["version"]})
    ).json()
    pid, workflow_id = uuid.UUID(product["id"]), accepted["workflow_id"]
    async with app.state.db.sessionmaker() as s:
        repo = PublishingRepo(s)
        assert await repo.validate(pid, workflow_id) == [uuid.UUID(media_id)]
        source = await repo.media_source(uuid.UUID(media_id))
        assert source.storage_key in storage.objects
        await repo.record_media(
            uuid.UUID(media_id),
            workflow_id,
            derived=None,
            error="boom",
            actor_id=callers.merchant_a_id,
        )
        assert await repo.validate(pid, workflow_id) == [uuid.UUID(media_id)]  # failed: retried
        await repo.record_media(
            uuid.UUID(media_id),
            workflow_id,
            derived={"thumb": "k"},
            error=None,
            actor_id=callers.merchant_a_id,
        )
        media = await s.get(ProductMedia, uuid.UUID(media_id))
        await s.refresh(media)
        assert media.processing_status == MediaProcessingStatus.READY
        assert media.derived == {"thumb": "k"}
        assert await repo.validate(pid, workflow_id) == []
