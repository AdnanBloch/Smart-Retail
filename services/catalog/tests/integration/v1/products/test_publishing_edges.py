"""``PublishingRepo`` edge cases the happy-path tests do not reach (RFC 0004 T5, T6)."""

import uuid

import pytest

from app.core.errors import MediaNotFound, ProductIncomplete
from app.v1.products.publishing_repo import PublishingRepo
from tests.integration.conftest import complete_product, create_product, force_published, publish


async def test_status_is_idle_after_an_unpublish(app, client, callers, starter) -> None:
    """A product that left ``published`` keeps its last workflow id but has no failure."""
    product = await complete_product(client, callers)
    accepted = (await publish(client, callers.merchant_a, product)).json()
    await force_published(app, product["id"])
    r = await client.post(
        f"/api/v1/products/{product['id']}/status",
        json={"status": "draft", "version": accepted["version"]},
        headers=callers.merchant_a,
    )
    assert r.status_code == 200, r.text
    status = (
        await client.get(
            f"/api/v1/products/{product['id']}/publish-status", headers=callers.merchant_a
        )
    ).json()
    assert status["state"] == "idle" and status["workflow_id"] == accepted["workflow_id"]
    assert status["failed_step"] is None


async def test_media_lookups_for_unknown_media_raise(app) -> None:
    async with app.state.db.sessionmaker() as s:
        repo = PublishingRepo(s)
        with pytest.raises(MediaNotFound):
            await repo.media_source(uuid.uuid4())
        with pytest.raises(MediaNotFound):
            await repo.record_media(
                uuid.uuid4(), "wf", derived={"t": "k"}, error=None, actor_id="a"
            )


async def test_url_media_has_no_source_to_process(app, client, callers) -> None:
    product = await complete_product(client, callers)
    [media] = product["media"]
    async with app.state.db.sessionmaker() as s:
        with pytest.raises(MediaNotFound):
            await PublishingRepo(s).media_source(uuid.UUID(media["id"]))


async def test_complete_refuses_when_only_unprocessed_media_remain(app, client, callers) -> None:
    """Step 5 re-checks completeness with ``require_ready_media``: pending uploads
    do not count, so a skipped or failed process step can never publish."""
    product = await create_product(client, callers.merchant_a, description="d", category_id=None)
    category = (
        await client.post(
            "/api/v1/categories", json={"name": "C", "slug": "c"}, headers=callers.admin
        )
    ).json()
    r = await client.patch(
        f"/api/v1/products/{product['id']}",
        json={"category_id": category["id"], "version": 1},
        headers=callers.merchant_a,
    )
    r = await client.post(
        f"/api/v1/products/{product['id']}/variants",
        json={"sku": "V", "options": {}, "version": r.json()["version"]},
        headers=callers.merchant_a,
    )
    r = await client.post(
        f"/api/v1/products/{product['id']}/media/upload",
        data={"version": str(r.json()["version"])},
        files={"file": ("a.png", b"\x89PNG\r\n\x1a\n" + b"0" * 16, "image/png")},
        headers=callers.merchant_a,
    )
    accepted = (
        await publish(client, callers.merchant_a, {**product, "version": r.json()["version"]})
    ).json()
    pid = uuid.UUID(product["id"])
    async with app.state.db.sessionmaker() as s:
        repo = PublishingRepo(s)
        # Validation passes (the upload exists) and lists it for processing...
        assert len(await repo.validate(pid, accepted["workflow_id"])) == 1
        # ...but completing without processing it is refused.
        with pytest.raises(ProductIncomplete) as err:
            await repo.complete(pid, accepted["workflow_id"], callers.merchant_a_id)
    assert err.value.extras["missing"] == ["media"]
    detail = (await client.get(f"/api/v1/products/{product['id']}", headers=callers.admin)).json()
    assert detail["status"] == "publishing"
