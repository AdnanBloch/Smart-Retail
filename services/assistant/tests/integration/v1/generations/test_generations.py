"""Merchant generation, bulk jobs, usage and prompt seeding (§3.1.2.1, §3.1.1.4)."""

import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select

from app.core.clients.llm import LLMRequest, LLMResult, ScriptedProvider
from app.core.enums import ContentType
from app.core.errors import LLMUnavailable
from app.seed import seed
from app.v1.generations.bulk_repo import run_bulk_job
from app.v1.generations.enums import BulkJobStatus
from app.v1.generations.models import BulkGenerationJob, Generation, TokenBudget
from app.v1.prompts.models import PromptTemplate
from app.v1.prompts.repo import PromptRepo, seed_templates
from smartretail_common.outbox import OutboxEvent
from tests.integration.conftest import seed_catalogue
from tests.integration.v1.chat.test_chat import parse_sse

GEN = "/api/v1/assistant/generate"


async def count(app, model, *where) -> int:
    async with app.state.db.sessionmaker() as s:
        return int(await s.scalar(select(func.count()).select_from(model).where(*where)))


@pytest.mark.parametrize("content_type", list(ContentType))
async def test_sync_generation_per_content_type(app, client, upstreams, callers, content_type):
    cat = await seed_catalogue(app, upstreams, callers)
    r = await client.post(
        f"{GEN}/sync",
        json={
            "product_id": cat.shoes,
            "content_type": content_type,
            "tone": "playful",
            "keywords": ["breathable"],
        },
        headers=callers.merchant_a,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["content_type"] == content_type and "Acme Runner X1" in body["content"]
    assert body["finish_reason"] == "stop" and body["usage"]["model"] == "scripted"
    async with app.state.db.sessionmaker() as s:
        row = await s.get(Generation, uuid.UUID(body["generation_id"]))
        event = (await s.scalars(select(OutboxEvent))).one()
    assert row.applied is False and row.parameters["tone"] == "playful"
    assert row.parameters["prompt_version"] == 0  # file fallback before seeding
    assert event.payload["kind"] == "generate" and event.payload["content_type"] == content_type
    assert str(event.aggregate_id) == body["generation_id"]
    assert event.payload["merchant_id"] == callers.merchant_a_id


async def test_cross_sell_uses_same_merchant_siblings(app, client, upstreams, callers, llm):
    cat = await seed_catalogue(app, upstreams, callers)
    r = await client.post(
        f"{GEN}/sync",
        json={"product_id": cat.shoes, "content_type": "cross_sell"},
        headers=callers.merchant_a,
    )
    assert r.status_code == 200
    request = llm.requests[-1]
    assert request.hints["siblings"] == ["Trail Boot Pro"]  # not merchant B's blender
    assert "- Trail Boot Pro" in request.messages[0]["content"]


async def test_ownership_and_catalog_outage(app, client, upstreams, callers):
    cat = await seed_catalogue(app, upstreams, callers)
    body = {"product_id": cat.blender, "content_type": "description"}
    other = await client.post(f"{GEN}/sync", json=body, headers=callers.merchant_a)
    assert other.status_code == 403 and other.json()["type"].endswith("/not-product-owner")
    stream = await client.post(GEN, json=body, headers=callers.merchant_a)
    assert stream.status_code == 403
    draft = str(uuid.uuid4())
    upstreams.add_product(draft, callers.merchant_b_id, name="Draft Thing", status="draft")
    hidden = await client.post(
        f"{GEN}/sync", json={**body, "product_id": draft}, headers=callers.merchant_a
    )
    assert hidden.status_code == 404
    mine = await client.post(
        f"{GEN}/sync", json={**body, "product_id": draft}, headers=callers.merchant_b
    )
    assert mine.status_code == 200  # drafts are generable by their owner
    upstreams.catalog_down = True
    down = await client.post(f"{GEN}/sync", json=body, headers=callers.merchant_b)
    assert down.status_code == 503 and "retry-after" in down.headers
    for who in (callers.customer, callers.admin):
        assert (await client.post(GEN, json=body, headers=who)).status_code == 403
    assert (await client.post(GEN, json=body)).status_code == 401
    bad = await client.post(
        f"{GEN}/sync", json={**body, "content_type": "poem"}, headers=callers.merchant_b
    )
    assert bad.status_code == 422


async def test_streamed_generation_and_failures(app, client, upstreams, callers):
    cat = await seed_catalogue(app, upstreams, callers)
    body = {"product_id": cat.shoes, "content_type": "description"}
    r = await client.post(GEN, json=body, headers=callers.merchant_a)
    events = parse_sse(r.text)
    names = [n for n, _ in events]
    assert names[0] == "status" and names[-2:] == ["usage", "done"]
    assert set(names[1:-2]) == {"token"}
    generation_id = events[-1][1]["generation_id"]
    assert await count(app, Generation, Generation.id == uuid.UUID(generation_id)) == 1

    app.state.generator.provider = ScriptedProvider(fail_after_tokens=2)
    mid = parse_sse((await client.post(GEN, json=body, headers=callers.merchant_a)).text)
    assert [n for n, _ in mid][-1] == "error"
    app.state.generator.provider = ScriptedProvider(fail_before=frozenset({"generate"}))
    early = parse_sse((await client.post(GEN, json=body, headers=callers.merchant_a)).text)
    assert [n for n, _ in early] == ["status", "error"]
    assert early[-1][1]["status"] == 503
    sync = await client.post(f"{GEN}/sync", json=body, headers=callers.merchant_a)
    assert sync.status_code == 503
    assert await count(app, Generation) == 1  # failures store nothing


async def test_output_filter_removes_contact_details(app, client, upstreams, callers):
    cat = await seed_catalogue(app, upstreams, callers)

    class Leaky(ScriptedProvider):
        async def complete(self, request: LLMRequest) -> LLMResult:
            return LLMResult("Buy at https://evil.example or mail x@y.io", 5, 5, "scripted", "stop")  # type: ignore[arg-type]

    app.state.generator.provider = Leaky()
    r = await client.post(
        f"{GEN}/sync",
        json={"product_id": cat.shoes, "content_type": "promotional"},
        headers=callers.merchant_a,
    )
    body = r.json()
    assert body["filtered"] is True
    assert "https" not in body["content"] and "@" not in body["content"]


async def test_generations_list_and_applied(app, client, upstreams, callers):
    cat = await seed_catalogue(app, upstreams, callers)
    for ct in ("description", "faq"):
        await client.post(
            f"{GEN}/sync",
            json={"product_id": cat.shoes, "content_type": ct},
            headers=callers.merchant_a,
        )
    listing = await client.get(
        "/api/v1/assistant/generations?content_type=faq", headers=callers.merchant_a
    )
    assert listing.status_code == 200 and listing.json()["total"] == 1
    everything = (
        await client.get(
            f"/api/v1/assistant/generations?product_id={cat.shoes}", headers=callers.merchant_a
        )
    ).json()
    assert everything["total"] == 2
    assert (await client.get("/api/v1/assistant/generations", headers=callers.merchant_b)).json()[
        "total"
    ] == 0
    assert (
        await client.get("/api/v1/assistant/generations", headers=callers.customer)
    ).status_code == 403
    gid = everything["items"][0]["id"]
    applied = f"/api/v1/assistant/generations/{gid}/applied"
    assert (await client.post(applied, headers=callers.merchant_b)).status_code == 404
    assert (await client.post(applied, headers=callers.merchant_a)).status_code == 204
    assert (await client.post(applied, headers=callers.merchant_a)).status_code == 204
    async with app.state.db.sessionmaker() as s:
        assert (await s.get(Generation, uuid.UUID(gid))).applied is True


async def submit(client, callers, product_ids, key="key-1", **extra):
    return await client.post(
        f"{GEN}/bulk",
        json={"product_ids": product_ids, "content_type": "faq", **extra},
        headers={**callers.merchant_a, "Idempotency-Key": key},
    )


async def test_bulk_submission_is_idempotent(app, client, upstreams, callers, enqueued, settings):
    cat = await seed_catalogue(app, upstreams, callers)
    first = await submit(client, callers, [cat.shoes, cat.boots])
    assert first.status_code == 202
    job_id = first.json()["job_id"]
    assert first.headers["location"] == f"/api/v1/assistant/generate/jobs/{job_id}"
    assert first.json()["status"] == "queued"
    replay = await submit(client, callers, [cat.shoes, cat.boots])
    assert replay.status_code == 202 and replay.json()["job_id"] == job_id
    assert enqueued == [("smartretail.assistant.generate_bulk_content", job_id)]
    reused = await submit(client, callers, [cat.shoes])
    assert reused.status_code == 409 and reused.json()["type"].endswith("/idempotency-key-reused")
    no_key = await client.post(
        f"{GEN}/bulk",
        json={"product_ids": [cat.shoes], "content_type": "faq"},
        headers=callers.merchant_a,
    )
    assert no_key.status_code == 422
    not_mine = await submit(client, callers, [cat.shoes, cat.blender], key="key-2")
    assert not_mine.status_code == 403
    too_many = await submit(
        client,
        callers,
        [str(uuid.uuid4()) for _ in range(settings.bulk_max_products + 1)],
        key="key-3",
    )
    assert too_many.status_code == 422
    assert await count(app, BulkGenerationJob) == 1
    async with app.state.db.sessionmaker() as s:
        job = await s.get(BulkGenerationJob, uuid.UUID(job_id))
    assert [p["document"]["name"] for p in job.inputs["products"]] == [
        "Acme Runner X1",
        "Trail Boot Pro",
    ]


async def test_bulk_job_runs_to_completion_and_redelivery_is_a_no_op(
    app, client, upstreams, callers, settings, llm
):
    cat = await seed_catalogue(app, upstreams, callers)
    job_id = (await submit(client, callers, [cat.shoes, cat.boots])).json()["job_id"]
    status = await run_bulk_job(app.state.db, llm, settings, uuid.UUID(job_id))
    assert status == BulkJobStatus.COMPLETED
    body = (await client.get(f"{GEN}/jobs/{job_id}", headers=callers.merchant_a)).json()
    assert body["status"] == "completed" and body["completed"] == 2 and body["total"] == 2
    assert body["finished_at"] is not None and len(body["generations"]) == 2
    assert await count(app, OutboxEvent) == 2
    # A redelivered task does nothing.
    assert await run_bulk_job(app.state.db, llm, settings, uuid.UUID(job_id)) == "completed"
    assert await count(app, Generation) == 2
    assert (await client.get(f"{GEN}/jobs/{job_id}", headers=callers.merchant_b)).status_code == 404
    cancel = await client.post(f"{GEN}/jobs/{job_id}/cancel", headers=callers.merchant_a)
    assert cancel.status_code == 409


class FailsFor(ScriptedProvider):
    def __init__(self, name: str) -> None:
        super().__init__()
        self.name_to_fail = name

    async def complete(self, request: LLMRequest) -> LLMResult:
        if request.hints["product"]["name"] == self.name_to_fail:
            raise LLMUnavailable("down for this one")
        return await super().complete(request)


async def test_bulk_partial_failure_and_resume(
    app, client, upstreams, callers, settings, monkeypatch
):
    import app.v1.generations.bulk_repo as bulk

    async def no_sleep(_):
        return None

    monkeypatch.setattr(bulk.asyncio, "sleep", no_sleep)
    cat = await seed_catalogue(app, upstreams, callers)
    job_id = uuid.UUID((await submit(client, callers, [cat.shoes, cat.boots])).json()["job_id"])
    status = await run_bulk_job(app.state.db, FailsFor("Trail Boot Pro"), settings, job_id)
    assert status == BulkJobStatus.PARTIALLY_FAILED
    body = (await client.get(f"{GEN}/jobs/{job_id}", headers=callers.merchant_a)).json()
    assert body["completed"] == 1 and body["failed"] == 1 and cat.boots in body["errors"]
    all_fail = uuid.UUID((await submit(client, callers, [cat.boots], key="k2")).json()["job_id"])
    assert (
        await run_bulk_job(app.state.db, FailsFor("Trail Boot Pro"), settings, all_fail)
        == BulkJobStatus.FAILED
    )


async def test_bulk_cancel_and_budget_abort(app, client, upstreams, callers, settings, llm):
    cat = await seed_catalogue(app, upstreams, callers)
    job_id = (await submit(client, callers, [cat.shoes, cat.boots])).json()["job_id"]
    cancel = await client.post(f"{GEN}/jobs/{job_id}/cancel", headers=callers.merchant_a)
    assert cancel.status_code == 202 and cancel.json()["status"] == "cancelled"
    assert await run_bulk_job(app.state.db, llm, settings, uuid.UUID(job_id)) == "cancelled"
    assert await count(app, Generation) == 0
    assert (
        await client.post(f"{GEN}/jobs/{uuid.uuid4()}/cancel", headers=callers.merchant_a)
    ).status_code == 404

    # Cancelled mid-run: the worker checks between items.
    second = uuid.UUID(
        (await submit(client, callers, [cat.shoes, cat.boots], key="k2")).json()["job_id"]
    )

    class CancelAfterFirst(ScriptedProvider):
        async def complete(self, request):
            result = await super().complete(request)
            async with app.state.db.sessionmaker() as s, s.begin():
                job = await s.get(BulkGenerationJob, second)
                job.status = BulkJobStatus.CANCELLED
            return result

    assert await run_bulk_job(app.state.db, CancelAfterFirst(), settings, second) == "cancelled"
    assert await count(app, Generation, Generation.bulk_job_id == second) == 0

    third = uuid.UUID((await submit(client, callers, [cat.shoes], key="k3")).json()["job_id"])
    async with app.state.db.sessionmaker() as s, s.begin():
        s.add(
            TokenBudget(
                scope=f"user:{callers.merchant_a_id}",
                date=datetime.now(UTC).date(),
                tokens_used=settings.daily_token_budget_user,
            )
        )
    assert await run_bulk_job(app.state.db, llm, settings, third) == BulkJobStatus.FAILED
    async with app.state.db.sessionmaker() as s:
        job = await s.get(BulkGenerationJob, third)
    assert "budget" in job.item_errors["_job"]
    assert await run_bulk_job(app.state.db, llm, settings, uuid.uuid4()) is None
    blocked = await submit(client, callers, [cat.shoes], key="k4")
    assert blocked.status_code == 429


async def test_usage_endpoints(app, client, upstreams, callers):
    await seed_catalogue(app, upstreams, callers)
    await client.post(
        "/api/v1/assistant/chat/sync", json={"message": "running shoes"}, headers=callers.customer
    )
    mine = await client.get("/api/v1/assistant/usage", headers=callers.customer)
    assert mine.status_code == 200
    body = mine.json()
    assert body["requests"] == 1 and body["tokens_used"] > 0 and body["limit"] == 50000
    assert body["resets_at"].endswith("00:00:00Z") or "T00:00:00" in body["resets_at"]
    assert (await client.get("/api/v1/assistant/usage", headers=callers.merchant_a)).json()[
        "tokens_used"
    ] == 0
    assert (await client.get("/api/v1/assistant/usage")).status_code == 401
    admin = await client.get("/api/v1/assistant/usage/admin", headers=callers.admin)
    assert admin.status_code == 200
    assert admin.json()["global_usage"]["requests"] == 1
    assert [u["scope"] for u in admin.json()["scopes"]] == [f"user:{callers.customer_id}"]
    assert (
        await client.get("/api/v1/assistant/usage/admin", headers=callers.customer)
    ).status_code == 403


async def test_seed_versions_templates(app, settings, tmp_path):
    assert await seed(settings) == 9
    assert await seed(settings) == 0
    (tmp_path / "faq.md").write_text("New FAQ body $product_facts")
    (tmp_path / "notes.txt").write_text("ignored")
    async with app.state.db.sessionmaker() as s, s.begin():
        assert await seed_templates(s, tmp_path) == 1
    async with app.state.db.sessionmaker() as s:
        rows = (
            await s.scalars(
                select(PromptTemplate)
                .where(PromptTemplate.content_type == ContentType.FAQ)
                .order_by(PromptTemplate.version)
            )
        ).all()
        assert [(r.version, r.is_active) for r in rows] == [(1, False), (2, True)]
        repo = PromptRepo(s, tmp_path)
        assert (await repo.active(ContentType.FAQ)).version == 2
        assert (await repo.system("chat")).version == 1
        with pytest.raises(LookupError):
            await PromptRepo(s, tmp_path).active(None, "nonexistent")
