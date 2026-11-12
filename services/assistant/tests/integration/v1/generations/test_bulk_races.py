"""Bulk generation and token budgets under concurrency and redelivery (E10,
E12, AST-D23, AST-D34).

Shared resources: the ``(merchant_id, idempotency_key)`` claim, the job row
(cancel vs. worker vs. a second delivery), the ``(bulk_job_id, product_id)``
unique, and the ``token_budgets`` row every request charges.
"""

import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

import app.v1.generations.bulk_repo as bulk
from app.core.clients.llm import LLMError, LLMRequest, LLMResult, ScriptedProvider
from app.core.enums import ContentType
from app.core.errors import IdempotencyKeyReused, LLMUnavailable
from app.v1.chat.models import Conversation
from app.v1.generations.budget_repo import GLOBAL, BudgetRepo
from app.v1.generations.bulk_repo import BulkJobRepo, request_hash, run_bulk_job
from app.v1.generations.enums import BulkJobStatus
from app.v1.generations.models import BulkGenerationJob, Generation, TokenBudget
from app.v1.generations.repo import GenerationParams, ProductSnapshot
from tests.integration.conftest import seed_catalogue
from tests.integration.v1.generations.test_generations import GEN, count, submit

SYNC = "/api/v1/assistant/chat/sync"


@pytest.fixture
def no_backoff(monkeypatch) -> list[float]:
    slept: list[float] = []

    async def record(delay: float) -> None:
        slept.append(delay)

    monkeypatch.setattr(bulk.asyncio, "sleep", record)
    return slept


async def job_of(app, job_id: uuid.UUID) -> BulkGenerationJob:
    async with app.state.db.sessionmaker() as s:
        job = await s.get(BulkGenerationJob, job_id)
        assert job is not None
        return job


async def generated_products(app, job_id: uuid.UUID) -> list[uuid.UUID]:
    async with app.state.db.sessionmaker() as s:
        return list(
            (
                await s.scalars(
                    select(Generation.product_id).where(Generation.bulk_job_id == job_id)
                )
            ).all()
        )


class Recording(ScriptedProvider):
    """Scripted output; records product names, can fail or crash per name."""

    def __init__(
        self,
        *,
        reject: str | None = None,
        crash: str | None = None,
        flaky: str | None = None,
        delay: float = 0.0,
    ) -> None:
        super().__init__()
        self.reject, self.crash, self.flaky, self.delay = reject, crash, flaky, delay
        self.names: list[str] = []

    async def complete(self, request: LLMRequest) -> LLMResult:
        name = request.hints["product"]["name"]
        self.names.append(name)
        if self.delay:
            await asyncio.sleep(self.delay)
        if name == self.crash:
            raise RuntimeError("worker killed")
        if name == self.reject:
            raise LLMError("rejected by the provider")
        if name == self.flaky and self.names.count(name) == 1:
            raise LLMUnavailable("overloaded")
        return await super().complete(request)


class CancelOnSession:
    """A ``Database`` stand-in that cancels the job just before the worker opens
    its ``at``-th session: a merchant's cancel landing between two steps."""

    def __init__(self, db, job_id: uuid.UUID, merchant_id: str, at: int) -> None:
        self.db, self.job_id, self.merchant_id, self.at = db, job_id, merchant_id, at
        self.opened = 0

    @asynccontextmanager
    async def _cancel_then_open(self):
        async with self.db.sessionmaker() as s:
            await BulkJobRepo(s).cancel(self.job_id, self.merchant_id)
        async with self.db.sessionmaker() as session:
            yield session

    def sessionmaker(self):
        self.opened += 1
        if self.opened == self.at:
            return self._cancel_then_open()
        return self.db.sessionmaker()


# --- submission ---------------------------------------------------------------------------


async def test_concurrent_submissions_with_one_key_create_one_job(
    app, client, upstreams, callers, enqueued
):
    cat = await seed_catalogue(app, upstreams, callers)
    responses = await asyncio.gather(
        *(submit(client, callers, [cat.shoes, cat.boots], key="same") for _ in range(4))
    )
    assert [r.status_code for r in responses] == [202] * 4
    assert len({r.json()["job_id"] for r in responses}) == 1
    assert len(enqueued) == 1  # only the creator enqueues the worker
    assert await count(app, BulkGenerationJob) == 1


async def test_the_losing_insert_returns_the_winner_or_rejects_another_body(app, callers):
    merchant = callers.merchant_a_id
    params = GenerationParams(ContentType.FAQ)
    snaps = [ProductSnapshot(uuid.uuid4(), uuid.UUID(merchant), {"name": "Shoe"})]
    digest = request_hash({"product_ids": [str(snaps[0].product_id)], "content_type": "faq"})

    async with app.state.db.sessionmaker() as s:
        winner, created = await BulkJobRepo(s).create(merchant, "k", digest, params, snaps)
    assert created
    # A request that passed the replay check before the winner committed.
    async with app.state.db.sessionmaker() as s:
        loser, created = await BulkJobRepo(s).create(merchant, "k", digest, params, snaps)
    assert not created and loser.id == winner.id
    async with app.state.db.sessionmaker() as s:
        with pytest.raises(IdempotencyKeyReused):
            await BulkJobRepo(s).create(merchant, "k", "another-body", params, snaps)
    # The key is per merchant.
    async with app.state.db.sessionmaker() as s:
        _, created = await BulkJobRepo(s).create(callers.merchant_b_id, "k", digest, params, snaps)
    assert created
    assert await count(app, BulkGenerationJob) == 2


# --- cancel -------------------------------------------------------------------------------


async def test_two_concurrent_cancels_one_wins(app, client, upstreams, callers):
    cat = await seed_catalogue(app, upstreams, callers)
    job_id = (await submit(client, callers, [cat.shoes])).json()["job_id"]
    url = f"{GEN}/jobs/{job_id}/cancel"
    responses = await asyncio.gather(
        *(client.post(url, headers=callers.merchant_a) for _ in range(3))
    )
    assert sorted(r.status_code for r in responses) == [202, 409, 409]
    assert all(
        r.json()["type"].endswith("/bulk-job-terminal") for r in responses if r.status_code == 409
    )
    job = await job_of(app, uuid.UUID(job_id))
    assert job.status == BulkJobStatus.CANCELLED and job.finished_at is not None


async def test_a_cancel_between_items_stops_before_the_next(
    app, client, upstreams, callers, settings
):
    cat = await seed_catalogue(app, upstreams, callers)
    job_id = uuid.UUID((await submit(client, callers, [cat.shoes, cat.boots])).json()["job_id"])
    provider = Recording()
    # Sessions: 1 claim, 2-3 the first item, 4 the second item's check.
    db = CancelOnSession(app.state.db, job_id, callers.merchant_a_id, at=4)
    assert await run_bulk_job(db, provider, settings, job_id) == BulkJobStatus.CANCELLED
    assert provider.names == ["Acme Runner X1"]
    job = await job_of(app, job_id)
    assert job.status == BulkJobStatus.CANCELLED and job.completed == 1
    assert await generated_products(app, job_id) == [uuid.UUID(cat.shoes)]


async def test_a_cancel_after_the_last_item_wins_over_completion(
    app, client, upstreams, callers, settings
):
    cat = await seed_catalogue(app, upstreams, callers)
    job_id = uuid.UUID((await submit(client, callers, [cat.shoes])).json()["job_id"])
    # Sessions: 1 claim, 2-3 the only item, 4 the finish.
    db = CancelOnSession(app.state.db, job_id, callers.merchant_a_id, at=4)
    assert await run_bulk_job(db, Recording(), settings, job_id) == BulkJobStatus.CANCELLED
    job = await job_of(app, job_id)
    assert job.status == BulkJobStatus.CANCELLED and job.completed == 1


async def test_a_cancel_racing_a_slow_item_discards_it(app, client, upstreams, callers, settings):
    cat = await seed_catalogue(app, upstreams, callers)
    job_id = (await submit(client, callers, [cat.shoes, cat.boots])).json()["job_id"]
    worker = asyncio.create_task(
        run_bulk_job(app.state.db, Recording(delay=0.3), settings, uuid.UUID(job_id))
    )
    await asyncio.sleep(0.1)  # the worker holds no lock while the provider runs
    cancel = await client.post(f"{GEN}/jobs/{job_id}/cancel", headers=callers.merchant_a)
    assert cancel.status_code == 202
    assert await worker == BulkJobStatus.CANCELLED
    assert await generated_products(app, uuid.UUID(job_id)) == []


# --- redelivery -----------------------------------------------------------------------------


async def test_two_concurrent_deliveries_generate_each_item_once(
    app, client, upstreams, callers, settings
):
    """``acks_late`` redelivery while the first worker still runs. The unique
    ``(bulk_job_id, product_id)`` stops a second row; the row lock keeps the
    counters exact; whichever worker survives finishes the job."""
    cat = await seed_catalogue(app, upstreams, callers)
    job_id = uuid.UUID((await submit(client, callers, [cat.shoes, cat.boots])).json()["job_id"])
    outcomes = await asyncio.gather(
        *(run_bulk_job(app.state.db, Recording(delay=0.05), settings, job_id) for _ in range(2)),
        return_exceptions=True,
    )
    for outcome in outcomes:
        assert outcome == BulkJobStatus.COMPLETED or isinstance(outcome, IntegrityError), outcome
    assert BulkJobStatus.COMPLETED in outcomes
    assert sorted(await generated_products(app, job_id)) == sorted(
        [uuid.UUID(cat.shoes), uuid.UUID(cat.boots)]
    )
    job = await job_of(app, job_id)
    assert job.status == BulkJobStatus.COMPLETED
    assert (job.completed, job.failed) == (2, 0)


async def test_a_crashed_worker_is_resumed_by_redelivery(
    app, client, upstreams, callers, settings, no_backoff
):
    """Item one is rejected (no retry), the worker dies on item two; the
    redelivered task resumes the running job and touches only item two."""
    cat = await seed_catalogue(app, upstreams, callers)
    job_id = uuid.UUID((await submit(client, callers, [cat.shoes, cat.boots])).json()["job_id"])
    first = Recording(reject="Acme Runner X1", crash="Trail Boot Pro")
    with pytest.raises(RuntimeError, match="worker killed"):
        await run_bulk_job(app.state.db, first, settings, job_id)
    assert first.names == ["Acme Runner X1", "Trail Boot Pro"]  # a rejection is not retried
    assert no_backoff == []
    job = await job_of(app, job_id)
    assert job.status == BulkJobStatus.RUNNING and job.failed == 1

    second = Recording()
    status = await run_bulk_job(app.state.db, second, settings, job_id)
    assert status == BulkJobStatus.PARTIALLY_FAILED
    assert second.names == ["Trail Boot Pro"]
    job = await job_of(app, job_id)
    assert (job.completed, job.failed) == (1, 1)
    assert "rejected" in job.item_errors[cat.shoes]
    assert await generated_products(app, job_id) == [uuid.UUID(cat.boots)]


async def test_a_transient_provider_error_is_retried_with_backoff(
    app, client, upstreams, callers, settings, no_backoff
):
    cat = await seed_catalogue(app, upstreams, callers)
    job_id = uuid.UUID((await submit(client, callers, [cat.shoes])).json()["job_id"])
    provider = Recording(flaky="Acme Runner X1")
    assert await run_bulk_job(app.state.db, provider, settings, job_id) == BulkJobStatus.COMPLETED
    assert provider.names == ["Acme Runner X1"] * 2 and no_backoff == [0.5]


# --- token budgets ----------------------------------------------------------------------------


async def test_concurrent_charges_are_never_lost(app, settings):
    scope = f"user:{uuid.uuid4()}"

    async def charge(tokens: int) -> None:
        async with app.state.db.sessionmaker() as s, s.begin():
            await BudgetRepo(s, settings).charge(scope, tokens, cost=0)

    spends = list(range(1, 21))
    await asyncio.gather(*(charge(t) for t in spends))
    async with app.state.db.sessionmaker() as s:
        repo = BudgetRepo(s, settings)
        mine, everyone = await repo.usage(scope), await repo.usage(GLOBAL)
    assert (mine.tokens_used, mine.requests) == (sum(spends), len(spends))
    assert (everyone.tokens_used, everyone.requests) == (sum(spends), len(spends))


async def test_concurrent_chat_turns_charge_exactly_and_then_stop(
    app, client, upstreams, callers, settings
):
    """Turns racing on one budget each charge what they spent, no update is
    lost; check-then-charge lets the racing turns through, and once the limit
    is crossed every later turn is refused (§3.1.1.2: overshoot is bounded by
    the in-flight turns' spend)."""
    await seed_catalogue(app, upstreams, callers)
    scope = f"user:{callers.customer_id}"
    start = settings.daily_token_budget_user - 1
    async with app.state.db.sessionmaker() as s, s.begin():
        s.add(TokenBudget(scope=scope, date=datetime.now(UTC).date(), tokens_used=start))

    turns = await asyncio.gather(
        *(
            client.post(SYNC, json={"message": "running shoes"}, headers=callers.customer)
            for _ in range(5)
        )
    )
    served = [r for r in turns if r.status_code == 200]
    assert served and all(r.status_code in (200, 429) for r in turns)
    async with app.state.db.sessionmaker() as s:
        usage = await BudgetRepo(s, settings).usage(scope)
        spent = sum(
            (
                await s.scalars(
                    select(Conversation.total_tokens).where(
                        Conversation.user_id == uuid.UUID(callers.customer_id)
                    )
                )
            ).all()
        )
    assert usage.tokens_used == start + spent and usage.requests == len(served)
    after = await client.post(SYNC, json={"message": "running shoes"}, headers=callers.customer)
    assert after.status_code == 429 and after.json()["scope"] == "user"
