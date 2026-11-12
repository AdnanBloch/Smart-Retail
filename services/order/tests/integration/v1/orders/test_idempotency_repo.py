"""``IdempotencyRepo`` directly (ORD-7, ORD-8): the in-progress lease is the
configured one, and a claim released mid-flight is claimed afresh."""

import asyncio
import uuid

import asyncpg
import pytest

from app.v1.orders.enums import IdempotencyState
from app.v1.orders.idempotency_repo import (
    IdempotencyKeyReused,
    IdempotencyRepo,
    RequestInProgress,
    StoredReply,
)


async def _claim(app, user, key, digest):
    async with app.state.db.sessionmaker() as s:
        return await IdempotencyRepo(s, app.state.settings).claim(user, key, digest)


async def test_the_in_progress_lease_is_the_configured_one(app) -> None:
    app.state.settings.idempotency_in_progress_lease_seconds = 1
    user = uuid.uuid4()
    assert await _claim(app, user, "K", "h") is None
    with pytest.raises(RequestInProgress):
        await _claim(app, user, "K", "h")
    await asyncio.sleep(1.2)
    assert await _claim(app, user, "K", "h") is None  # the first holder's lease ran out
    with pytest.raises(RequestInProgress):  # and the takeover holds a fresh lease
        await _claim(app, user, "K", "h")


async def test_a_completed_key_replays_and_rejects_another_body(app) -> None:
    user = uuid.uuid4()
    async with app.state.db.sessionmaker() as s:
        repo = IdempotencyRepo(s, app.state.settings)
        assert await repo.claim(user, "K", "h") is None
        await repo.complete(user, "K", StoredReply(202, {"order_id": "o-1"}))
        await s.commit()
        # Releasing a completed key is a no-op: only in-progress claims are forgotten.
        await repo.release(user, "K")
    assert await _claim(app, user, "K", "h") == StoredReply(202, {"order_id": "o-1"})
    with pytest.raises(IdempotencyKeyReused):
        await _claim(app, user, "K", "other")


async def test_a_claim_released_between_insert_and_read_is_claimed_again(app, database_url) -> None:
    """The holder released its claim after our insert lost but before our read."""
    user = uuid.uuid4()
    assert await _claim(app, user, "K", "theirs") is None
    url = database_url.replace("postgresql+asyncpg://", "postgresql://", 1)

    async with app.state.db.sessionmaker() as s:
        repo = IdempotencyRepo(s, app.state.settings)
        commit = s.commit
        released = False

        async def commit_then_release() -> None:
            nonlocal released
            await commit()
            if not released:  # right after the conflicting insert
                released = True
                conn = await asyncpg.connect(url)
                try:
                    await conn.execute("DELETE FROM idempotency_keys WHERE user_id = $1", user)
                finally:
                    await conn.close()

        s.commit = commit_then_release  # type: ignore[method-assign]
        assert await repo.claim(user, "K", "mine") is None

    conn = await asyncpg.connect(url)
    try:
        row = await conn.fetchrow("SELECT request_hash, state FROM idempotency_keys")
    finally:
        await conn.close()
    assert (row["request_hash"], row["state"]) == ("mine", IdempotencyState.IN_PROGRESS.value)
