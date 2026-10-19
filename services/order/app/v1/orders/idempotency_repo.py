"""``Idempotency-Key`` claims for order creation (RFC 0005 §3.1.1.4, B7, ORD-7, ORD-8).

A key is claimed by a unique insert — never read-then-write, which has a race
window exactly as wide as burst traffic. The database arbitrates:

* claimed           → this request owns the key and creates the order
* completed         → the stored reply is replayed verbatim
* in progress       → 409 ``Retry-After: 1``; never a second order
* other body        → 422; the existing order is untouched
* stale or expired  → reclaimed by a conditional UPDATE only one contender wins

Business rejections after the claim are completed and replayed like a success;
dependency failures release the claim so a retry can proceed.
"""

import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from sqlalchemy import and_, delete, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.v1.orders.enums import IdempotencyState
from app.v1.orders.models import IdempotencyKey
from smartretail_common.errors import Conflict, Unprocessable


class IdempotencyKeyReused(Unprocessable):
    type_slug = "idempotency-key-reused"
    title = "This Idempotency-Key was used with a different request"


class RequestInProgress(Conflict):
    type_slug = "request-in-progress"
    title = "A request with this Idempotency-Key is in progress"

    def __init__(self) -> None:
        super().__init__("Retry shortly to receive its result", headers={"Retry-After": "1"})


@dataclass(frozen=True)
class StoredReply:
    status: int
    body: dict[str, Any]


class IdempotencyRepo:
    def __init__(self, session: AsyncSession, settings: Settings) -> None:
        self.session = session
        self.settings = settings

    def _where(self, user_id: uuid.UUID, key: str) -> Any:
        return and_(IdempotencyKey.user_id == user_id, IdempotencyKey.key == key)

    async def claim(self, user_id: uuid.UUID, key: str, request_hash: str) -> StoredReply | None:
        """``None`` when this request now owns the key; otherwise the reply to replay."""
        ttl = timedelta(hours=self.settings.idempotency_ttl_hours)
        claimed = await self.session.scalar(
            pg_insert(IdempotencyKey)
            .values(
                user_id=user_id,
                key=key,
                request_hash=request_hash,
                state=IdempotencyState.IN_PROGRESS,
                expires_at=func.now() + ttl,
            )
            .on_conflict_do_nothing(index_elements=["user_id", "key"])
            .returning(IdempotencyKey.key)
        )
        await self.session.commit()
        if claimed is not None:
            return None

        existing = await self.session.scalar(
            select(IdempotencyKey)
            .where(self._where(user_id, key))
            .execution_options(populate_existing=True)
        )
        if existing is None:  # released between our insert and read: claim again
            return await self.claim(user_id, key, request_hash)
        now = await self.session.scalar(select(func.now()))
        assert now is not None
        if existing.expires_at <= now:
            # Expired rows are treated as absent (TTL): any body may reclaim the key.
            if await self._reclaim(user_id, key, request_hash, expired=True):
                return None
            raise RequestInProgress()
        if existing.request_hash != request_hash:
            raise IdempotencyKeyReused("The existing order is untouched")
        if existing.state == IdempotencyState.COMPLETED:
            assert existing.response_status is not None and existing.response_body is not None
            return StoredReply(existing.response_status, existing.response_body)
        lease = timedelta(seconds=self.settings.idempotency_in_progress_lease_seconds)
        if existing.created_at < now - lease and await self._reclaim(
            user_id, key, request_hash, expired=False
        ):
            return None  # ORD-8: the request that held it died
        raise RequestInProgress()

    async def _reclaim(
        self, user_id: uuid.UUID, key: str, request_hash: str, *, expired: bool
    ) -> bool:
        """A conditional UPDATE only one contender can win."""
        lease = timedelta(seconds=self.settings.idempotency_in_progress_lease_seconds)
        stale = and_(
            IdempotencyKey.state == IdempotencyState.IN_PROGRESS,
            IdempotencyKey.created_at < func.now() - lease,
        )
        result = await self.session.execute(
            update(IdempotencyKey)
            .where(
                self._where(user_id, key),
                or_(IdempotencyKey.expires_at <= func.now(), stale) if expired else stale,
            )
            .values(
                request_hash=request_hash,
                state=IdempotencyState.IN_PROGRESS,
                order_id=None,
                response_status=None,
                response_body=None,
                created_at=func.now(),
                expires_at=func.now() + timedelta(hours=self.settings.idempotency_ttl_hours),
            )
        )
        await self.session.commit()
        return (result.rowcount or 0) == 1

    async def complete(
        self,
        user_id: uuid.UUID,
        key: str,
        reply: StoredReply,
        *,
        order_id: uuid.UUID | None = None,
    ) -> None:
        """Records the reply in the caller's transaction (the order's, on success)."""
        await self.session.execute(
            update(IdempotencyKey)
            .where(self._where(user_id, key))
            .values(
                state=IdempotencyState.COMPLETED,
                response_status=reply.status,
                response_body=reply.body,
                order_id=order_id,
            )
        )

    async def release(self, user_id: uuid.UUID, key: str) -> None:
        """A dependency failed: forget the claim so a retry can proceed."""
        await self.session.rollback()
        await self.session.execute(
            delete(IdempotencyKey).where(
                self._where(user_id, key), IdempotencyKey.state == IdempotencyState.IN_PROGRESS
            )
        )
        await self.session.commit()
