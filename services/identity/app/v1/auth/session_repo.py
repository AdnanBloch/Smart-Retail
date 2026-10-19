"""Refresh rotation, reuse detection and logout (RFC 0003 §3.1.4.3).

Rotated and revoked sessions are indistinguishable, so presenting any revoked
token is treated as reuse and revokes every live session of the user.
"""

import uuid
from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import InvalidRefreshToken
from app.core.security import hash_refresh_token
from app.v1.auth.authentication_repo import ensure_may_hold_tokens
from app.v1.auth.models import RefreshSession
from app.v1.auth.token_repo import IssuedTokens, TokenRepo
from app.v1.users.models import User
from smartretail_common.errors import DomainError


async def revoke_all_sessions(
    session: AsyncSession, user_id: uuid.UUID, *, except_id: uuid.UUID | None = None
) -> None:
    stmt = (
        update(RefreshSession)
        .where(RefreshSession.user_id == user_id, RefreshSession.revoked_at.is_(None))
        .values(revoked_at=datetime.now(UTC))
    )
    if except_id is not None:
        stmt = stmt.where(RefreshSession.id != except_id)
    await session.execute(stmt)


class SessionRepo:
    def __init__(self, session: AsyncSession, issuer: TokenRepo) -> None:
        self.session = session
        self.issuer = issuer

    async def refresh(self, raw: str, *, user_agent: str | None, ip: str | None) -> IssuedTokens:
        now = datetime.now(UTC)
        current = await self.session.scalar(
            select(RefreshSession)
            .where(RefreshSession.token_hash == hash_refresh_token(raw))
            .with_for_update()
        )
        if current is None:
            raise InvalidRefreshToken()
        if current.revoked_at is not None:
            await revoke_all_sessions(self.session, current.user_id)
            await self.session.commit()
            raise InvalidRefreshToken()
        if current.expires_at <= now:
            raise InvalidRefreshToken()

        user = await self.session.get(User, current.user_id)
        if user is None:
            raise InvalidRefreshToken()
        try:
            ensure_may_hold_tokens(user)
        except DomainError as exc:
            current.revoked_at = now
            await self.session.commit()
            raise InvalidRefreshToken() from exc

        current.revoked_at = now
        successor, new_raw = self.issuer.new_session(user.id, user_agent=user_agent, ip=ip)
        self.session.add(successor)
        await self.session.commit()
        return self.issuer.pair(user, successor, new_raw)

    async def logout(self, user_id: uuid.UUID, raw: str) -> None:
        """Revokes the token if it belongs to the caller; otherwise a no-op."""
        await self.session.execute(
            update(RefreshSession)
            .where(
                RefreshSession.token_hash == hash_refresh_token(raw),
                RefreshSession.user_id == user_id,
                RefreshSession.revoked_at.is_(None),
            )
            .values(revoked_at=datetime.now(UTC))
        )
        await self.session.commit()
