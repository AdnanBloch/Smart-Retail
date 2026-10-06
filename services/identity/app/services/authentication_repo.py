"""Login (RFC 0003 §3.1.4.2).

Unknown email and wrong password produce byte-identical 401s and take the
same time (IDN-17). ``is_active`` and merchant status are only revealed once
the password has verified.
"""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.exception_handlers.exception_handlers import (
    AccountInactive,
    InvalidCredentials,
    MerchantSuspended,
)
from app.infrastructure.security import PasswordHasher
from app.models import User
from app.models.auth import LoginIn, TokenPair
from app.models.enums import MerchantStatus, UserRole
from app.services.token_repo import TokenRepo


def ensure_may_hold_tokens(user: User) -> None:
    """I9: no token for an inactive user or a suspended merchant."""
    if not user.is_active:
        raise AccountInactive()
    if (
        user.role == UserRole.MERCHANT
        and user.merchant_profile is not None
        and user.merchant_profile.status == MerchantStatus.SUSPENDED
    ):
        raise MerchantSuspended()


class AuthenticationRepo:
    def __init__(self, session: AsyncSession, hasher: PasswordHasher, issuer: TokenRepo) -> None:
        self.session = session
        self.hasher = hasher
        self.issuer = issuer

    async def login(self, data: LoginIn, *, user_agent: str | None, ip: str | None) -> TokenPair:
        user = await self.session.scalar(select(User).where(User.email == data.email))
        if user is None:
            await self.hasher.verify_dummy(data.password)
            raise InvalidCredentials()
        if not await self.hasher.verify(user.password_hash, data.password):
            raise InvalidCredentials()
        ensure_may_hold_tokens(user)

        refresh_session, raw = self.issuer.new_session(user.id, user_agent=user_agent, ip=ip)
        self.session.add(refresh_session)
        if self.hasher.needs_rehash(user.password_hash):
            user.password_hash = await self.hasher.hash(data.password)
        await self.session.commit()
        return self.issuer.pair(user, refresh_session, raw)
