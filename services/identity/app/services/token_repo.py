"""Access-token issuance (RFC 0003 IDN-6).

Claims never include email, name, phone or is_active.
"""

import time
import uuid
from datetime import UTC, datetime, timedelta

from app.configs.env import Settings
from app.infrastructure.security import KeyRing, new_refresh_token
from app.models import RefreshSession, User
from app.models.auth import TokenPair
from app.models.enums import TokenType, UserRole


class TokenRepo:
    def __init__(self, settings: Settings, keyring: KeyRing) -> None:
        self.settings = settings
        self.keyring = keyring

    def access_token(self, user: User, session_id: uuid.UUID) -> str:
        now = int(time.time())
        claims: dict[str, object] = {
            "iss": self.settings.jwt_issuer,
            "aud": self.settings.jwt_audience,
            "sub": str(user.id),
            "typ": TokenType.ACCESS.value,
            "role": str(user.role),
            "sid": str(session_id),
            "iat": now,
            "nbf": now,
            "exp": now + self.settings.access_token_ttl_seconds,
            "jti": str(uuid.uuid4()),
        }
        if user.role == UserRole.MERCHANT and user.merchant_profile is not None:
            claims["merchant_status"] = str(user.merchant_profile.status)
        return self.keyring.sign(claims)

    def new_session(
        self, user_id: uuid.UUID, *, user_agent: str | None, ip: str | None
    ) -> tuple[RefreshSession, str]:
        raw, token_hash = new_refresh_token()
        session = RefreshSession(
            id=uuid.uuid4(),
            user_id=user_id,
            token_hash=token_hash,
            expires_at=datetime.now(UTC)
            + timedelta(seconds=self.settings.refresh_token_ttl_seconds),
            user_agent=(user_agent or "")[:512] or None,
            ip=ip,
        )
        return session, raw

    def pair(self, user: User, session: RefreshSession, raw_refresh: str) -> TokenPair:
        return TokenPair(
            access_token=self.access_token(user, session.id),
            refresh_token=raw_refresh,
            expires_in=self.settings.access_token_ttl_seconds,
        )
