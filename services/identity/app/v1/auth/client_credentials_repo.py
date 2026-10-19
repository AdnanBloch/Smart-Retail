"""Client-credentials grant for internal callers (RFC 0003 IDN-T3, IDN-12).

Temporal activities have no user in context, and a workflow outlives the
user's token, so ``temporal-worker`` authenticates as itself. The registry is
configuration (``SERVICE_CLIENTS``); nothing is stored in ``identity_db``.

Secrets are compared against a stored hash: ``$argon2id$…`` hashes are
verified with the password hasher, and ``sha256:<hex>`` digests in constant
time. A client secret is 256 random bits, so a fast digest is as safe as a
slow KDF for it, and ``make keys`` can produce one with openssl alone.
"""

import hashlib
import hmac
from dataclasses import dataclass

from app.core.config import Settings
from app.core.errors import OAuthError
from app.core.security import PasswordHasher
from app.v1.auth.token_repo import TokenRepo

GRANT_TYPE = "client_credentials"
SHA256_PREFIX = "sha256:"


@dataclass(frozen=True)
class IssuedServiceToken:
    access_token: str
    expires_in: int
    scope: str


class ClientCredentialsRepo:
    def __init__(self, settings: Settings, hasher: PasswordHasher, issuer: TokenRepo) -> None:
        self.settings = settings
        self.hasher = hasher
        self.issuer = issuer

    async def issue(
        self, *, grant_type: str, client_id: str, client_secret: str, scope: str | None
    ) -> IssuedServiceToken:
        if grant_type != GRANT_TYPE:
            raise OAuthError(400, "unsupported_grant_type")
        client = self.settings.service_clients.get(client_id)
        if client is None:
            await self.hasher.verify_dummy(client_secret)  # same timing as a known client
            raise OAuthError(401, "invalid_client")
        if not await self._secret_matches(client.secret_hash, client_secret):
            raise OAuthError(401, "invalid_client")

        granted = list(client.scopes)
        if scope:
            requested = scope.split()
            if not set(requested) <= set(granted):
                raise OAuthError(400, "invalid_scope")
            granted = requested
        return IssuedServiceToken(
            access_token=self.issuer.service_token(client_id, granted),
            expires_in=self.settings.service_token_ttl_seconds,
            scope=" ".join(granted),
        )

    async def _secret_matches(self, stored: str, secret: str) -> bool:
        if stored.startswith(SHA256_PREFIX):
            digest = hashlib.sha256(secret.encode()).hexdigest()
            return hmac.compare_digest(digest, stored.removeprefix(SHA256_PREFIX).lower())
        return await self.hasher.verify(stored, secret)
