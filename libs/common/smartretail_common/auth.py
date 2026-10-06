"""Local RS256 JWT verification and role guards (RFC 0001 §3.1.2.1, PLT-9).

Every service verifies tokens itself (RFC 0000 A5); the gateway's check is an
early rejection, never the authorisation decision. Ownership and merchant
scoping are enforced in each service's service layer, not here.
"""

import json
import logging
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import jwt
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from fastapi import Request
from jwt.algorithms import RSAAlgorithm

from smartretail_common.context import set_user_id
from smartretail_common.errors import Forbidden, Unauthorized

logger = logging.getLogger(__name__)

ALGORITHM = "RS256"
# Role carried by a merchant token; the claim value is opaque to this library
# (R8) except for deriving ``merchant_id``, which RFC 0000 I4 defines as the
# subject of a merchant token.
_MERCHANT_ROLE_CLAIM = "merchant"


class InvalidToken(Unauthorized):
    type_slug = "invalid-token"
    title = "Invalid or missing token"

    def __init__(self, detail: str = "invalid") -> None:
        error = "" if detail == "missing" else ' error="invalid_token"'
        super().__init__(detail, headers={"WWW-Authenticate": f"Bearer{error}"})


@dataclass(frozen=True)
class Principal:
    subject: str
    role: str | None
    token_type: str
    merchant_id: str | None
    merchant_status: str | None
    session_id: str | None
    expires_at: int
    key_id: str | None
    claims: Mapping[str, Any]

    @property
    def is_service(self) -> bool:
        return self.token_type == "service"


def load_public_key(path: str | Path) -> RSAPublicKey:
    key = load_pem_public_key(Path(path).read_bytes())
    if not isinstance(key, RSAPublicKey):
        raise ValueError(f"{path} is not an RSA public key")
    return key


class JwtVerifier:
    """Verifies access tokens against a key set selected by ``kid``.

    Keys come from a static set (the mounted public key, or identity's own
    key ring) and optionally from a JWKS endpoint. An unknown ``kid``
    triggers a throttled JWKS refetch, then falls back to the static key; the
    signature check still decides.
    """

    def __init__(
        self,
        *,
        issuer: str,
        audience: str,
        leeway_seconds: int = 10,
        static_keys: Mapping[str | None, RSAPublicKey] | None = None,
        jwks_url: str | None = None,
        min_refetch_seconds: int = 30,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.issuer = issuer
        self.audience = audience
        self.leeway = leeway_seconds
        self._static: dict[str | None, RSAPublicKey] = dict(static_keys or {})
        self._jwks: dict[str, RSAPublicKey] = {}
        self.jwks_url = jwks_url
        self.min_refetch = min_refetch_seconds
        self._last_fetch = 0.0
        self._http = http_client
        if not self._static and not jwks_url:
            raise ValueError("JwtVerifier needs a static key or a JWKS URL")

    @classmethod
    def from_settings(cls, settings: Any) -> "JwtVerifier":
        static: dict[str | None, RSAPublicKey] = {}
        if settings.jwt_public_key_path and Path(settings.jwt_public_key_path).exists():
            static[None] = load_public_key(settings.jwt_public_key_path)
        return cls(
            issuer=settings.jwt_issuer,
            audience=settings.jwt_audience,
            leeway_seconds=settings.jwt_leeway_seconds,
            static_keys=static,
            jwks_url=settings.jwks_url,
            min_refetch_seconds=settings.jwks_min_refetch_seconds,
        )

    @property
    def has_keys(self) -> bool:
        return bool(self._static or self._jwks)

    async def refresh_jwks(self, *, force: bool = False) -> bool:
        if not self.jwks_url:
            return False
        now = time.monotonic()
        if not force and now - self._last_fetch < self.min_refetch:
            return False
        self._last_fetch = now
        try:
            client = self._http or httpx.AsyncClient(timeout=2.0)
            try:
                response = await client.get(self.jwks_url)
                response.raise_for_status()
                document = response.json()
            finally:
                if self._http is None:
                    await client.aclose()
            keys: dict[str, RSAPublicKey] = {}
            for jwk in document.get("keys", []):
                if jwk.get("kty") == "RSA" and jwk.get("kid"):
                    key = RSAAlgorithm.from_jwk(json.dumps(jwk))
                    if isinstance(key, RSAPublicKey):
                        keys[jwk["kid"]] = key
            if keys:
                self._jwks = keys
            return True
        except (httpx.HTTPError, ValueError) as exc:
            # A failed refresh keeps the keys we already have.
            logger.warning("jwks refresh failed", extra={"error": str(exc)})
            return False

    async def _key_for(self, kid: str | None) -> RSAPublicKey:
        if kid is not None:
            if kid in self._jwks:
                return self._jwks[kid]
            if kid in self._static:
                return self._static[kid]
            if self.jwks_url and await self.refresh_jwks() and kid in self._jwks:
                return self._jwks[kid]
        if None in self._static:
            return self._static[None]
        raise InvalidToken("invalid")

    async def verify(self, token: str) -> Principal:
        try:
            header = jwt.get_unverified_header(token)
        except jwt.PyJWTError as exc:
            raise InvalidToken("invalid") from exc
        # Pin the algorithm before any key lookup: rejects "none" and HS256.
        if header.get("alg") != ALGORITHM:
            raise InvalidToken("invalid")
        kid = header.get("kid")
        key = await self._key_for(kid)
        try:
            claims = jwt.decode(
                token,
                key,
                algorithms=[ALGORITHM],
                audience=self.audience,
                issuer=self.issuer,
                leeway=self.leeway,
                options={"require": ["exp", "iat", "sub", "iss", "aud"]},
            )
        except jwt.ExpiredSignatureError as exc:
            raise InvalidToken("expired") from exc
        except jwt.PyJWTError as exc:
            raise InvalidToken("invalid") from exc

        subject = str(claims["sub"])
        token_type = "service" if subject.startswith("service:") else "user"
        if token_type == "user" and claims.get("typ") != "access":
            raise InvalidToken("invalid")
        role = claims.get("role")
        return Principal(
            subject=subject,
            role=role,
            token_type=token_type,
            merchant_id=subject if role == _MERCHANT_ROLE_CLAIM else None,
            merchant_status=claims.get("merchant_status"),
            session_id=claims.get("sid"),
            expires_at=int(claims["exp"]),
            key_id=kid,
            claims=claims,
        )


def bearer_token(request: Request) -> str | None:
    header = request.headers.get("authorization")
    if not header:
        return None
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise InvalidToken("invalid")
    return token.strip()


async def optional_principal(request: Request) -> Principal | None:
    """No token → ``None``; a present but invalid token → 401."""
    cached = getattr(request.state, "principal", None)
    if cached is not None:
        return cached
    token = bearer_token(request)
    if token is None:
        return None
    verifier: JwtVerifier = request.app.state.verifier
    principal = await verifier.verify(token)
    request.state.principal = principal
    set_user_id(principal.subject)
    return principal


async def current_principal(request: Request) -> Principal:
    principal = await optional_principal(request)
    if principal is None:
        raise InvalidToken("missing")
    return principal


def require_roles(*roles: str) -> Callable[[Request], Awaitable[Principal]]:
    """Dependency: an authenticated user whose role is in ``roles``.

    Service tokens are always refused here; internal routes use scopes (M2).
    """
    allowed = frozenset(roles)

    async def dependency(request: Request) -> Principal:
        principal = await current_principal(request)
        if principal.is_service or principal.role not in allowed:
            raise Forbidden("This action is not permitted for your role")
        return principal

    return dependency
