"""Helpers for tests: RSA key pairs and token minting.

Kept in the library so every service's suite mints tokens the same way the
identity service does, without depending on identity.
"""

import time
import uuid
from dataclasses import dataclass
from typing import Any

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey

ISSUER = "smartretail-identity"
AUDIENCE = "smartretail"


@dataclass
class KeyPair:
    kid: str
    private_key: RSAPrivateKey

    @property
    def public_key(self) -> RSAPublicKey:
        return self.private_key.public_key()

    def public_pem(self) -> bytes:
        return self.public_key.public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )

    def private_pem(self) -> bytes:
        return self.private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )


def generate_keypair(kid: str | None = None) -> KeyPair:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return KeyPair(kid=kid or f"test-{uuid.uuid4().hex[:8]}", private_key=key)


def mint_token(
    keypair: KeyPair,
    *,
    sub: str | None = None,
    role: str | None = "customer",
    ttl: int = 900,
    issuer: str = ISSUER,
    audience: str = AUDIENCE,
    extra: dict[str, Any] | None = None,
    headers: dict[str, Any] | None = None,
) -> str:
    now = int(time.time())
    claims: dict[str, Any] = {
        "iss": issuer,
        "aud": audience,
        "sub": sub or str(uuid.uuid4()),
        "typ": "access",
        "iat": now,
        "nbf": now,
        "exp": now + ttl,
        "jti": str(uuid.uuid4()),
        "sid": str(uuid.uuid4()),
    }
    if role is not None:
        claims["role"] = role
    if extra:
        claims.update(extra)
    return jwt.encode(
        claims,
        keypair.private_key,
        algorithm="RS256",
        headers={"kid": keypair.kid, **(headers or {})},
    )


# --- integration databases ------------------------------------------------

DEFAULT_TEST_ADMIN_URL = "postgresql://postgres:postgres@127.0.0.1:55432/postgres"


def test_admin_url() -> str:
    import os

    return os.environ.get("TEST_DATABASE_ADMIN_URL", DEFAULT_TEST_ADMIN_URL)


def provision_test_database(name: str, service_dir: str) -> str | None:
    """Recreate ``name`` on the test server and migrate it to head.

    Returns the asyncpg SQLAlchemy URL, or ``None`` if the server is not
    reachable (callers skip integration tests). Run ``make test-db`` first.
    """
    import asyncio
    import os
    import subprocess
    import sys

    import asyncpg

    admin = test_admin_url()

    async def recreate() -> bool:
        try:
            conn = await asyncpg.connect(admin, timeout=3)
        except (OSError, asyncpg.PostgresError):
            return False
        try:
            await conn.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1", name
            )
            await conn.execute(f'DROP DATABASE IF EXISTS "{name}"')
            await conn.execute(f'CREATE DATABASE "{name}"')
        finally:
            await conn.close()
        return True

    if not asyncio.run(recreate()):
        return None
    base, _, _ = admin.rpartition("/")
    url = f"{base.replace('postgresql://', 'postgresql+asyncpg://', 1)}/{name}"
    subprocess.run(  # noqa: S603
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=service_dir,
        env={**os.environ, "DATABASE_URL": url},
        check=True,
        capture_output=True,
    )
    return url


async def truncate_all(url: str, tables: list[str]) -> None:
    import asyncpg

    conn = await asyncpg.connect(url.replace("postgresql+asyncpg://", "postgresql://", 1))
    try:
        await conn.execute(f"TRUNCATE {', '.join(tables)} RESTART IDENTITY CASCADE")
    finally:
        await conn.close()
