"""Password hashing and the signing key ring (RFC 0003 IDN-1, IDN-2, IDN-5)."""

import hashlib
import json
import secrets
import unicodedata
from pathlib import Path
from typing import Any

import anyio
import jwt
from argon2 import PasswordHasher as Argon2Hasher
from argon2 import Type
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from jwt.algorithms import RSAAlgorithm

from app.configs.env import Settings


def normalise_password(password: str) -> str:
    return unicodedata.normalize("NFKC", password)


class PasswordHasher:
    """Argon2id, run off the event loop (RFC 0000 A6)."""

    def __init__(self, settings: Settings) -> None:
        self._hasher = Argon2Hasher(
            time_cost=settings.argon2_time_cost,
            memory_cost=settings.argon2_memory_cost_kib,
            parallelism=settings.argon2_parallelism,
            hash_len=settings.argon2_hash_len,
            salt_len=settings.argon2_salt_len,
            type=Type.ID,
        )
        self._dummy_hash: str | None = None

    async def hash(self, password: str) -> str:
        return await anyio.to_thread.run_sync(self._hasher.hash, normalise_password(password))

    async def verify(self, password_hash: str, password: str) -> bool:
        def _verify() -> bool:
            try:
                return self._hasher.verify(password_hash, normalise_password(password))
            except (VerifyMismatchError, VerificationError, InvalidHashError):
                return False

        return await anyio.to_thread.run_sync(_verify)

    async def verify_dummy(self, password: str) -> None:
        """Spend the same time as a real verify for unknown emails (IDN-17)."""
        if self._dummy_hash is None:
            self._dummy_hash = await self.hash(secrets.token_urlsafe(16))
        await self.verify(self._dummy_hash, password)

    def needs_rehash(self, password_hash: str) -> bool:
        return self._hasher.check_needs_rehash(password_hash)


class KeyRing:
    """RSA signing keys keyed by kid. All are published; the active one signs."""

    def __init__(self, keys: dict[str, RSAPrivateKey], active_kid: str) -> None:
        if active_kid not in keys:
            raise RuntimeError(f"active signing key {active_kid!r} not found")
        self._keys = keys
        self.active_kid = active_kid
        self._jwks = {"keys": [self._jwk(kid, k.public_key()) for kid, k in sorted(keys.items())]}
        serialised = json.dumps(self._jwks, sort_keys=True, separators=(",", ":")).encode()
        self.jwks_etag = f'"{hashlib.sha256(serialised).hexdigest()[:16]}"'

    @classmethod
    def load(cls, directory: str | Path, active_kid: str) -> "KeyRing":
        keys: dict[str, RSAPrivateKey] = {}
        for path in sorted(Path(directory).glob("*.pem")):
            try:
                key = load_pem_private_key(path.read_bytes(), password=None)
            except ValueError:
                continue  # public keys share the directory; skip them
            if isinstance(key, RSAPrivateKey):
                keys[path.stem] = key
        return cls(keys, active_kid)

    @staticmethod
    def _jwk(kid: str, key: RSAPublicKey) -> dict[str, Any]:
        jwk = RSAAlgorithm.to_jwk(key, as_dict=True)
        return {
            "kty": "RSA",
            "use": "sig",
            "alg": "RS256",
            "kid": kid,
            "n": jwk["n"],
            "e": jwk["e"],
        }

    @property
    def jwks(self) -> dict[str, Any]:
        return self._jwks

    def public_keys(self) -> dict[str | None, RSAPublicKey]:
        return {kid: key.public_key() for kid, key in self._keys.items()}

    def sign(self, claims: dict[str, Any]) -> str:
        return jwt.encode(
            claims,
            self._keys[self.active_kid],
            algorithm="RS256",
            headers={"kid": self.active_kid, "typ": "JWT"},
        )


def new_refresh_token() -> tuple[str, str]:
    """32 random bytes, base64url. Returns (raw token, SHA-256 hex)."""
    raw = secrets.token_urlsafe(32)
    return raw, hash_refresh_token(raw)


def hash_refresh_token(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()
