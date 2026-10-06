"""Password hashing, refresh tokens and the signing key ring (RFC 0003 IDN-1, IDN-5)."""

import pytest

from app.core.security import KeyRing, PasswordHasher, hash_refresh_token, new_refresh_token
from tests.conftest import make_settings


async def test_hash_round_trip_and_distinct_salts(settings) -> None:
    hasher = PasswordHasher(settings)
    a, b = await hasher.hash("correct horse battery"), await hasher.hash("correct horse battery")
    assert a != b
    assert a.startswith("$argon2id$v=19$m=1024,t=1,p=1$")
    assert await hasher.verify(a, "correct horse battery")
    assert not await hasher.verify(a, "wrong password!!")
    assert not await hasher.verify("not-a-hash", "whatever")


async def test_needs_rehash_when_parameters_change(settings, keys_dir, keypair) -> None:
    old = await PasswordHasher(settings).hash("correct horse battery")
    stronger = make_settings(keys_dir, keypair.kid).model_copy(update={"argon2_time_cost": 2})
    assert PasswordHasher(stronger).needs_rehash(old)
    assert not PasswordHasher(settings).needs_rehash(old)


def test_refresh_tokens() -> None:
    raw, digest = new_refresh_token()
    assert len(raw) == 43
    assert digest == hash_refresh_token(raw) and len(digest) == 64


def test_keyring_requires_active_kid(keys_dir) -> None:
    with pytest.raises(RuntimeError):
        KeyRing.load(keys_dir, "missing-kid")


def test_jwks_exposes_public_components_only(keys_dir, keypair) -> None:
    ring = KeyRing.load(keys_dir, keypair.kid)
    (jwk,) = ring.jwks["keys"]
    assert set(jwk) == {"kty", "use", "alg", "kid", "n", "e"}
    assert jwk["kid"] == keypair.kid
    assert ring.jwks_etag == KeyRing.load(keys_dir, keypair.kid).jwks_etag
