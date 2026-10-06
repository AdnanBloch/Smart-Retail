import uuid

import jwt
import pytest

from app.infrastructure.security import (
    KeyRing,
    PasswordHasher,
    hash_refresh_token,
    new_refresh_token,
)
from app.models import MerchantProfile, User
from app.models.enums import MerchantStatus, UserRole
from app.services.token_repo import TokenRepo
from app.utils.helpers import base_slug, check_password_policy, next_free_slug
from smartretail_common.errors import Unprocessable
from tests.conftest import make_settings


@pytest.fixture
def settings(keys_dir, keypair):
    return make_settings(keys_dir, keypair.kid)


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


@pytest.mark.parametrize(
    ("password", "ok"),
    [("a" * 11, False), ("a" * 12, True), ("a" * 128, True), ("a" * 129, False)],
)
def test_password_length_policy(settings, password, ok) -> None:
    if ok:
        check_password_policy(settings, password, "x@y.z", field="password")
    else:
        with pytest.raises(Unprocessable):
            check_password_policy(settings, password, "x@y.z", field="password")


def test_password_must_not_equal_email(settings) -> None:
    with pytest.raises(Unprocessable):
        check_password_policy(
            settings, "Longer.Email@Example.com", "longer.email@example.com", field="p"
        )


def test_refresh_tokens() -> None:
    raw, digest = new_refresh_token()
    assert len(raw) == 43
    assert digest == hash_refresh_token(raw) and len(digest) == 64


def test_slugs() -> None:
    assert base_slug("Café Déjà Vu!") == "cafe-deja-vu"
    assert base_slug("!!!") == "merchant"
    assert next_free_slug("shop", set()) == "shop"
    assert next_free_slug("shop", {"shop", "shop-2"}) == "shop-3"


def test_keyring_requires_active_kid(keys_dir) -> None:
    with pytest.raises(RuntimeError):
        KeyRing.load(keys_dir, "missing-kid")


def test_jwks_exposes_public_components_only(keys_dir, keypair) -> None:
    ring = KeyRing.load(keys_dir, keypair.kid)
    (jwk,) = ring.jwks["keys"]
    assert set(jwk) == {"kty", "use", "alg", "kid", "n", "e"}
    assert jwk["kid"] == keypair.kid
    assert ring.jwks_etag == KeyRing.load(keys_dir, keypair.kid).jwks_etag


def _user(role: UserRole) -> User:
    user = User(id=uuid.uuid4(), email="m@x.io", full_name="M", role=role, password_hash="x")
    if role == UserRole.MERCHANT:
        user.merchant_profile = MerchantProfile(
            user_id=user.id, business_name="B", slug="b", status=MerchantStatus.PENDING
        )
    return user


def test_access_token_claims(settings, keys_dir, keypair) -> None:
    issuer = TokenRepo(settings, KeyRing.load(keys_dir, keypair.kid))
    sid = uuid.uuid4()
    merchant = _user(UserRole.MERCHANT)
    token = issuer.access_token(merchant, sid)
    header = jwt.get_unverified_header(token)
    claims = jwt.decode(token, keypair.public_key, algorithms=["RS256"], audience="smartretail")
    assert header["kid"] == keypair.kid and header["alg"] == "RS256"
    assert set(claims) == {
        "iss",
        "aud",
        "sub",
        "typ",
        "role",
        "merchant_status",
        "sid",
        "iat",
        "nbf",
        "exp",
        "jti",
    }
    assert claims["sub"] == str(merchant.id)
    assert claims["role"] == "merchant" and claims["merchant_status"] == "pending"
    assert claims["sid"] == str(sid) and claims["exp"] - claims["iat"] == 900
    assert "email" not in claims

    customer_claims = jwt.decode(
        issuer.access_token(_user(UserRole.CUSTOMER), sid),
        keypair.public_key,
        algorithms=["RS256"],
        audience="smartretail",
    )
    assert "merchant_status" not in customer_claims
