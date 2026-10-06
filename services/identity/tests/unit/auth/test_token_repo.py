"""Access-token claims (RFC 0003 IDN-6)."""

import uuid

import jwt

from app.auth.token_repo import TokenRepo
from app.core.enums import MerchantStatus, UserRole
from app.core.security import KeyRing
from app.merchants.models import MerchantProfile
from app.users.models import User


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
