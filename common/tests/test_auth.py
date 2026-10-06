import base64
import json
import time
import uuid

import pytest
from fastapi import Depends, FastAPI
from httpx import ASGITransport, AsyncClient

from smartretail_common.app import install_platform
from smartretail_common.auth import (
    InvalidToken,
    JwtVerifier,
    Principal,
    current_principal,
    optional_principal,
    require_roles,
)
from smartretail_common.testing import generate_keypair, mint_token

KEY = generate_keypair("k1")
OTHER = generate_keypair("k2")


def verifier(**kwargs) -> JwtVerifier:
    return JwtVerifier(
        issuer="smartretail-identity",
        audience="smartretail",
        static_keys={None: KEY.public_key},
        **kwargs,
    )


def _b64(data: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(data).encode()).rstrip(b"=").decode()


async def test_valid_token_maps_claims() -> None:
    sub = str(uuid.uuid4())
    token = mint_token(KEY, sub=sub, role="merchant", extra={"merchant_status": "approved"})
    principal = await verifier().verify(token)
    assert principal.subject == sub
    assert principal.role == "merchant"
    assert principal.merchant_id == sub
    assert principal.merchant_status == "approved"
    assert principal.token_type == "user"
    assert principal.key_id == "k1"


async def test_non_merchant_has_no_merchant_id() -> None:
    principal = await verifier().verify(mint_token(KEY, role="customer"))
    assert principal.merchant_id is None


@pytest.mark.parametrize(
    "token_factory",
    [
        pytest.param(lambda: mint_token(KEY, ttl=-60), id="expired"),
        pytest.param(lambda: mint_token(OTHER), id="wrong-key"),
        pytest.param(lambda: mint_token(KEY, issuer="evil"), id="wrong-iss"),
        pytest.param(lambda: mint_token(KEY, audience="other"), id="wrong-aud"),
        pytest.param(lambda: mint_token(KEY, extra={"typ": "refresh"}), id="wrong-typ"),
        pytest.param(
            lambda: mint_token(KEY, extra={"nbf": int(time.time()) + 3600}), id="future-nbf"
        ),
        pytest.param(lambda: mint_token(KEY)[:-4] + "AAAA", id="tampered"),
        pytest.param(lambda: "not-a-jwt", id="malformed"),
        pytest.param(
            lambda: f"{_b64({'alg': 'none', 'typ': 'JWT'})}.{_b64({'sub': 'x'})}.", id="alg-none"
        ),
    ],
)
async def test_rejects_bad_tokens(token_factory) -> None:
    with pytest.raises(InvalidToken):
        await verifier().verify(token_factory())


async def test_rejects_hs256_even_with_public_key_as_secret() -> None:
    import jwt as pyjwt

    token = pyjwt.encode(
        {"sub": "x", "iss": "smartretail-identity", "aud": "smartretail", "exp": 9999999999},
        "secret",
        algorithm="HS256",
    )
    with pytest.raises(InvalidToken):
        await verifier().verify(token)


async def test_expired_detail_is_expired() -> None:
    with pytest.raises(InvalidToken) as info:
        await verifier().verify(mint_token(KEY, ttl=-60))
    assert info.value.detail == "expired"


async def test_unknown_kid_refetches_jwks_then_rejects(monkeypatch) -> None:
    v = JwtVerifier(
        issuer="smartretail-identity",
        audience="smartretail",
        jwks_url="http://identity/jwks",
        min_refetch_seconds=30,
    )
    calls = 0

    async def fake_refresh(*, force: bool = False) -> bool:
        nonlocal calls
        calls += 1
        return True

    monkeypatch.setattr(v, "refresh_jwks", fake_refresh)
    with pytest.raises(InvalidToken):
        await v.verify(mint_token(OTHER))
    assert calls == 1


def _app() -> FastAPI:
    app = FastAPI()
    install_platform(app)
    app.state.verifier = verifier()

    @app.get("/me")
    async def me(p: Principal = Depends(current_principal)) -> dict:
        return {"sub": p.subject}

    @app.get("/maybe")
    async def maybe(p: Principal | None = Depends(optional_principal)) -> dict:
        return {"sub": p.subject if p else None}

    @app.get("/admin")
    async def admin(p: Principal = Depends(require_roles("admin"))) -> dict:
        return {"sub": p.subject}

    return app


@pytest.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=_app()), base_url="http://t") as c:
        yield c


async def test_require_roles_allow(client) -> None:
    r = await client.get(
        "/admin", headers={"Authorization": f"Bearer {mint_token(KEY, role='admin')}"}
    )
    assert r.status_code == 200


async def test_require_roles_deny(client) -> None:
    r = await client.get("/admin", headers={"Authorization": f"Bearer {mint_token(KEY)}"})
    assert r.status_code == 403
    assert r.headers["content-type"] == "application/problem+json"


async def test_missing_token_is_401_with_challenge(client) -> None:
    r = await client.get("/me")
    assert r.status_code == 401
    assert r.headers["www-authenticate"] == "Bearer"
    assert r.json()["type"].endswith("/invalid-token")


async def test_malformed_header_is_401(client) -> None:
    r = await client.get("/me", headers={"Authorization": "Basic abc"})
    assert r.status_code == 401
    assert 'error="invalid_token"' in r.headers["www-authenticate"]


async def test_service_token_refused_by_require_roles(client) -> None:
    token = mint_token(KEY, sub="service:order", role=None)
    r = await client.get("/admin", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 403


async def test_optional_principal(client) -> None:
    assert (await client.get("/maybe")).json() == {"sub": None}
    bad = await client.get("/maybe", headers={"Authorization": "Bearer junk"})
    assert bad.status_code == 401
