"""JwtVerifier key loading: PEM files, settings and JWKS refresh."""

import json
from types import SimpleNamespace

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from jwt.algorithms import RSAAlgorithm

from smartretail_common.auth import InvalidToken, JwtVerifier, load_public_key
from smartretail_common.testing import generate_keypair, mint_token

KEY = generate_keypair("k1")
ROTATED = generate_keypair("k2")


def _jwks(*pairs) -> dict:
    keys = []
    for pair in pairs:
        jwk = json.loads(RSAAlgorithm.to_jwk(pair.public_key))
        keys.append({**jwk, "kid": pair.kid})
    return {"keys": [*keys, {"kty": "EC", "kid": "ignored"}, {"kty": "RSA"}]}


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _verifier(handler, **kwargs) -> JwtVerifier:
    return JwtVerifier(
        issuer="smartretail-identity",
        audience="smartretail",
        jwks_url="http://identity/.well-known/jwks.json",
        http_client=_client(handler),
        **kwargs,
    )


def test_load_public_key_reads_pem(tmp_path) -> None:
    path = tmp_path / "pub.pem"
    path.write_bytes(KEY.public_pem())
    assert load_public_key(path).public_numbers() == KEY.public_key.public_numbers()


def test_load_public_key_rejects_non_rsa(tmp_path) -> None:
    path = tmp_path / "ec.pem"
    ec_key = ec.generate_private_key(ec.SECP256R1()).public_key()
    path.write_bytes(ec_key.public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo))
    with pytest.raises(ValueError, match="not an RSA public key"):
        load_public_key(path)


def test_verifier_needs_a_key_source() -> None:
    with pytest.raises(ValueError):
        JwtVerifier(issuer="i", audience="a")


def _settings(**overrides) -> SimpleNamespace:
    base = {
        "jwt_public_key_path": None,
        "jwt_issuer": "smartretail-identity",
        "jwt_audience": "smartretail",
        "jwt_leeway_seconds": 10,
        "jwks_url": None,
        "jwks_min_refetch_seconds": 30,
    }
    return SimpleNamespace(**{**base, **overrides})


async def test_from_settings_loads_the_mounted_key(tmp_path) -> None:
    path = tmp_path / "pub.pem"
    path.write_bytes(KEY.public_pem())
    verifier = JwtVerifier.from_settings(_settings(jwt_public_key_path=str(path)))
    assert verifier.has_keys
    principal = await verifier.verify(mint_token(KEY, sub="u1"))
    assert principal.subject == "u1"


def test_from_settings_with_jwks_only() -> None:
    verifier = JwtVerifier.from_settings(
        _settings(jwt_public_key_path="/missing.pem", jwks_url="http://identity/jwks")
    )
    assert not verifier.has_keys


async def test_unknown_kid_triggers_jwks_fetch() -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url)
        return httpx.Response(200, json=_jwks(KEY, ROTATED))

    verifier = _verifier(handler)
    principal = await verifier.verify(mint_token(ROTATED, sub="u2"))
    assert principal.key_id == "k2"
    # Second token with a known kid: served from the cache.
    await verifier.verify(mint_token(KEY, sub="u3"))
    assert len(calls) == 1


async def test_refetch_is_throttled() -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(200, json={"keys": []})

    verifier = _verifier(handler, min_refetch_seconds=60)
    assert await verifier.refresh_jwks()
    assert not await verifier.refresh_jwks()
    assert await verifier.refresh_jwks(force=True)
    assert len(calls) == 2


async def test_failed_refresh_keeps_existing_keys() -> None:
    status = {"code": 200}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status["code"], json=_jwks(KEY))

    verifier = _verifier(handler)
    assert await verifier.refresh_jwks(force=True)
    status["code"] = 503
    assert not await verifier.refresh_jwks(force=True)
    assert (await verifier.verify(mint_token(KEY, sub="u4"))).subject == "u4"


async def test_unknown_kid_without_fallback_is_invalid() -> None:
    verifier = _verifier(lambda request: httpx.Response(200, json={"keys": []}))
    with pytest.raises(InvalidToken):
        await verifier.verify(mint_token(ROTATED, sub="u5"))


async def test_refresh_without_jwks_url_is_a_no_op() -> None:
    verifier = JwtVerifier(issuer="i", audience="a", static_keys={None: KEY.public_key})
    assert not await verifier.refresh_jwks(force=True)


async def test_refresh_with_its_own_client(monkeypatch: pytest.MonkeyPatch) -> None:
    real = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kw: real(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, json=_jwks(KEY)))
        ),
    )
    verifier = JwtVerifier(issuer="i", audience="a", jwks_url="http://identity/jwks")
    assert await verifier.refresh_jwks()
    assert verifier.has_keys
