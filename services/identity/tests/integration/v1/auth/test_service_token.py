"""IDN-T3: the client-credentials grant and the service tokens it issues."""

import hashlib

import pytest

from app.core.config import ServiceClient
from smartretail_common.auth import JwtVerifier

SECRET = "a-long-random-client-secret"


@pytest.fixture
async def settings(settings):
    settings.service_clients = {
        "temporal-worker": ServiceClient(
            secret_hash="sha256:" + hashlib.sha256(SECRET.encode()).hexdigest(),
            scopes=["inventory:internal", "catalog:internal"],
        )
    }
    return settings


def form(**overrides: str) -> dict[str, str]:
    return {
        "grant_type": "client_credentials",
        "client_id": "temporal-worker",
        "client_secret": SECRET,
        **overrides,
    }


async def test_grant_issues_a_scoped_service_token(client, app) -> None:
    r = await client.post("/api/v1/auth/token", data=form())
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["token_type"] == "Bearer" and body["expires_in"] == 300
    assert body["scope"] == "inventory:internal catalog:internal"
    assert "refresh_token" not in body

    verifier: JwtVerifier = app.state.verifier
    principal = await verifier.verify(body["access_token"])
    assert principal.is_service and principal.subject == "service:temporal-worker"
    assert principal.scopes == {"inventory:internal", "catalog:internal"}
    assert principal.role is None and principal.session_id is None


async def test_scope_can_be_narrowed_but_not_widened(client, app) -> None:
    r = await client.post("/api/v1/auth/token", data=form(scope="inventory:internal"))
    assert r.status_code == 200 and r.json()["scope"] == "inventory:internal"
    r = await client.post("/api/v1/auth/token", data=form(scope="inventory:internal order:admin"))
    assert r.status_code == 400 and r.json() == {"error": "invalid_scope"}


@pytest.mark.parametrize(
    ("overrides", "status", "error"),
    [
        ({"client_secret": "wrong"}, 401, "invalid_client"),
        ({"client_id": "nobody"}, 401, "invalid_client"),
        ({"grant_type": "password"}, 400, "unsupported_grant_type"),
    ],
)
async def test_grant_errors_use_the_oauth_body(client, overrides, status, error) -> None:
    r = await client.post("/api/v1/auth/token", data=form(**overrides))
    assert r.status_code == status
    assert r.json() == {"error": error}
    if status == 401:
        assert r.headers["www-authenticate"] == "Basic"


async def test_argon2_hashes_are_accepted_too(client, app, settings) -> None:
    hashed = await app.state.hasher.hash(SECRET)
    settings.service_clients["argon-client"] = ServiceClient(secret_hash=hashed, scopes=["x"])
    r = await client.post("/api/v1/auth/token", data=form(client_id="argon-client"))
    assert r.status_code == 200 and r.json()["scope"] == "x"


async def test_service_token_is_refused_on_user_routes(client) -> None:
    token = (await client.post("/api/v1/auth/token", data=form())).json()["access_token"]
    r = await client.get("/api/v1/users/me", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 401


async def test_the_full_scope_set_is_granted_when_none_is_requested(client) -> None:
    for extra in ({}, {"scope": ""}):
        r = await client.post("/api/v1/auth/token", data=form(**extra))
        assert r.status_code == 200 and r.json()["scope"] == "inventory:internal catalog:internal"


async def test_requested_scopes_are_granted_in_the_requested_order(client, app) -> None:
    r = await client.post(
        "/api/v1/auth/token", data=form(scope="catalog:internal  inventory:internal")
    )
    assert r.status_code == 200 and r.json()["scope"] == "catalog:internal inventory:internal"
    principal = await app.state.verifier.verify(r.json()["access_token"])
    assert principal.scopes == {"catalog:internal", "inventory:internal"}


@pytest.mark.parametrize(
    "scope", ["order:admin", "inventory:INTERNAL", "inventory", "inventory:internal:extra"]
)
async def test_a_scope_outside_the_clients_set_is_refused(client, scope: str) -> None:
    r = await client.post("/api/v1/auth/token", data=form(scope=scope))
    assert r.status_code == 400 and r.json() == {"error": "invalid_scope"}


async def test_a_secret_is_checked_before_the_scope(client) -> None:
    # A wrong secret never learns whether its requested scope would be allowed.
    r = await client.post(
        "/api/v1/auth/token", data=form(client_secret="wrong", scope="order:admin")
    )
    assert r.status_code == 401 and r.json() == {"error": "invalid_client"}


async def test_an_upper_case_sha256_digest_is_accepted(client, settings) -> None:
    digest = hashlib.sha256(SECRET.encode()).hexdigest().upper()
    settings.service_clients["upper"] = ServiceClient(secret_hash=f"sha256:{digest}", scopes=["x"])
    r = await client.post("/api/v1/auth/token", data=form(client_id="upper"))
    assert r.status_code == 200


@pytest.mark.parametrize(
    "secret",
    [SECRET + " ", SECRET[:-1], SECRET.upper(), "x" + SECRET],
    ids=["space", "short", "case", "prefix"],
)
async def test_near_miss_secrets_are_refused(client, secret: str) -> None:
    r = await client.post("/api/v1/auth/token", data=form(client_secret=secret))
    assert r.status_code == 401 and r.json() == {"error": "invalid_client"}


async def test_a_wrong_secret_for_an_argon2_client_is_refused(client, app, settings) -> None:
    hashed = await app.state.hasher.hash(SECRET)
    settings.service_clients["argon-client"] = ServiceClient(secret_hash=hashed, scopes=["x"])
    r = await client.post(
        "/api/v1/auth/token", data=form(client_id="argon-client", client_secret="nope")
    )
    assert r.status_code == 401 and r.json() == {"error": "invalid_client"}


async def test_an_unknown_client_spends_a_dummy_verify(client, app, monkeypatch) -> None:
    """IDN-17 for clients: an unknown client id costs the same Argon2 verify as
    a known one, so response time does not reveal which ids exist."""
    calls: list[str] = []
    real = app.state.hasher.verify_dummy

    async def spy(secret: str) -> None:
        calls.append(secret)
        await real(secret)

    monkeypatch.setattr(app.state.hasher, "verify_dummy", spy)
    r = await client.post("/api/v1/auth/token", data=form(client_id="nobody"))
    assert r.status_code == 401 and calls == [SECRET]
    await client.post("/api/v1/auth/token", data=form())
    assert calls == [SECRET]  # a known client is verified for real, not dummied


@pytest.mark.parametrize("missing", ["grant_type", "client_id", "client_secret"])
async def test_a_missing_form_field_is_422(client, missing: str) -> None:
    data = form()
    del data[missing]
    assert (await client.post("/api/v1/auth/token", data=data)).status_code == 422
