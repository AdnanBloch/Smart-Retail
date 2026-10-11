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
