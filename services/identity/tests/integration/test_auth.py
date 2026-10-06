import asyncio

from sqlalchemy import select, update

from app.models import RefreshSession, User
from tests.integration.conftest import PASSWORD, bearer, login, make_admin, register


async def test_register_customer(client) -> None:
    r = await client.post(
        "/api/v1/auth/register",
        json={"email": "c@test.local", "password": PASSWORD, "full_name": "C", "role": "customer"},
    )
    assert r.status_code == 201
    body = r.json()
    assert r.headers["location"] == f"/api/v1/users/{body['id']}"
    assert body["role"] == "customer" and body["is_active"] and "password_hash" not in body
    assert "access_token" not in body


async def test_register_merchant_creates_pending_profile(client) -> None:
    user = await register(client, "m@test.local", role="merchant", business_name="Acme Goods")
    tokens = await login(client, "m@test.local")
    r = await client.get("/api/v1/merchants/me", headers=bearer(tokens))
    assert r.status_code == 200
    assert r.json() == {
        "merchant_id": user["id"],
        "business_name": "Acme Goods",
        "slug": "acme-goods",
        "description": None,
        "status": "pending",
    }


async def test_slug_collision_gets_suffix(client) -> None:
    await register(client, "a@test.local", role="merchant", business_name="Acme")
    await register(client, "b@test.local", role="merchant", business_name="Acme")
    tokens = await login(client, "b@test.local")
    assert (await client.get("/api/v1/merchants/me", headers=bearer(tokens))).json()[
        "slug"
    ] == "acme-2"


async def test_duplicate_email_case_insensitive(client) -> None:
    await register(client, "dup@test.local")
    for email in ("dup@test.local", "DUP@Test.Local"):
        r = await client.post(
            "/api/v1/auth/register",
            json={"email": email, "password": PASSWORD, "full_name": "X"},
        )
        assert r.status_code == 409
        assert r.json()["type"].endswith("/email-taken")


async def test_register_validation(client) -> None:
    base = {"email": "v@test.local", "password": PASSWORD, "full_name": "V"}
    for body in (
        {**base, "password": "short"},
        {**base, "role": "admin"},
        {**base, "email": "not-an-email"},
        {**base, "is_admin": True},
    ):
        r = await client.post("/api/v1/auth/register", json=body)
        assert r.status_code == 422, body
        assert r.json()["violations"]


async def test_login_failures_are_byte_identical(client) -> None:
    await register(client, "known@test.local")
    unknown = await client.post(
        "/api/v1/auth/login", json={"email": "nobody@test.local", "password": PASSWORD}
    )
    wrong = await client.post(
        "/api/v1/auth/login", json={"email": "known@test.local", "password": "wrong password!"}
    )
    assert unknown.status_code == wrong.status_code == 401
    strip = lambda r: {k: v for k, v in r.json().items() if k != "request_id"}  # noqa: E731
    assert strip(unknown) == strip(wrong)
    assert unknown.json()["type"].endswith("/invalid-credentials")


async def test_login_inactive_is_403(app, client) -> None:
    await register(client, "off@test.local")
    async with app.state.db.sessionmaker() as s:
        await s.execute(update(User).values(is_active=False))
        await s.commit()
    r = await client.post(
        "/api/v1/auth/login", json={"email": "off@test.local", "password": PASSWORD}
    )
    assert r.status_code == 403 and r.json()["type"].endswith("/account-inactive")


async def test_login_returns_token_pair(client) -> None:
    await register(client, "t@test.local")
    tokens = await login(client, "t@test.local")
    assert tokens["token_type"] == "Bearer" and tokens["expires_in"] == 900
    me = await client.get("/api/v1/users/me", headers=bearer(tokens))
    assert me.status_code == 200 and me.json()["email"] == "t@test.local"


async def test_refresh_rotates(client) -> None:
    await register(client, "r@test.local")
    first = await login(client, "r@test.local")
    r = await client.post("/api/v1/auth/refresh", json={"refresh_token": first["refresh_token"]})
    assert r.status_code == 200
    second = r.json()
    assert second["refresh_token"] != first["refresh_token"]
    again = await client.post(
        "/api/v1/auth/refresh", json={"refresh_token": second["refresh_token"]}
    )
    assert again.status_code == 200


async def test_refresh_reuse_revokes_everything(app, client) -> None:
    await register(client, "reuse@test.local")
    first = await login(client, "reuse@test.local")
    other_device = await login(client, "reuse@test.local")
    rotated = (
        await client.post("/api/v1/auth/refresh", json={"refresh_token": first["refresh_token"]})
    ).json()

    replay = await client.post(
        "/api/v1/auth/refresh", json={"refresh_token": first["refresh_token"]}
    )
    assert replay.status_code == 401
    assert replay.json()["type"].endswith("/invalid-refresh-token")

    for token in (rotated["refresh_token"], other_device["refresh_token"]):
        r = await client.post("/api/v1/auth/refresh", json={"refresh_token": token})
        assert r.status_code == 401
    async with app.state.db.sessionmaker() as s:
        live = await s.scalars(select(RefreshSession).where(RefreshSession.revoked_at.is_(None)))
        assert list(live) == []


async def test_refresh_unknown_and_expired(app, client) -> None:
    r = await client.post("/api/v1/auth/refresh", json={"refresh_token": "nope"})
    assert r.status_code == 401
    await register(client, "exp@test.local")
    tokens = await login(client, "exp@test.local")
    async with app.state.db.sessionmaker() as s:
        await s.execute(update(RefreshSession).values(expires_at=RefreshSession.created_at))
        await s.commit()
    r = await client.post("/api/v1/auth/refresh", json={"refresh_token": tokens["refresh_token"]})
    assert r.status_code == 401


async def test_concurrent_refresh_exactly_one_wins(client) -> None:
    await register(client, "race@test.local")
    tokens = await login(client, "race@test.local")
    results = await asyncio.gather(
        *(
            client.post("/api/v1/auth/refresh", json={"refresh_token": tokens["refresh_token"]})
            for _ in range(2)
        )
    )
    assert sorted(r.status_code for r in results) == [200, 401]


async def test_refresh_after_deactivation_is_401(app, client) -> None:
    await register(client, "deact@test.local")
    tokens = await login(client, "deact@test.local")
    async with app.state.db.sessionmaker() as s:
        await s.execute(update(User).values(is_active=False))
        await s.commit()
    r = await client.post("/api/v1/auth/refresh", json={"refresh_token": tokens["refresh_token"]})
    assert r.status_code == 401


async def test_logout_is_idempotent_and_scoped(client) -> None:
    await register(client, "a@test.local")
    await register(client, "b@test.local")
    a, b = await login(client, "a@test.local"), await login(client, "b@test.local")

    # Another user's token: 204, and their session is untouched.
    r = await client.post(
        "/api/v1/auth/logout", json={"refresh_token": b["refresh_token"]}, headers=bearer(a)
    )
    assert r.status_code == 204
    assert (
        await client.post("/api/v1/auth/refresh", json={"refresh_token": b["refresh_token"]})
    ).status_code == 200

    for _ in range(2):
        r = await client.post(
            "/api/v1/auth/logout", json={"refresh_token": a["refresh_token"]}, headers=bearer(a)
        )
        assert r.status_code == 204
    assert (
        await client.post("/api/v1/auth/logout", json={"refresh_token": "x"})
    ).status_code == 401


async def test_jwks_and_etag(client, keypair) -> None:
    r = await client.get("/api/v1/auth/jwks")
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/jwk-set+json"
    assert "max-age=300" in r.headers["cache-control"]
    assert r.json()["keys"][0]["kid"] == keypair.kid
    cached = await client.get("/api/v1/auth/jwks", headers={"If-None-Match": r.headers["etag"]})
    assert cached.status_code == 304


async def test_suspended_merchant_cannot_login(app, client) -> None:
    await make_admin(app)
    merchant = await register(client, "sm@test.local", role="merchant")
    admin = await login(client, "admin@test.local")
    # Moving away from merchant suspends the profile (IDN-7) ...
    await client.patch(
        f"/api/v1/users/{merchant['id']}", json={"role": "customer"}, headers=bearer(admin)
    )
    # ... and the user is now a customer, so login still works.
    assert (
        await client.post(
            "/api/v1/auth/login", json={"email": "sm@test.local", "password": PASSWORD}
        )
    ).status_code == 200


async def test_expired_access_token_rejected(client, keypair) -> None:
    from smartretail_common.testing import mint_token

    token = mint_token(keypair, ttl=-120)
    r = await client.get("/api/v1/users/me", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 401
