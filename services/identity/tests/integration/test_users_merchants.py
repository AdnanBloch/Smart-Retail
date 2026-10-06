import uuid

import pytest
from sqlalchemy import select

from app.models import RefreshSession
from tests.integration.conftest import PASSWORD, bearer, login, make_admin, register


async def test_patch_me_only_name_and_phone(client) -> None:
    await register(client, "me@test.local")
    tokens = await login(client, "me@test.local")
    r = await client.patch(
        "/api/v1/users/me",
        json={"full_name": "New Name", "phone": "+1 555"},
        headers=bearer(tokens),
    )
    assert r.status_code == 200 and r.json()["full_name"] == "New Name"
    for body in ({"role": "admin"}, {"email": "x@y.z"}, {"is_active": False}):
        r = await client.patch("/api/v1/users/me", json=body, headers=bearer(tokens))
        assert r.status_code == 422, body


async def test_password_change_revokes_other_sessions(app, client) -> None:
    await register(client, "pw@test.local")
    current = await login(client, "pw@test.local")
    other = await login(client, "pw@test.local")

    wrong = await client.post(
        "/api/v1/users/me/password",
        json={"current_password": "nope nope nope", "new_password": "another good password"},
        headers=bearer(current),
    )
    assert wrong.status_code == 401

    r = await client.post(
        "/api/v1/users/me/password",
        json={"current_password": PASSWORD, "new_password": "another good password"},
        headers=bearer(current),
    )
    assert r.status_code == 204
    # The current session survives; check it first, because presenting the
    # revoked token afterwards counts as reuse and revokes everything.
    assert (
        await client.post("/api/v1/auth/refresh", json={"refresh_token": current["refresh_token"]})
    ).status_code == 200
    assert (
        await client.post("/api/v1/auth/refresh", json={"refresh_token": other["refresh_token"]})
    ).status_code == 401
    await login(client, "pw@test.local", "another good password")


@pytest.mark.parametrize(
    ("method", "path"),
    [("GET", "/api/v1/users"), ("GET", "/api/v1/merchants"), ("GET", "/api/v1/users/{id}")],
)
async def test_admin_routes_forbidden_to_others(client, method, path) -> None:
    user = await register(client, "plain@test.local")
    tokens = await login(client, "plain@test.local")
    url = path.format(id=user["id"])
    assert (await client.request(method, url)).status_code == 401
    assert (await client.request(method, url, headers=bearer(tokens))).status_code == 403


async def test_admin_list_and_filter(app, client) -> None:
    await make_admin(app)
    await register(client, "c1@test.local")
    await register(client, "m1@test.local", role="merchant")
    admin = await login(client, "admin@test.local")
    r = await client.get("/api/v1/users?role=merchant", headers=bearer(admin))
    assert r.status_code == 200
    page = r.json()
    assert page["total"] == 1 and page["items"][0]["email"] == "m1@test.local"
    r = await client.get("/api/v1/users?q=c1@&limit=500", headers=bearer(admin))
    assert r.json()["total"] == 1 and r.json()["limit"] == 100


async def test_admin_get_user_embeds_profile(app, client) -> None:
    await make_admin(app)
    merchant = await register(client, "m@test.local", role="merchant")
    admin = await login(client, "admin@test.local")
    r = await client.get(f"/api/v1/users/{merchant['id']}", headers=bearer(admin))
    assert r.json()["merchant_profile"]["status"] == "pending"
    assert (
        await client.get(f"/api/v1/users/{uuid.uuid4()}", headers=bearer(admin))
    ).status_code == 404


async def test_last_admin_cannot_be_demoted_or_deactivated(app, client) -> None:
    await make_admin(app)
    admin = await login(client, "admin@test.local")
    me = (await client.get("/api/v1/users/me", headers=bearer(admin))).json()
    for body in ({"role": "customer"}, {"is_active": False}):
        r = await client.patch(f"/api/v1/users/{me['id']}", json=body, headers=bearer(admin))
        assert r.status_code == 409 and r.json()["type"].endswith("/last-admin")

    await make_admin(app, "admin2@test.local")
    r = await client.patch(
        f"/api/v1/users/{me['id']}", json={"role": "customer"}, headers=bearer(admin)
    )
    assert r.status_code == 200


async def test_role_change_revokes_sessions_and_manages_profile(app, client) -> None:
    await make_admin(app)
    user = await register(client, "promo@test.local")
    user_tokens = await login(client, "promo@test.local")
    admin = await login(client, "admin@test.local")

    r = await client.patch(
        f"/api/v1/users/{user['id']}", json={"role": "merchant"}, headers=bearer(admin)
    )
    assert r.status_code == 200
    assert r.json()["merchant_profile"]["status"] == "pending"
    async with app.state.db.sessionmaker() as s:
        live = await s.scalars(
            select(RefreshSession).where(
                RefreshSession.user_id == uuid.UUID(user["id"]), RefreshSession.revoked_at.is_(None)
            )
        )
        assert list(live) == []
    assert (
        await client.post(
            "/api/v1/auth/refresh", json={"refresh_token": user_tokens["refresh_token"]}
        )
    ).status_code == 401

    r = await client.patch(
        f"/api/v1/users/{user['id']}", json={"role": "customer"}, headers=bearer(admin)
    )
    assert r.json()["merchant_profile"]["status"] == "suspended"


async def test_merchant_visibility(app, client) -> None:
    await make_admin(app)
    merchant = await register(client, "vis@test.local", role="merchant")
    await register(client, "cust@test.local")
    owner, cust = await login(client, "vis@test.local"), await login(client, "cust@test.local")
    admin = await login(client, "admin@test.local")
    url = f"/api/v1/merchants/{merchant['id']}"

    assert (await client.get(url)).status_code == 404  # pending: not public
    assert (await client.get(url, headers=bearer(cust))).status_code == 404
    assert (await client.get(url, headers=bearer(owner))).status_code == 200
    assert (await client.get(url, headers=bearer(admin))).status_code == 200

    async with app.state.db.sessionmaker() as s:
        from app.models import MerchantProfile

        profile = await s.get(MerchantProfile, uuid.UUID(merchant["id"]))
        profile.status = "approved"
        await s.commit()
    assert (await client.get(url)).status_code == 200
    assert (await client.get("/api/v1/merchants/me", headers=bearer(cust))).status_code == 403


async def test_merchant_patch_me(client) -> None:
    await register(client, "pm@test.local", role="merchant", business_name="Old")
    tokens = await login(client, "pm@test.local")
    r = await client.patch(
        "/api/v1/merchants/me",
        json={"business_name": "New", "description": "We sell things"},
        headers=bearer(tokens),
    )
    assert r.status_code == 200 and r.json()["business_name"] == "New"
    assert r.json()["slug"] == "old"
    r = await client.patch("/api/v1/merchants/me", json={"slug": "x"}, headers=bearer(tokens))
    assert r.status_code == 422


async def test_admin_lists_merchants_by_status(app, client) -> None:
    await make_admin(app)
    await register(client, "lm@test.local", role="merchant")
    admin = await login(client, "admin@test.local")
    r = await client.get("/api/v1/merchants?status=pending", headers=bearer(admin))
    assert r.json()["total"] == 1
    r = await client.get("/api/v1/merchants?status=approved", headers=bearer(admin))
    assert r.json()["total"] == 0


async def test_health(client) -> None:
    assert (await client.get("/health")).json() == {"status": "ok"}
    r = await client.get("/health/ready")
    assert r.status_code == 200 and r.json()["checks"] == {"database": "ok", "signing_key": "ok"}
