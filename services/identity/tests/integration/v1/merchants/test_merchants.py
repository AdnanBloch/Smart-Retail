"""/api/v1/merchants against a real database."""

import uuid

from tests.integration.conftest import bearer, login, make_admin, register


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
