"""/api/v1/merchants edge cases: admin search and paging, unknown profiles,
and a merchant token whose subject has no profile."""

import uuid

from sqlalchemy import update

from app.core.enums import MerchantStatus
from app.models import MerchantProfile
from smartretail_common.testing import mint_token
from tests.integration.conftest import bearer, login, make_admin, register


async def test_admin_searches_merchants_by_name_or_slug(app, client) -> None:
    await make_admin(app)
    await register(client, "n1@test.local", role="merchant", business_name="Northwind Outfitters")
    await register(client, "n2@test.local", role="merchant", business_name="Contoso Home")
    await register(client, "n3@test.local", role="merchant", business_name="North Star")
    admin = await login(client, "admin@test.local")

    async def names(query: str) -> list[str]:
        r = await client.get(f"/api/v1/merchants?{query}", headers=bearer(admin))
        assert r.status_code == 200, r.text
        return sorted(m["business_name"] for m in r.json()["items"])

    assert await names("") == ["Contoso Home", "North Star", "Northwind Outfitters"]
    assert await names("q=NORTH") == ["North Star", "Northwind Outfitters"]
    assert await names("q=star") == ["North Star"]  # slug "north-star" matches as well
    assert await names("q=north-star") == ["North Star"]
    assert await names("q=north&status=approved") == []


async def test_admin_pages_merchants(app, client) -> None:
    await make_admin(app)
    for i in range(3):
        await register(client, f"p{i}@test.local", role="merchant", business_name=f"Shop {i}")
    admin = await login(client, "admin@test.local")
    first = (await client.get("/api/v1/merchants?limit=2", headers=bearer(admin))).json()
    second = (await client.get("/api/v1/merchants?limit=2&offset=2", headers=bearer(admin))).json()
    assert first["total"] == second["total"] == 3
    names = [m["business_name"] for m in first["items"] + second["items"]]
    assert names == ["Shop 2", "Shop 1", "Shop 0"]  # newest first, no overlap


async def test_unknown_merchant_is_404_for_everyone(app, client) -> None:
    await make_admin(app)
    admin = await login(client, "admin@test.local")
    url = f"/api/v1/merchants/{uuid.uuid4()}"
    for headers in ({}, bearer(admin)):
        r = await client.get(url, headers=headers)
        assert r.status_code == 404 and r.json()["type"].endswith("/merchant-not-found")
    assert (await client.get("/api/v1/merchants/not-a-uuid")).status_code == 422


async def test_a_customers_id_is_not_a_merchant(app, client) -> None:
    await make_admin(app)
    customer = await register(client, "plain@test.local")
    admin = await login(client, "admin@test.local")
    r = await client.get(f"/api/v1/merchants/{customer['id']}", headers=bearer(admin))
    assert r.status_code == 404


async def test_a_suspended_profile_is_hidden_from_the_public_but_not_its_owner(app, client) -> None:
    merchant = await register(client, "hide@test.local", role="merchant")
    owner = await login(client, "hide@test.local")
    async with app.state.db.sessionmaker() as s:
        await s.execute(update(MerchantProfile).values(status=MerchantStatus.SUSPENDED))
        await s.commit()
    url = f"/api/v1/merchants/{merchant['id']}"
    assert (await client.get(url)).status_code == 404
    r = await client.get(url, headers=bearer(owner))
    assert r.status_code == 200 and r.json()["status"] == "suspended"


async def test_a_merchant_token_without_a_profile_finds_nothing(client, keypair) -> None:
    for subject in (str(uuid.uuid4()), "not-a-uuid"):
        headers = {"Authorization": f"Bearer {mint_token(keypair, sub=subject, role='merchant')}"}
        r = await client.get("/api/v1/merchants/me", headers=headers)
        assert r.status_code == 404 and r.json()["type"].endswith("/merchant-not-found")
        r = await client.patch("/api/v1/merchants/me", json={"description": "x"}, headers=headers)
        assert r.status_code == 404
