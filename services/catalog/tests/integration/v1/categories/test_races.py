"""Races on one category (mentor review 2026-10-20 §8).

The "in use" check in ``CategoryRepo.delete`` and the "exists" check in
``ProductRepo.create`` are reads; the foreign key (``ON DELETE RESTRICT``) is
what actually decides when a delete and a product insert overlap. Whichever
transaction loses must get the domain answer — 409 ``category-in-use`` or 422
unknown category — never a raw ``IntegrityError`` (500).

The same holds for a child category and its parent. Each interleaving is
forced: the first transaction holds its row lock uncommitted while the second
is observed blocked in Postgres, then the first commits.
"""

import asyncio
import uuid

import pytest
from sqlalchemy import func, select

from app.core.errors import CategoryInUse
from app.models import Category, Product
from app.v1.categories.repo import CategoryRepo
from app.v1.products.repo import ProductRepo
from smartretail_common.errors import Unprocessable
from tests.integration.conftest import create_category, principal, wait_until_blocked

ROUNDS = 5


async def test_delete_blocked_behind_an_uncommitted_product_insert_is_409(
    app, client, callers, database_url
) -> None:
    category = await create_category(client, callers, "racing")
    cid = uuid.UUID(category["id"])
    async with app.state.db.sessionmaker() as inserting, app.state.db.sessionmaker() as deleting:
        inserting.add(
            Product(
                id=uuid.uuid4(),
                merchant_id=uuid.UUID(callers.merchant_a_id),
                sku="RACE",
                name="n",
                base_price=1,
                category_id=cid,
            )
        )
        await inserting.flush()  # the FK check holds a key-share lock on the category
        delete = asyncio.create_task(CategoryRepo(deleting).delete(cid))
        await wait_until_blocked(database_url)
        await inserting.commit()
        with pytest.raises(CategoryInUse):
            await delete
    assert (await client.get(f"/api/v1/categories/{cid}")).status_code == 200


async def test_product_create_blocked_behind_an_uncommitted_delete_is_422(
    app, client, callers, database_url
) -> None:
    category = await create_category(client, callers, "doomed")
    cid = uuid.UUID(category["id"])
    async with app.state.db.sessionmaker() as deleting, app.state.db.sessionmaker() as creating:
        doomed = await deleting.get(Category, cid)
        await deleting.delete(doomed)
        await deleting.flush()  # row deleted, not yet committed
        create = asyncio.create_task(
            ProductRepo(creating).create(
                principal("merchant", callers.merchant_a_id),
                {"sku": "LATE", "name": "n", "base_price": 1, "category_id": cid},
                merchant_id=None,
            )
        )
        await wait_until_blocked(database_url)
        await deleting.commit()
        with pytest.raises(Unprocessable) as err:
            await create
    assert err.value.extras["violations"][0]["field"] == "category_id"
    async with app.state.db.sessionmaker() as s:
        assert await s.scalar(select(func.count()).select_from(Product)) == 0


async def test_child_create_blocked_behind_an_uncommitted_parent_delete_is_422(
    app, client, callers, database_url
) -> None:
    parent = await create_category(client, callers, "parent")
    pid = uuid.UUID(parent["id"])
    async with app.state.db.sessionmaker() as deleting, app.state.db.sessionmaker() as creating:
        await deleting.delete(await deleting.get(Category, pid))
        await deleting.flush()
        create = asyncio.create_task(
            CategoryRepo(creating).create(name="Child", slug="child", parent_id=pid)
        )
        await wait_until_blocked(database_url)
        await deleting.commit()
        with pytest.raises(Unprocessable) as err:
            await create
    assert err.value.extras["violations"][0]["field"] == "parent_id"


async def test_parent_delete_blocked_behind_an_uncommitted_child_is_409(
    app, client, callers, database_url
) -> None:
    parent = await create_category(client, callers, "parent")
    pid = uuid.UUID(parent["id"])
    async with app.state.db.sessionmaker() as creating, app.state.db.sessionmaker() as deleting:
        creating.add(Category(name="Child", slug="child", parent_id=pid))
        await creating.flush()
        delete = asyncio.create_task(CategoryRepo(deleting).delete(pid))
        await wait_until_blocked(database_url)
        await creating.commit()
        with pytest.raises(CategoryInUse):
            await delete
    tree = (await client.get("/api/v1/categories")).json()
    assert [(n["slug"], [c["slug"] for c in n["children"]]) for n in tree] == [
        ("parent", ["child"])
    ]


async def test_concurrent_delete_and_create_over_http_never_500(client, callers) -> None:
    """Unforced: whatever order Postgres picks, both answers are domain errors or success,
    and the end state is consistent (the category exists iff a product points at it)."""
    for i in range(ROUNDS):
        category = await create_category(client, callers, f"contended-{i}")
        delete, create = await asyncio.gather(
            client.delete(f"/api/v1/categories/{category['id']}", headers=callers.admin),
            client.post(
                "/api/v1/products",
                json={
                    "sku": f"C-{i}",
                    "name": "n",
                    "base_price": "1",
                    "category_id": category["id"],
                },
                headers=callers.merchant_a,
            ),
        )
        assert (delete.status_code, create.status_code) in {(204, 422), (409, 201)}, (
            delete.text,
            create.text,
        )
        exists = (await client.get(f"/api/v1/categories/{category['id']}")).status_code == 200
        assert exists == (create.status_code == 201)


async def test_concurrent_duplicate_slug_exactly_one_wins(client, callers) -> None:
    for i in range(ROUNDS):
        responses = await asyncio.gather(
            *(
                client.post(
                    "/api/v1/categories",
                    json={"name": "Same", "slug": f"same-{i}"},
                    headers=callers.admin,
                )
                for _ in range(4)
            )
        )
        codes = sorted(r.status_code for r in responses)
        assert codes == [201, 409, 409, 409], [r.text for r in responses]
        assert all(
            r.json()["type"].endswith("/duplicate-slug") for r in responses if r.status_code == 409
        )
