import pytest
from fastapi import Depends, FastAPI
from httpx import ASGITransport, AsyncClient
from pydantic import BaseModel, Field

from smartretail_common.app import install_platform
from smartretail_common.errors import Conflict, NotFound, NotImplementedYet
from smartretail_common.pagination import PageParams, page_params


class Body(BaseModel):
    quantity: int = Field(ge=1)


def _app() -> FastAPI:
    app = FastAPI()
    install_platform(app)

    @app.get("/missing")
    async def missing() -> None:
        raise NotFound("no such thing", type_slug="widget-not-found", title="Widget not found")

    @app.get("/conflict")
    async def conflict() -> None:
        raise Conflict(extras={"current_version": 3})

    @app.get("/todo")
    async def todo() -> None:
        raise NotImplementedYet()

    @app.get("/boom")
    async def boom() -> None:
        raise RuntimeError("secret SQL: SELECT password FROM users")

    @app.post("/validate")
    async def validate(body: Body) -> dict:
        return body.model_dump()

    @app.get("/page")
    async def page(p: PageParams = Depends(page_params)) -> dict:
        return {"limit": p.limit, "offset": p.offset}

    return app


@pytest.fixture
async def client():
    transport = ASGITransport(app=_app(), raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://t") as c:
        yield c


async def test_domain_error_is_problem_detail(client) -> None:
    r = await client.get("/missing", headers={"X-Request-ID": "req-123"})
    assert r.status_code == 404
    assert r.headers["content-type"] == "application/problem+json"
    body = r.json()
    assert body["type"] == "https://smartretail.local/errors/widget-not-found"
    assert body["title"] == "Widget not found"
    assert body["detail"] == "no such thing"
    assert body["instance"] == "/missing"
    assert body["request_id"] == "req-123"
    assert r.headers["x-request-id"] == "req-123"


async def test_extras_are_merged(client) -> None:
    body = (await client.get("/conflict")).json()
    assert body["status"] == 409 and body["current_version"] == 3


async def test_501(client) -> None:
    assert (await client.get("/todo")).status_code == 501


async def test_500_leaks_nothing(client) -> None:
    r = await client.get("/boom")
    assert r.status_code == 500
    assert "SQL" not in r.text and "password" not in r.text
    assert r.json()["request_id"]


async def test_validation_violations(client) -> None:
    r = await client.post("/validate", json={"quantity": 0})
    assert r.status_code == 422
    (violation,) = r.json()["violations"]
    assert violation["field"] == "quantity"
    assert violation["code"] == "greater_than_equal"


async def test_unknown_route_is_problem(client) -> None:
    r = await client.get("/nope")
    assert r.status_code == 404
    assert r.headers["content-type"] == "application/problem+json"


async def test_request_id_generated_when_absent(client) -> None:
    r = await client.get("/page")
    assert len(r.headers["x-request-id"]) == 36


@pytest.mark.parametrize(
    ("query", "expected"),
    [("", (20, 0)), ("?limit=5&offset=10", (5, 10)), ("?limit=1000", (100, 0))],
)
async def test_pagination(client, query, expected) -> None:
    body = (await client.get(f"/page{query}")).json()
    assert (body["limit"], body["offset"]) == expected


@pytest.mark.parametrize("query", ["?limit=-1", "?offset=-1", "?limit=0"])
async def test_pagination_rejects_negative(client, query) -> None:
    assert (await client.get(f"/page{query}")).status_code == 422
