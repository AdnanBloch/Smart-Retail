"""Edge cases of the request plumbing every service mounts: request ids
(RFC 0002 §2.5), ``X-Client-Id`` (C21), problem details for connection
failures (NTF-14), pagination slices, enum columns, and principal caching."""

import asyncio
import contextvars
import json
import logging

import pytest
from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import DBAPIError, OperationalError

from smartretail_common.app import install_platform
from smartretail_common.auth import JwtVerifier, Principal, current_principal
from smartretail_common.client_id import client_id, valid_client_id
from smartretail_common.context import (
    RequestContextMiddleware,
    get_request_id,
    get_user_id,
    set_request_id,
)
from smartretail_common.db_types import DomainEnumText
from smartretail_common.enums import DomainEnum
from smartretail_common.errors import Unprocessable, is_connection_error, problem_response
from smartretail_common.pagination import PageParams, Slice
from smartretail_common.testing import generate_keypair, mint_token

KEY = generate_keypair("k1")
NAMED = generate_keypair("named")


def _app(*, adopt_inbound: bool = True) -> FastAPI:
    app = FastAPI()
    install_platform(app, adopt_inbound_request_id=adopt_inbound)
    app.state.verifier = JwtVerifier(
        issuer="smartretail-identity",
        audience="smartretail",
        static_keys={None: KEY.public_key, "named": NAMED.public_key},
    )

    @app.get("/ids")
    async def ids(request: Request) -> dict:
        return {
            "context": get_request_id(),
            "state": request.state.request_id,
            "user": get_user_id(),
        }

    @app.get("/slow")
    async def slow(delay: float) -> dict:
        before = get_request_id()
        await asyncio.sleep(delay)
        return {"before": before, "after": get_request_id()}

    @app.get("/own-header")
    async def own_header() -> JSONResponse:
        return JSONResponse({}, headers={"X-Request-ID": "spoofed-by-handler"})

    @app.get("/client")
    async def client_route(cid: str | None = Depends(client_id)) -> dict:
        return {"client_id": cid}

    @app.get("/db-down")
    async def db_down() -> None:
        raise OperationalError("SELECT 1", {}, ConnectionRefusedError("refused"))

    @app.get("/refused")
    async def refused() -> None:
        raise ConnectionRefusedError("redis refused")

    @app.get("/unprocessable")
    async def unprocessable() -> None:
        raise Unprocessable(
            "line items are invalid",
            violations=[{"field": "items.0.quantity", "message": "too many", "code": "max"}],
        )

    @app.get("/twice")
    async def twice(request: Request, first: Principal = Depends(current_principal)) -> dict:
        # A second dependency in the same request reuses the verified principal.
        second = await current_principal(request)
        return {"same": first is second, "user": get_user_id(), "kid": first.key_id}

    return app


@pytest.fixture
async def client():
    transport = ASGITransport(app=_app(), raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://t") as c:
        yield c


# --- request ids -------------------------------------------------------------------


async def test_a_valid_inbound_request_id_is_adopted_everywhere(client) -> None:
    r = await client.get("/ids", headers={"X-Request-ID": "gw-7f3a"})
    assert r.json() == {"context": "gw-7f3a", "state": "gw-7f3a", "user": None}
    assert r.headers["x-request-id"] == "gw-7f3a"


@pytest.mark.parametrize(
    "inbound",
    ["", "has space", "x" * 129, "tab\there"],
    ids=["empty", "space", "too-long", "unprintable"],
)
async def test_an_invalid_inbound_request_id_is_replaced(client, inbound: str) -> None:
    r = await client.get("/ids", headers={"X-Request-ID": inbound})
    generated = r.headers["x-request-id"]
    assert generated != inbound and len(generated) == 36
    assert r.json()["context"] == generated


async def test_the_longest_valid_inbound_id_is_kept(client) -> None:
    inbound = "a" * 128
    r = await client.get("/ids", headers={"X-Request-ID": inbound})
    assert r.headers["x-request-id"] == inbound


async def test_the_gateway_never_adopts_an_inbound_id() -> None:
    transport = ASGITransport(app=_app(adopt_inbound=False))
    async with AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/ids", headers={"X-Request-ID": "client-chosen"})
    assert r.headers["x-request-id"] != "client-chosen"
    assert r.json()["context"] == r.headers["x-request-id"]


async def test_a_handlers_own_request_id_header_is_overwritten_not_duplicated(client) -> None:
    r = await client.get("/own-header", headers={"X-Request-ID": "gw-1"})
    assert r.headers.get_list("x-request-id") == ["gw-1"]


async def test_concurrent_requests_never_see_each_others_request_id(client) -> None:
    # Interleave 20 requests: each one's context must survive the others' awaits.
    ids = [f"req-{i:02d}" for i in range(20)]
    responses = await asyncio.gather(
        *(
            client.get("/slow", params={"delay": 0.01 * (i % 5)}, headers={"X-Request-ID": rid})
            for i, rid in enumerate(ids)
        )
    )
    for rid, r in zip(ids, responses, strict=True):
        assert r.json() == {"before": rid, "after": rid}
        assert r.headers["x-request-id"] == rid


async def test_non_http_scopes_pass_through_untouched() -> None:
    seen: list[dict] = []

    async def inner(scope, receive, send) -> None:
        seen.append(scope)

    middleware = RequestContextMiddleware(inner)
    scope = {"type": "lifespan"}
    set_request_id("before")
    await middleware(scope, None, None)  # type: ignore[arg-type]
    assert seen == [{"type": "lifespan"}]  # no state, no request id injected
    assert get_request_id() == "before"


async def test_a_request_starts_with_no_user_even_if_the_task_had_one(client) -> None:
    token = mint_token(KEY, sub="user-1")
    authed = await client.get("/twice", headers={"Authorization": f"Bearer {token}"})
    assert authed.json()["user"] == "user-1"
    assert (await client.get("/ids")).json()["user"] is None


# --- X-Client-Id -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("device_01-AB", "device_01-AB"),
        ("a" * 8, "a" * 8),
        ("a" * 64, "a" * 64),
        ("a" * 7, None),
        ("a" * 65, None),
        ("bad id!!", None),
        ("../../etc", None),
        (None, None),
    ],
)
def test_client_id_shape(value: str | None, expected: str | None) -> None:
    assert valid_client_id(value) == expected


async def test_a_malformed_client_id_is_treated_as_absent_not_refused(client) -> None:
    ok = await client.get("/client", headers={"X-Client-Id": "tablet-0001"})
    assert ok.json() == {"client_id": "tablet-0001"}
    bad = await client.get("/client", headers={"X-Client-Id": "<script>"})
    assert bad.status_code == 200 and bad.json() == {"client_id": None}
    assert (await client.get("/client")).json() == {"client_id": None}


# --- problem details ---------------------------------------------------------------


async def test_an_unreachable_database_is_503_with_retry_after(client, caplog) -> None:
    with caplog.at_level(logging.WARNING, logger="smartretail_common.errors"):
        r = await client.get("/db-down")
    assert r.status_code == 503
    assert r.headers["retry-after"] == "5"
    body = r.json()
    assert body["type"].endswith("/dependency-unavailable")
    assert "SELECT" not in r.text and "refused" not in r.text
    assert any(rec.message == "dependency unavailable" for rec in caplog.records)


async def test_a_refused_socket_is_503_too(client) -> None:
    assert (await client.get("/refused")).status_code == 503


async def test_unprocessable_carries_its_violations(client) -> None:
    r = await client.get("/unprocessable")
    assert r.status_code == 422
    body = r.json()
    assert body["detail"] == "line items are invalid"
    assert body["violations"] == [
        {"field": "items.0.quantity", "message": "too many", "code": "max"}
    ]


def test_unprocessable_without_violations_adds_none() -> None:
    assert "violations" not in Unprocessable("x").extras
    assert "violations" not in Unprocessable("x", violations=[]).extras


def test_is_connection_error() -> None:
    assert is_connection_error(TimeoutError())
    assert is_connection_error(OSError("dns"))
    invalidated = DBAPIError("SELECT 1", {}, Exception("gone"), connection_invalidated=True)
    assert is_connection_error(invalidated)
    assert not is_connection_error(DBAPIError("SELECT 1", {}, Exception("syntax")))
    assert not is_connection_error(ValueError("bug"))


def test_a_problem_outside_a_request_has_only_the_required_members() -> None:
    # e.g. raised from a background task: no request id, no instance, no extras.
    def run() -> dict:
        response = problem_response(status=409, type_slug="conflict", title="Conflict")
        assert "x-request-id" not in response.headers
        return json.loads(response.body)

    body = contextvars.Context().run(run)  # an empty context: no request id set
    assert body == {
        "type": "https://smartretail.local/errors/conflict",
        "title": "Conflict",
        "status": 409,
    }


# --- pagination --------------------------------------------------------------------


def test_a_slice_becomes_a_page_converting_each_item() -> None:
    params = PageParams(limit=2, offset=4)
    page = Slice(items=[1, 2], total=7, params=params).to_page(lambda n: f"#{n}")
    assert page.model_dump() == {"items": ["#1", "#2"], "total": 7, "limit": 2, "offset": 4}


def test_an_empty_slice_past_the_end_keeps_the_total() -> None:
    page = Slice(items=[], total=3, params=PageParams(limit=20, offset=40)).to_page(str)
    assert page.items == [] and page.total == 3 and page.offset == 40


# --- enum columns ------------------------------------------------------------------


class Status(DomainEnum):
    ACTIVE = "active"


def test_an_enum_column_reads_null_as_none_and_reports_its_python_type() -> None:
    column = DomainEnumText(Status, on_unknown="raise")
    assert column.process_result_value(None, postgresql.dialect()) is None
    assert column.python_type is Status


# --- auth --------------------------------------------------------------------------


async def test_a_named_static_key_verifies_its_kid(client) -> None:
    token = mint_token(NAMED, sub="user-2")
    r = await client.get("/twice", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200
    assert r.json()["kid"] == "named"


async def test_the_principal_is_verified_once_per_request(client) -> None:
    token = mint_token(KEY, sub="user-3")
    r = await client.get("/twice", headers={"Authorization": f"Bearer {token}"})
    assert r.json() == {"same": True, "user": "user-3", "kid": "k1"}
