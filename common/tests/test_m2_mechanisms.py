"""Milestone 2 mechanisms: outbox writer, idempotency helpers, service tokens,
scope guard and the Temporal helpers."""

import json
import uuid
from decimal import Decimal

import httpx
import pytest
from fastapi import Depends, FastAPI
from httpx import ASGITransport, AsyncClient
from temporalio.exceptions import ApplicationError

from smartretail_common.app import install_platform
from smartretail_common.auth import (
    InvalidToken,
    JwtVerifier,
    Principal,
    current_principal,
    optional_principal,
    require_roles,
    require_scope,
)
from smartretail_common.context import set_request_id
from smartretail_common.idempotency import (
    IdempotencyKeyInvalid,
    IdempotencyKeyRequired,
    canonical_key,
    request_hash,
)
from smartretail_common.outbox import OutboxEvent, write_outbox
from smartretail_common.service_token import (
    ServiceCredentials,
    ServiceTokenClient,
    ServiceTokenError,
)
from smartretail_common.temporal import (
    TemporalClientFactory,
    activity_idempotency_token,
    business_failure,
    default_retry,
    raise_for_activity,
)
from smartretail_common.testing import generate_keypair, mint_service_token, mint_token

KEY = generate_keypair("k-m2")


def _verifier() -> JwtVerifier:
    return JwtVerifier(
        issuer="smartretail-identity", audience="smartretail", static_keys={None: KEY.public_key}
    )


# --- outbox ----------------------------------------------------------------


class _Session:
    def __init__(self) -> None:
        self.added: list[object] = []

    def add(self, obj: object) -> None:
        self.added.append(obj)


def test_write_outbox_adds_a_row_to_the_callers_transaction() -> None:
    set_request_id("req-1")
    session = _Session()
    aggregate = uuid.uuid4()
    event_id = write_outbox(
        session,  # type: ignore[arg-type]
        event_type="thing.happened",
        aggregate_type="thing",
        aggregate_id=aggregate,
        payload={"amount": Decimal("1.50"), "id": aggregate},
        client_id="web",
    )
    [row] = session.added
    assert isinstance(row, OutboxEvent)
    assert row.event_id == event_id and row.aggregate_id == aggregate
    # Decimals and UUIDs become JSON strings (S2): never floats.
    assert row.payload == {"amount": "1.50", "id": str(aggregate)}
    assert row.correlation_id == "req-1" and row.client_id == "web"
    assert row.published_at is None


def test_outbox_correlation_id_can_be_explicit() -> None:
    session = _Session()
    write_outbox(
        session,  # type: ignore[arg-type]
        event_type="e",
        aggregate_type="a",
        aggregate_id=uuid.uuid4(),
        payload={},
        correlation_id="wf-corr",
        causation_id="cause",
    )
    row = session.added[0]
    assert isinstance(row, OutboxEvent)
    assert (row.correlation_id, row.causation_id) == ("wf-corr", "cause")


# --- idempotency -----------------------------------------------------------


def test_canonical_key_rules() -> None:
    assert canonical_key("  abc-1 ") == "abc-1"
    upper = str(uuid.uuid4()).upper()
    assert canonical_key(upper) == upper.lower()
    for bad in (None, "", "   "):
        with pytest.raises(IdempotencyKeyRequired):
            canonical_key(bad)
    for bad in ("x" * 256, "naïve", "tab\there"):
        with pytest.raises(IdempotencyKeyInvalid):
            canonical_key(bad)


def test_request_hash_is_canonical() -> None:
    a = {"items": [{"q": 2, "id": "x"}], "pay": "tok"}
    b = json.loads('{"pay": "tok", "items": [{"id": "x", "q": 2.0}]}')
    assert request_hash(a) == request_hash(b)
    assert request_hash({"n": Decimal("10.0")}) == request_hash({"n": 10})
    # List order is significant, and so is every value.
    assert request_hash({"items": [1, 2]}) != request_hash({"items": [2, 1]})
    assert request_hash({"flag": True, "none": None}) != request_hash({"flag": False, "none": None})
    assert request_hash({"o": object.__new__(object)}) != request_hash({"o": 1})


# --- service tokens and scopes ---------------------------------------------


async def test_service_token_maps_scopes() -> None:
    token = mint_service_token(KEY, scopes=("inventory:internal", "catalog:internal"))
    principal = await _verifier().verify(token)
    assert principal.is_service and principal.subject == "service:temporal-worker"
    assert principal.scopes == {"inventory:internal", "catalog:internal"}
    assert principal.role is None


async def test_typ_and_subject_must_agree() -> None:
    # A user subject with typ=service, and a service subject with typ=access.
    forged_user = mint_token(KEY, extra={"typ": "service"})
    forged_service = mint_token(KEY, sub="service:temporal-worker")
    for token in (forged_user, forged_service):
        with pytest.raises(InvalidToken):
            await _verifier().verify(token)


def _app() -> FastAPI:
    app = FastAPI()
    app.state.verifier = _verifier()
    install_platform(app)

    @app.get("/internal")
    async def internal(p: Principal = Depends(require_scope("inventory:internal"))) -> dict:
        return {"sub": p.subject}

    @app.get("/me")
    async def me(p: Principal = Depends(current_principal)) -> dict:
        return {"sub": p.subject}

    @app.get("/maybe")
    async def maybe(p: Principal | None = Depends(optional_principal)) -> dict:
        return {"sub": p.subject if p else None}

    @app.get("/admin")
    async def admin(p: Principal = Depends(require_roles("admin"))) -> dict:
        return {"sub": p.subject}

    return app


async def test_scope_guard_and_user_routes_are_mutually_exclusive() -> None:
    good = {"Authorization": f"Bearer {mint_service_token(KEY, scopes=('inventory:internal',))}"}
    wrong = {"Authorization": f"Bearer {mint_service_token(KEY, scopes=('catalog:internal',))}"}
    admin = {"Authorization": f"Bearer {mint_token(KEY, role='admin')}"}
    async with AsyncClient(transport=ASGITransport(app=_app()), base_url="http://t") as c:
        assert (await c.get("/internal", headers=good)).json() == {"sub": "service:temporal-worker"}
        assert (await c.get("/internal")).status_code == 401
        assert (await c.get("/internal", headers=wrong)).status_code == 403
        # A user token is refused on an internal route, even an admin's.
        assert (await c.get("/internal", headers=admin)).status_code == 403
        # A service token is refused on every user-facing route.
        for path in ("/me", "/maybe", "/admin"):
            assert (await c.get(path, headers=good)).status_code == 401, path
        assert (await c.get("/maybe")).json() == {"sub": None}


def _token_endpoint(responses: list[httpx.Response], seen: list[dict]) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(dict(httpx.QueryParams(request.content.decode())))
        return responses.pop(0)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_service_token_client_caches_and_refreshes() -> None:
    seen: list[dict] = []
    ok = httpx.Response(200, json={"access_token": "t1", "expires_in": 300})
    ok2 = httpx.Response(200, json={"access_token": "t2", "expires_in": 300})
    creds = ServiceCredentials("http://identity/token", "temporal-worker", "s3cret", "a b")
    client = ServiceTokenClient(creds, http_client=_token_endpoint([ok, ok2], seen))
    assert await client.headers() == {"Authorization": "Bearer t1"}
    assert await client.token() == "t1"  # cached
    assert seen == [
        {
            "grant_type": "client_credentials",
            "client_id": "temporal-worker",
            "client_secret": "s3cret",
            "scope": "a b",
        }
    ]
    client.invalidate()
    assert await client.token() == "t2"


async def test_service_token_client_failures() -> None:
    creds = ServiceCredentials("http://identity/token", "w", "s")
    denied = ServiceTokenClient(
        creds, http_client=_token_endpoint([httpx.Response(401, json={})], [])
    )
    with pytest.raises(ServiceTokenError):
        await denied.token()

    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("down", request=request)

    down = ServiceTokenClient(
        creds, http_client=httpx.AsyncClient(transport=httpx.MockTransport(boom))
    )
    with pytest.raises(ServiceTokenError):
        await down.token()


async def test_service_token_client_owns_its_http_client(monkeypatch) -> None:
    seen: list[dict] = []
    endpoint = _token_endpoint(
        [httpx.Response(200, json={"access_token": "t", "expires_in": 60})], seen
    )
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_: endpoint)
    client = ServiceTokenClient(ServiceCredentials("http://identity/token", "w", "s"))
    assert await client.token() == "t"
    assert "scope" not in seen[0]


# --- temporal helpers ------------------------------------------------------


def _response(status: int, body: object = None) -> httpx.Response:
    request = httpx.Request("POST", "http://inventory/internal/v1/x")
    if body is None:
        return httpx.Response(status, request=request, content=b"not json")
    return httpx.Response(status, request=request, json=body)


def test_raise_for_activity_follows_the_error_contract() -> None:
    raise_for_activity(_response(200, {}))

    with pytest.raises(ApplicationError) as business:
        raise_for_activity(_response(409, {"type": "https://x/errors/insufficient-stock"}))
    assert business.value.non_retryable and business.value.type == "insufficient-stock"

    for status in (500, 503, 429):
        with pytest.raises(ApplicationError) as transient:
            raise_for_activity(_response(status))
        assert not transient.value.non_retryable

    with pytest.raises(ApplicationError) as unknown_body:
        raise_for_activity(_response(422, ["not", "a", "problem"]))
    assert unknown_body.value.type == "http-422"


def test_raise_for_activity_drops_an_expired_service_token() -> None:
    tokens = ServiceTokenClient(ServiceCredentials("u", "c", "s"))
    tokens._token = "stale"
    with pytest.raises(ApplicationError) as err:
        raise_for_activity(_response(401, {"type": "x/invalid-token"}), tokens=tokens)
    assert not err.value.non_retryable and tokens._token is None


def test_retry_and_tokens() -> None:
    policy = default_retry(attempts=5, non_retryable=("PaymentDeclined",))
    assert policy.maximum_attempts == 5 and policy.backoff_coefficient == 2.0
    assert policy.non_retryable_error_types == ["PaymentDeclined"]
    assert activity_idempotency_token("order-1", "charge", 7) == "order-1:charge:7"
    failure = business_failure("ProductIncomplete", "missing", missing=["name"])
    assert failure.non_retryable and failure.details == ({"missing": ["name"]},)
    assert business_failure("X", "m").details == ()


async def test_temporal_client_factory_connects_once(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    async def fake_connect(address: str, *, namespace: str) -> str:
        calls.append((address, namespace))
        return "client"

    from temporalio.client import Client

    monkeypatch.setattr(Client, "connect", fake_connect)
    factory = TemporalClientFactory("temporal:7233")
    assert await factory.get() == "client"
    assert await factory.get() == "client"
    assert calls == [("temporal:7233", "smartretail")]
    factory.set("other")  # type: ignore[arg-type]
    assert await factory.get() == "other"
