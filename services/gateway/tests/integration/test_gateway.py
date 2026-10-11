import httpx
import pytest

from tests.conftest import KEY, problem, token


async def test_forwards_path_query_and_body(client, upstreams) -> None:
    r = await client.post(
        "/api/v1/auth/register?x=1", json={"email": "a@b.c"}, headers={"X-User-Id": "spoofed"}
    )
    assert r.status_code == 200 and r.json()["upstream"] == "identity"
    sent = upstreams["identity"].last
    assert sent.url.path == "/api/v1/auth/register" and sent.url.query == b"x=1"
    assert sent.content == b'{"email":"a@b.c"}'
    assert "x-user-id" not in sent.headers  # GW-5
    assert sent.headers["x-request-id"] == r.headers["x-request-id"]
    assert sent.headers["x-forwarded-proto"] == "http"


async def test_authorization_forwarded_unchanged(client, upstreams) -> None:
    headers = token("merchant")
    r = await client.get("/api/v1/products/me", headers=headers)
    assert r.status_code == 200
    assert upstreams["catalog"].last.headers["authorization"] == headers["Authorization"]


async def test_request_id_is_generated_not_adopted(client) -> None:
    r = await client.get("/api/v1/products", headers={"X-Request-ID": "client-chosen"})
    assert r.headers["x-request-id"] != "client-chosen"
    assert len(r.headers["x-request-id"]) == 36


async def test_route_not_found_and_method_not_allowed(client) -> None:
    r = await client.get("/api/v1/nowhere")
    assert r.status_code == 404 and problem(r)["type"].endswith("/route-not-found")
    assert r.headers["x-request-id"]
    r = await client.put("/api/v1/products/abc")
    assert r.status_code == 405 and r.headers["allow"] == "DELETE, GET, PATCH"
    assert (await client.post("/api/v1/auth/token")).status_code == 404
    assert (await client.get("/internal/v1/x")).status_code == 404


@pytest.mark.parametrize(
    ("headers", "status"),
    [
        ({}, 401),
        ({"Authorization": "Bearer garbage"}, 401),
        (token("admin", ttl=-60), 401),
        (token("customer"), 403),
        (token("merchant"), 200),
        (token("admin"), 200),
    ],
)
async def test_bearer_policy_with_roles(client, headers, status) -> None:
    r = await client.post("/api/v1/products", json={}, headers=headers)
    assert r.status_code == status
    if status == 401:
        assert r.headers["www-authenticate"].startswith("Bearer")
        assert problem(r)["type"].endswith("/invalid-token")


async def test_wrong_key_and_alg_none_rejected(client) -> None:
    from smartretail_common.testing import generate_keypair, mint_token

    other = generate_keypair("other")
    r = await client.get(
        "/api/v1/users/me", headers={"Authorization": f"Bearer {mint_token(other)}"}
    )
    assert r.status_code == 401


async def test_optional_policy(client) -> None:
    assert (await client.get("/api/v1/products")).status_code == 200
    assert (await client.get("/api/v1/products", headers=token())).status_code == 200
    expired = await client.get("/api/v1/products", headers=token(ttl=-60))
    assert expired.status_code == 401 and problem(expired)["detail"] == "expired"


async def test_public_policy_ignores_bad_token(client, upstreams) -> None:
    r = await client.post("/api/v1/auth/login", json={}, headers={"Authorization": "Bearer junk"})
    assert r.status_code == 200
    assert upstreams["identity"].last.headers["authorization"] == "Bearer junk"


async def test_service_token_refused_at_the_edge(client) -> None:
    """GW-T10: a service token is never good for a routed (user-facing) route."""
    from smartretail_common.testing import mint_service_token

    svc = {"Authorization": f"Bearer {mint_service_token(KEY, scopes=('inventory:internal',))}"}
    assert (await client.post("/api/v1/products", json={}, headers=svc)).status_code == 401
    assert (await client.get("/api/v1/products", headers=svc)).status_code == 401
    # Internal routes do not exist at the edge, whatever the token.
    r = await client.post("/internal/v1/inventory/reservations", json={}, headers=svc)
    assert r.status_code == 404


async def test_order_and_inventory_rows(client, upstreams) -> None:
    r = await client.post("/api/v1/orders", json={}, headers=token("customer"))
    assert r.status_code == 200 and upstreams["order"].last.url.path == "/api/v1/orders"
    assert (
        await client.post("/api/v1/orders", json={}, headers=token("merchant"))
    ).status_code == 403
    assert (
        await client.get("/api/v1/orders/merchant", headers=token("merchant"))
    ).status_code == 200
    r = await client.get("/api/v1/inventory/abc")
    assert r.status_code == 200 and upstreams["inventory"].last.url.path == "/api/v1/inventory/abc"
    assert (await client.get("/api/v1/inventory", headers=token("customer"))).status_code == 403


async def test_upload_route_has_the_larger_limit(client, upstreams, settings) -> None:
    body = b"x" * (settings.gateway_max_body_bytes + 10)
    headers = {**token("merchant"), "Content-Type": "application/octet-stream"}
    r = await client.post("/api/v1/products/abc/media/upload", content=body, headers=headers)
    assert r.status_code == 200
    r = await client.post("/api/v1/products/abc/media", content=body, headers=headers)
    assert r.status_code == 413


@pytest.mark.parametrize(
    ("exc", "status", "slug"),
    [
        (httpx.ConnectError("refused"), 503, "upstream-unavailable"),
        (httpx.ReadTimeout("slow"), 504, "upstream-timeout"),
        (httpx.RemoteProtocolError("closed"), 502, "upstream-bad-response"),
    ],
)
async def test_upstream_failures_are_normalised(client, upstreams, exc, status, slug) -> None:
    def fail(request):
        raise exc

    upstreams["catalog"].handler = fail
    r = await client.get("/api/v1/products", headers={"Origin": "http://localhost:3000"})
    assert r.status_code == status
    body = problem(r)
    assert body["type"].endswith(f"/{slug}") and body["upstream"] == "catalog"
    assert r.headers["x-request-id"] and body["request_id"] == r.headers["x-request-id"]
    assert r.headers["access-control-allow-origin"] == "http://localhost:3000"
    if status == 503:
        assert r.headers["retry-after"] == "5"
    if status == 504:
        assert body["timeout_seconds"] == 10


async def test_plaintext_5xx_is_wrapped_problem_5xx_relayed(client, upstreams) -> None:
    upstreams["catalog"].handler = lambda req: httpx.Response(500, text="Traceback: secret")
    r = await client.get("/api/v1/products")
    assert r.status_code == 500 and "secret" not in r.text
    assert problem(r)["type"].endswith("/upstream-error")

    upstreams["catalog"].handler = lambda req: httpx.Response(
        503, json={"type": "x", "status": 503}, headers={"content-type": "application/problem+json"}
    )
    r = await client.get("/api/v1/products")
    assert r.status_code == 503 and r.json()["type"] == "x"


async def test_upstream_4xx_relayed_unchanged(client, upstreams) -> None:
    upstreams["catalog"].handler = lambda req: httpx.Response(
        409,
        json={"type": "conflict"},
        headers={"content-type": "application/problem+json", "Location": "/x"},
    )
    r = await client.patch("/api/v1/products/abc", json={}, headers=token("merchant"))
    assert (
        r.status_code == 409 and r.json() == {"type": "conflict"} and r.headers["location"] == "/x"
    )


async def test_size_limit(client, upstreams) -> None:
    r = await client.post("/api/v1/auth/register", content=b"x" * 2048)
    assert r.status_code == 413 and problem(r)["limit_bytes"] == 1024
    assert upstreams["identity"].requests == []

    async def chunked():
        for _ in range(4):
            yield b"x" * 400

    r = await client.post("/api/v1/auth/register", content=chunked())
    assert r.status_code == 413
    assert upstreams["identity"].requests == []


async def test_cors_preflight_and_headers(client, upstreams) -> None:
    pre = {"Origin": "http://localhost:5173", "Access-Control-Request-Method": "POST"}
    r = await client.options("/api/v1/products", headers=pre)
    assert r.status_code == 204
    assert r.headers["access-control-allow-origin"] == "http://localhost:5173"
    assert r.headers["access-control-max-age"] == "600"
    assert upstreams["catalog"].requests == []  # never proxied

    r = await client.options("/api/v1/products", headers={**pre, "Origin": "https://evil.example"})
    assert r.status_code == 204 and "access-control-allow-origin" not in r.headers

    r = await client.get("/api/v1/nowhere", headers={"Origin": "http://localhost:5173"})
    assert r.headers["access-control-allow-origin"] == "http://localhost:5173"
    assert "X-Request-ID" in r.headers["access-control-expose-headers"]


def test_cors_wildcard_rejected() -> None:
    from app.core.config import GatewaySettings

    with pytest.raises(ValueError):
        GatewaySettings(gateway_cors_allow_origins="*")


async def test_health_and_ready(client, upstreams) -> None:
    assert (await client.get("/health")).json() == {"status": "ok"}
    upstreams["catalog"].handler = lambda req: (_ for _ in ()).throw(httpx.ConnectError("down"))
    r = await client.get("/health/ready")
    assert r.status_code == 200
    body = r.json()
    assert body["checks"] == {"settings": "ok", "public_key": "ok", "route_table": "ok"}
    assert body["informational"]["upstreams"] == {
        "identity": "ok",
        "catalog": "unreachable",
        "order": "ok",
        "inventory": "ok",
    }


async def test_aggregated_openapi(client, upstreams) -> None:
    doc = (await client.get("/openapi.json")).json()
    assert doc["servers"] == [{"url": "http://localhost:8000"}]
    assert "/api/v1/identity-thing" in doc["paths"] and "/api/v1/catalog-thing" in doc["paths"]
    assert "catalog.CatalogOut" in doc["components"]["schemas"]
    ref = doc["paths"]["/api/v1/catalog-thing"]["get"]["responses"]["200"]["content"][
        "application/json"
    ]["schema"]["$ref"]
    assert ref == "#/components/schemas/catalog.CatalogOut"
    assert doc["paths"]["/api/v1/catalog-thing"]["get"]["tags"][0] == "catalog"
    assert "x-upstreams-unavailable" not in doc
    assert (await client.get("/docs")).status_code == 200


async def test_openapi_degrades_when_upstream_down(client, upstreams) -> None:
    upstreams["identity"].handler = lambda req: (_ for _ in ()).throw(httpx.ConnectError("down"))
    r = await client.get("/openapi.json")
    assert r.status_code == 200
    assert r.json()["x-upstreams-unavailable"] == ["identity"]
