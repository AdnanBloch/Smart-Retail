import json
from collections.abc import Callable

import httpx
import pytest
from httpx import ASGITransport, AsyncClient

from app.core.config import GatewaySettings
from main import create_application
from smartretail_common.auth import JwtVerifier
from smartretail_common.testing import KeyPair, generate_keypair, mint_token

KEY = generate_keypair("gw-test")


class Upstream:
    """A scriptable fake upstream. Records what the gateway sent."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.requests: list[httpx.Request] = []
        self.handler: Callable[[httpx.Request], httpx.Response] = self.default

    def default(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/openapi.json":
            return httpx.Response(200, json=openapi_doc(self.name))
        if request.url.path == "/health":
            return httpx.Response(200, json={"status": "ok"})
        return httpx.Response(200, json={"upstream": self.name, "path": request.url.path})

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self.handler(request)

    @property
    def last(self) -> httpx.Request:
        return self.requests[-1]


def openapi_doc(name: str) -> dict:
    schema = f"{name.title()}Out"
    return {
        "openapi": "3.1.0",
        "info": {"title": name, "version": "1"},
        "paths": {
            f"/api/v1/{name}-thing": {
                "get": {
                    "tags": [name],
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": f"#/components/schemas/{schema}"}
                                }
                            },
                        }
                    },
                }
            },
            "/health": {"get": {"responses": {"200": {"description": "ok"}}}},
        },
        "components": {"schemas": {schema: {"type": "object"}}},
    }


@pytest.fixture
def keypair() -> KeyPair:
    return KEY


@pytest.fixture
def upstreams() -> dict[str, Upstream]:
    return {name: Upstream(name) for name in ("identity", "catalog", "order", "inventory")}


@pytest.fixture
def settings() -> GatewaySettings:
    return GatewaySettings(jwt_public_key_path=None, gateway_max_body_bytes=1024)


@pytest.fixture
async def client(settings, upstreams):
    verifier = JwtVerifier(
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
        static_keys={None: KEY.public_key},
    )
    app = create_application(
        settings,
        verifier=verifier,
        transports={n: httpx.MockTransport(u) for n, u in upstreams.items()},
    )
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://gateway") as c:
        yield c
    for upstream_client in app.state.clients.values():
        await upstream_client.aclose()


def token(role: str | None = "customer", **kwargs) -> dict[str, str]:
    return {"Authorization": f"Bearer {mint_token(KEY, role=role, **kwargs)}"}


def problem(response: httpx.Response) -> dict:
    assert response.headers["content-type"] == "application/problem+json", response.text
    return json.loads(response.content)
