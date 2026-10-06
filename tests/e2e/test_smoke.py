"""GW-T9 smoke test: the M1 user journey through the gateway on :8000.

Needs a running, migrated and seeded stack: ``make up migrate seed``.
"""

import os
import uuid
from pathlib import Path

import httpx
import pytest

BASE_URL = os.environ.get("GATEWAY_URL", "http://127.0.0.1:8000")
REPO = Path(__file__).resolve().parents[2]


def _env(name: str, default: str) -> str:
    if name in os.environ:
        return os.environ[name]
    env_file = REPO / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith(f"{name}="):
                return line.split("=", 1)[1]
    return default


ADMIN_EMAIL = _env("SEED_ADMIN_EMAIL", "admin@smartretail.local")
ADMIN_PASSWORD = _env("SEED_ADMIN_PASSWORD", "change-me-admin-password")


@pytest.fixture(scope="module")
def client():
    with httpx.Client(base_url=BASE_URL, timeout=10) as c:
        try:
            c.get("/health").raise_for_status()
        except httpx.HTTPError:
            pytest.skip(f"gateway not reachable at {BASE_URL}; run `make up migrate seed`")
        yield c


def login(client: httpx.Client, email: str, password: str) -> dict[str, str]:
    r = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def test_m1_journey(client: httpx.Client) -> None:
    run = uuid.uuid4().hex[:8]
    password = "e2e smoke password"

    # 1. Register a merchant and a customer, then log in.
    for role in ("merchant", "customer"):
        r = client.post(
            "/api/v1/auth/register",
            json={
                "email": f"{role}-{run}@smartretail.local",
                "password": password,
                "full_name": f"E2E {role}",
                "role": role,
            },
        )
        assert r.status_code == 201, r.text
        assert r.headers["x-request-id"]
    merchant = login(client, f"merchant-{run}@smartretail.local", password)
    customer = login(client, f"customer-{run}@smartretail.local", password)
    admin = login(client, ADMIN_EMAIL, ADMIN_PASSWORD)

    # 2. Admin creates a category; others cannot (rejected at the edge).
    category = {"name": f"E2E {run}", "slug": f"e2e-{run}"}
    assert client.post("/api/v1/categories", json=category, headers=customer).status_code == 403
    r = client.post("/api/v1/categories", json=category, headers=admin)
    assert r.status_code == 201, r.text
    category_id = r.json()["id"]

    # 3. Merchant creates a product; a customer is refused at the gateway.
    body = {
        "sku": f"E2E-{run}",
        "name": "Smoke Shoe",
        "base_price": "59.00",
        "category_id": category_id,
    }
    assert client.post("/api/v1/products", json=body, headers=customer).status_code == 403
    r = client.post("/api/v1/products", json=body, headers=merchant)
    assert r.status_code == 201, r.text
    product = r.json()
    assert product["status"] == "draft" and product["version"] == 1

    # 4. Variant and URL media; each bumps the product version.
    r = client.post(
        f"/api/v1/products/{product['id']}/variants",
        json={"sku": f"E2E-{run}-M", "options": {"size": "M"}, "version": 1},
        headers=merchant,
    )
    assert r.status_code == 201, r.text
    r = client.post(
        f"/api/v1/products/{product['id']}/media",
        json={"url": "https://cdn.example.com/e2e.jpg", "version": 2},
        headers=merchant,
    )
    assert r.status_code == 201, r.text
    version = r.json()["version"]

    # 5. Anonymous browse does not show the draft, nor can it be fetched.
    listing = client.get(f"/api/v1/products?category_id={category_id}").json()
    assert listing["total"] == 0
    assert client.get(f"/api/v1/products/{product['id']}").status_code == 404
    mine = client.get(f"/api/v1/products/me?category_id={category_id}", headers=merchant).json()
    assert [p["id"] for p in mine["items"]] == [product["id"]]

    # 6. Status transitions: draft → inactive → draft; publishing is 501 in M1.
    for status in ("inactive", "draft"):
        r = client.post(
            f"/api/v1/products/{product['id']}/status",
            json={"status": status, "version": version},
            headers=merchant,
        )
        assert r.status_code == 200, r.text
        assert r.json()["status"] == status
        version = r.json()["version"]
    r = client.post(
        f"/api/v1/products/{product['id']}/status",
        json={"status": "published", "version": version},
        headers=merchant,
    )
    assert r.status_code == 501

    # 7. The audit trail recorded every step.
    history = client.get(f"/api/v1/products/{product['id']}/history", headers=merchant).json()
    assert [e["action"] for e in history["items"]] == [
        "status_changed",
        "status_changed",
        "media_added",
        "variant_added",
        "created",
    ]


def test_edge_behaviour(client: httpx.Client) -> None:
    r = client.get("/api/v1/does-not-exist")
    assert r.status_code == 404 and r.headers["content-type"] == "application/problem+json"
    assert client.get("/api/v1/users/me").status_code == 401
    ready = client.get("/health/ready").json()
    assert ready["status"] == "ready"
    assert ready["informational"]["upstreams"] == {"identity": "ok", "catalog": "ok"}
    doc = client.get("/openapi.json").json()
    assert "/api/v1/products" in doc["paths"] and "/api/v1/auth/login" in doc["paths"]
    assert "x-upstreams-unavailable" not in doc
    jwks = client.get("/api/v1/auth/jwks").json()
    assert jwks["keys"][0]["alg"] == "RS256"
