import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import jwt
import pytest
from httpx import ASGITransport, AsyncClient

from app.core.clients.llm import ScriptedProvider
from app.core.config import Settings
from app.core.errors import EmbeddingUnavailable
from app.v1.search.clients.embeddings import HashingEmbeddings
from app.v1.search.indexing_repo import IndexingRepo
from main import create_application
from smartretail_common.auth import JwtVerifier
from smartretail_common.testing import provision_test_database, truncate_all

SERVICE_DIR = str(Path(__file__).resolve().parents[2])
TABLES = [
    "messages",
    "conversations",
    "generations",
    "bulk_generation_jobs",
    "token_budgets",
    "prompt_templates",
    "product_chunks",
    "embedding_cache",
    "index_state",
    "query_log",
    "outbox",
    "processed_events",
]


@pytest.fixture(scope="session")
def database_url() -> str:
    url = provision_test_database("assistant_test", SERVICE_DIR)
    if url is None:
        pytest.skip("test Postgres not reachable; run `make test-db`")
    return url


class CountingEmbeddings(HashingEmbeddings):
    """The hashing provider, counting calls and texts; ``down`` simulates an outage."""

    def __init__(self) -> None:
        super().__init__(1024)
        self.calls = 0
        self.texts = 0
        self.down = False

    async def embed(self, texts, *, input_type):
        if self.down:
            raise EmbeddingUnavailable("embedding provider down (test)")
        self.calls += 1
        self.texts += len(texts)
        return await super().embed(texts, input_type=input_type)


@dataclass
class Upstreams:
    """catalog and inventory, faked behind ``httpx.MockTransport``."""

    products: dict[str, dict[str, Any]] = field(default_factory=dict)
    stock: dict[str, int] = field(default_factory=dict)
    catalog_down: bool = False
    inventory_down: bool = False
    catalog_calls: int = 0

    def add_product(
        self,
        product_id: str,
        merchant_id: str,
        *,
        name: str,
        status: str = "published",
        price: str = "10.00",
        description: str = "",
        category: dict[str, Any] | None = None,
        attributes: dict[str, Any] | None = None,
        stock: int = 5,
    ) -> dict[str, Any]:
        variant_id = str(uuid.uuid4())
        product = {
            "id": product_id,
            "merchant_id": merchant_id,
            "sku": f"SKU-{product_id[:8]}",
            "name": name,
            "description": description,
            "category_id": category["id"] if category else None,
            "category": category,
            "base_price": price,
            "currency": "USD",
            "status": status,
            "attributes": attributes or {},
            "variants": [
                {"id": variant_id, "sku": "V1", "options": {"size": "M"}, "is_active": True}
            ],
            "media": [],
        }
        self.products[product_id] = product
        self.stock[variant_id] = stock
        return product

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.startswith("/api/v1/products/"):
            self.catalog_calls += 1
            if self.catalog_down:
                return httpx.Response(503, json={"title": "down"})
            product = self.products.get(path.rsplit("/", 1)[-1])
            if product is None:
                return httpx.Response(404, json={"title": "not found"})
            auth = request.headers.get("authorization", "")
            caller = None
            if auth.startswith("Bearer "):
                claims = jwt.decode(auth[7:], options={"verify_signature": False})
                caller = claims.get("sub") if claims.get("role") == "merchant" else None
            if product["status"] != "published" and caller != product["merchant_id"]:
                return httpx.Response(404, json={"title": "not found"})
            return httpx.Response(200, json=product)
        if path.startswith("/api/v1/inventory/"):
            if self.inventory_down:
                return httpx.Response(503, json={"title": "down"})
            variant = path.rsplit("/", 1)[-1]
            if variant not in self.stock:
                return httpx.Response(404, json={"title": "not found"})
            on_hand = self.stock[variant]
            return httpx.Response(
                200,
                json={
                    "variant_id": variant,
                    "on_hand": on_hand,
                    "reserved": 0,
                    "available": on_hand,
                    "in_stock": on_hand > 0,
                },
            )
        return httpx.Response(404)


@pytest.fixture
def upstreams() -> Upstreams:
    return Upstreams()


@pytest.fixture
def embeddings() -> CountingEmbeddings:
    return CountingEmbeddings()


@pytest.fixture
def llm() -> ScriptedProvider:
    return ScriptedProvider()


@pytest.fixture
def enqueued() -> list[tuple[Any, ...]]:
    return []


@pytest.fixture
def settings(database_url) -> Settings:
    return Settings(
        database_url=database_url,
        jwt_public_key_path=None,
        db_pool_size=10,
        db_max_overflow=20,
        catalog_base_url="http://catalog",
        inventory_base_url="http://inventory",
        sse_keepalive_seconds=0.2,
    )


@pytest.fixture
async def app(settings, keypair, upstreams, embeddings, llm, enqueued):
    verifier = JwtVerifier(
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
        static_keys={None: keypair.public_key},
    )
    http = httpx.AsyncClient(transport=httpx.MockTransport(upstreams.handler))
    application = create_application(
        settings,
        verifier=verifier,
        http=http,
        embeddings=embeddings,
        llm=llm,
        enqueue=lambda name, *args: enqueued.append((name, *args)),
    )
    yield application
    await http.aclose()
    await application.state.db.dispose()
    await truncate_all(settings.database_url, TABLES)


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://assistant") as c:
        yield c


def product_payload(
    product_id: str,
    merchant_id: str,
    *,
    name: str,
    description: str = "",
    version: int = 1,
    status: str = "published",
    price: str = "10.00",
    category: dict[str, Any] | None = None,
    attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """A ``catalog.product.published`` / ``.updated`` payload with a search document."""
    variant = {"variant_id": str(uuid.uuid4()), "sku": "V1", "options": {"size": "M"}}
    return {
        "product_id": product_id,
        "merchant_id": merchant_id,
        "version": version,
        "sku": f"SKU-{product_id[:8]}",
        "name": name,
        "status": status,
        "category_id": category["id"] if category else None,
        "base_price": price,
        "currency": "USD",
        "published_at": None,
        "variants": [{**variant, "price_override": None, "is_active": True}],
        "search_document": {
            "product_id": product_id,
            "merchant_id": merchant_id,
            "version": version,
            "name": name,
            "sku": f"SKU-{product_id[:8]}",
            "description": description,
            "category": category,
            "attributes": attributes or {},
            "variants": [{**variant, "price": price, "is_active": True}],
            "base_price": price,
            "currency": "USD",
            "media": [{"url": "http://img/1.jpg"}],
        },
        "changed_fields": [],
    }


async def index(app, payload: dict[str, Any]):
    async with app.state.db.sessionmaker() as session, session.begin():
        return await IndexingRepo(session, app.state.embeddings, app.state.settings).index_payload(
            payload
        )


@dataclass
class Catalogue:
    """Three published products across two merchants and two categories."""

    merchant_a: str
    merchant_b: str
    shoes: str
    boots: str
    blender: str
    footwear: dict[str, Any]
    kitchen: dict[str, Any]


async def seed_catalogue(app, upstreams: Upstreams, callers) -> Catalogue:
    footwear = {"id": str(uuid.uuid4()), "name": "Running", "path": ["Footwear", "Running"]}
    kitchen = {"id": str(uuid.uuid4()), "name": "Blenders", "path": ["Kitchen", "Blenders"]}
    a, b = callers.merchant_a_id, callers.merchant_b_id
    items = [
        (
            a,
            "Acme Runner X1",
            "Lightweight breathable running shoes with mesh upper for beginners.",
            "89.00",
            footwear,
            {"brand": "Acme", "material": "mesh"},
        ),
        (
            a,
            "Trail Boot Pro",
            "Waterproof hiking boots with rugged grip for mountain trails.",
            "149.00",
            footwear,
            {"brand": "Acme", "waterproof": True},
        ),
        (
            b,
            "Vortex Blender 500",
            "Powerful kitchen blender for smoothies and soups, 500 watt motor.",
            "59.00",
            kitchen,
            {"brand": "Vortex", "watts": 500},
        ),
    ]
    ids = []
    for merchant, name, description, price, category, attributes in items:
        product_id = str(uuid.uuid4())
        ids.append(product_id)
        upstreams.add_product(
            product_id,
            merchant,
            name=name,
            price=price,
            description=description,
            category=category,
            attributes=attributes,
        )
        await index(
            app,
            product_payload(
                product_id,
                merchant,
                name=name,
                description=description,
                price=price,
                category=category,
                attributes=attributes,
            ),
        )
    return Catalogue(a, b, ids[0], ids[1], ids[2], footwear, kitchen)
