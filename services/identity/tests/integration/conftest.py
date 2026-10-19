from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from main import create_application
from smartretail_common.testing import provision_test_database, truncate_all
from tests.conftest import make_settings

SERVICE_DIR = str(Path(__file__).resolve().parents[2])
TABLES = ["refresh_sessions", "merchant_profiles", "users", "outbox"]


@pytest.fixture(scope="session")
def database_url() -> str:
    url = provision_test_database("identity_test", SERVICE_DIR)
    if url is None:
        pytest.skip("test Postgres not reachable; run `make test-db`")
    return url


@pytest.fixture
def settings(database_url, keys_dir, keypair):
    return make_settings(keys_dir, keypair.kid, database_url)


@pytest.fixture
async def app(settings):
    application = create_application(settings)
    yield application
    await application.state.db.dispose()
    await truncate_all(settings.database_url, TABLES)


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://identity") as c:
        yield c


PASSWORD = "correct horse battery"


async def register(client, email, role="customer", **extra):
    body = {"email": email, "password": PASSWORD, "full_name": "Test User", "role": role, **extra}
    r = await client.post("/api/v1/auth/register", json=body)
    assert r.status_code == 201, r.text
    return r.json()


async def login(client, email, password=PASSWORD):
    r = await client.post("/api/v1/auth/login", json={"email": email, "password": password})
    assert r.status_code == 200, r.text
    return r.json()


def bearer(tokens) -> dict:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


async def make_admin(app, email="admin@test.local"):
    """Admins cannot self-register; create one directly."""
    from app.core.enums import UserRole
    from app.models import User

    async with app.state.db.sessionmaker() as session:
        session.add(
            User(
                email=email,
                full_name="Admin",
                role=UserRole.ADMIN,
                password_hash=await app.state.hasher.hash(PASSWORD),
            )
        )
        await session.commit()
