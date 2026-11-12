"""Startup and shutdown (A5): no verification key, no service; a clean shutdown
releases the database pool."""

import pytest

from app.core.config import Settings
from app.v1.media.storage import MemoryStorage
from main import create_application
from smartretail_common.auth import JwtVerifier
from tests.integration.conftest import FakeStarter


def _app(database_url, verifier):
    settings = Settings(service_name="catalog", database_url=database_url, jwt_public_key_path=None)
    return create_application(
        settings, verifier=verifier, publish_starter=FakeStarter(), storage=MemoryStorage()
    )


async def test_startup_fails_without_a_verification_key(database_url) -> None:
    """The JWKS endpoint is unreachable and there is no static key."""
    app = _app(
        database_url, JwtVerifier(issuer="i", audience="a", jwks_url="http://127.0.0.1:1/jwks")
    )
    try:
        with pytest.raises(RuntimeError, match="no JWT verification key"):
            async with app.router.lifespan_context(app):
                pass
    finally:
        await app.state.db.dispose()


async def test_startup_with_a_key_serves_then_disposes_the_pool(database_url, keypair) -> None:
    verifier = JwtVerifier(issuer="i", audience="a", static_keys={None: keypair.public_key})
    app = _app(database_url, verifier)
    async with app.router.lifespan_context(app):
        assert await app.state.db.ping()
    assert app.state.db._engine is None  # disposed on shutdown
