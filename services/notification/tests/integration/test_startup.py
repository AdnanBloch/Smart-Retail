"""The service refuses to start without a JWT verification key (A5): every route
but health would otherwise answer 401 to every caller."""

import pytest

from main import create_application
from smartretail_common.auth import JwtVerifier
from tests.integration.conftest import close_app


async def test_startup_fails_without_a_verification_key(settings) -> None:
    keyless = JwtVerifier(  # JWKS unreachable, no static key
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
        jwks_url="http://127.0.0.1:1/jwks",
    )
    app = create_application(settings, verifier=keyless)
    try:
        with pytest.raises(RuntimeError, match="no JWT verification key"):
            async with app.router.lifespan_context(app):
                pass
    finally:
        await close_app(app)
