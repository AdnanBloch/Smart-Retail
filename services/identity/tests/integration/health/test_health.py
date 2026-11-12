"""/health and /health/ready."""


async def test_health(client) -> None:
    assert (await client.get("/health")).json() == {"status": "ok"}
    r = await client.get("/health/ready")
    assert r.status_code == 200 and r.json()["checks"] == {"database": "ok", "signing_key": "ok"}


async def test_shutdown_disposes_the_database_pool(app) -> None:
    await app.state.db.ping()  # opens the pool
    assert app.state.db._engine is not None
    async with app.router.lifespan_context(app):
        pass
    assert app.state.db._engine is None
