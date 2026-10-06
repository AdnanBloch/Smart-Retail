"""/health and /health/ready."""


async def test_health(client) -> None:
    assert (await client.get("/health")).json() == {"status": "ok"}
    r = await client.get("/health/ready")
    assert r.status_code == 200 and r.json()["checks"] == {"database": "ok", "signing_key": "ok"}
