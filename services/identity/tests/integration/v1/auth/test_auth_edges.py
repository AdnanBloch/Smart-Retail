"""Login and refresh edge cases (RFC 0003 §3.1.4.2-3): suspended merchants,
expiry versus reuse, password rehash on login, session metadata, and reuse
detection racing a legitimate rotation."""

import asyncio
import uuid

from sqlalchemy import func, select, update

from app.core.enums import MerchantStatus
from app.core.security import PasswordHasher
from app.models import MerchantProfile, RefreshSession, User
from tests.conftest import make_settings
from tests.integration.conftest import PASSWORD, login, register


async def refresh(client, token: str):
    return await client.post("/api/v1/auth/refresh", json={"refresh_token": token})


async def live_sessions(app, user_id: str | None = None) -> int:
    stmt = (
        select(func.count()).select_from(RefreshSession).where(RefreshSession.revoked_at.is_(None))
    )
    if user_id is not None:
        stmt = stmt.where(RefreshSession.user_id == uuid.UUID(user_id))
    async with app.state.db.sessionmaker() as s:
        return int(await s.scalar(stmt) or 0)


async def suspend(app, user_id: str) -> None:
    async with app.state.db.sessionmaker() as s:
        await s.execute(
            update(MerchantProfile)
            .where(MerchantProfile.user_id == uuid.UUID(user_id))
            .values(status=MerchantStatus.SUSPENDED)
        )
        await s.commit()


# --- login -------------------------------------------------------------------------


async def test_a_suspended_merchant_cannot_log_in(app, client) -> None:
    merchant = await register(client, "susp@test.local", role="merchant")
    await suspend(app, merchant["id"])
    r = await client.post(
        "/api/v1/auth/login", json={"email": "susp@test.local", "password": PASSWORD}
    )
    assert r.status_code == 403 and r.json()["type"].endswith("/merchant-suspended")
    assert await live_sessions(app) == 0


async def test_suspension_is_revealed_only_after_the_password_verifies(app, client) -> None:
    merchant = await register(client, "susp2@test.local", role="merchant")
    await suspend(app, merchant["id"])
    r = await client.post(
        "/api/v1/auth/login", json={"email": "susp2@test.local", "password": "wrong password!"}
    )
    assert r.status_code == 401 and r.json()["type"].endswith("/invalid-credentials")


async def test_login_upgrades_a_hash_made_with_old_parameters(app, client, settings) -> None:
    await register(client, "rehash@test.local")
    async with app.state.db.sessionmaker() as s:
        old_hash = await s.scalar(select(User.password_hash))
    # The operator raises the Argon2 cost; existing hashes are upgraded at next login.
    stronger = make_settings(settings.jwt_keys_dir, settings.jwt_active_kid)
    stronger.argon2_memory_cost_kib = 2048
    stronger.argon2_time_cost = 2
    app.state.hasher = PasswordHasher(stronger)
    assert app.state.hasher.needs_rehash(old_hash)

    await login(client, "rehash@test.local")
    async with app.state.db.sessionmaker() as s:
        new_hash = await s.scalar(select(User.password_hash))
    assert new_hash != old_hash and "m=2048,t=2" in new_hash
    assert not app.state.hasher.needs_rehash(new_hash)
    await login(client, "rehash@test.local")  # the upgraded hash still verifies


async def test_a_session_records_the_first_forwarded_address_and_user_agent(app, client) -> None:
    await register(client, "meta@test.local")
    await client.post(
        "/api/v1/auth/login",
        json={"email": "meta@test.local", "password": PASSWORD},
        headers={"X-Forwarded-For": " 203.0.113.9 , 10.0.0.1", "User-Agent": "app/1.0"},
    )
    await login(client, "meta@test.local")  # direct: the transport's peer address
    async with app.state.db.sessionmaker() as s:
        rows = (
            await s.execute(
                select(RefreshSession.ip, RefreshSession.user_agent).order_by(
                    RefreshSession.created_at
                )
            )
        ).all()
    assert rows[0] == ("203.0.113.9", "app/1.0")
    assert rows[1].ip == "127.0.0.1"


# --- refresh -----------------------------------------------------------------------


async def test_refresh_for_a_merchant_suspended_after_login_is_refused_and_revoked(
    app, client
) -> None:
    merchant = await register(client, "later@test.local", role="merchant")
    tokens = await login(client, "later@test.local")
    await suspend(app, merchant["id"])
    r = await refresh(client, tokens["refresh_token"])
    assert r.status_code == 401 and r.json()["type"].endswith("/invalid-refresh-token")
    assert await live_sessions(app, merchant["id"]) == 0


async def test_an_expired_token_is_refused_but_is_not_treated_as_reuse(app, client) -> None:
    user = await register(client, "stale@test.local")
    old_device = await login(client, "stale@test.local")
    new_device = await login(client, "stale@test.local")
    old_sid = (await app.state.verifier.verify(old_device["access_token"])).session_id
    async with app.state.db.sessionmaker() as s:
        await s.execute(
            update(RefreshSession)
            .where(RefreshSession.id == uuid.UUID(old_sid))
            .values(expires_at=RefreshSession.created_at)
        )
        await s.commit()
    assert (await refresh(client, old_device["refresh_token"])).status_code == 401
    # The other device's session is untouched: expiry is not theft.
    assert await live_sessions(app, user["id"]) == 2
    assert (await refresh(client, new_device["refresh_token"])).status_code == 200


async def test_the_rotated_pair_carries_the_new_session(app, client) -> None:
    from smartretail_common.auth import JwtVerifier

    await register(client, "sid@test.local")
    first = await login(client, "sid@test.local")
    second = (await refresh(client, first["refresh_token"])).json()
    verifier: JwtVerifier = app.state.verifier
    first_sid = (await verifier.verify(first["access_token"])).session_id
    second_sid = (await verifier.verify(second["access_token"])).session_id
    assert first_sid != second_sid
    async with app.state.db.sessionmaker() as s:
        live = list(
            await s.scalars(select(RefreshSession.id).where(RefreshSession.revoked_at.is_(None)))
        )
    assert [str(x) for x in live] == [second_sid]


async def test_a_replay_racing_a_rotation_still_revokes_the_new_successor(app, client) -> None:
    """Reuse detection must win even when the replay of an old token arrives
    while its successor is being rotated: the successor minted in that
    rotation must not survive (it may belong to the thief)."""
    user = await register(client, "replay@test.local")
    for _ in range(8):
        first = await login(client, "replay@test.local")
        second = (await refresh(client, first["refresh_token"])).json()
        rotation, replay = await asyncio.gather(
            refresh(client, second["refresh_token"]), refresh(client, first["refresh_token"])
        )
        assert replay.status_code == 401
        assert rotation.status_code in (200, 401)
        assert await live_sessions(app, user["id"]) == 0
        if rotation.status_code == 200:
            # Whatever the rotation handed out is already dead.
            assert (await refresh(client, rotation.json()["refresh_token"])).status_code == 401


async def test_concurrent_replays_of_a_rotated_token_all_fail_and_revoke_everything(
    app, client
) -> None:
    user = await register(client, "replays@test.local")
    stolen = await login(client, "replays@test.local")
    other = await login(client, "replays@test.local")
    await refresh(client, stolen["refresh_token"])
    results = await asyncio.gather(*(refresh(client, stolen["refresh_token"]) for _ in range(4)))
    assert [r.status_code for r in results] == [401] * 4
    assert await live_sessions(app, user["id"]) == 0
    assert (await refresh(client, other["refresh_token"])).status_code == 401
