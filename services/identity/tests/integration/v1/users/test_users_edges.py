"""/api/v1/users edge cases: admin pagination and filters, reactivation,
re-promotion of a suspended merchant, the last-admin race (I10), password
policy on change, and tokens whose subject no longer resolves to a user."""

import asyncio
import uuid

from sqlalchemy import func, select, update

from app.core.enums import UserRole
from app.models import RefreshSession, User
from smartretail_common.testing import mint_token
from tests.integration.conftest import PASSWORD, bearer, login, make_admin, register


async def admin_tokens(app, client, email: str = "admin@test.local") -> dict:
    await make_admin(app, email)
    return await login(client, email)


async def user_id(client, tokens) -> str:
    return (await client.get("/api/v1/users/me", headers=bearer(tokens))).json()["id"]


# --- admin listing -----------------------------------------------------------------


async def test_admin_pages_through_users_newest_first(app, client) -> None:
    admin = await admin_tokens(app, client)
    for i in range(5):
        await register(client, f"page{i}@test.local")
    seen: list[str] = []
    for offset in (0, 2, 4):
        r = await client.get(f"/api/v1/users?limit=2&offset={offset}", headers=bearer(admin))
        page = r.json()
        assert page["total"] == 6 and page["limit"] == 2 and page["offset"] == offset
        seen += [u["email"] for u in page["items"]]
    # Newest first, every user exactly once across the pages.
    assert seen == [f"page{i}@test.local" for i in range(4, -1, -1)] + ["admin@test.local"]
    past_end = (await client.get("/api/v1/users?offset=50", headers=bearer(admin))).json()
    assert past_end["items"] == [] and past_end["total"] == 6


async def test_admin_filters_combine(app, client) -> None:
    admin = await admin_tokens(app, client)
    await register(client, "alice@test.local", full_name="Alice Active")
    await register(client, "bob@test.local", full_name="Bob Benched")
    await register(client, "alina@test.local", role="merchant", full_name="Alina Seller")
    async with app.state.db.sessionmaker() as s:
        await s.execute(update(User).where(User.email == "bob@test.local").values(is_active=False))
        await s.commit()

    async def emails(query: str) -> list[str]:
        r = await client.get(f"/api/v1/users?{query}", headers=bearer(admin))
        assert r.status_code == 200, r.text
        return sorted(u["email"] for u in r.json()["items"])

    assert await emails("is_active=false") == ["bob@test.local"]
    assert await emails("q=ALI") == ["alice@test.local", "alina@test.local"]  # case-insensitive
    assert await emails("q=benched") == ["bob@test.local"]  # matches the name too
    assert await emails("q=ali&role=customer&is_active=true") == ["alice@test.local"]
    assert await emails("q=nobody") == []


async def test_admin_list_rejects_unknown_filters(app, client) -> None:
    admin = await admin_tokens(app, client)
    for query in ("role=superuser", "is_active=maybe", "limit=0", "offset=-1", "q=" + "x" * 201):
        r = await client.get(f"/api/v1/users?{query}", headers=bearer(admin))
        assert r.status_code == 422, query


# --- admin updates -----------------------------------------------------------------


async def test_deactivation_revokes_sessions_and_reactivation_allows_login(app, client) -> None:
    admin = await admin_tokens(app, client)
    user = await register(client, "toggle@test.local")
    tokens = await login(client, "toggle@test.local")
    url = f"/api/v1/users/{user['id']}"

    r = await client.patch(url, json={"is_active": False}, headers=bearer(admin))
    assert r.status_code == 200 and r.json()["is_active"] is False
    assert (
        await client.post("/api/v1/auth/refresh", json={"refresh_token": tokens["refresh_token"]})
    ).status_code == 401

    r = await client.patch(url, json={"is_active": True}, headers=bearer(admin))
    assert r.status_code == 200 and r.json()["is_active"] is True
    await login(client, "toggle@test.local")


async def test_a_no_op_update_keeps_the_users_sessions(app, client) -> None:
    admin = await admin_tokens(app, client)
    user = await register(client, "same@test.local")
    tokens = await login(client, "same@test.local")
    r = await client.patch(
        f"/api/v1/users/{user['id']}",
        json={"role": "customer", "is_active": True},
        headers=bearer(admin),
    )
    assert r.status_code == 200
    assert (
        await client.post("/api/v1/auth/refresh", json={"refresh_token": tokens["refresh_token"]})
    ).status_code == 200


async def test_a_suspended_merchant_promoted_again_returns_to_pending(app, client) -> None:
    admin = await admin_tokens(app, client)
    merchant = await register(client, "back@test.local", role="merchant", business_name="Back")
    url = f"/api/v1/users/{merchant['id']}"
    r = await client.patch(url, json={"role": "customer"}, headers=bearer(admin))
    assert r.json()["merchant_profile"]["status"] == "suspended"
    r = await client.patch(url, json={"role": "merchant"}, headers=bearer(admin))
    profile = r.json()["merchant_profile"]
    # The same profile comes back (slug kept), for re-approval.
    assert profile["status"] == "pending" and profile["slug"] == "back"


async def test_updating_an_unknown_user_is_404(app, client) -> None:
    admin = await admin_tokens(app, client)
    r = await client.patch(
        f"/api/v1/users/{uuid.uuid4()}", json={"is_active": False}, headers=bearer(admin)
    )
    assert r.status_code == 404 and r.json()["type"].endswith("/user-not-found")
    assert (await client.get("/api/v1/users/not-a-uuid", headers=bearer(admin))).status_code == 422


async def test_two_admins_demoting_each_other_at_once_leave_one_admin(app, client) -> None:
    """I10 under concurrency: the active-admin rows are locked, so two
    demotions cannot both see "another admin remains"."""
    for _ in range(3):
        first = await admin_tokens(app, client, "a1@test.local")
        second = await admin_tokens(app, client, "a2@test.local")
        first_id, second_id = await user_id(client, first), await user_id(client, second)
        results = await asyncio.gather(
            client.patch(
                f"/api/v1/users/{second_id}", json={"role": "customer"}, headers=bearer(first)
            ),
            client.patch(
                f"/api/v1/users/{first_id}", json={"is_active": False}, headers=bearer(second)
            ),
        )
        # Both callers hold valid admin access tokens, so only the lock decides.
        statuses = sorted(r.status_code for r in results)
        assert statuses == [200, 409], [r.text for r in results]
        async with app.state.db.sessionmaker() as s:
            admins = await s.scalar(
                select(func.count())
                .select_from(User)
                .where(User.role == UserRole.ADMIN, User.is_active.is_(True))
            )
            assert admins == 1
            await s.execute(User.__table__.delete())
            await s.commit()


# --- self-service ------------------------------------------------------------------


async def test_a_new_password_must_meet_the_policy(app, client) -> None:
    await register(client, "policy@test.local")
    tokens = await login(client, "policy@test.local")
    for new, message in (
        ("short", "must be at least 12 characters"),
        ("POLICY@test.local", "must not equal the email address"),
    ):
        r = await client.post(
            "/api/v1/users/me/password",
            json={"current_password": PASSWORD, "new_password": new},
            headers=bearer(tokens),
        )
        assert r.status_code == 422, new
        assert any(v["message"] == message for v in r.json()["violations"])
    # Nothing changed: the old password still works and no session was revoked.
    await login(client, "policy@test.local")
    async with app.state.db.sessionmaker() as s:
        revoked = await s.scalar(
            select(func.count())
            .select_from(RefreshSession)
            .where(RefreshSession.revoked_at.is_not(None))
        )
    assert revoked == 0


async def test_a_token_without_a_session_revokes_every_session_on_password_change(
    app, client, keypair
) -> None:
    user = await register(client, "nosid@test.local")
    tokens = await login(client, "nosid@test.local")
    # An access token without a session id: there is no current session to keep.
    token = mint_token(keypair, sub=user["id"], role="customer", extra={"sid": None})
    r = await client.post(
        "/api/v1/users/me/password",
        json={"current_password": PASSWORD, "new_password": "a brand new passphrase"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 204
    assert (
        await client.post("/api/v1/auth/refresh", json={"refresh_token": tokens["refresh_token"]})
    ).status_code == 401


async def test_a_token_for_a_missing_or_malformed_subject_finds_no_user(client, keypair) -> None:
    for subject in (str(uuid.uuid4()), "not-a-uuid"):
        headers = {"Authorization": f"Bearer {mint_token(keypair, sub=subject)}"}
        r = await client.get("/api/v1/users/me", headers=headers)
        assert r.status_code == 404 and r.json()["type"].endswith("/user-not-found")
        r = await client.patch("/api/v1/users/me", json={"full_name": "X"}, headers=headers)
        assert r.status_code == 404
