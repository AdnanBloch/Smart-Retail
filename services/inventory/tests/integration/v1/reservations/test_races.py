"""Races on the same rows (mentor review 2026-10-20 §8).

Each test fires conflicting operations at one reservation or one stock row at
the same moment, over several rounds so the interleavings vary. Whatever order
Postgres grants the locks in, exactly one outcome must win, the CHECK must
hold, and the ledger must reconcile (I5, I6) afterwards.
"""

import asyncio
import random
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import update

from app.v1.reservations import sweeper
from app.v1.reservations.models import Reservation
from app.v1.reservations.repo import ReservationRepo
from app.v1.stock.reconcile import find_drift
from tests.integration.conftest import init_stock, public, reserve

BASE = "/internal/v1/inventory/reservations"
ROUNDS = 8


async def _no_drift(app) -> None:
    async with app.state.db.sessionmaker() as session:
        assert await find_drift(session) == []


async def _hold(client, callers, variant: str, quantity: int = 1) -> dict:
    r = await reserve(client, callers, str(uuid.uuid4()), [(variant, quantity)])
    assert r.status_code == 201, r.text
    [held] = r.json()["reservations"]
    return held


async def _make_due(app, reservation_id: str) -> None:
    async with app.state.db.sessionmaker() as s:
        await s.execute(
            update(Reservation)
            .where(Reservation.id == uuid.UUID(reservation_id))
            .values(expires_at=datetime.now(UTC) - timedelta(minutes=1))
        )
        await s.commit()


async def test_commit_and_release_race_exactly_one_wins(app, client, callers) -> None:
    [v] = await init_stock(client, callers, callers.merchant_a_id, on_hand=ROUNDS)
    committed = 0
    for _ in range(ROUNDS):
        held = await _hold(client, callers, v)
        commit, release = await asyncio.gather(
            client.post(f"{BASE}/{held['id']}/commit", headers=callers.service),
            client.delete(f"{BASE}/{held['id']}", headers=callers.service),
        )
        assert sorted([commit.status_code, release.status_code]) == [200, 409], (
            commit.text,
            release.text,
        )
        committed += commit.status_code == 200
    stock = await public(client, v)
    assert stock["reserved"] == 0
    assert stock["on_hand"] == ROUNDS - committed
    await _no_drift(app)


async def test_commit_and_expiry_sweep_race(app, client, callers, settings) -> None:
    [v] = await init_stock(client, callers, callers.merchant_a_id, on_hand=ROUNDS)
    committed = 0
    for _ in range(ROUNDS):
        held = await _hold(client, callers, v)
        await _make_due(app, held["id"])
        commit, swept = await asyncio.gather(
            client.post(f"{BASE}/{held['id']}/commit", headers=callers.service),
            sweeper.sweep_once(app.state.db, settings),
        )
        # Either the commit took the lock first (INV-D13: status, not clock,
        # decides) and the sweep found nothing, or the sweep expired it first.
        if commit.status_code == 200:
            assert swept == 0
            committed += 1
        else:
            assert commit.status_code == 409 and swept == 1
    stock = await public(client, v)
    assert stock["reserved"] == 0 and stock["on_hand"] == ROUNDS - committed
    await _no_drift(app)


async def test_saga_release_and_cancel_consumer_release_once(app, client, callers, settings):
    [v] = await init_stock(client, callers, callers.merchant_a_id, on_hand=10)

    async def consumer(order_id: str) -> list[Reservation]:
        async with app.state.db.sessionmaker() as s, s.begin():
            return await ReservationRepo(s, settings).release_for_order(
                uuid.UUID(order_id), actor="test:consumer"
            )

    for _ in range(ROUNDS):
        order = str(uuid.uuid4())
        r = await reserve(client, callers, order, [(v, 2)])
        [held] = r.json()["reservations"]
        saga, released = await asyncio.gather(
            client.delete(f"{BASE}/{held['id']}", headers=callers.service), consumer(order)
        )
        # The saga's release is idempotent (200 either way); the consumer
        # releases only what is still held, so the stock comes back once.
        assert saga.status_code == 200 and saga.json()["status"] == "released"
        assert len(released) in (0, 1)
    assert (await public(client, v))["reserved"] == 0
    await _no_drift(app)


async def test_reverse_twice_at_once_returns_stock_once(app, client, callers) -> None:
    [v] = await init_stock(client, callers, callers.merchant_a_id, on_hand=10)
    held = await _hold(client, callers, v, 3)
    assert (await client.post(f"{BASE}/{held['id']}/commit", headers=callers.service)).is_success
    assert (await public(client, v))["on_hand"] == 7
    results = await asyncio.gather(
        *(client.post(f"{BASE}/{held['id']}/reverse", headers=callers.service) for _ in range(5))
    )
    assert {r.status_code for r in results} == {200}
    assert (await public(client, v))["on_hand"] == 10
    await _no_drift(app)


async def test_stock_reduction_racing_reserves_never_oversells(app, client, callers) -> None:
    [v] = await init_stock(client, callers, callers.merchant_a_id, on_hand=10)
    adjust = client.post(
        f"/api/v1/inventory/{v}/adjust",
        json={"delta": -6, "reason": "manual_adjustment"},
        headers=callers.admin,
    )
    reserves = [reserve(client, callers, str(uuid.uuid4()), [(v, 1)]) for _ in range(10)]
    adj, *res = await asyncio.gather(adjust, *reserves)
    won = sum(r.status_code == 201 for r in res)
    assert {r.status_code for r in res} <= {201, 409}
    stock = await public(client, v)
    assert stock["reserved"] == won
    # The adjust is refused only if it would take on_hand below reserved.
    if adj.status_code == 200:
        assert stock["on_hand"] == 4 and won <= 4
    else:
        assert adj.status_code == 409 and stock["on_hand"] == 10 and won > 4
    assert stock["on_hand"] >= stock["reserved"]
    await _no_drift(app)


async def test_set_on_hand_racing_reserves_keeps_the_check(app, client, callers) -> None:
    [v] = await init_stock(client, callers, callers.merchant_a_id, on_hand=10)
    put = client.put(f"/api/v1/inventory/{v}", json={"on_hand": 3}, headers=callers.admin)
    reserves = [reserve(client, callers, str(uuid.uuid4()), [(v, 1)]) for _ in range(8)]
    setter, *res = await asyncio.gather(put, *reserves)
    won = sum(r.status_code == 201 for r in res)
    stock = await public(client, v)
    assert setter.status_code in (200, 409)
    assert stock["on_hand"] == (3 if setter.status_code == 200 else 10)
    assert stock["reserved"] == won <= stock["on_hand"]
    await _no_drift(app)


async def test_many_orders_over_many_skus_in_random_line_order(app, client, callers) -> None:
    variants = await init_stock(client, callers, callers.merchant_a_id, variants=5, on_hand=100)
    rng = random.Random(20261020)
    calls = []
    for _ in range(30):
        lines = [(v, 1) for v in rng.sample(variants, k=rng.randint(2, 5))]
        calls.append(reserve(client, callers, str(uuid.uuid4()), lines))
    results = await asyncio.gather(*calls)
    # No deadlock surfaces: canonical lock order, or the bounded retry absorbs it.
    assert {r.status_code for r in results} == {201}
    await _no_drift(app)


async def test_last_unit_goes_to_exactly_one_of_many_orders(app, client, callers) -> None:
    [v] = await init_stock(client, callers, callers.merchant_a_id, on_hand=1)
    results = await asyncio.gather(
        *(reserve(client, callers, str(uuid.uuid4()), [(v, 1)]) for _ in range(20))
    )
    assert sorted(r.status_code for r in results) == [201] + [409] * 19
    await _no_drift(app)


async def test_commit_after_expiry_but_before_the_sweep_succeeds(app, client, callers) -> None:
    """INV-D13: the status decides, not the clock."""
    [v] = await init_stock(client, callers, callers.merchant_a_id, on_hand=4)
    held = await _hold(client, callers, v, 2)
    await _make_due(app, held["id"])
    r = await client.post(f"{BASE}/{held['id']}/commit", headers=callers.service)
    assert r.status_code == 200 and r.json()["status"] == "committed"
    assert (await public(client, v))["on_hand"] == 2
    await _no_drift(app)


async def test_cancel_consumer_leaves_expired_and_committed_alone(
    app, client, callers, settings
) -> None:
    a, b, c = await init_stock(client, callers, callers.merchant_a_id, variants=3, on_hand=5)
    order = str(uuid.uuid4())
    r = await reserve(client, callers, order, [(a, 1), (b, 1), (c, 1)])
    by_variant = {x["variant_id"]: x for x in r.json()["reservations"]}
    await _make_due(app, by_variant[a]["id"])
    assert await sweeper.sweep_once(app.state.db, settings) == 1
    commit = await client.post(f"{BASE}/{by_variant[b]['id']}/commit", headers=callers.service)
    assert commit.status_code == 200

    async with app.state.db.sessionmaker() as s, s.begin():
        released = await ReservationRepo(s, settings).release_for_order(
            uuid.UUID(order), actor="test:consumer"
        )
    assert [str(x.variant_id) for x in released] == [c]
    assert [(await public(client, x))["reserved"] for x in (a, b, c)] == [0, 0, 0]
    assert (await public(client, b))["on_hand"] == 4
    await _no_drift(app)


async def test_reserve_edge_cases_are_refused(client, callers) -> None:
    [v] = await init_stock(client, callers, callers.merchant_a_id, on_hand=5)
    zero = await reserve(client, callers, str(uuid.uuid4()), [(v, 0)])
    negative = await reserve(client, callers, str(uuid.uuid4()), [(v, -1)])
    unknown = await reserve(client, callers, str(uuid.uuid4()), [(str(uuid.uuid4()), 1)])
    empty = await reserve(client, callers, str(uuid.uuid4()), [])
    too_many = await reserve(client, callers, str(uuid.uuid4()), [(v, 6)])
    assert zero.status_code == 422 and negative.status_code == 422 and empty.status_code == 422
    assert unknown.status_code in (404, 409, 422), unknown.text
    assert too_many.status_code == 409
    assert (await public(client, v))["reserved"] == 0
