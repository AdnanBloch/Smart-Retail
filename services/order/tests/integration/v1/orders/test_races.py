"""Races on one order and one key, arbitrated by the database (S6, I2, G4, ORD-8).

Each contender has its own session, as concurrent requests and activities do.
``advance`` locks the row, so concurrent changes serialise: only legal
transitions win, the losers are refused, and history and outbox record each
change exactly once.
"""

import asyncio
import uuid
from datetime import timedelta

import asyncpg
import pytest
from sqlalchemy import select

from app.core.errors import VersionConflict
from app.models import OrderStatusHistory
from app.v1.orders.enums import OrderStatus, PaymentStatus, ShipmentStatus
from app.v1.orders.idempotency_repo import IdempotencyRepo, RequestInProgress
from app.v1.orders.repo import Change, OrderRepo
from app.v1.orders.shipment_repo import ShipmentRepo
from app.v1.orders.state_machine import IllegalTransition
from tests.integration.conftest import checkout_body, outbox_rows, place

S, P, H = OrderStatus, PaymentStatus, ShipmentStatus


async def _order(client, callers, catalog) -> uuid.UUID:
    r = await place(
        client, callers.customer_a, checkout_body((catalog.add(callers.merchant_a_id), 1))
    )
    assert r.status_code == 202, r.text
    return uuid.UUID(r.json()["order_id"])


async def _advance(app, order_id: uuid.UUID, change: Change) -> tuple[str, int]:
    """``(status, version)`` as the change left them, read while the session is open."""
    async with app.state.db.sessionmaker() as s:
        await OrderRepo(s, app.state.settings).advance(order_id, change)
        order = await OrderRepo(s, app.state.settings).get_any(order_id)
        return str(order.status), order.version


async def _contend(*calls) -> tuple[list, list[BaseException]]:
    results = await asyncio.gather(*calls, return_exceptions=True)
    expected = IllegalTransition | VersionConflict | RequestInProgress
    unexpected = [
        r for r in results if isinstance(r, BaseException) and not isinstance(r, expected)
    ]
    assert not unexpected, unexpected
    return (
        [r for r in results if not isinstance(r, BaseException)],
        [r for r in results if isinstance(r, BaseException)],
    )


async def _timeline(app, order_id: uuid.UUID) -> list[tuple[str | None, str]]:
    async with app.state.db.sessionmaker() as s:
        rows = await s.scalars(
            select(OrderStatusHistory)
            .where(OrderStatusHistory.order_id == order_id)
            .order_by(OrderStatusHistory.id)
        )
        return [(r.from_status, r.to_status) for r in rows]


async def _ready_to_confirm(app, order_id: uuid.UUID) -> None:
    for change in (
        Change("workflow", status=S.AWAITING_PAYMENT, payment=P.PENDING),
        Change("workflow", payment=P.SUCCEEDED, payment_reference="pay_1"),
        Change("workflow", shipment=H.CREATED, shipment_reference="shp_1"),
    ):
        await _advance(app, order_id, change)


async def test_competing_terminal_moves_one_wins(app, client, callers, catalog, database_url):
    """Ten ``failed`` and ten ``cancelled`` at once: one terminal state, recorded once."""
    order_id = await _order(client, callers, catalog)
    to_failed = Change("workflow", status=S.FAILED)
    to_cancelled = Change("customer:x", status=S.CANCELLED)
    calls = [_advance(app, order_id, c) for _ in range(10) for c in (to_failed, to_cancelled)]
    won, refused = await _contend(*calls)

    final = won[0][0]
    assert final in ("failed", "cancelled")
    assert won == [(final, 2)] * 10
    # Every repeat of the winning move is a no-op; every opposing move is refused.
    assert len(won) == len(refused) == 10
    assert all(isinstance(e, IllegalTransition) for e in refused)
    assert await _timeline(app, order_id) == [(None, "pending"), ("pending", str(final))]
    types = [e["type"] for e in await outbox_rows(database_url)]
    assert types == ["order.created", f"order.{final}"]


async def test_cancel_racing_confirm_never_does_both(app, client, callers, catalog, database_url):
    """The customer's cancel and the workflow's confirm, interleaved over five orders."""
    orders = [await _order(client, callers, catalog) for _ in range(5)]
    for order_id in orders:
        await _ready_to_confirm(app, order_id)

    outcomes = await asyncio.gather(
        *(
            _contend(
                _advance(app, o, Change("workflow", status=S.CONFIRMED)),
                _advance(app, o, Change("customer:x", status=S.CANCELLED)),
            )
            for o in orders
        )
    )
    for order_id, (won, refused) in zip(orders, outcomes, strict=True):
        assert len(won) == len(refused) == 1 and isinstance(refused[0], IllegalTransition)
        final = won[0][0]
        assert await _timeline(app, order_id) == [
            (None, "pending"),
            ("pending", "awaiting_payment"),
            ("awaiting_payment", final),
        ]
    events = await outbox_rows(database_url)
    for order_id in orders:
        terminal = [
            e["type"]
            for e in events
            if e["order_id"] == str(order_id)
            and e["type"] in ("order.confirmed", "order.cancelled")
        ]
        assert len(terminal) == 1, terminal


async def test_expected_version_lets_exactly_one_writer_through(app, client, callers, catalog):
    order_id = await _order(client, callers, catalog)
    change = Change("admin:a", status=S.AWAITING_PAYMENT, payment=P.PENDING, expected_version=1)
    won, refused = await _contend(*(_advance(app, order_id, change) for _ in range(6)))
    assert won == [("awaiting_payment", 2)]
    assert len(refused) == 5 and all(isinstance(e, VersionConflict) for e in refused)
    assert all(e.extras == {"current_version": 2} for e in refused)


async def test_duplicate_carrier_updates_record_one_change(
    app, client, callers, catalog, database_url
) -> None:
    """A carrier redelivering a webhook concurrently: one history row, one event."""
    order_id = await _order(client, callers, catalog)
    await _ready_to_confirm(app, order_id)
    await _advance(app, order_id, Change("workflow", status=S.CONFIRMED))

    async def ship():
        async with app.state.db.sessionmaker() as s:
            update = await ShipmentRepo(s, app.state.settings).apply_carrier_update(
                order_id, H.SHIPPED, actor="carrier:acme"
            )
            return update.applied, str(update.order.status), str(update.order.shipment_status)

    won, refused = await _contend(*(ship() for _ in range(8)))
    assert not refused and any(applied for applied, *_ in won)
    assert {tuple(rest) for _, *rest in won} == {("shipped", "shipped")}
    assert (await _timeline(app, order_id))[-1] == ("confirmed", "shipped")
    assert [t for _, t in await _timeline(app, order_id)].count("shipped") == 1
    assert len(await outbox_rows(database_url, "shipment.status_changed")) == 1


async def test_many_checkouts_with_distinct_keys_are_all_accepted(
    client, callers, catalog, workflows, database_url
) -> None:
    product = catalog.add(callers.merchant_a_id)
    body = checkout_body((product, 1))
    replies = await asyncio.gather(*(place(client, callers.customer_a, body) for _ in range(12)))
    assert [r.status_code for r in replies] == [202] * 12
    ids = {r.json()["order_id"] for r in replies}
    numbers = {r.json()["order_number"] for r in replies}
    assert len(ids) == len(numbers) == 12
    assert len(workflows.started) == 12
    created = await outbox_rows(database_url, "order.created")
    assert {e["order_id"] for e in created} == ids


# --- the Idempotency-Key claim -------------------------------------------------------


def _pg(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


async def _seed_claim(database_url, user, key, digest, *, age: timedelta, expires_in: timedelta):
    conn = await asyncpg.connect(_pg(database_url))
    try:
        await conn.execute(
            "INSERT INTO idempotency_keys (user_id, key, request_hash, state, created_at, "
            "expires_at) VALUES ($1, $2, $3, 'in_progress', now() - $4::interval, "
            "now() + $5::interval)",
            user,
            key,
            digest,
            age,
            expires_in,
        )
    finally:
        await conn.close()


async def _claim(app, user, key, digest):
    async with app.state.db.sessionmaker() as s:
        return await IdempotencyRepo(s, app.state.settings).claim(user, key, digest)


@pytest.mark.parametrize(
    ("seeded_hash", "age", "expires_in"),
    [
        # ORD-8: the holder died; its claim outlived the in-progress lease.
        ("same", timedelta(minutes=5), timedelta(days=1)),
        # Past its TTL the row counts as absent, whatever body it held.
        ("other", timedelta(days=2), -timedelta(hours=1)),
    ],
)
async def test_one_contender_takes_over_a_dead_claim(
    app, database_url, seeded_hash, age, expires_in
) -> None:
    user = uuid.uuid4()
    await _seed_claim(database_url, user, "K", seeded_hash, age=age, expires_in=expires_in)
    won, refused = await _contend(*(_claim(app, user, "K", "same") for _ in range(8)))
    assert won == [None]  # exactly one now owns the key
    assert len(refused) == 7 and all(isinstance(e, RequestInProgress) for e in refused)
    assert all(e.headers == {"Retry-After": "1"} for e in refused)
