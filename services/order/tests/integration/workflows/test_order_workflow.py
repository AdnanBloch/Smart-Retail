"""``OrderWorkflow`` end to end on Temporal's time-skipping test server: real
workflow, real activities, real ``order_db``; inventory and the providers are
in-memory fakes with the real contracts (RFC 0005 §7: G2, G3, G5, B3, ORD-1).

Every failure point is injected and every compensation checked: a failed
order releases or reverses its stock and refunds any charge.
"""

import asyncio
import uuid
from dataclasses import dataclass

import pytest
from sqlalchemy import select
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from app.models import Order
from app.providers.payment import FakePaymentProvider
from app.providers.shipping import FakeShippingProvider
from app.workflows.activities import OrderActivities
from app.workflows.client import TemporalOrderWorkflows
from app.workflows.order_workflow import OrderWorkflow
from smartretail_common.temporal import TemporalClientFactory, business_failure
from tests.integration.conftest import checkout_body, outbox_rows, place

QUEUE = "order-lifecycle"


@dataclass
class Held:
    order_id: str
    variant_id: str
    quantity: int
    status: str = "held"
    reversed: bool = False


class FakeInventory:
    """inventory's reservation contract, in memory: ``available`` per variant."""

    def __init__(self) -> None:
        self.available: dict[str, int] = {}
        self.reservations: dict[str, Held] = {}
        self.gate: asyncio.Event | None = None
        self.commit_gate: asyncio.Event | None = None
        self.reserve_calls = 0

    async def reserve(self, order_id, lines, correlation_id) -> list[str]:
        self.reserve_calls += 1
        if self.gate is not None:
            await self.gate.wait()
        existing = [rid for rid, r in self.reservations.items() if r.order_id == str(order_id)]
        if existing:
            return existing
        short = [str(v) for v, q in lines if self.available.get(str(v), 0) < q]
        if short:
            raise business_failure("insufficient-stock", f"short: {short}")
        ids = []
        for variant, quantity in lines:
            self.available[str(variant)] -= quantity
            rid = str(uuid.uuid4())
            self.reservations[rid] = Held(str(order_id), str(variant), quantity)
            ids.append(rid)
        return ids

    async def commit(self, reservation_id: str) -> None:
        if self.commit_gate is not None:
            await self.commit_gate.wait()
        r = self.reservations[reservation_id]
        if r.status == "committed":
            return
        if r.status != "held":
            raise business_failure("reservation-not-held", r.status)
        r.status = "committed"

    async def release(self, reservation_id: str) -> None:
        r = self.reservations[reservation_id]
        if r.status in ("released", "expired"):
            return
        if r.status == "committed":
            raise business_failure("reservation-committed", "reverse it instead")
        r.status = "released"
        self.available[r.variant_id] += r.quantity

    async def reverse(self, reservation_id: str) -> None:
        r = self.reservations[reservation_id]
        if r.status != "committed":
            raise business_failure("reservation-not-committed", r.status)
        if not r.reversed:
            r.reversed = True
            self.available[r.variant_id] += r.quantity

    def expire_all(self) -> None:
        for r in self.reservations.values():
            if r.status == "held":
                r.status = "expired"
                self.available[r.variant_id] += r.quantity


class GatedPayments(FakePaymentProvider):
    gate: asyncio.Event | None = None

    async def charge(self, **kwargs):
        if self.gate is not None:
            await self.gate.wait()
        return await super().charge(**kwargs)


class RecordingWorkflows(TemporalOrderWorkflows):
    """Keeps started handles: only the time-skipping client's own handles skip timers."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.handles: dict[str, object] = {}

    async def start(self, **kwargs) -> str:
        handle = await self.start_handle(**kwargs)
        self.handles[kwargs["workflow_id"]] = handle
        return handle.result_run_id or ""


@pytest.fixture
async def env():
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except Exception as exc:  # pragma: no cover - offline machines
        pytest.skip(f"Temporal test server unavailable: {exc}")
    yield environment
    await environment.shutdown()


@pytest.fixture
def settings(settings):
    settings.fake_provider_timeout_seconds = 0.01  # a "timeout" without a real sleep
    return settings


@pytest.fixture
def workflows(env, settings):
    factory = TemporalClientFactory("unused")
    factory.set(env.client)
    return RecordingWorkflows(factory, settings)


@pytest.fixture
def inventory() -> FakeInventory:
    return FakeInventory()


@pytest.fixture
def payments(settings) -> GatedPayments:
    return GatedPayments(settings)


def make_worker(env, app, inventory, payments, settings, **options) -> Worker:
    activities = OrderActivities(
        app.state.db, settings, inventory, payments, FakeShippingProvider(settings)
    )
    return Worker(
        env.client,
        task_queue=QUEUE,
        workflows=[OrderWorkflow],
        activities=activities.all(),
        **options,
    )


@pytest.fixture
async def worker(env, app, inventory, payments, settings):
    async with make_worker(env, app, inventory, payments, settings) as running:
        yield running


async def outcome_of(handle) -> str:
    for _ in range(30):
        try:
            return await handle.result()
        except RuntimeError as exc:  # the test server may end a long-poll early
            if "No completion event found" not in str(exc):
                raise
    raise AssertionError("workflow did not complete")


async def checkout(client, callers, catalog, inventory, *, stock=10, quantity=2, **body_kw):
    product = catalog.add(callers.merchant_a_id, price="20.00")
    inventory.available[product["variants"][0]["id"]] = stock
    r = await place(client, callers.customer_a, checkout_body((product, quantity), **body_kw))
    assert r.status_code == 202, r.text
    return r.json(), product["variants"][0]["id"]


async def run_order(client, callers, catalog, inventory, workflows, **kw):
    order, variant = await checkout(client, callers, catalog, inventory, **kw)
    outcome = await outcome_of(workflows.handles[f"order-{order['order_id']}"])
    detail = (await client.get(f"/api/v1/orders/{order['order_id']}", headers=callers.admin)).json()
    return outcome, detail, variant


async def test_happy_path_confirms(
    client, callers, catalog, inventory, workflows, worker, payments, database_url
) -> None:
    outcome, order, variant = await run_order(client, callers, catalog, inventory, workflows)
    assert outcome == "confirmed"
    assert order["status"] == "confirmed" and order["notified_at"]
    assert order["payment"]["status"] == "succeeded" and order["payment"]["reference"]
    assert order["shipment"]["status"] == "created" and order["shipment"]["tracking_number"]
    assert [t["to"] for t in order["timeline"]] == ["pending", "awaiting_payment", "confirmed"]
    assert inventory.available[variant] == 8
    assert {r.status for r in inventory.reservations.values()} == {"committed"}
    assert payments.charge_count(uuid.UUID(order["order_id"])) == 1
    types = [e["type"] for e in await outbox_rows(database_url)]
    assert types == ["order.created", "payment.succeeded", "order.confirmed"]


async def test_insufficient_stock_fails_before_any_charge(
    client, callers, catalog, inventory, workflows, worker, payments
) -> None:
    outcome, order, _ = await run_order(client, callers, catalog, inventory, workflows, stock=1)
    assert outcome == "failed"
    assert (order["status"], order["failure_reason"]) == ("failed", "INSUFFICIENT_STOCK")
    assert order["payment"]["status"] == "none" and payments.calls == []


async def test_decline_releases_stock_exactly_as_it_began(
    client, callers, catalog, inventory, workflows, worker, payments, database_url
) -> None:
    """G3: a failed payment leaves available stock exactly as it began."""
    outcome, order, variant = await run_order(
        client, callers, catalog, inventory, workflows, method="tok_decline"
    )
    assert (order["status"], order["failure_reason"]) == ("failed", "PAYMENT_DECLINED")
    assert order["payment"]["status"] == "failed"
    assert inventory.available[variant] == 10
    assert len(payments.calls) == 1  # a declined card is retried zero times
    types = [e["type"] for e in await outbox_rows(database_url)]
    assert types == ["order.created", "payment.failed", "order.failed"]


async def test_payment_timeout_is_refunded_as_ambiguous(
    client, callers, catalog, inventory, workflows, worker, payments
) -> None:
    outcome, order, variant = await run_order(
        client, callers, catalog, inventory, workflows, method="tok_timeout"
    )
    assert (order["status"], order["failure_reason"]) == ("failed", "PAYMENT_TIMEOUT")
    # Nothing was captured, which the refund reports; the payment ends failed.
    assert order["payment"]["status"] == "failed"
    assert [c[0] for c in payments.calls] == ["charge", "charge", "charge", "refund"]
    assert inventory.available[variant] == 10


async def test_flaky_provider_charges_exactly_once(
    client, callers, catalog, inventory, workflows, worker, payments
) -> None:
    outcome, order, _ = await run_order(
        client, callers, catalog, inventory, workflows, method="tok_flaky_2"
    )
    assert outcome == "confirmed"
    assert [c[0] for c in payments.calls] == ["charge"] * 3
    assert payments.charge_count(uuid.UUID(order["order_id"])) == 1


async def test_expired_reservation_refunds_after_charge(
    client, callers, catalog, inventory, workflows, worker, payments
) -> None:
    """ORD-1: commit fails after a successful charge → refunding → failed."""
    payments.gate = asyncio.Event()
    order, variant = await checkout(client, callers, catalog, inventory)
    handle = workflows.handles[f"order-{order['order_id']}"]
    while not inventory.reservations:
        await asyncio.sleep(0.01)
    inventory.expire_all()  # the sweeper got there first
    payments.gate.set()
    assert await outcome_of(handle) == "failed"
    detail = (await client.get(f"/api/v1/orders/{order['order_id']}", headers=callers.admin)).json()
    assert (detail["status"], detail["failure_reason"]) == ("failed", "RESERVATION_EXPIRED")
    assert detail["payment"]["status"] == "refunded"
    assert [t["to"] for t in detail["timeline"]][-2:] == ["refunding", "failed"]
    assert inventory.available[variant] == 10


async def test_rejected_address_refunds_and_reverses_the_commit(
    client, callers, catalog, inventory, workflows, worker, payments
) -> None:
    outcome, order, variant = await run_order(
        client, callers, catalog, inventory, workflows, postal_code="00000"
    )
    assert (order["status"], order["failure_reason"]) == ("failed", "FULFILMENT_FAILED")
    assert order["payment"]["status"] == "refunded"
    assert inventory.available[variant] == 10
    assert all(r.reversed for r in inventory.reservations.values())


async def test_stuck_refund_leaves_the_order_refunding(
    client, callers, catalog, inventory, workflows, worker
) -> None:
    """tok_refund_fail: the compensation retries for its whole hour, then gives up loudly."""
    order, _ = await checkout(
        client, callers, catalog, inventory, method="tok_refund_fail", postal_code="00000"
    )
    with pytest.raises(Exception):  # noqa: B017 - the workflow itself fails
        await outcome_of(workflows.handles[f"order-{order['order_id']}"])
    detail = (await client.get(f"/api/v1/orders/{order['order_id']}", headers=callers.admin)).json()
    assert detail["status"] == "refunding" and detail["payment"]["status"] == "succeeded"


async def test_cancel_while_reserving(
    client, callers, catalog, inventory, workflows, worker, database_url
) -> None:
    inventory.gate = asyncio.Event()
    order, variant = await checkout(client, callers, catalog, inventory)
    while inventory.reserve_calls == 0:
        await asyncio.sleep(0.01)
    r = await client.post(
        f"/api/v1/orders/{order['order_id']}/cancel",
        json={"reason": "changed my mind"},
        headers=callers.customer_a,
    )
    assert r.status_code == 202, r.text
    inventory.gate.set()
    assert await outcome_of(workflows.handles[f"order-{order['order_id']}"]) == "cancelled"
    detail = (await client.get(f"/api/v1/orders/{order['order_id']}", headers=callers.admin)).json()
    assert detail["status"] == "cancelled" and detail["payment"]["status"] == "none"
    assert detail["timeline"][-1]["actor"] == f"customer:{callers.customer_a_id}"
    assert inventory.available[variant] == 10
    [cancelled] = await outbox_rows(database_url, "order.cancelled")
    assert (cancelled["stage"], cancelled["reason"]) == ("reserving", "changed my mind")


async def test_cancel_while_charging_refunds(
    client, callers, catalog, inventory, workflows, worker, payments
) -> None:
    payments.gate = asyncio.Event()
    order, variant = await checkout(client, callers, catalog, inventory)
    handle = workflows.handles[f"order-{order['order_id']}"]
    while (await handle.query("get_state"))["stage"] != "charging":
        await asyncio.sleep(0.01)
    r = await client.post(f"/api/v1/orders/{order['order_id']}/cancel", headers=callers.admin)
    assert r.status_code == 202
    payments.gate.set()
    assert await outcome_of(handle) == "cancelled"
    detail = (await client.get(f"/api/v1/orders/{order['order_id']}", headers=callers.admin)).json()
    assert detail["payment"]["status"] == "refunded"
    assert inventory.available[variant] == 10


async def test_cancel_after_the_cancellable_point_is_refused(
    client, callers, catalog, inventory, workflows, worker
) -> None:
    inventory.commit_gate = asyncio.Event()
    order, _ = await checkout(client, callers, catalog, inventory)
    handle = workflows.handles[f"order-{order['order_id']}"]
    while (await handle.query("get_state"))["stage"] != "committing":
        await asyncio.sleep(0.01)
    r = await client.post(f"/api/v1/orders/{order['order_id']}/cancel", headers=callers.customer_a)
    assert r.status_code == 409 and r.json()["stage"] == "committing"
    # A signal that slips past the API's check is recorded in history and ignored.
    await workflows.request_cancellation(handle.id, actor="customer:x", reason="late")
    assert (await handle.query("get_state"))["cancel_requested"] is False
    inventory.commit_gate.set()
    assert await outcome_of(handle) == "confirmed"


async def test_worker_killed_mid_order_resumes(
    env, client, callers, catalog, inventory, workflows, app, payments, settings
) -> None:
    """G5: the worker goes away mid-order; a fresh one finishes it from history.

    No workflow cache, so the second worker cannot lean on the first's sticky
    state (and the test server does not time sticky tasks out): it rebuilds the
    workflow by replaying its history, which is exactly a restart.
    """
    inventory.gate = asyncio.Event()
    first = make_worker(env, app, inventory, payments, settings, max_cached_workflows=0)
    task = asyncio.create_task(first.run())
    order, variant = await checkout(client, callers, catalog, inventory)
    while inventory.reserve_calls == 0:
        await asyncio.sleep(0.01)
    await first.shutdown()
    await task
    inventory.gate.set()
    async with make_worker(env, app, inventory, payments, settings, max_cached_workflows=0):
        outcome = await outcome_of(workflows.handles[f"order-{order['order_id']}"])
    assert outcome == "confirmed"
    assert len(inventory.reservations) == 1  # no orphaned reservation
    assert inventory.available[variant] == 8
    async with app.state.db.sessionmaker() as s:
        stored = await s.scalar(select(Order).where(Order.id == uuid.UUID(order["order_id"])))
        assert stored is not None and stored.status == "confirmed"


async def test_history_replays_deterministically(
    client, callers, catalog, inventory, workflows, worker
) -> None:
    order, _ = await checkout(client, callers, catalog, inventory, method="tok_decline")
    handle = workflows.handles[f"order-{order['order_id']}"]
    await outcome_of(handle)
    await Replayer(workflows=[OrderWorkflow]).replay_workflow(await handle.fetch_history())
    described = await workflows.describe(handle.id)
    assert described["execution_status"] == "COMPLETED" and described["stage"] == "failed"
