"""``OrderWorkflow`` branching, without a database (RFC 0005 §3.1.1.4, ORD-1, ORD-10).

The real workflow runs on Temporal's time-skipping test server; every activity
is a stub registered under the production activity name, so each forward step
and each compensation path is driven directly and the exact sequence of
activity calls is asserted. The integration suite proves the activities; this
proves the saga's decisions.
"""

import asyncio
import uuid
from collections.abc import Callable
from typing import Any

import pytest
from temporalio import activity
from temporalio.client import WorkflowFailureError, WorkflowHandle
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from app.v1.orders.workflows import types as t
from app.v1.orders.workflows.order_workflow import OrderWorkflow

RESERVATIONS = ["r-1", "r-2"]


def fatal(kind: str) -> ApplicationError:
    """A business failure the step's retry policy does not retry."""
    return ApplicationError(kind, type=kind, non_retryable=True)


def transient(kind: str = "ProviderTransientError") -> ApplicationError:
    return ApplicationError(kind, type=kind)


class Stubs:
    """Every activity, recording each call; ``fail`` and ``gates`` script them by name.

    ``fail[name]`` is raised on every attempt (or from attempt ``n`` on, for a
    callable). ``gates[key]`` holds a call until the test opens it; the key is
    the activity name, or ``status:<target>`` for ``set_order_status``.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.fail: dict[str, BaseException | Callable[[int], BaseException | None]] = {}
        self.gates: dict[str, asyncio.Event] = {}
        self.entered: dict[str, asyncio.Event] = {}

    def gate(self, key: str) -> None:
        self.gates[key] = asyncio.Event()
        self.entered[key] = asyncio.Event()

    def open_all(self) -> None:
        for gate in self.gates.values():
            gate.set()

    async def _step(self, name: str, arg: Any, key: str | None = None) -> None:
        self.calls.append((name, arg))
        key = key or name
        if key in self.gates:
            self.entered[key].set()
            await self.gates[key].wait()
        failure = self.fail.get(name)
        if callable(failure) and not isinstance(failure, BaseException):
            failure = failure(activity.info().attempt)
        if failure is not None:
            raise failure

    @activity.defn(name=t.RESERVE_INVENTORY)
    async def reserve(self, inp: t.OrderInput) -> list[str]:
        await self._step(t.RESERVE_INVENTORY, inp)
        return RESERVATIONS

    @activity.defn(name=t.SET_ORDER_STATUS)
    async def set_status(self, inp: t.StatusInput) -> str:
        await self._step(t.SET_ORDER_STATUS, inp, f"status:{inp.status}")
        return inp.status

    @activity.defn(name=t.CHARGE_PAYMENT)
    async def charge(self, inp: t.OrderInput) -> str:
        await self._step(t.CHARGE_PAYMENT, inp)
        return "pay_1"

    @activity.defn(name=t.COMMIT_INVENTORY)
    async def commit(self, inp: t.ReservationsInput) -> None:
        await self._step(t.COMMIT_INVENTORY, inp)

    @activity.defn(name=t.CREATE_SHIPMENT)
    async def ship(self, inp: t.OrderInput) -> str:
        await self._step(t.CREATE_SHIPMENT, inp)
        return "shp_1"

    @activity.defn(name=t.NOTIFY)
    async def notify(self, order_id: str) -> None:
        await self._step(t.NOTIFY, order_id)

    @activity.defn(name=t.RELEASE_INVENTORY)
    async def release(self, inp: t.ReservationsInput) -> None:
        await self._step(t.RELEASE_INVENTORY, inp)

    @activity.defn(name=t.REFUND_PAYMENT)
    async def refund(self, inp: t.OrderInput) -> str:
        await self._step(t.REFUND_PAYMENT, inp)
        return "refunded"

    @activity.defn(name=t.REVERSE_COMMIT_INVENTORY)
    async def reverse(self, inp: t.ReservationsInput) -> None:
        await self._step(t.REVERSE_COMMIT_INVENTORY, inp)

    def all(self) -> list[Callable[..., Any]]:
        return [
            self.reserve,
            self.set_status,
            self.charge,
            self.commit,
            self.ship,
            self.notify,
            self.release,
            self.refund,
            self.reverse,
        ]

    # --- what the test reads -------------------------------------------------

    def steps(self) -> list[str]:
        """Activity names in call order; a status write reads ``status:<target>``."""
        return [
            f"status:{arg.status}" if name == t.SET_ORDER_STATUS else name
            for name, arg in self.calls
        ]

    def statuses(self) -> list[t.StatusInput]:
        return [arg for name, arg in self.calls if name == t.SET_ORDER_STATUS]

    def last_status(self) -> t.StatusInput:
        return self.statuses()[-1]


@pytest.fixture
async def env():
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except Exception as exc:  # pragma: no cover - offline machines
        pytest.skip(f"Temporal test server unavailable: {exc}")
    yield environment
    await environment.shutdown()


@pytest.fixture
def stubs() -> Stubs:
    return Stubs()


@pytest.fixture
async def start(env, stubs):
    """Runs a worker and returns a starter; every gate is opened on the way out."""
    queue = f"q-{uuid.uuid4()}"
    worker = Worker(env.client, task_queue=queue, workflows=[OrderWorkflow], activities=stubs.all())

    async def starter(method: str = "tok_ok") -> WorkflowHandle:
        order_id = str(uuid.uuid4())
        return await env.client.start_workflow(
            OrderWorkflow.run,
            t.OrderInput(order_id, "corr-1", method),
            id=f"order-{order_id}",
            task_queue=queue,
        )

    async with worker:
        yield starter
        stubs.open_all()


async def outcome(handle: WorkflowHandle) -> str:
    for _ in range(30):
        try:
            return await handle.result()
        except RuntimeError as exc:  # the test server may end a long-poll early
            if "No completion event found" not in str(exc):
                raise
    raise AssertionError("workflow did not complete")


async def state(handle: WorkflowHandle) -> t.WorkflowState:
    return await handle.query(OrderWorkflow.get_state)


async def entered(stubs: Stubs, key: str) -> None:
    await asyncio.wait_for(stubs.entered[key].wait(), timeout=10)


FORWARD = [
    t.RESERVE_INVENTORY,
    "status:awaiting_payment",
    t.CHARGE_PAYMENT,
    t.COMMIT_INVENTORY,
    t.CREATE_SHIPMENT,
    "status:confirmed",
    t.NOTIFY,
]


# --- the forward path ----------------------------------------------------------


async def test_happy_path_runs_every_step_in_order(start, stubs) -> None:
    handle = await start()
    assert await outcome(handle) == "confirmed"
    assert stubs.steps() == FORWARD
    assert await state(handle) == t.WorkflowState("done", None, False)
    # The reservations reserve returned are what commit is given.
    [(_, commit)] = [c for c in stubs.calls if c[0] == t.COMMIT_INVENTORY]
    assert commit.reservation_ids == RESERVATIONS
    assert all(s.correlation_id == "corr-1" and s.actor == "workflow" for s in stubs.statuses())


async def test_failed_notify_still_confirms(start, stubs) -> None:
    """The order is confirmed regardless; a null ``notified_at`` makes it visible."""
    stubs.fail[t.NOTIFY] = fatal("SmtpDown")
    handle = await start()
    assert await outcome(handle) == "confirmed"
    assert stubs.steps() == FORWARD
    assert (await state(handle)).stage == "done"


async def test_transient_charge_failure_is_retried_then_succeeds(start, stubs) -> None:
    stubs.fail[t.CHARGE_PAYMENT] = lambda attempt: transient() if attempt < 3 else None
    assert await outcome(await start()) == "confirmed"
    assert stubs.steps().count(t.CHARGE_PAYMENT) == 3


# --- reserve fails: nothing to undo ----------------------------------------------


@pytest.mark.parametrize(
    ("failure", "attempts", "why"),
    [
        (fatal("insufficient-stock"), 1, "INSUFFICIENT_STOCK"),
        (fatal("unknown-variant"), 1, "INVENTORY_UNAVAILABLE"),
        # inventory down: five attempts (ORD-10), then the order fails.
        (transient("InventoryUnavailable"), 5, "INVENTORY_UNAVAILABLE"),
    ],
)
async def test_reserve_failure_fails_without_compensation(
    start, stubs, failure, attempts, why
) -> None:
    stubs.fail[t.RESERVE_INVENTORY] = failure
    handle = await start()
    assert await outcome(handle) == "failed"
    assert stubs.steps() == [t.RESERVE_INVENTORY] * attempts + ["status:failed"]
    final = stubs.last_status()
    assert (final.failure_reason, final.stage, final.payment_status) == (why, "reserving", None)
    assert await state(handle) == t.WorkflowState("failed", why, False)


async def test_failed_awaiting_payment_write_fails_as_inventory_unavailable(start, stubs) -> None:
    """The status write is still in the reserving stage: no charge, nothing released."""
    stubs.fail[t.SET_ORDER_STATUS] = lambda attempt: (
        None if len(stubs.statuses()) > 1 else fatal("IllegalTransition")
    )
    assert await outcome(await start()) == "failed"
    assert stubs.steps() == [t.RESERVE_INVENTORY, "status:awaiting_payment", "status:failed"]
    assert stubs.last_status().failure_reason == "INVENTORY_UNAVAILABLE"


# --- charge fails ------------------------------------------------------------------


async def test_declined_charge_releases_and_records_a_failed_payment(start, stubs) -> None:
    stubs.fail[t.CHARGE_PAYMENT] = fatal("PaymentDeclined")
    assert await outcome(await start()) == "failed"
    assert stubs.steps() == [
        t.RESERVE_INVENTORY,
        "status:awaiting_payment",
        t.CHARGE_PAYMENT,  # a decline is never retried
        t.RELEASE_INVENTORY,
        "status:failed",
    ]
    final = stubs.last_status()
    assert (final.failure_reason, final.payment_status, final.stage) == (
        "PAYMENT_DECLINED",
        "failed",
        "charging",
    )
    [(_, released)] = [c for c in stubs.calls if c[0] == t.RELEASE_INVENTORY]
    assert released.reservation_ids == RESERVATIONS


async def test_timed_out_charge_is_refunded_before_release(start, stubs) -> None:
    """Ambiguous: the charge may have captured, so it is refunded, then stock released."""
    stubs.fail[t.CHARGE_PAYMENT] = transient("Timeout")
    assert await outcome(await start()) == "failed"
    assert stubs.steps() == [
        t.RESERVE_INVENTORY,
        "status:awaiting_payment",
        *[t.CHARGE_PAYMENT] * 3,
        t.REFUND_PAYMENT,
        t.RELEASE_INVENTORY,
        "status:failed",
    ]
    final = stubs.last_status()
    assert (final.failure_reason, final.payment_status) == ("PAYMENT_TIMEOUT", None)


# --- failures after the charge: refund first (ORD-1) --------------------------------


@pytest.mark.parametrize(
    ("kind", "why"),
    [("reservation-not-held", "RESERVATION_EXPIRED"), ("InventoryRejected", "COMMIT_FAILED")],
)
async def test_failed_commit_refunds_then_releases(start, stubs, kind, why) -> None:
    stubs.fail[t.COMMIT_INVENTORY] = fatal(kind)
    handle = await start()
    assert await outcome(handle) == "failed"
    assert stubs.steps() == [
        t.RESERVE_INVENTORY,
        "status:awaiting_payment",
        t.CHARGE_PAYMENT,
        t.COMMIT_INVENTORY,
        "status:refunding",
        t.REFUND_PAYMENT,
        t.RELEASE_INVENTORY,  # never committed: released, not reversed
        "status:failed",
    ]
    refunding, failed = stubs.statuses()[-2:]
    assert refunding.failure_reason == failed.failure_reason == why
    assert failed.stage == "committing"
    assert (await state(handle)).failure_reason == why


async def test_failed_shipment_refunds_and_reverses_the_commit(start, stubs) -> None:
    stubs.fail[t.CREATE_SHIPMENT] = fatal("AddressRejected")
    assert await outcome(await start()) == "failed"
    assert stubs.steps() == [
        t.RESERVE_INVENTORY,
        "status:awaiting_payment",
        t.CHARGE_PAYMENT,
        t.COMMIT_INVENTORY,
        t.CREATE_SHIPMENT,
        "status:refunding",
        t.REFUND_PAYMENT,
        t.REVERSE_COMMIT_INVENTORY,
        "status:failed",
    ]
    final = stubs.last_status()
    assert (final.failure_reason, final.stage) == ("FULFILMENT_FAILED", "shipping")


async def test_failed_confirm_write_is_a_fulfilment_failure(start, stubs) -> None:
    stubs.fail[t.SET_ORDER_STATUS] = lambda attempt: (
        fatal("IllegalTransition") if stubs.last_status().status == "confirmed" else None
    )
    assert await outcome(await start()) == "failed"
    assert stubs.steps()[5:] == [
        "status:confirmed",
        "status:refunding",
        t.REFUND_PAYMENT,
        t.REVERSE_COMMIT_INVENTORY,
        "status:failed",
    ]
    final = stubs.last_status()
    assert (final.failure_reason, final.stage) == ("FULFILMENT_FAILED", "confirming")


async def test_refund_that_keeps_failing_fails_the_workflow_loudly(start, stubs) -> None:
    """A compensation retries for its whole hour; then the workflow fails, left refunding."""
    stubs.fail[t.CREATE_SHIPMENT] = fatal("AddressRejected")
    stubs.fail[t.REFUND_PAYMENT] = transient()
    handle = await start()
    with pytest.raises(WorkflowFailureError):
        await outcome(handle)
    steps = stubs.steps()
    assert steps.count(t.REFUND_PAYMENT) > 1
    assert stubs.last_status().status == "refunding"
    assert t.REVERSE_COMMIT_INVENTORY not in steps  # never past the stuck refund
    assert (await state(handle)).stage == "compensating"


# --- the cancel signal ---------------------------------------------------------------


async def test_cancel_while_reserving_releases_without_refund(start, stubs) -> None:
    stubs.gate(t.RESERVE_INVENTORY)
    handle = await start()
    await entered(stubs, t.RESERVE_INVENTORY)
    await handle.signal(OrderWorkflow.request_cancellation, t.CancelRequest("customer:c1", "nah"))
    assert (await state(handle)).cancel_requested is True
    stubs.open_all()
    assert await outcome(handle) == "cancelled"
    assert stubs.steps() == [t.RESERVE_INVENTORY, t.RELEASE_INVENTORY, "status:cancelled"]
    final = stubs.last_status()
    assert (final.actor, final.reason, final.stage) == ("customer:c1", "nah", "reserving")
    assert await state(handle) == t.WorkflowState("cancelled", None, True)


async def test_cancel_during_the_awaiting_payment_write_stops_before_charging(start, stubs) -> None:
    stubs.gate("status:awaiting_payment")
    handle = await start()
    await entered(stubs, "status:awaiting_payment")
    await handle.signal(OrderWorkflow.request_cancellation, t.CancelRequest("admin:a1"))
    stubs.open_all()
    assert await outcome(handle) == "cancelled"
    assert stubs.steps() == [
        t.RESERVE_INVENTORY,
        "status:awaiting_payment",
        t.RELEASE_INVENTORY,
        "status:cancelled",
    ]
    assert stubs.last_status().actor == "admin:a1"


async def test_cancel_while_charging_refunds_then_releases(start, stubs) -> None:
    stubs.gate(t.CHARGE_PAYMENT)
    handle = await start()
    await entered(stubs, t.CHARGE_PAYMENT)
    assert (await state(handle)).stage == "charging"
    await handle.signal(OrderWorkflow.request_cancellation, t.CancelRequest("customer:c1"))
    stubs.open_all()
    assert await outcome(handle) == "cancelled"
    assert stubs.steps() == [
        t.RESERVE_INVENTORY,
        "status:awaiting_payment",
        t.CHARGE_PAYMENT,
        t.REFUND_PAYMENT,
        t.RELEASE_INVENTORY,
        "status:cancelled",
    ]
    assert stubs.last_status().stage == "charging"


async def test_second_cancel_signal_does_not_replace_the_first(start, stubs) -> None:
    stubs.gate(t.RESERVE_INVENTORY)
    handle = await start()
    await entered(stubs, t.RESERVE_INVENTORY)
    await handle.signal(OrderWorkflow.request_cancellation, t.CancelRequest("customer:c1", "one"))
    await handle.signal(OrderWorkflow.request_cancellation, t.CancelRequest("admin:a1", "two"))
    stubs.open_all()
    assert await outcome(handle) == "cancelled"
    assert (stubs.last_status().actor, stubs.last_status().reason) == ("customer:c1", "one")


@pytest.mark.parametrize("gate", [t.COMMIT_INVENTORY, t.CREATE_SHIPMENT])
async def test_cancel_after_the_cancellable_point_is_ignored(start, stubs, gate) -> None:
    stubs.gate(gate)
    handle = await start()
    await entered(stubs, gate)
    await handle.signal(OrderWorkflow.request_cancellation, t.CancelRequest("customer:c1"))
    assert (await state(handle)).cancel_requested is False
    stubs.open_all()
    assert await outcome(handle) == "confirmed"
    assert stubs.steps() == FORWARD


# --- operator cancellation (a Temporal cancel, not the signal) -------------------------


async def test_operator_cancel_while_reserving_cancels_as_system(start, stubs) -> None:
    stubs.gate(t.RESERVE_INVENTORY)
    handle = await start()
    await entered(stubs, t.RESERVE_INVENTORY)
    await handle.cancel()
    assert await outcome(handle) == "cancelled"
    # Reserve never returned, so there is nothing to release.
    assert stubs.steps() == [t.RESERVE_INVENTORY, "status:cancelled"]
    final = stubs.last_status()
    assert (final.actor, final.reason, final.stage) == ("system", "operator", "reserving")


async def test_operator_cancel_mid_charge_refunds_what_may_have_captured(start, stubs) -> None:
    stubs.gate(t.CHARGE_PAYMENT)
    handle = await start()
    await entered(stubs, t.CHARGE_PAYMENT)
    await handle.cancel()
    assert await outcome(handle) == "cancelled"
    assert stubs.steps()[2:] == [
        t.CHARGE_PAYMENT,
        t.REFUND_PAYMENT,
        t.RELEASE_INVENTORY,
        "status:cancelled",
    ]


@pytest.mark.parametrize(
    ("gate", "undo"),
    [
        (t.COMMIT_INVENTORY, t.RELEASE_INVENTORY),
        (t.CREATE_SHIPMENT, t.REVERSE_COMMIT_INVENTORY),
    ],
)
async def test_operator_cancel_past_the_cancellable_point_compensates(
    start, stubs, gate, undo
) -> None:
    stubs.gate(gate)
    handle = await start()
    await entered(stubs, gate)
    await handle.cancel()
    assert await outcome(handle) == "failed"
    index = stubs.steps().index(gate)
    assert stubs.steps()[index + 1 :] == [
        "status:refunding",
        t.REFUND_PAYMENT,
        undo,
        "status:failed",
    ]
    assert stubs.last_status().failure_reason == "OPERATOR_TERMINATED"
    assert await state(handle) == t.WorkflowState("failed", "OPERATOR_TERMINATED", False)


async def test_operator_cancel_before_any_worker_ran_it(env, stubs) -> None:
    """Cancelled before its first task: the reserve is never even scheduled."""
    queue = f"q-{uuid.uuid4()}"
    handle = await env.client.start_workflow(
        OrderWorkflow.run, t.OrderInput("o-1", None, "tok_ok"), id="order-o-1", task_queue=queue
    )
    await handle.cancel()
    async with Worker(
        env.client, task_queue=queue, workflows=[OrderWorkflow], activities=stubs.all()
    ):
        assert await outcome(handle) == "cancelled"
    assert stubs.steps() == ["status:cancelled"]
    assert (stubs.last_status().actor, stubs.last_status().stage) == ("system", "reserving")
