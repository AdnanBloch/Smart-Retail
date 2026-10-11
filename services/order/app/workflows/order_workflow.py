"""``OrderWorkflow`` — the order saga (RFC 0005 §3.1.1.4).

Sequence and compensation only (B1); every rule is in the service layer the
activities reach. Reserve before charging (B4): the cheap failure comes
first. Any step failing runs the compensations of the steps already done, in
reverse — which is why this is a workflow and not a chain of event handlers:
"where is order X?" is a query, not an inference from six consumers' logs.

    reserve → awaiting_payment → charge → commit → ship → confirmed → notify
    undo:  release ← refund ← reverse_commit

Cancellation is honoured only before ``committing``; afterwards the signal is
recorded in history and ignored. Determinism (PLT-15): no I/O, time from the
workflow, sandbox on. Timeouts and retries are ORD-10.
"""

import asyncio
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError

with workflow.unsafe.imports_passed_through():
    from app.orders.enums import CANCELLABLE_STAGES
    from app.orders.enums import OrderFailureReason as Why
    from app.orders.enums import WorkflowStage as Stage
    from app.workflows import types as t


def _retry(initial: float, maximum: float, attempts: int, *non_retryable: str) -> RetryPolicy:
    return RetryPolicy(
        initial_interval=timedelta(seconds=initial),
        backoff_coefficient=2.0,
        maximum_interval=timedelta(seconds=maximum),
        maximum_attempts=attempts,
        non_retryable_error_types=list(non_retryable),
    )


def _options(start: float, total: float, retry: RetryPolicy) -> dict[str, Any]:
    return {
        "start_to_close_timeout": timedelta(seconds=start),
        "schedule_to_close_timeout": timedelta(seconds=total),
        "retry_policy": retry,
    }


# ORD-10. Every forward step fits inside the 15-minute reservation TTL, so a
# merely slow workflow never has its reservation swept out from under it.
RESERVE = _options(10, 60, _retry(1, 10, 5, "insufficient-stock", "unknown-variant"))
LOCAL = _options(5, 300, _retry(1, 30, 0, "IllegalTransition"))
CHARGE = _options(15, 60, _retry(1, 10, 3, "PaymentDeclined"))
COMMIT = _options(10, 300, _retry(1, 30, 0, "reservation-not-held"))
SHIP = _options(15, 120, _retry(1, 15, 5, "AddressRejected"))
# A compensation that gives up is worse than one that is late.
COMPENSATE = _options(15, 3600, _retry(2, 60, 0))


def error_type(err: BaseException) -> str | None:
    cause: BaseException | None = err
    while isinstance(cause, ActivityError) and cause.cause is not None:
        cause = cause.cause
    return cause.type if isinstance(cause, ApplicationError) else None


def _cancelled(err: ActivityError) -> bool:
    return isinstance(err.cause, CancelledError)


@workflow.defn(name=t.WORKFLOW_NAME)
class OrderWorkflow:
    def __init__(self) -> None:
        self._stage = Stage.STARTING
        self._cancel: t.CancelRequest | None = None
        self._failure: str | None = None

    @workflow.signal(name=t.CANCEL_SIGNAL)
    def request_cancellation(self, request: t.CancelRequest) -> None:
        if self._stage in CANCELLABLE_STAGES and self._cancel is None:
            self._cancel = request

    @workflow.query(name=t.GET_STATE_QUERY)
    def get_state(self) -> t.WorkflowState:
        return t.WorkflowState(self._stage.value, self._failure, self._cancel is not None)

    @workflow.run
    async def run(self, inp: t.OrderInput) -> str:
        reservations: list[str] = []
        charged = committed = False
        try:
            self._stage = Stage.RESERVING
            reservations = await self._act(t.RESERVE_INVENTORY, inp, RESERVE)
            if self._cancel:
                return await self._cancellation(inp, reservations, charged=False)
            await self._status(inp, "awaiting_payment")
            if self._cancel:
                return await self._cancellation(inp, reservations, charged=False)

            self._stage = Stage.CHARGING
            await self._act(t.CHARGE_PAYMENT, inp, CHARGE)
            charged = True
            if self._cancel:
                return await self._cancellation(inp, reservations, charged=True)

            self._stage = Stage.COMMITTING  # past the cancellable point
            await self._act(
                t.COMMIT_INVENTORY, t.ReservationsInput(inp.order_id, reservations), COMMIT
            )
            committed = True

            self._stage = Stage.SHIPPING
            await self._act(t.CREATE_SHIPMENT, inp, SHIP)

            self._stage = Stage.CONFIRMING
            await self._status(inp, "confirmed")
        except ActivityError as err:
            if _cancelled(err):
                return await self._operator_cancelled(inp, reservations, charged, committed)
            return await self._fail(inp, error_type(err), reservations, charged, committed)
        except asyncio.CancelledError:
            return await self._operator_cancelled(inp, reservations, charged, committed)

        self._stage = Stage.NOTIFYING
        try:
            await self._act(t.NOTIFY, inp.order_id, LOCAL)
        except ActivityError:
            # The order is confirmed regardless; a null notified_at makes it visible.
            workflow.logger.warning("notify failed", extra={"order_id": inp.order_id})
        self._stage = Stage.DONE
        return "confirmed"

    # --- failure paths ---------------------------------------------------------

    async def _fail(
        self,
        inp: t.OrderInput,
        kind: str | None,
        reservations: list[str],
        charged: bool,
        committed: bool,
    ) -> str:
        stage = self._stage
        self._stage = Stage.COMPENSATING
        if stage == Stage.RESERVING:
            why = (
                Why.INSUFFICIENT_STOCK
                if kind == "insufficient-stock"
                else Why.INVENTORY_UNAVAILABLE
            )
            return await self._final(inp, "failed", why, stage)

        if not charged:
            payment: str | None = None
            if stage == Stage.CHARGING and kind == "PaymentDeclined":
                why, payment = Why.PAYMENT_DECLINED, "failed"
            elif stage == Stage.CHARGING:
                # Ambiguous: the timed-out charge may have captured. The refund is
                # keyed to the charge, and "nothing captured" is recorded as failed.
                why = Why.PAYMENT_TIMEOUT
                await self._act(t.REFUND_PAYMENT, inp, COMPENSATE)
            else:
                why = Why.INVENTORY_UNAVAILABLE
            await self._release(inp, reservations)
            return await self._final(inp, "failed", why, stage, payment_status=payment)

        # Charged: refund first (ORD-1 awaiting_payment → refunding), then stock.
        if stage == Stage.COMMITTING:
            why = Why.RESERVATION_EXPIRED if kind == "reservation-not-held" else Why.COMMIT_FAILED
        else:
            why = Why.FULFILMENT_FAILED
        await self._status(inp, "refunding", failure_reason=why)
        await self._act(t.REFUND_PAYMENT, inp, COMPENSATE)
        if committed:
            await self._act(
                t.REVERSE_COMMIT_INVENTORY,
                t.ReservationsInput(inp.order_id, reservations),
                COMPENSATE,
            )
        else:
            await self._release(inp, reservations)
        return await self._final(inp, "failed", why, stage)

    async def _cancellation(
        self, inp: t.OrderInput, reservations: list[str], *, charged: bool
    ) -> str:
        assert self._cancel is not None
        stage = self._stage
        self._stage = Stage.COMPENSATING
        if charged:
            await self._act(t.REFUND_PAYMENT, inp, COMPENSATE)
        await self._release(inp, reservations)
        await self._status(
            inp,
            "cancelled",
            stage=stage.value,
            actor=self._cancel.actor,
            reason=self._cancel.reason,
        )
        self._stage = Stage.CANCELLED
        return "cancelled"

    async def _operator_cancelled(
        self, inp: t.OrderInput, reservations: list[str], charged: bool, committed: bool
    ) -> str:
        """A Temporal cancellation: before the cancellable point it behaves like the
        signal; after it, the saga compensates and ends OPERATOR_TERMINATED."""
        if self._stage in CANCELLABLE_STAGES:
            self._cancel = self._cancel or t.CancelRequest(actor="system", reason="operator")
            # A charge interrupted mid-flight may have captured: refund it.
            return await self._cancellation(
                inp, reservations, charged=charged or self._stage == Stage.CHARGING
            )
        self._stage = Stage.COMPENSATING
        await self._status(inp, "refunding", failure_reason=Why.OPERATOR_TERMINATED)
        await self._act(t.REFUND_PAYMENT, inp, COMPENSATE)
        undo = t.REVERSE_COMMIT_INVENTORY if committed else t.RELEASE_INVENTORY
        await self._act(undo, t.ReservationsInput(inp.order_id, reservations), COMPENSATE)
        return await self._final(inp, "failed", Why.OPERATOR_TERMINATED, Stage.COMPENSATING)

    async def _final(
        self,
        inp: t.OrderInput,
        status: str,
        why: Why,
        stage: Stage,
        *,
        payment_status: str | None = None,
    ) -> str:
        self._failure = why.value
        await self._status(
            inp, status, failure_reason=why, payment_status=payment_status, stage=stage.value
        )
        self._stage = Stage.FAILED
        return status

    # --- plumbing --------------------------------------------------------------

    async def _release(self, inp: t.OrderInput, reservations: list[str]) -> None:
        if reservations:
            await self._act(
                t.RELEASE_INVENTORY, t.ReservationsInput(inp.order_id, reservations), COMPENSATE
            )

    async def _status(
        self,
        inp: t.OrderInput,
        status: str,
        *,
        failure_reason: Why | None = None,
        payment_status: str | None = None,
        stage: str | None = None,
        actor: str = "workflow",
        reason: str | None = None,
    ) -> None:
        await self._act(
            t.SET_ORDER_STATUS,
            t.StatusInput(
                order_id=inp.order_id,
                status=status,
                correlation_id=inp.correlation_id,
                failure_reason=failure_reason.value if failure_reason else None,
                payment_status=payment_status,
                stage=stage,
                actor=actor,
                reason=reason,
            ),
            LOCAL,
        )

    async def _act(self, name: str, arg: object, options: dict[str, Any]) -> Any:
        return await workflow.execute_activity(name, arg, **options)
