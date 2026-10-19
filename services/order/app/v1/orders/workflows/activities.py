"""``OrderWorkflow``'s activities: thin adapters (B1), each idempotent (B3).

* order state       → ``OrderRepo`` in-process, with order's role (R1, PLT-1)
* stock             → inventory's internal routes, with a service token
* payment, shipping → the provider protocols, keyed ``order-{id}:charge`` /
  ``:refund`` / ``:shipment``, so a retry after a timeout gets the first result
"""

import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from temporalio import activity
from temporalio.exceptions import ApplicationError

from app.core.config import Settings
from app.v1.orders.clients.inventory import InventoryClient
from app.v1.orders.enums import OrderFailureReason, OrderStatus, PaymentStatus, ShipmentStatus
from app.v1.orders.providers.errors import AddressRejected, PaymentDeclined, ProviderTransientError
from app.v1.orders.providers.payment import PaymentProvider
from app.v1.orders.providers.shipping import ShipLine, ShippingProvider
from app.v1.orders.repo import WORKFLOW_ACTOR, Change, OrderRepo, item_lines
from app.v1.orders.workflows import types as t
from smartretail_common.db import Database
from smartretail_common.temporal import as_activity_errors, business_failure

logger = logging.getLogger("order.activities")

T = TypeVar("T")


def charge_key(order_id: uuid.UUID) -> str:
    return f"order-{order_id}:charge"


class OrderActivities:
    def __init__(
        self,
        db: Database,
        settings: Settings,
        inventory: InventoryClient,
        payments: PaymentProvider,
        shipping: ShippingProvider,
    ) -> None:
        self.db = db
        self.settings = settings
        self.inventory = inventory
        self.payments = payments
        self.shipping = shipping

    def all(self) -> list[Callable[..., Any]]:
        return [
            self.reserve_inventory,
            self.set_order_status,
            self.charge_payment,
            self.commit_inventory,
            self.create_shipment,
            self.notify,
            self.release_inventory,
            self.refund_payment,
            self.reverse_commit_inventory,
        ]

    async def _orders(self, fn: Callable[[OrderRepo], Awaitable[T]]) -> T:
        async def call() -> T:
            async with self.db.sessionmaker() as session:
                return await fn(OrderRepo(session, self.settings))

        return await as_activity_errors(call)

    # --- forward steps ---------------------------------------------------------

    @activity.defn(name=t.RESERVE_INVENTORY)
    async def reserve_inventory(self, inp: t.OrderInput) -> list[str]:
        order = await self._orders(lambda r: r.for_workflow(uuid.UUID(inp.order_id)))
        return await self.inventory.reserve(order.id, item_lines(order.items), inp.correlation_id)

    @activity.defn(name=t.SET_ORDER_STATUS)
    async def set_order_status(self, inp: t.StatusInput) -> str:
        status = OrderStatus(inp.status)
        payment = PaymentStatus(inp.payment_status) if inp.payment_status else None
        if status == OrderStatus.AWAITING_PAYMENT:
            payment = PaymentStatus.PENDING  # the order now waits for its charge
        change = Change(
            actor=inp.actor,
            status=status,
            payment=payment,
            failure_reason=OrderFailureReason(inp.failure_reason) if inp.failure_reason else None,
            reason=inp.reason,
            correlation_id=inp.correlation_id,
            context={"stage": inp.stage},
        )
        order = await self._orders(lambda r: r.advance(uuid.UUID(inp.order_id), change))
        return str(order.status)

    @activity.defn(name=t.CHARGE_PAYMENT)
    async def charge_payment(self, inp: t.OrderInput) -> str:
        order = await self._orders(lambda r: r.for_workflow(uuid.UUID(inp.order_id)))
        try:
            result = await self.payments.charge(
                idempotency_key=charge_key(order.id),
                order_id=order.id,
                amount=order.total,
                currency=order.currency,
                payment_method=inp.payment_method,
            )
        except PaymentDeclined as exc:
            raise business_failure("PaymentDeclined", str(exc)) from exc
        except ProviderTransientError as exc:
            raise ApplicationError(str(exc), type="ProviderTransientError") from exc
        change = Change(
            actor=WORKFLOW_ACTOR,
            payment=PaymentStatus.SUCCEEDED,
            payment_reference=result.reference,
            correlation_id=inp.correlation_id,
        )
        await self._orders(lambda r: r.advance(order.id, change))
        return result.reference

    @activity.defn(name=t.COMMIT_INVENTORY)
    async def commit_inventory(self, inp: t.ReservationsInput) -> None:
        for reservation_id in inp.reservation_ids:
            await self.inventory.commit(reservation_id)  # committing twice is a no-op

    @activity.defn(name=t.CREATE_SHIPMENT)
    async def create_shipment(self, inp: t.OrderInput) -> str:
        order = await self._orders(lambda r: r.for_workflow(uuid.UUID(inp.order_id)))
        try:
            result = await self.shipping.create_shipment(
                idempotency_key=f"order-{order.id}:shipment",
                order_number=order.order_number,
                address=order.shipping_address,
                lines=[ShipLine(i.sku, i.quantity) for i in order.items],
            )
        except AddressRejected as exc:
            raise business_failure("AddressRejected", str(exc)) from exc
        except ProviderTransientError as exc:
            raise ApplicationError(str(exc), type="ProviderTransientError") from exc
        change = Change(
            actor=WORKFLOW_ACTOR,
            shipment=ShipmentStatus.CREATED,
            shipment_reference=result.reference,
            tracking_number=result.tracking_number,
            correlation_id=inp.correlation_id,
        )
        await self._orders(lambda r: r.advance(order.id, change))
        return result.reference

    @activity.defn(name=t.NOTIFY)
    async def notify(self, order_id: str) -> None:
        """Stamps ``notified_at``; notification itself consumes the order events (M3)."""
        await self._orders(lambda r: r.mark_notified(uuid.UUID(order_id)))
        logger.info("order notified", extra={"order_id": order_id})

    # --- compensations -----------------------------------------------------------

    @activity.defn(name=t.RELEASE_INVENTORY)
    async def release_inventory(self, inp: t.ReservationsInput) -> None:
        for reservation_id in inp.reservation_ids:
            await self.inventory.release(reservation_id)  # released or expired: success

    @activity.defn(name=t.REFUND_PAYMENT)
    async def refund_payment(self, inp: t.OrderInput) -> str:
        """Keyed to the charge: "nothing captured" is a success, recorded as failed."""
        order = await self._orders(lambda r: r.for_workflow(uuid.UUID(inp.order_id)))
        try:
            result = await self.payments.refund(
                idempotency_key=f"order-{order.id}:refund",
                charge_idempotency_key=charge_key(order.id),
                amount=order.total,
                currency=order.currency,
            )
        except ProviderTransientError as exc:
            raise ApplicationError(str(exc), type="ProviderTransientError") from exc
        await self._orders(lambda r: r.record_refund(order.id, result.outcome, inp.correlation_id))
        return result.outcome.value

    @activity.defn(name=t.REVERSE_COMMIT_INVENTORY)
    async def reverse_commit_inventory(self, inp: t.ReservationsInput) -> None:
        for reservation_id in inp.reservation_ids:
            await self.inventory.reverse(reservation_id)  # ORD-12 / INV-16; repeat is a no-op
