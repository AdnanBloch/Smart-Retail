"""``OrderRepo`` — the only writer of order, payment and shipment state (R3, I2).

* ``place`` is checkout: claim the key, snapshot the lines from catalog,
  persist the order with its history and outbox rows and the completed key
  in one transaction, then start ``OrderWorkflow`` (B7, B8, B9).
* ``advance`` is every later change: version check (S6), the transition
  tables (I2), the history row (S7) and the outbox rows (B11), committed
  together. A change already in effect is a no-op, so a retried activity
  never fails on its own earlier success.
"""

import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol

from sqlalchemy import ColumnElement, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.config import Settings
from app.core.errors import (
    OrderNotFound,
    OrderTerminal,
    PastCancellablePoint,
    TemporalUnavailable,
    VersionConflict,
    problem_body,
)
from app.v1.orders import access, events, snapshot, utils
from app.v1.orders.clients.catalog import CatalogClient
from app.v1.orders.enums import (
    CANCELLABLE_STAGES,
    OrderEvent,
    OrderFailureReason,
    OrderStatus,
    PaymentStatus,
    RefundOutcome,
    ShipmentStatus,
)
from app.v1.orders.idempotency_repo import IdempotencyRepo, StoredReply
from app.v1.orders.models import ORDER_NUMBER_SEQ, Order, OrderItem, OrderStatusHistory
from app.v1.orders.state_machine import (
    ORDER_TRANSITIONS,
    PAYMENT_TRANSITIONS,
    SHIPMENT_TRANSITIONS,
    TERMINAL_ORDER_STATES,
    IllegalTransition,
    transition,
)
from smartretail_common.auth import Principal
from smartretail_common.context import get_request_id
from smartretail_common.errors import Unprocessable
from smartretail_common.pagination import PageParams, Slice

logger = logging.getLogger("order.repo")

WORKFLOW_ACTOR = "workflow"
CARRIER_STATUSES = frozenset(
    {ShipmentStatus.SHIPPED, ShipmentStatus.IN_TRANSIT, ShipmentStatus.DELIVERED}
)


class OrderWorkflows(Protocol):
    """The orchestrator, as the repo needs it. ``workflows/client.py`` beside it is Temporal's."""

    async def start(
        self,
        *,
        order_id: uuid.UUID,
        workflow_id: str,
        correlation_id: str | None,
        payment_method: str,
    ) -> str: ...

    async def state(self, workflow_id: str) -> dict[str, Any]: ...

    async def request_cancellation(
        self, workflow_id: str, *, actor: str, reason: str | None
    ) -> None: ...


@dataclass(frozen=True)
class Checkout:
    lines: list[snapshot.LineRequest]
    shipping_address: dict[str, Any]
    payment_method: str
    origin_client_id: str | None = None


@dataclass(frozen=True)
class Placement:
    reply: StoredReply
    replayed: bool


@dataclass(frozen=True)
class Change:
    """One state change. Fields left ``None`` are not changed."""

    actor: str
    status: OrderStatus | None = None
    payment: PaymentStatus | None = None
    shipment: ShipmentStatus | None = None
    failure_reason: OrderFailureReason | None = None
    reason: str | None = None
    payment_reference: str | None = None
    shipment_reference: str | None = None
    tracking_number: str | None = None
    correlation_id: str | None = None
    expected_version: int | None = None
    # Event context: the workflow stage, a carrier event id.
    context: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OrderFilters:
    status: OrderStatus | None = None
    since: datetime | None = None
    until: datetime | None = None
    customer_id: uuid.UUID | None = None


@dataclass(frozen=True)
class OrderDetail:
    order: Order
    timeline: list[OrderStatusHistory]


def workflow_id_for(order_id: uuid.UUID) -> str:
    """One workflow per order; the id is Temporal's duplicate-start guard (Q3)."""
    return f"order-{order_id}"


def accepted_body(order: Order) -> dict[str, Any]:
    return {
        "order_id": str(order.id),
        "order_number": order.order_number,
        "status": str(order.status),
        "_links": {"self": f"/api/v1/orders/{order.id}"},
    }


class OrderRepo:
    def __init__(self, session: AsyncSession, settings: Settings) -> None:
        self.session = session
        self.settings = settings

    # --- checkout ------------------------------------------------------------

    async def place(
        self,
        principal: Principal,
        checkout: Checkout,
        *,
        key: str,
        request_hash: str,
        token: str,
        idempotency: IdempotencyRepo,
        catalog: CatalogClient,
        workflows: OrderWorkflows,
    ) -> Placement:
        customer_id = uuid.UUID(principal.subject)
        stored = await idempotency.claim(customer_id, key, request_hash)
        if stored is not None:
            return Placement(stored, replayed=True)

        try:
            snapshot.check_shape(
                checkout.lines,
                max_lines=self.settings.order_max_lines,
                max_quantity=self.settings.order_max_quantity_per_line,
            )
            lines = await snapshot.take(catalog, token, checkout.lines)
        except Unprocessable as exc:
            # A business rejection is completed and replayed like a success.
            reply = StoredReply(exc.status, problem_body(exc, "/api/v1/orders"))
            await idempotency.complete(customer_id, key, reply)
            await self.session.commit()
            return Placement(reply, replayed=False)
        except BaseException:
            # A dependency failure releases the claim, so a retry can proceed.
            await idempotency.release(customer_id, key)
            raise

        try:
            order = await self._persist(customer_id, checkout, lines)
            reply = StoredReply(202, accepted_body(order))
            await idempotency.complete(customer_id, key, reply, order_id=order.id)
            await self.session.commit()
        except BaseException:
            await idempotency.release(customer_id, key)
            raise

        try:
            run_id = await workflows.start(
                order_id=order.id,
                workflow_id=workflow_id_for(order.id),
                correlation_id=get_request_id(),
                payment_method=checkout.payment_method,
            )
        except Exception:
            # Acceptance does not depend on the orchestrator being up (ORD-9): the
            # order stays pending with no run id until its workflow is started.
            logger.warning("order workflow not started", extra={"order_id": str(order.id)})
        else:
            await self.session.execute(
                update(Order).where(Order.id == order.id).values(workflow_run_id=run_id)
            )
            await self.session.commit()
        return Placement(reply, replayed=False)

    async def _persist(
        self, customer_id: uuid.UUID, checkout: Checkout, lines: list[snapshot.LineSnapshot]
    ) -> Order:
        number = await self.session.scalar(ORDER_NUMBER_SEQ.next_value())
        now = datetime.now(UTC)
        order_id = uuid.uuid4()
        items = [
            OrderItem(
                id=uuid.uuid4(),
                order_id=order_id,
                product_id=line.product_id,
                variant_id=line.variant_id,
                merchant_id=line.merchant_id,
                product_name=line.product_name,
                sku=line.sku,
                variant_options=line.variant_options,
                unit_price=utils.money(line.unit_price),
                quantity=line.quantity,
                line_total=utils.line_total(line.unit_price, line.quantity),
            )
            for line in lines
        ]
        fee = utils.money(Decimal(self.settings.order_shipping_fee))
        subtotal, total = utils.totals((i.line_total for i in items), fee)
        order = Order(
            id=order_id,
            order_number=f"SR-{now.year}-{number:07d}",
            customer_id=customer_id,
            status=OrderStatus.PENDING,
            subtotal=subtotal,
            shipping_fee=fee,
            total=total,
            currency=lines[0].currency,
            shipping_address=checkout.shipping_address,
            payment_status=PaymentStatus.NONE,
            shipment_status=ShipmentStatus.NONE,
            workflow_id=workflow_id_for(order_id),
            version=1,
            placed_at=now,
            origin_client_id=checkout.origin_client_id,
            payment_method=checkout.payment_method,
        )
        self.session.add(order)
        await self.session.flush()
        self.session.add_all(items)
        self.session.add(
            OrderStatusHistory(
                order_id=order.id,
                from_status=None,
                to_status=OrderStatus.PENDING.value,
                actor=f"customer:{customer_id}",
                correlation_id=get_request_id(),
            )
        )
        events.created(self.session, order, items, get_request_id())
        await self.session.flush()
        return order

    # --- the single writer ---------------------------------------------------

    async def advance(self, order_id: uuid.UUID, change: Change) -> Order:
        order = await self._lock(order_id)
        if change.expected_version is not None and order.version != change.expected_version:
            raise VersionConflict(order.version)

        previous_status = order.status
        previous_shipment = order.shipment_status
        status = payment = shipment = None
        if change.status is not None and change.status != order.status:
            status = transition(ORDER_TRANSITIONS, "status", order.status, change.status)
        if change.payment is not None and change.payment != order.payment_status:
            payment = transition(
                PAYMENT_TRANSITIONS, "payment_status", order.payment_status, change.payment
            )
        if change.shipment is not None and change.shipment != order.shipment_status:
            shipment = transition(
                SHIPMENT_TRANSITIONS, "shipment_status", order.shipment_status, change.shipment
            )
        if status is None and payment is None and shipment is None:
            await self.session.rollback()
            return order  # already in effect: an idempotent no-op

        if status == OrderStatus.CONFIRMED and (
            (payment or order.payment_status) != PaymentStatus.SUCCEEDED
            or (shipment or order.shipment_status) != ShipmentStatus.CREATED
        ):
            raise IllegalTransition("status", order.status, status)  # the confirm guard

        now = datetime.now(UTC)
        if status is not None:
            order.status = status
            if status == OrderStatus.CONFIRMED:
                order.confirmed_at = now
        if payment is not None:
            order.payment_status = payment
        if shipment is not None:
            order.shipment_status = shipment
        if change.failure_reason is not None:
            order.failure_reason = change.failure_reason.value
        for name in ("payment_reference", "shipment_reference", "tracking_number"):
            if getattr(change, name) is not None:
                setattr(order, name, getattr(change, name))
        order.version += 1

        correlation_id = change.correlation_id or get_request_id()
        if status is not None:
            self.session.add(
                OrderStatusHistory(
                    order_id=order.id,
                    from_status=str(previous_status),
                    to_status=status.value,
                    reason=change.failure_reason.value if change.failure_reason else change.reason,
                    actor=change.actor,
                    correlation_id=correlation_id,
                )
            )
        await self._write_events(
            order, change, status, payment, shipment, previous_shipment, correlation_id
        )
        await self.session.commit()
        return order

    async def _write_events(
        self,
        order: Order,
        change: Change,
        status: OrderStatus | None,
        payment: PaymentStatus | None,
        shipment: ShipmentStatus | None,
        previous_shipment: ShipmentStatus | str,
        correlation_id: str | None,
    ) -> None:
        happened: list[OrderEvent] = []
        if payment == PaymentStatus.SUCCEEDED:
            happened.append(OrderEvent.PAYMENT_SUCCEEDED)
        if payment == PaymentStatus.FAILED:
            happened.append(OrderEvent.PAYMENT_FAILED)
        if status == OrderStatus.CONFIRMED:
            happened.append(OrderEvent.CONFIRMED)
        if status == OrderStatus.FAILED:
            happened.append(OrderEvent.FAILED)
        if status == OrderStatus.CANCELLED:
            happened.append(OrderEvent.CANCELLED)
        # none → created happens inside the workflow and is announced by
        # order.confirmed; only carrier-driven changes are shipment events.
        if shipment in CARRIER_STATUSES:
            happened.append(OrderEvent.SHIPMENT_STATUS_CHANGED)
        if not happened:
            return
        causation = await events.created_event_id(self.session, order.id)
        context = {
            **change.context,
            "actor": change.actor,
            "reason": change.reason,
            "failure_reason": order.failure_reason,
            "from_shipment": str(previous_shipment),
        }
        for event in happened:
            carrier = event == OrderEvent.SHIPMENT_STATUS_CHANGED
            events.write(
                self.session,
                event,
                order,
                events.payloads(order, event, context),
                correlation_id=correlation_id,
                causation_id=None if carrier else causation,
                client_id=None if carrier else order.origin_client_id,
            )

    # --- what the workflow's activities need ----------------------------------

    async def for_workflow(self, order_id: uuid.UUID) -> Order:
        """The order with its lines: what reserve, charge and ship read."""
        order = await self.session.scalar(
            select(Order).where(Order.id == order_id).options(selectinload(Order.items))
        )
        if order is None:
            raise OrderNotFound()
        return order

    async def record_refund(
        self, order_id: uuid.UUID, outcome: RefundOutcome, correlation_id: str | None
    ) -> Order:
        """``refunded`` → payment refunded; ``nothing_captured`` → the charge never
        happened, so payment failed (the ambiguous-timeout path)."""
        target = (
            PaymentStatus.REFUNDED if outcome == RefundOutcome.REFUNDED else PaymentStatus.FAILED
        )
        order = await self.for_workflow(order_id)
        if order.payment_status in (PaymentStatus.REFUNDED, PaymentStatus.FAILED):
            return order
        return await self.advance(
            order_id, Change(actor=WORKFLOW_ACTOR, payment=target, correlation_id=correlation_id)
        )

    async def mark_notified(self, order_id: uuid.UUID) -> None:
        """First write wins: a confirmed order with null ``notified_at`` never notified."""
        await self.session.execute(
            update(Order)
            .where(Order.id == order_id)
            .values(notified_at=func.coalesce(Order.notified_at, func.now()))
        )
        await self.session.commit()

    # --- cancellation ----------------------------------------------------------

    async def request_cancellation(
        self,
        principal: Principal,
        order_id: uuid.UUID,
        reason: str | None,
        workflows: OrderWorkflows,
    ) -> Order:
        """Decides synchronously where it can; the cancellation itself is the workflow's."""
        order = await self._visible(principal, order_id)
        if order.status in TERMINAL_ORDER_STATES:
            raise OrderTerminal(f"The order is {order.status}")
        assert order.workflow_id is not None
        try:
            state = await workflows.state(order.workflow_id)
        except Exception as exc:
            raise TemporalUnavailable("Retry the cancellation shortly") from exc
        stage = str(state.get("stage"))
        if stage not in {s.value for s in CANCELLABLE_STAGES}:
            raise PastCancellablePoint(stage)
        actor = access.actor(principal)
        try:
            await workflows.request_cancellation(order.workflow_id, actor=actor, reason=reason)
        except Exception as exc:
            raise TemporalUnavailable("Retry the cancellation shortly") from exc
        return order

    # --- reads -----------------------------------------------------------------

    async def detail(self, principal: Principal, order_id: uuid.UUID) -> OrderDetail:
        order = await self._visible(principal, order_id)
        timeline = await self.session.scalars(
            select(OrderStatusHistory)
            .where(OrderStatusHistory.order_id == order_id)
            .order_by(OrderStatusHistory.created_at, OrderStatusHistory.id)
        )
        return OrderDetail(order, list(timeline))

    async def get_any(self, order_id: uuid.UUID) -> Order:
        order = await self.session.get(Order, order_id)
        if order is None:
            raise OrderNotFound()
        return order

    async def list_orders(
        self, principal: Principal, filters: OrderFilters, page: PageParams
    ) -> Slice[Order]:
        """Customers see their own orders; admins see all and may filter by customer."""
        conditions = self._filters(filters)
        if access.is_admin(principal):
            if filters.customer_id is not None:
                conditions.append(Order.customer_id == filters.customer_id)
        else:
            conditions.append(Order.customer_id == uuid.UUID(principal.subject))
        total = await self.session.scalar(
            select(func.count()).select_from(Order).where(*conditions)
        )
        rows = await self.session.scalars(
            select(Order)
            .where(*conditions)
            .order_by(Order.created_at.desc(), Order.id)
            .limit(page.limit)
            .offset(page.offset)
        )
        return Slice(items=list(rows), total=total or 0, params=page)

    async def list_for_merchant(
        self, principal: Principal, filters: OrderFilters, page: PageParams
    ) -> Slice[tuple[Order, list[OrderItem]]]:
        """Orders containing the caller's products, with **only the caller's lines** (ORD-16)."""
        merchant_id = access.caller_merchant_id(principal)
        assert merchant_id is not None  # the route requires the merchant role
        mine = select(OrderItem.order_id).where(OrderItem.merchant_id == merchant_id)
        conditions = [*self._filters(filters), Order.id.in_(mine)]
        total = await self.session.scalar(
            select(func.count()).select_from(Order).where(*conditions)
        )
        orders = list(
            await self.session.scalars(
                select(Order)
                .where(*conditions)
                .order_by(Order.created_at.desc(), Order.id)
                .limit(page.limit)
                .offset(page.offset)
            )
        )
        lines: dict[uuid.UUID, list[OrderItem]] = {o.id: [] for o in orders}
        if orders:
            for item in await self.session.scalars(
                select(OrderItem)
                .where(OrderItem.order_id.in_(lines), OrderItem.merchant_id == merchant_id)
                .order_by(OrderItem.variant_id)
            ):
                lines[item.order_id].append(item)
        return Slice(items=[(o, lines[o.id]) for o in orders], total=total or 0, params=page)

    # --- helpers -----------------------------------------------------------------

    def _filters(self, filters: OrderFilters) -> list[ColumnElement[bool]]:
        conditions: list[ColumnElement[bool]] = []
        if filters.status is not None:
            conditions.append(Order.status == filters.status)
        if filters.since is not None:
            conditions.append(Order.created_at >= filters.since)
        if filters.until is not None:
            conditions.append(Order.created_at < filters.until)
        return conditions

    async def _visible(self, principal: Principal, order_id: uuid.UUID) -> Order:
        """Owner or admin; anyone else gets 404, which does not confirm the order exists."""
        order = await self.session.scalar(
            select(Order).where(Order.id == order_id).options(selectinload(Order.items))
        )
        if order is None or not access.can_view(principal, order):
            raise OrderNotFound()
        return order

    async def _lock(self, order_id: uuid.UUID) -> Order:
        order = await self.session.scalar(
            select(Order)
            .where(Order.id == order_id)
            .options(selectinload(Order.items))
            .with_for_update(of=Order)
            .execution_options(populate_existing=True)
        )
        if order is None:
            raise OrderNotFound()
        return order


def item_lines(items: Sequence[OrderItem]) -> list[tuple[uuid.UUID, int]]:
    return [(i.variant_id, i.quantity) for i in items]
