"""Carrier updates (RFC 0005 §3.1.1.4). One implementation for the admin
simulation route and, in Milestone 3, the HMAC-signed webhook."""

import uuid
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.orders.enums import OrderStatus, ShipmentStatus
from app.orders.models import Order
from app.orders.repo import Change, OrderRepo
from app.orders.state_machine import IllegalTransition

# What a carrier status does to the order, when it does anything (ORD-2).
ORDER_EFFECT = {
    ShipmentStatus.SHIPPED: OrderStatus.SHIPPED,
    ShipmentStatus.DELIVERED: OrderStatus.DELIVERED,
}


@dataclass(frozen=True)
class CarrierUpdate:
    order: Order
    applied: bool


class ShipmentRepo:
    def __init__(self, session: AsyncSession, settings: Settings) -> None:
        self.orders = OrderRepo(session, settings)

    async def apply_carrier_update(
        self,
        order_id: uuid.UUID,
        status: ShipmentStatus,
        *,
        actor: str,
        tracking_number: str | None = None,
        carrier_event_id: str | None = None,
    ) -> CarrierUpdate:
        """Repeating the current status is a no-op; any other illegal move is 409."""
        order = await self.orders.get_any(order_id)
        if order.shipment_status == status:
            return CarrierUpdate(order, applied=False)
        if order.status not in (OrderStatus.CONFIRMED, OrderStatus.SHIPPED):
            raise IllegalTransition("shipment_status", order.shipment_status, status)
        order = await self.orders.advance(
            order_id,
            Change(
                actor=actor,
                shipment=status,
                status=ORDER_EFFECT.get(status),
                tracking_number=tracking_number,
                context={"carrier_event_id": carrier_event_id},
            ),
        )
        return CarrierUpdate(order, applied=True)
