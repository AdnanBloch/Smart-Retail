"""Reserve / commit / release / reverse / expire (RFC 0006 §3.1.1.4) — the one hard function.

Every path follows the canonical lock order — ``stock_items`` rows in
ascending ``variant_id``, then ``reservations`` rows — inside
``in_locked_transaction``, and writes the ledger and outbox rows in the same
transaction. Idempotent re-requests are answered before ``transition()``.
"""

import uuid
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import NoReturn

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.errors import (
    InsufficientStock,
    ReservationCommitted,
    ReservationConflict,
    ReservationNotCommitted,
    ReservationNotFound,
    ReservationNotHeld,
    UnknownVariant,
)
from app.v1.reservations.enums import ReleaseReason, ReservationStatus
from app.v1.reservations.models import Reservation
from app.v1.reservations.state_machine import transition
from app.v1.stock import events, ledger_repo
from app.v1.stock.enums import ReferenceType, StockMovementReason, StockoutTrigger
from app.v1.stock.locking import in_locked_transaction, locked
from app.v1.stock.models import StockItem, StockMovement

SWEEPER_ACTOR = "task:sweep_expired_reservations"


@dataclass(frozen=True)
class Line:
    variant_id: uuid.UUID
    quantity: int


@dataclass(frozen=True)
class Shortage:
    item: StockItem
    requested: int
    available: int


@dataclass(frozen=True)
class ReserveResult:
    reservations: list[Reservation]
    created: bool


def normalise(lines: Iterable[Line]) -> list[Line]:
    """Merges lines for the same variant and sorts by ``variant_id`` (the lock order)."""
    totals: dict[uuid.UUID, int] = defaultdict(int)
    for line in lines:
        totals[line.variant_id] += line.quantity
    return [Line(v, q) for v, q in sorted(totals.items())]


def stockout_trigger(before: int, after: int, threshold: int) -> StockoutTrigger | None:
    """Edge-triggered: only the reserve that crosses the boundary emits (INV-7)."""
    if after == 0:
        return StockoutTrigger.DEPLETED
    if before > threshold >= after:
        return StockoutTrigger.LOW_STOCK
    return None


class ReservationRepo:
    def __init__(self, session: AsyncSession, settings: Settings) -> None:
        self.session = session
        self.settings = settings

    # --- reserve -----------------------------------------------------------

    async def reserve(
        self,
        order_id: uuid.UUID,
        lines: Iterable[Line],
        *,
        actor: str,
        correlation_id: str | None = None,
    ) -> ReserveResult:
        """All-or-nothing across lines (I7); a repeat for the order replays (B3)."""
        merged = normalise(lines)
        # Fast reject (INV-8): an unlocked read can only refuse, never grant. It is
        # skipped for an order that already holds reservations, so that a retried
        # reserve replays instead of being refused against its own holding.
        if not await self._has_reservations(order_id):
            shortages = await self._shortages(merged, lock=False)
            if shortages:
                await self._refuse(order_id, shortages, correlation_id)

        async def work() -> ReserveResult | list[Shortage]:
            return await self._reserve_locked(order_id, merged, actor, correlation_id)

        outcome = await in_locked_transaction(self.session, self.settings, work)
        if isinstance(outcome, list):
            await self._refuse(order_id, outcome, correlation_id)
        assert isinstance(outcome, ReserveResult)
        return outcome

    async def _reserve_locked(
        self, order_id: uuid.UUID, lines: list[Line], actor: str, correlation_id: str | None
    ) -> ReserveResult | list[Shortage]:
        # Stock rows first, in one ordered statement. The replay check comes after
        # the locks, so two concurrent reserves for one order are serialised on
        # them and the second always sees the first's reservations.
        items = await self._lock_items([line.variant_id for line in lines])
        existing = list(
            await self.session.scalars(
                select(Reservation)
                .where(Reservation.order_id == order_id)
                .order_by(Reservation.variant_id)
            )
        )
        if existing:
            same = {(r.variant_id, r.quantity) for r in existing} == {
                (line.variant_id, line.quantity) for line in lines
            }
            if same and all(r.status == ReservationStatus.HELD for r in existing):
                return ReserveResult(existing, created=False)
            raise ReservationConflict("Reservations for this order differ from the request")

        missing = [str(line.variant_id) for line in lines if line.variant_id not in items]
        if missing:
            raise UnknownVariant(missing)
        shortages = [
            Shortage(items[line.variant_id], line.quantity, items[line.variant_id].available)
            for line in lines
            if items[line.variant_id].available < line.quantity
        ]
        if shortages:
            return shortages

        expires_at = datetime.now(UTC) + timedelta(
            seconds=self.settings.inventory_reservation_ttl_seconds
        )
        created: list[Reservation] = []
        for line in lines:
            item = items[line.variant_id]
            before = item.on_hand - item.reserved
            item.reserved += line.quantity
            item.version += 1
            after = before - line.quantity
            reservation = Reservation(
                id=uuid.uuid4(),
                order_id=order_id,
                variant_id=line.variant_id,
                quantity=line.quantity,
                status=ReservationStatus.HELD,
                expires_at=expires_at,
            )
            self.session.add(reservation)
            created.append(reservation)
            ledger_repo.record(
                self.session,
                variant_id=line.variant_id,
                reason=StockMovementReason.RESERVE,
                delta_reserved=line.quantity,
                reference_type=ReferenceType.ORDER,
                reference_id=order_id,
                actor=actor,
                correlation_id=correlation_id,
            )
            events.reserved(self.session, item, reservation, after, correlation_id)
            trigger = stockout_trigger(before, after, item.low_stock_threshold)
            if trigger is not None:
                events.stockout(self.session, item, trigger, after, correlation_id=correlation_id)
        await self.session.flush()
        return ReserveResult(created, created=True)

    async def _refuse(
        self, order_id: uuid.UUID, shortages: list[Shortage], correlation_id: str | None
    ) -> NoReturn:
        """The stockout signal for a refused reserve, in its own short transaction.

        Written after the reserve rolled back: an outbox row inside a rolled-back
        transaction would vanish with it. There is no state change to pair it with.
        """
        for s in shortages:
            events.stockout(
                self.session,
                s.item,
                StockoutTrigger.RESERVATION_REFUSED,
                s.available,
                requested=s.requested,
                order_id=order_id,
                correlation_id=correlation_id,
            )
        await self.session.commit()
        raise InsufficientStock(
            [
                {
                    "variant_id": str(s.item.variant_id),
                    "requested": s.requested,
                    "available": s.available,
                }
                for s in shortages
            ]
        )

    async def _has_reservations(self, order_id: uuid.UUID) -> bool:
        found = await self.session.scalar(
            select(Reservation.id).where(Reservation.order_id == order_id).limit(1)
        )
        return found is not None

    async def _shortages(self, lines: list[Line], *, lock: bool) -> list[Shortage]:
        items = await self._items([line.variant_id for line in lines], lock=lock)
        return [
            Shortage(items[line.variant_id], line.quantity, items[line.variant_id].available)
            for line in lines
            if line.variant_id in items and items[line.variant_id].available < line.quantity
        ]

    async def _lock_items(self, variant_ids: list[uuid.UUID]) -> dict[uuid.UUID, StockItem]:
        return await self._items(variant_ids, lock=True)

    async def _items(
        self, variant_ids: list[uuid.UUID], *, lock: bool
    ) -> dict[uuid.UUID, StockItem]:
        stmt = (
            select(StockItem)
            .where(StockItem.variant_id.in_(variant_ids))
            .order_by(StockItem.variant_id)
            .execution_options(populate_existing=True)
        )
        if lock:
            stmt = stmt.with_for_update()
        return {item.variant_id: item for item in await self.session.scalars(stmt)}

    # --- single-reservation paths -------------------------------------------

    async def commit(
        self, reservation_id: uuid.UUID, *, actor: str, correlation_id: str | None = None
    ) -> Reservation:
        """held → committed: ``reserved -= q; on_hand -= q``. No event (INV-Q2)."""

        async def work() -> Reservation:
            item, reservation = await self._lock_reservation(reservation_id)
            if reservation.status == ReservationStatus.COMMITTED:
                return reservation
            if reservation.status != ReservationStatus.HELD:
                raise ReservationNotHeld(f"Reservation is {reservation.status}")
            transition(reservation, ReservationStatus.COMMITTED)
            q = reservation.quantity
            item.reserved -= q
            item.on_hand -= q
            item.version += 1
            ledger_repo.record(
                self.session,
                variant_id=item.variant_id,
                reason=StockMovementReason.COMMIT,
                delta_on_hand=-q,
                delta_reserved=-q,
                reference_type=ReferenceType.ORDER,
                reference_id=reservation.order_id,
                actor=actor,
                correlation_id=correlation_id,
            )
            await self.session.flush()
            return reservation

        return await in_locked_transaction(self.session, self.settings, work)

    async def release(
        self,
        reservation_id: uuid.UUID,
        *,
        reason: ReleaseReason = ReleaseReason.COMPENSATION,
        actor: str,
        correlation_id: str | None = None,
    ) -> Reservation:
        """held → released. Releasing a released or expired reservation is success."""

        async def work() -> Reservation:
            item, reservation = await self._lock_reservation(reservation_id)
            if reservation.status in (ReservationStatus.RELEASED, ReservationStatus.EXPIRED):
                return reservation
            if reservation.status == ReservationStatus.COMMITTED:
                raise ReservationCommitted(
                    "Post-commit stock goes back through reverse, not release"
                )
            transition(reservation, ReservationStatus.RELEASED)
            self._return_reserved(
                item, reservation, StockMovementReason.RELEASE, actor, correlation_id
            )
            events.released(
                self.session,
                item,
                reservation,
                item.on_hand - item.reserved,
                reason,
                correlation_id,
            )
            await self.session.flush()
            return reservation

        return await in_locked_transaction(self.session, self.settings, work)

    async def release_for_order(
        self,
        order_id: uuid.UUID,
        *,
        reason: ReleaseReason = ReleaseReason.ORDER_CANCELLED,
        actor: str,
        correlation_id: str | None = None,
    ) -> list[Reservation]:
        """Every ``held`` reservation of the order → ``released``, in the caller's
        transaction (the ``order.cancelled`` consumer, RFC 0006 §3.1.2.4).

        One transaction for the whole order, so the consumer's dedupe claim is
        atomic with the effect. Committed, released and expired reservations are
        left alone: normally the order workflow has released them already and
        this is a no-op safety net. Returns the reservations it released.
        """

        async def work() -> list[Reservation]:
            variant_ids = sorted(
                set(
                    await self.session.scalars(
                        select(Reservation.variant_id).where(
                            Reservation.order_id == order_id,
                            Reservation.status == ReservationStatus.HELD,
                        )
                    )
                )
            )
            if not variant_ids:
                return []
            items = await self._lock_items(variant_ids)  # stock rows first (INV-2)
            held = list(
                await self.session.scalars(
                    select(Reservation)
                    .where(
                        Reservation.order_id == order_id,
                        Reservation.status == ReservationStatus.HELD,
                    )
                    .order_by(Reservation.variant_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            )
            for reservation in held:
                item = items[reservation.variant_id]
                transition(reservation, ReservationStatus.RELEASED)
                self._return_reserved(
                    item, reservation, StockMovementReason.RELEASE, actor, correlation_id
                )
                events.released(
                    self.session,
                    item,
                    reservation,
                    item.on_hand - item.reserved,
                    reason,
                    correlation_id,
                )
            await self.session.flush()
            return held

        return await locked(self.session, self.settings, work)

    async def reverse(
        self, reservation_id: uuid.UUID, *, actor: str, correlation_id: str | None = None
    ) -> Reservation:
        """INV-16: a committed reservation's quantity back to ``on_hand``.

        The status stays ``committed`` (I8); the ``compensation`` ledger row is
        what makes a second reverse a no-op.
        """

        async def work() -> Reservation:
            item, reservation = await self._lock_reservation(reservation_id)
            if reservation.status != ReservationStatus.COMMITTED:
                raise ReservationNotCommitted(f"Reservation is {reservation.status}")
            if await self._reversed(reservation.id):
                return reservation
            item.on_hand += reservation.quantity
            item.version += 1
            ledger_repo.record(
                self.session,
                variant_id=item.variant_id,
                reason=StockMovementReason.COMPENSATION,
                delta_on_hand=reservation.quantity,
                reference_type=ReferenceType.RESERVATION,
                reference_id=reservation.id,
                actor=actor,
                correlation_id=correlation_id,
            )
            await self.session.flush()
            return reservation

        return await in_locked_transaction(self.session, self.settings, work)

    async def expire_due(self, now: datetime | None = None) -> int:
        """The sweeper (B5): a safety net beneath compensation, not a replacement.

        Candidates are read without a lock; each is re-checked under the
        canonical locks in its own short transaction, so a lost race (the
        workflow committed or released it first) is a no-op.
        """
        expired = 0
        while True:
            cutoff = now or datetime.now(UTC)
            candidates = list(
                await self.session.scalars(
                    select(Reservation.id)
                    .where(
                        Reservation.status == ReservationStatus.HELD,
                        Reservation.expires_at < cutoff,
                    )
                    .order_by(Reservation.expires_at)
                    .limit(self.settings.inventory_sweep_batch_size)
                )
            )
            await self.session.commit()
            for reservation_id in candidates:
                if await self._expire_one(reservation_id, cutoff):
                    expired += 1
            if len(candidates) < self.settings.inventory_sweep_batch_size:
                return expired

    async def _expire_one(self, reservation_id: uuid.UUID, cutoff: datetime) -> bool:
        async def work() -> bool:
            item, reservation = await self._lock_reservation(reservation_id)
            if reservation.status != ReservationStatus.HELD or reservation.expires_at >= cutoff:
                return False
            transition(reservation, ReservationStatus.EXPIRED)
            self._return_reserved(
                item, reservation, StockMovementReason.EXPIRE, SWEEPER_ACTOR, None
            )
            events.released(
                self.session,
                item,
                reservation,
                item.on_hand - item.reserved,
                ReleaseReason.EXPIRED,
            )
            await self.session.flush()
            return True

        return await in_locked_transaction(self.session, self.settings, work)

    def _return_reserved(
        self,
        item: StockItem,
        reservation: Reservation,
        reason: StockMovementReason,
        actor: str,
        correlation_id: str | None,
    ) -> None:
        item.reserved -= reservation.quantity
        item.version += 1
        ledger_repo.record(
            self.session,
            variant_id=item.variant_id,
            reason=reason,
            delta_reserved=-reservation.quantity,
            reference_type=ReferenceType.ORDER,
            reference_id=reservation.order_id,
            actor=actor,
            correlation_id=correlation_id,
        )

    async def _lock_reservation(self, reservation_id: uuid.UUID) -> tuple[StockItem, Reservation]:
        """Stock row first, then the reservation: the canonical order (INV-2)."""
        variant_id = await self.session.scalar(
            select(Reservation.variant_id).where(Reservation.id == reservation_id)
        )
        if variant_id is None:
            raise ReservationNotFound()
        items = await self._lock_items([variant_id])
        reservation = await self.session.scalar(
            select(Reservation)
            .where(Reservation.id == reservation_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        assert reservation is not None  # reservations are never deleted
        return items[variant_id], reservation

    async def _reversed(self, reservation_id: uuid.UUID) -> bool:
        found = await self.session.scalar(
            select(StockMovement.id)
            .where(
                StockMovement.reason == StockMovementReason.COMPENSATION,
                StockMovement.reference_type == ReferenceType.RESERVATION.value,
                StockMovement.reference_id == reservation_id,
            )
            .limit(1)
        )
        return found is not None
