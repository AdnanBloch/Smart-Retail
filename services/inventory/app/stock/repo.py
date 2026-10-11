"""Stock rows: initialise, read, set, adjust, ledger reads (RFC 0006 §3.1.2.1).

Every quantity change takes the row lock through ``in_locked_transaction``
and writes its ledger row in the same transaction (I6).
"""

import uuid
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import ColumnElement, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.errors import StockBelowReserved, StockItemNotFound, VersionMismatch
from app.stock import access, ledger_repo
from app.stock.enums import AdjustReason, ReferenceType, StockMovementReason
from app.stock.locking import in_locked_transaction
from app.stock.models import StockItem, StockMovement
from smartretail_common.auth import Principal
from smartretail_common.pagination import PageParams, Slice

# Namespace for turning an Idempotency-Key into the ledger's uuid reference (INV-11).
ADJUSTMENT_KEY_NAMESPACE = uuid.UUID("7d0c8a3e-6a55-4f4e-9d0b-3a1c2f5e9b10")


@dataclass(frozen=True)
class VariantRef:
    variant_id: uuid.UUID
    sku: str


@dataclass(frozen=True)
class StockFilters:
    low_stock: bool = False
    out_of_stock: bool = False
    product_id: uuid.UUID | None = None
    merchant_id: uuid.UUID | None = None


@dataclass(frozen=True)
class MovementFilters:
    reason: StockMovementReason | None = None
    since: datetime | None = None
    until: datetime | None = None


def adjustment_reference(principal: Principal, variant_id: uuid.UUID, key: str) -> uuid.UUID:
    return uuid.uuid5(ADJUSTMENT_KEY_NAMESPACE, f"{principal.subject}:{variant_id}:{key}")


async def lock_item(session: AsyncSession, variant_id: uuid.UUID) -> StockItem:
    item = await session.scalar(
        select(StockItem)
        .where(StockItem.variant_id == variant_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if item is None:
        raise StockItemNotFound()
    return item


class StockRepo:
    def __init__(self, session: AsyncSession, settings: Settings) -> None:
        self.session = session
        self.settings = settings

    async def initialise(
        self,
        product_id: uuid.UUID,
        merchant_id: uuid.UUID,
        variants: list[VariantRef],
        *,
        actor: str,
    ) -> list[StockItem]:
        """INV-4: zero-quantity rows per variant; existing rows are never touched."""
        threshold = self.settings.inventory_default_low_stock_threshold
        if variants:
            inserted = await self.session.scalars(
                pg_insert(StockItem)
                .values(
                    [
                        {
                            "id": uuid.uuid4(),
                            "variant_id": v.variant_id,
                            "product_id": product_id,
                            "merchant_id": merchant_id,
                            "sku": v.sku,
                            "low_stock_threshold": threshold,
                        }
                        for v in variants
                    ]
                )
                .on_conflict_do_nothing(index_elements=["variant_id"])
                .returning(StockItem.variant_id)
            )
            for variant_id in inserted:
                ledger_repo.record(
                    self.session,
                    variant_id=variant_id,
                    reason=StockMovementReason.INITIAL,
                    reference_type=ReferenceType.PRODUCT,
                    reference_id=product_id,
                    actor=actor,
                )
        await self.session.commit()
        rows = await self.session.scalars(
            select(StockItem)
            .where(StockItem.variant_id.in_([v.variant_id for v in variants]))
            .order_by(StockItem.variant_id)
        )
        return list(rows)

    async def get(self, variant_id: uuid.UUID) -> StockItem:
        item = await self.session.scalar(
            select(StockItem).where(StockItem.variant_id == variant_id)
        )
        if item is None:
            raise StockItemNotFound()
        return item

    async def list(
        self, principal: Principal, filters: StockFilters, page: PageParams
    ) -> Slice[StockItem]:
        conditions: list[ColumnElement[bool]] = []
        if access.is_admin(principal):
            if filters.merchant_id is not None:
                conditions.append(StockItem.merchant_id == filters.merchant_id)
        else:
            # A merchant's scope comes from the token; a merchant_id filter is ignored.
            conditions.append(StockItem.merchant_id == access.caller_merchant_id(principal))
        if filters.product_id is not None:
            conditions.append(StockItem.product_id == filters.product_id)
        if filters.low_stock:
            conditions.append(StockItem.available <= StockItem.low_stock_threshold)
        if filters.out_of_stock:
            conditions.append(StockItem.available == 0)
        total = await self.session.scalar(
            select(func.count()).select_from(StockItem).where(*conditions)
        )
        rows = await self.session.scalars(
            select(StockItem)
            .where(*conditions)
            .order_by(StockItem.sku, StockItem.variant_id)
            .limit(page.limit)
            .offset(page.offset)
        )
        return Slice(items=list(rows), total=total or 0, params=page)

    async def set_on_hand(
        self,
        principal: Principal,
        variant_id: uuid.UUID,
        *,
        on_hand: int,
        low_stock_threshold: int | None,
        if_match: int | None,
    ) -> StockItem:
        """Absolute set. Safe under concurrency only with ``If-Match`` (INV-9)."""

        async def work() -> StockItem:
            item = await lock_item(self.session, variant_id)
            access.ensure_can_manage(principal, item)
            if if_match is not None and if_match != item.version:
                raise VersionMismatch(item.version)
            if on_hand < item.reserved:
                raise StockBelowReserved(f"{item.reserved} units are reserved")
            delta = on_hand - item.on_hand
            changed = delta != 0
            if low_stock_threshold is not None and low_stock_threshold != item.low_stock_threshold:
                item.low_stock_threshold = low_stock_threshold
                changed = True
            if delta:
                item.on_hand = on_hand
                ledger_repo.record(
                    self.session,
                    variant_id=variant_id,
                    reason=StockMovementReason.MANUAL_ADJUSTMENT,
                    delta_on_hand=delta,
                    actor=principal.subject,
                )
            if changed:
                item.version += 1
                await self.session.flush()
            return item

        return await in_locked_transaction(self.session, self.settings, work)

    async def adjust(
        self,
        principal: Principal,
        variant_id: uuid.UUID,
        *,
        delta: int,
        reason: AdjustReason,
        idempotency_key: str | None,
        reference_id: uuid.UUID | None,
    ) -> StockItem:
        """Relative change, applied to whatever the row holds at lock time.

        With an ``Idempotency-Key`` a replay returns the row without a second
        ledger row (INV-11): the key is folded into the ledger reference.
        """
        key_ref = (
            adjustment_reference(principal, variant_id, idempotency_key)
            if idempotency_key
            else None
        )

        async def work() -> StockItem:
            item = await lock_item(self.session, variant_id)
            access.ensure_can_manage(principal, item)
            if key_ref is not None and await self._has_movement(variant_id, key_ref):
                return item
            if item.on_hand + delta < item.reserved:
                raise StockBelowReserved(
                    f"on_hand {item.on_hand} {delta:+d} would drop below {item.reserved} reserved"
                )
            item.on_hand += delta
            item.version += 1
            ref_type: ReferenceType | None = None
            ref_id: uuid.UUID | None = None
            if key_ref is not None:
                ref_type, ref_id = ReferenceType.ADJUSTMENT, key_ref
            elif reference_id is not None:
                is_return = reason == AdjustReason.RETURN
                ref_type = ReferenceType.ORDER if is_return else ReferenceType.ADJUSTMENT
                ref_id = reference_id
            ledger_repo.record(
                self.session,
                variant_id=variant_id,
                reason=StockMovementReason(reason.value),
                delta_on_hand=delta,
                reference_type=ref_type,
                reference_id=ref_id,
                actor=principal.subject,
            )
            await self.session.flush()
            return item

        return await in_locked_transaction(self.session, self.settings, work)

    async def movements(
        self,
        principal: Principal,
        variant_id: uuid.UUID,
        filters: MovementFilters,
        page: PageParams,
    ) -> Slice[StockMovement]:
        item = await self.get(variant_id)
        access.ensure_can_manage(principal, item)
        conditions: list[ColumnElement[bool]] = [StockMovement.variant_id == variant_id]
        if filters.reason is not None:
            conditions.append(StockMovement.reason == filters.reason)
        if filters.since is not None:
            conditions.append(StockMovement.created_at >= filters.since)
        if filters.until is not None:
            conditions.append(StockMovement.created_at < filters.until)
        total = await self.session.scalar(
            select(func.count()).select_from(StockMovement).where(*conditions)
        )
        rows = await self.session.scalars(
            select(StockMovement)
            .where(*conditions)
            .order_by(StockMovement.created_at.desc(), StockMovement.id.desc())
            .limit(page.limit)
            .offset(page.offset)
        )
        return Slice(items=list(rows), total=total or 0, params=page)

    async def _has_movement(self, variant_id: uuid.UUID, reference_id: uuid.UUID) -> bool:
        found = await self.session.scalar(
            select(StockMovement.id)
            .where(
                StockMovement.variant_id == variant_id,
                StockMovement.reference_type == ReferenceType.ADJUSTMENT.value,
                StockMovement.reference_id == reference_id,
            )
            .limit(1)
        )
        return found is not None
