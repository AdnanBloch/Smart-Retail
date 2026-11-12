"""``reconcile_inventory_ledger`` (RFC 0006 §3.1.1.4, INV-D23, INV-D32).

``stock_items.reserved`` is a stored counter that sits beside the
``reservations`` rows it summarises (INV-D32). Every transition updates both
in one locked transaction, so invariant I5 holds by construction; this check
is the evidence. It compares, per variant:

- I6: ``on_hand = Σ delta_on_hand`` and ``reserved = Σ delta_reserved``;
- I5: ``reserved = Σ quantity of held reservations``.

It **detects, never corrects**: a fix is a ``manual_adjustment`` a human makes,
so the ledger stays the record (INV-D23).
"""

import logging
import uuid
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from smartretail_common.db import Database

logger = logging.getLogger("inventory.reconcile")

DRIFT_SQL = text(
    """
    SELECT s.variant_id, s.on_hand, s.reserved,
           coalesce(m.oh, 0) AS ledger_on_hand,
           coalesce(m.rs, 0) AS ledger_reserved,
           coalesce(r.held, 0) AS held
    FROM stock_items s
    LEFT JOIN (SELECT variant_id, sum(delta_on_hand) AS oh, sum(delta_reserved) AS rs
               FROM stock_movements GROUP BY variant_id) m USING (variant_id)
    LEFT JOIN (SELECT variant_id, sum(quantity) AS held FROM reservations
               WHERE status = 'held' GROUP BY variant_id) r USING (variant_id)
    WHERE s.on_hand <> coalesce(m.oh, 0)
       OR s.reserved <> coalesce(m.rs, 0)
       OR s.reserved <> coalesce(r.held, 0)
    ORDER BY s.variant_id
    """
)


@dataclass(frozen=True)
class Drift:
    variant_id: uuid.UUID
    on_hand: int
    reserved: int
    ledger_on_hand: int
    ledger_reserved: int
    held: int


async def find_drift(session: AsyncSession) -> list[Drift]:
    """Every variant whose stored quantities disagree with the ledger or the
    held reservations. Read-only."""
    rows = (await session.execute(DRIFT_SQL)).mappings().all()
    return [
        Drift(
            variant_id=row["variant_id"],
            on_hand=int(row["on_hand"]),
            reserved=int(row["reserved"]),
            ledger_on_hand=int(row["ledger_on_hand"]),
            ledger_reserved=int(row["ledger_reserved"]),
            held=int(row["held"]),
        )
        for row in rows
    ]


async def reconcile_once(db: Database) -> list[Drift]:
    async with db.sessionmaker() as session:
        drift = await find_drift(session)
    for d in drift:
        logger.warning(
            "inventory ledger drift",
            extra={
                "variant_id": str(d.variant_id),
                "on_hand": d.on_hand,
                "reserved": d.reserved,
                "ledger_on_hand": d.ledger_on_hand,
                "ledger_reserved": d.ledger_reserved,
                "held": d.held,
            },
        )
    logger.info("ledger reconciled", extra={"inventory_ledger_drift_variants": len(drift)})
    return drift
