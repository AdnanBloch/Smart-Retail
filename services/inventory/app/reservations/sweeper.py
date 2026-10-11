"""In-process driver for the reservation expiry sweeper (RFC 0006 INV-10).

Used until Celery Beat exists (Milestone 3) and as its fallback when the
scheduler is down. It calls the same ``ReservationRepo.expire_due()`` the
Beat task will, and running both at once is safe for the same reason two
Celery workers are: every expiry is re-checked under its row locks.
"""

import asyncio
import logging

from app.core.config import Settings
from app.reservations.repo import ReservationRepo
from smartretail_common.db import Database

logger = logging.getLogger("inventory.sweeper")


async def sweep_once(db: Database, settings: Settings) -> int:
    async with db.sessionmaker() as session:
        expired = await ReservationRepo(session, settings).expire_due()
    if expired:
        logger.info("expired reservations", extra={"count": expired})
    return expired


async def run_forever(db: Database, settings: Settings) -> None:
    while True:
        try:
            await sweep_once(db, settings)
        except Exception:  # a failed sweep is retried on the next tick
            logger.exception("reservation sweep failed")
        await asyncio.sleep(settings.inventory_sweeper_interval_seconds)
