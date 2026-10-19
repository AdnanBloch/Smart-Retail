"""Operator CLI (ANL-D20, ANL-D27): ``python -m app.v1.metrics.cli <command>``.

* ``indexes`` — create every collection, index and TTL (idempotent).
* ``backfill --projection <name> --since <date> [--until <datetime>]`` —
  replay a projection over ``raw_events`` (``backfill_repo``).

CLI only, never an HTTP endpoint: replaying events through an API is a footgun.
"""

import argparse
import json
import sys
from collections.abc import Sequence
from datetime import UTC, date, datetime

from app.core.config import Settings
from app.core.db import Mongo, create_mongo
from app.v1.metrics.backfill_repo import backfill
from app.v1.metrics.models import ensure_indexes, prepare_store
from app.v1.metrics.rollups import PROJECTIONS


def _moment(raw: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not an ISO date or datetime: {raw}") from None
    if len(raw) == 10:  # a bare date: its UTC midnight
        parsed = datetime.combine(date.fromisoformat(raw), datetime.min.time())
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="python -m app.v1.metrics.cli")
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("indexes", help="create collections, indexes and TTLs")
    run = commands.add_parser("backfill", help="replay a projection over raw_events")
    run.add_argument("--projection", required=True, choices=sorted(PROJECTIONS))
    run.add_argument("--since", required=True, type=_moment)
    run.add_argument("--until", type=_moment, default=None)
    return root


async def run(
    argv: Sequence[str], settings: Settings, mongo: Mongo | None = None
) -> dict[str, object]:
    args = parser().parse_args(list(argv))
    own = mongo is None
    mongo = mongo or create_mongo(settings)
    try:
        if args.command == "indexes":
            return {"command": "indexes", "created": await ensure_indexes(mongo, settings)}
        await prepare_store(mongo, settings)
        report = await backfill(
            mongo, settings, projection=args.projection, since=args.since, until=args.until
        )
        return {"command": "backfill", **report.__dict__}
    finally:
        if own:
            await mongo.close()


def main(argv: Sequence[str] | None = None) -> None:  # pragma: no cover - process wiring
    import asyncio

    from app.core.config import get_settings
    from app.core.logger import init_logger

    settings = get_settings()
    init_logger(settings)
    result = asyncio.run(run(sys.argv[1:] if argv is None else argv, settings))
    print(json.dumps(result, default=str))
