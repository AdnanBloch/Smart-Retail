"""The MongoDB client, the transaction helper and the replica-set check (ANL-1).

``analytics`` uses PyMongo's native asyncio client (``AsyncMongoClient``,
ANL-D28); Motor, which RFC 0008 originally named, is end-of-life.
"""

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, TypeVar

from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import ConnectionFailure, PyMongoError
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from app.core.config import Settings
from app.core.errors import MongoUnavailable, TransactionsUnavailable

T = TypeVar("T")
Document = dict[str, Any]
Session = AsyncClientSession


@dataclass
class Mongo:
    client: AsyncMongoClient[Document]
    db: AsyncDatabase[Document]

    async def close(self) -> None:
        await self.client.close()

    async def ping(self) -> bool:
        async with mongo_errors():
            reply = await self.client.admin.command("hello")
        # Ready only when this node can take the consumer's transactions.
        return bool(reply.get("isWritablePrimary")) and bool(reply.get("setName"))


def create_mongo(settings: Settings, *, uri: str | None = None, db: str | None = None) -> Mongo:
    client: AsyncMongoClient[Document] = AsyncMongoClient(
        uri or settings.analytics_mongo_uri,
        tz_aware=True,
        serverSelectionTimeoutMS=settings.analytics_mongo_timeout_ms,
        connectTimeoutMS=settings.analytics_mongo_timeout_ms,
        appname=settings.service_name,
    )
    return Mongo(client=client, db=client[db or settings.analytics_mongo_db])


def is_unavailable(exc: BaseException) -> bool:
    """A store that cannot be reached, as opposed to a request it refused."""
    if isinstance(exc, ConnectionFailure):
        return True
    # with_transaction gave up retrying a transient failure (failover, write
    # conflict storm): the store is not healthy, the message is fine.
    return isinstance(exc, PyMongoError) and exc.has_error_label("TransientTransactionError")


@asynccontextmanager
async def mongo_errors() -> AsyncIterator[None]:
    """Maps an unreachable Mongo to ``MongoUnavailable`` (503 / consumer waits)."""
    try:
        yield
    except PyMongoError as exc:
        if is_unavailable(exc):
            raise MongoUnavailable(f"MongoDB unavailable: {type(exc).__name__}") from exc
        raise


async def require_replica_set(mongo: Mongo) -> None:
    """Refuses a stand-alone ``mongod`` (ANL-1, ANL-D6): without a replica set
    there are no transactions, and without transactions every ``$inc`` is
    either lost or doubled under a crash."""
    async with mongo_errors():
        reply = await mongo.client.admin.command("hello")
    if not reply.get("setName"):
        raise TransactionsUnavailable(
            "MongoDB is not a replica set; analytics needs transactions (ANL-1). "
            "Start mongod with --replSet and run rs.initiate()."
        )


TRANSACTION_ATTEMPTS = 5


async def in_transaction(mongo: Mongo, work: Callable[[Session], Awaitable[T]]) -> T:
    """Runs ``work`` in one snapshot/majority transaction.

    A transient error (a write conflict with another replica) reruns the
    whole callback, at most ``TRANSACTION_ATTEMPTS`` times. An unreachable
    server is *not* retried here: it surfaces at once as ``MongoUnavailable``
    so the caller decides how to wait (the driver's ``with_transaction``
    would block for two minutes on it)."""
    async with mongo_errors(), mongo.client.start_session() as session:
        for attempt in range(1, TRANSACTION_ATTEMPTS + 1):
            try:
                async with await session.start_transaction(
                    read_concern=ReadConcern("snapshot"),
                    write_concern=WriteConcern("majority"),
                ):
                    result = await work(session)
                return result
            except PyMongoError as exc:
                retry = (
                    not isinstance(exc, ConnectionFailure)
                    and exc.has_error_label("TransientTransactionError")
                    and attempt < TRANSACTION_ATTEMPTS
                )
                if not retry:
                    raise
        raise AssertionError("unreachable")  # pragma: no cover
