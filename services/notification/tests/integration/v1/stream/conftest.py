"""Live replicas for the stream tests, and the consumer's write path as a helper."""

import httpx
import pytest

from app.v1.notifications.consumers.after_commit import after_commit
from app.v1.notifications.recording_repo import RecordingRepo
from smartretail_common.testing import truncate_all
from tests.integration.conftest import TABLES, close_app, confirmed, envelope, make_app
from tests.integration.v1.stream.sse import LiveServer


class Cluster:
    """Replicas of the service, started on demand, all stopped at teardown."""

    def __init__(self, settings, keypair) -> None:
        self.settings = settings
        self.keypair = keypair
        self.servers: list[LiveServer] = []
        self.clients: list[httpx.AsyncClient] = []

    async def replica(self) -> tuple[LiveServer, httpx.AsyncClient]:
        server = await LiveServer(make_app(self.settings, self.keypair)).start()
        client = httpx.AsyncClient(base_url=server.url, timeout=10)
        self.servers.append(server)
        self.clients.append(client)
        return server, client

    async def close(self) -> None:
        for client in self.clients:
            await client.aclose()
        for server in self.servers:
            await server.stop()
            await close_app(server.app)  # idempotent after the lifespan's own shutdown


@pytest.fixture
async def cluster(settings, keypair):
    c = Cluster(settings, keypair)
    yield c
    await c.close()
    await truncate_all(settings.database_url, TABLES)


async def consume(app, env) -> None:
    """What the consumer process does: record in one transaction, then the after-commit step."""
    async with app.state.db.sessionmaker() as session, session.begin():
        await RecordingRepo(session).record(env)

    async def no_dispatch(_):
        return None

    await after_commit(app.state.db.sessionmaker, app.state.redis, no_dispatch)(env)


async def notify(app, user_id: str, n: int = 1) -> None:
    for i in range(n):
        await consume(app, envelope("order.confirmed", confirmed(user_id, f"SR-L{i}")))
