"""``locked``: the consumer's variant of the lock discipline maps errors but
never commits or retries — the consumer base owns both (RFC 0001 PLT-5)."""

import pytest
from sqlalchemy.exc import DBAPIError

from app.core.config import Settings
from app.core.errors import InventoryContended, StockConstraintViolated
from app.v1.stock.locking import locked
from smartretail_common.errors import DependencyUnavailable


class Orig(Exception):
    def __init__(self, sqlstate: str) -> None:
        super().__init__(sqlstate)
        self.sqlstate = sqlstate


class Session:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.commits = 0

    async def execute(self, statement: object) -> None:
        self.statements.append(str(statement))

    async def commit(self) -> None:
        self.commits += 1


def failing(sqlstate: str):
    async def work() -> None:
        raise DBAPIError("stmt", {}, Orig(sqlstate))

    return work


SETTINGS = Settings(service_name="inventory", database_url="postgresql+asyncpg://x/y")


async def test_work_runs_under_the_timeouts_without_a_commit() -> None:
    session = Session()

    async def work() -> str:
        return "done"

    assert await locked(session, SETTINGS, work) == "done"  # type: ignore[arg-type]
    assert any("lock_timeout" in s for s in session.statements)
    assert session.commits == 0


@pytest.mark.parametrize("sqlstate", ["55P03", "40P01", "40001"])
async def test_contention_is_a_dependency_wait_not_a_failure(sqlstate: str) -> None:
    with pytest.raises(InventoryContended) as caught:
        await locked(Session(), SETTINGS, failing(sqlstate))  # type: ignore[arg-type]
    assert isinstance(caught.value, DependencyUnavailable)


async def test_the_oversell_check_maps_to_its_domain_error() -> None:
    with pytest.raises(StockConstraintViolated):
        await locked(Session(), SETTINGS, failing("23514"))  # type: ignore[arg-type]


async def test_any_other_database_error_propagates() -> None:
    with pytest.raises(DBAPIError):
        await locked(Session(), SETTINGS, failing("42P01"))  # type: ignore[arg-type]
