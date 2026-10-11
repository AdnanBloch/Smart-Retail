"""Settings, JSON logging, health endpoints and the Database wrapper."""

import asyncio
import json
import logging
import sys

import pytest
import sqlalchemy
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from smartretail_common import testing
from smartretail_common.config import BaseServiceSettings, DatabaseSettings
from smartretail_common.context import set_request_id, set_user_id
from smartretail_common.db import Database, get_session
from smartretail_common.health import database_check, health_router
from smartretail_common.logging import JsonFormatter, configure_logging


def test_settings_read_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SERVICE_NAME", "catalog")
    monkeypatch.setenv("PORT", "8100")
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@h/db")
    settings = BaseServiceSettings()  # type: ignore[call-arg]
    assert settings.service_name == "catalog"
    assert settings.port == 8100
    assert settings.jwt_issuer == "smartretail-identity"
    assert DatabaseSettings().database_url.endswith("/db")  # type: ignore[call-arg]


def test_missing_required_setting_fails_at_start_up(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SERVICE_NAME", raising=False)
    with pytest.raises(ValueError, match="service_name"):
        BaseServiceSettings()  # type: ignore[call-arg]


def _record(msg: str, **extra: object) -> logging.LogRecord:
    record = logging.makeLogRecord({"name": "test", "levelname": "INFO", "msg": msg})
    record.__dict__.update(extra)
    return record


def test_json_formatter_emits_one_object_with_context() -> None:
    set_request_id("req-1")
    set_user_id("user-1")
    try:
        line = JsonFormatter("catalog").format(_record("hello %s", order_id="o-1", _private=1))
    finally:
        set_user_id(None)
    payload = json.loads(line)
    assert payload["service"] == "catalog"
    assert payload["msg"] == "hello %s"
    assert payload["request_id"] == "req-1"
    assert payload["user_id"] == "user-1"
    assert payload["order_id"] == "o-1"
    assert "_private" not in payload


def test_json_formatter_includes_exception() -> None:
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        record = _record("failed")
        record.exc_info = sys.exc_info()
    payload = json.loads(JsonFormatter("svc").format(record))
    assert payload["exc_type"] == "RuntimeError"
    assert payload["exc_message"] == "boom"
    assert "Traceback" in payload["stack"]


def test_configure_logging_routes_uvicorn_through_root() -> None:
    root = logging.getLogger()
    saved = (root.handlers[:], root.level)
    try:
        configure_logging("svc", "debug")
        assert root.level == logging.DEBUG
        assert isinstance(root.handlers[0].formatter, JsonFormatter)
        uvicorn = logging.getLogger("uvicorn.access")
        assert uvicorn.handlers == []
        assert uvicorn.propagate
    finally:
        root.handlers[:], _ = saved
        root.setLevel(saved[1])


async def _ok() -> bool:
    return True


async def _broken() -> bool:
    raise ConnectionError("down")


async def _slow() -> bool:
    await asyncio.sleep(1)
    return True


async def _get(app: FastAPI, path: str) -> tuple[int, dict]:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as client:
        response = await client.get(path)
    return response.status_code, response.json()


async def test_health_is_always_ok() -> None:
    app = FastAPI()
    app.include_router(health_router({"db": _broken}))
    assert await _get(app, "/health") == (200, {"status": "ok"})


async def test_ready_when_every_check_passes() -> None:
    app = FastAPI()
    app.include_router(health_router({"db": _ok, "cache": _ok}))
    status, body = await _get(app, "/health/ready")
    assert status == 200
    assert body == {"status": "ready", "checks": {"db": "ok", "cache": "ok"}}


async def test_not_ready_when_a_check_fails_or_times_out() -> None:
    app = FastAPI()
    app.include_router(health_router({"db": _ok, "kafka": _broken, "slow": _slow}, timeout=0.05))
    status, body = await _get(app, "/health/ready")
    assert status == 503
    assert body["status"] == "not_ready"
    assert body["checks"] == {"db": "ok", "kafka": "failed", "slow": "failed"}


async def test_database_is_lazy_and_disposable() -> None:
    db = Database("postgresql+asyncpg://u:p@127.0.0.1:1/none", pool_size=2, max_overflow=1)
    await db.dispose()  # nothing created yet
    assert db._engine is None
    engine = db.engine
    assert db.engine is engine
    assert db.sessionmaker is db.sessionmaker
    await db.dispose()
    assert db._engine is None
    assert db._sessionmaker is None


@pytest.fixture
async def live_db():
    url = testing.test_admin_url().replace("postgresql://", "postgresql+asyncpg://", 1)
    db = Database(url)
    try:
        await db.ping()
    except OSError:
        await db.dispose()
        pytest.skip("test Postgres not reachable; run `make test-db`")
    yield db
    await db.dispose()


async def test_ping_and_session_against_postgres(live_db: Database) -> None:
    assert await database_check(live_db)()

    class _Request:
        class app:  # mimics request.app.state.db
            class state:
                db = live_db

    sessions = get_session(_Request())  # type: ignore[arg-type]
    session = await anext(sessions)
    assert (await session.execute(sqlalchemy.text("SELECT 1"))).scalar() == 1
    await sessions.aclose()
