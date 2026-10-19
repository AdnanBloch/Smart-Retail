import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from app.core.config import Settings
from app.v1.notifications.recording_repo import RecordingRepo
from main import create_application
from smartretail_common.auth import JwtVerifier
from smartretail_common.events.envelope import Envelope
from smartretail_common.testing import (
    provision_test_database,
    redis_reachable,
    test_redis_url,
    truncate_all,
)

SERVICE_DIR = str(Path(__file__).resolve().parents[2])
TABLES = [
    "delivery_attempts",
    "notifications",
    "stream_cursors",
    "push_subscriptions",
    "processed_events",
]


@pytest.fixture(scope="session")
def database_url() -> str:
    url = provision_test_database("notification_test", SERVICE_DIR)
    if url is None:
        pytest.skip("test Postgres not reachable; run `make test-db`")
    return url


@pytest.fixture(scope="session")
def redis_url() -> str:
    url = test_redis_url()
    if not redis_reachable(url):
        pytest.skip("test Redis not reachable; run `make test-db`")
    return url


@pytest.fixture
def settings(database_url, redis_url) -> Settings:
    return Settings(
        service_name="notification",
        database_url=database_url,
        redis_url=redis_url,
        jwt_public_key_path=None,
        db_pool_size=10,
        db_max_overflow=20,
        seq_visibility_window_seconds=0,
    )


def make_app(settings: Settings, keypair):
    verifier = JwtVerifier(
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
        static_keys={None: keypair.public_key},
    )
    return create_application(settings, verifier=verifier)


async def close_app(application) -> None:
    await application.state.hub.close()
    await application.state.redis.aclose()
    await application.state.db.dispose()


@pytest.fixture
async def app(settings, keypair):
    application = make_app(settings, keypair)
    yield application
    await close_app(application)
    await truncate_all(settings.database_url, TABLES)


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://notification") as c:
        yield c


# --- payloads, as the producers' Avro schemas define them -------------------


def registered(user_id: str, role: str = "customer") -> dict[str, Any]:
    return {
        "user_id": user_id,
        "role": role,
        "full_name": "Ada Lovelace",
        "registered_at": "2026-10-15T00:00:00Z",
        "merchant_status": None,
    }


def confirmed(customer_id: str, order_number: str = "SR-1001") -> dict[str, Any]:
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "order_number": order_number,
        "currency": "USD",
        "subtotal": "40.00",
        "shipping_fee": "2.00",
        "total": "42.00",
        "items": [],
        "payment_reference": "pay_1",
        "shipment_reference": None,
        "tracking_number": None,
        "placed_at": "2026-10-15T00:00:00Z",
        "confirmed_at": "2026-10-15T00:00:05Z",
    }


def failed(customer_id: str, reason: str | None = "PAYMENT_DECLINED") -> dict[str, Any]:
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "order_number": "SR-1002",
        "failure_reason": reason,
        "failed_step": "charging",
        "payment_status": "failed",
        "currency": "USD",
        "total": "10.00",
        "merchant_ids": [],
        "failed_at": "2026-10-15T00:00:00Z",
    }


def cancelled(customer_id: str) -> dict[str, Any]:
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "order_number": "SR-1003",
        "cancelled_by": "customer",
        "reason": None,
        "stage": "charging",
        "payment_status": "refunded",
        "reservations_released": True,
        "items": [],
        "cancelled_at": "2026-10-15T00:00:00Z",
    }


def shipment(customer_id: str, to_status: str) -> dict[str, Any]:
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "order_number": "SR-1004",
        "from_status": "created",
        "to_status": to_status,
        "shipment_reference": "shp_1",
        "tracking_number": "TRK1",
        "carrier_event_id": "evt_1",
        "occurred_at": "2026-10-15T00:00:00Z",
    }


def stockout(merchant_id: str) -> dict[str, Any]:
    return {
        "variant_id": str(uuid.uuid4()),
        "product_id": str(uuid.uuid4()),
        "merchant_id": merchant_id,
        "sku": "TEE-RED-M",
        "trigger": "depleted",
        "available": 0,
        "low_stock_threshold": 5,
        "requested": None,
        "order_id": None,
    }


def payment_succeeded(customer_id: str) -> dict[str, Any]:
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "payment_reference": "pay_1",
        "amount": "42.00",
        "currency": "USD",
        "succeeded_at": "2026-10-15T00:00:00Z",
    }


def payment_failed(customer_id: str) -> dict[str, Any]:
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "reason": "card_declined",
        "amount": "42.00",
        "currency": "USD",
        "failed_at": "2026-10-15T00:00:00Z",
    }


def envelope(
    event_type: str,
    payload: dict[str, Any],
    *,
    event_id: uuid.UUID | None = None,
    client_id: str | None = None,
    correlation_id: str = "corr-1",
) -> Envelope:
    return Envelope(
        event_id=event_id or uuid.uuid4(),
        event_type=event_type,
        event_version=1,
        occurred_at=datetime.now(UTC),
        producer="test",
        correlation_id=correlation_id,
        payload=payload,
        client_id=client_id,
    )


async def seed(app, user_id: str, count: int = 1, *, age_hours: float = 0) -> list[uuid.UUID]:
    """Records ``count`` order_confirmed notifications for ``user_id`` the way the
    consumer does (in_app + email rows); returns the in_app ids in seq order."""
    ids: list[uuid.UUID] = []
    async with app.state.db.sessionmaker() as session:
        for i in range(count):
            env = envelope("order.confirmed", confirmed(user_id, f"SR-{i}"))
            await RecordingRepo(session).record(env)
            ids.append(
                await session.scalar(
                    text(
                        "SELECT id FROM notifications WHERE payload->>'event_id' = :e "
                        "AND channel = 'in_app'"
                    ),
                    {"e": str(env.event_id)},
                )
            )
        if age_hours:
            await session.execute(
                text(
                    "UPDATE notifications SET created_at = now() - make_interval(secs => :s) "
                    "WHERE user_id = :u"
                ),
                {"s": age_hours * 3600, "u": user_id},
            )
        await session.commit()
    return ids


def as_json(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))
