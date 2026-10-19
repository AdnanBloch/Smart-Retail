"""``identity.user.registered`` (IDN-T4): written in the registration
transaction, valid against its Avro schema, carrying the device's client id."""

from sqlalchemy import select

from smartretail_common.outbox import OutboxEvent

PASSWORD = "correct horse battery staple"


async def outbox(app) -> list[OutboxEvent]:
    async with app.state.db.sessionmaker() as session:
        return list(await session.scalars(select(OutboxEvent).order_by(OutboxEvent.id)))


async def test_a_customer_registration_writes_one_event(app, client) -> None:
    r = await client.post(
        "/api/v1/auth/register",
        json={"email": "ev-c@example.com", "password": PASSWORD, "full_name": "Eve Customer"},
        headers={"X-Client-Id": "web-tab-0001"},
    )
    assert r.status_code == 201, r.text
    [row] = await outbox(app)
    assert row.event_type == "identity.user.registered"
    assert str(row.aggregate_id) == r.json()["id"]
    assert row.client_id == "web-tab-0001"
    assert row.payload["role"] == "customer" and row.payload["merchant_status"] is None
    assert row.payload["full_name"] == "Eve Customer"


async def test_a_merchant_starts_pending_and_a_bad_client_id_is_dropped(app, client) -> None:
    r = await client.post(
        "/api/v1/auth/register",
        json={
            "email": "ev-m@example.com",
            "password": PASSWORD,
            "full_name": "Max Merchant",
            "role": "merchant",
            "business_name": "Max Shop",
        },
        headers={"X-Client-Id": "bad id!"},
    )
    assert r.status_code == 201, r.text
    [row] = await outbox(app)
    assert row.payload["merchant_status"] == "pending"
    assert row.client_id is None


async def test_a_rejected_registration_writes_no_event(app, client) -> None:
    body = {"email": "ev-dup@example.com", "password": PASSWORD, "full_name": "Dup"}
    assert (await client.post("/api/v1/auth/register", json=body)).status_code == 201
    assert (await client.post("/api/v1/auth/register", json=body)).status_code == 409
    assert len(await outbox(app)) == 1
