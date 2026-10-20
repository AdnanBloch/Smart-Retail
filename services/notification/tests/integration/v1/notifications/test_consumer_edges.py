"""Consumer edge cases (RFC 0007 §3.1.2.4, C6, NTF-7): what reaches the DLQ, what is
ignored, and what the after-commit step does with rows it does not deliver.

The consumer fakes and fixtures are in this package's ``conftest.py``.
"""

import uuid

import pytest
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.consumer import GROUP
from app.v1.notifications.consumers import after_commit as after_commit_module
from app.v1.notifications.consumers.after_commit import after_commit
from app.v1.notifications.models import Notification
from app.v1.notifications.recording_repo import envelope_document
from smartretail_common.events import registry
from smartretail_common.events.consumer import Outcome
from smartretail_common.events.dedupe import ProcessedEvent
from tests.integration.conftest import confirmed, envelope, stockout
from tests.integration.v1.notifications.conftest import Enqueued, Msg, Source, count, deliver

CONFIRMED_TOPIC = "smartretail.order.confirmed"


def _headers(produced: dict) -> dict[str, str]:
    return {k: v.decode() for k, v in produced["headers"] if k.startswith("dlq.")}


@pytest.mark.parametrize(
    "value",
    [
        None,  # a tombstone on a delete-cleanup topic
        b"",
        b"\x00\x00",  # shorter than the 5-byte header
        b"\x01\x00\x00\x00\x01payload",  # wrong magic byte
    ],
)
async def test_undecodable_messages_are_dead_lettered_at_once(app, consumer, dlq, value) -> None:
    """Poison: no retry (attempts 0), offset committed, nothing recorded or claimed."""
    source = Source()
    msg = Msg(CONFIRMED_TOPIC, value)  # type: ignore[arg-type]
    assert await consumer.process(msg, source) == Outcome.DEAD_LETTERED
    assert source.committed == [msg]
    [parked] = dlq.produced
    assert parked["topic"] == f"{CONFIRMED_TOPIC}.dlq" and parked["value"] == value
    headers = _headers(parked)
    assert headers["dlq.attempts"] == "0" and headers["dlq.error.class"] == "SerializationError"
    assert headers["dlq.consumer_group"] == GROUP
    assert await count(app, Notification) == 0 and await count(app, ProcessedEvent) == 0


async def test_corrupt_avro_body_is_dead_lettered(app, consumer, dlq) -> None:
    good = await consumer.codec.encode(envelope("order.confirmed", confirmed(str(uuid.uuid4()))))
    corrupt = good[:5] + b"\xff" * 8  # a valid header, then garbage
    assert await consumer.process(Msg(CONFIRMED_TOPIC, corrupt), Source()) == (
        Outcome.DEAD_LETTERED
    )
    assert _headers(dlq.produced[0])["dlq.attempts"] == "0"


async def test_an_event_type_without_a_handler_is_ignored(app, consumer, dlq) -> None:
    """A type this group does not handle is skipped: not dead-lettered, not claimed."""
    event_type = "catalog.product.unpublished"
    spec = registry.catalogue().spec(event_type)
    consumer.codec.schemas.register_now(spec.subject, registry.message_schema_str(event_type))
    env = envelope(
        event_type,
        {
            "product_id": str(uuid.uuid4()),
            "merchant_id": str(uuid.uuid4()),
            "version": 3,
            "previous_status": "published",
            "new_status": "draft",
            "reason": "merchant_unpublished",
            "variant_ids": [],
            "unpublished_at": "2026-10-15T00:00:00Z",
        },
    )
    source = Source()
    msg = Msg(spec.topic, await consumer.codec.encode(env))
    assert await consumer.process(msg, source) == Outcome.IGNORED
    assert source.committed == [msg] and dlq.produced == []
    assert await count(app, ProcessedEvent) == 0 and await count(app, Notification) == 0


async def test_a_malformed_recipient_is_retried_then_dead_lettered(app, consumer, dlq) -> None:
    """C6: a non-uuid merchant_id is the message's fault — max_attempts, then the DLQ,
    with the error in the headers; nothing is recorded."""
    payload = stockout("merchant-without-a-uuid")
    env = envelope("inventory.stockout", payload)
    assert await deliver(consumer, env) == Outcome.DEAD_LETTERED
    [parked] = dlq.produced
    assert parked["topic"] == "smartretail.inventory.stockout.dlq"
    headers = _headers(parked)
    assert headers["dlq.attempts"] == str(consumer.max_attempts)
    assert headers["dlq.error.class"] == "ValueError"
    assert await count(app, Notification) == 0 and await count(app, ProcessedEvent) == 0
    # A later redelivery of the same bytes is still poison, not a DUPLICATE.
    assert await deliver(consumer, env) == Outcome.DEAD_LETTERED


async def test_a_failed_dead_letter_publish_leaves_the_offset_uncommitted(
    app, consumer, dlq
) -> None:
    """Never commit past a message that could not be parked: it is redelivered."""

    def broken_produce(topic, **kwargs):
        kwargs["on_delivery"]("broker down", None)

    dlq.produce = broken_produce  # type: ignore[method-assign]
    source = Source()
    with pytest.raises(RuntimeError, match="dead-letter publish failed"):
        await consumer.process(Msg(CONFIRMED_TOPIC, b"\x00"), source)
    assert source.committed == []


async def test_after_commit_skips_channels_without_an_adapter(app, settings) -> None:
    """``push`` has no adapter (NTF-D22): neither published nor enqueued."""
    user = uuid.uuid4()
    env = envelope("order.confirmed", confirmed(str(user)))
    async with app.state.db.sessionmaker() as session:
        await session.execute(
            pg_insert(Notification).values(
                user_id=user,
                type="order_confirmed",
                channel="push",
                title="t",
                body="b",
                payload=envelope_document(env),
                correlation_id="c",
                delivery_scope="all_devices",
            )
        )
        await session.commit()
    recorder = Enqueued()
    listener = app.state.redis.pubsub()
    await listener.subscribe(f"nt:user:{user}")
    assert (await listener.get_message(timeout=2.0))["type"] == "subscribe"  # confirmed
    try:
        await after_commit(app.state.db.sessionmaker, app.state.redis, recorder)(env)
        # Anything after_commit published would arrive before this sentinel.
        await app.state.redis.publish(f"nt:user:{user}", "sentinel")
        message = await listener.get_message(timeout=2.0)
    finally:
        await listener.aclose()
    assert message is not None and message["data"] == "sentinel"
    assert recorder.ids == []


async def test_celery_enqueue_sends_the_dispatch_task(monkeypatch) -> None:
    from app.core import celery_app

    sent: list[tuple] = []
    monkeypatch.setattr(
        celery_app.app, "send_task", lambda name, args: sent.append((name, args)) or None
    )
    notification_id = uuid.uuid4()
    await after_commit_module.celery_enqueue(notification_id)
    assert sent == [(celery_app.DISPATCH_TASK, [str(notification_id)])]
