"""The Celery tasks, run the way a worker runs them: synchronously, each on its
own event loop (here, in a thread, as a prefork child would). NTF-5, NTF-6."""

import asyncio
import uuid

import pytest
from sqlalchemy import select, text

from app.core.celery_app import app as celery_app
from app.core.config import get_settings
from app.core.tasks import sweep_processed_events
from app.v1.notifications.adapters.base import Sent
from app.v1.notifications.dispatch_repo import DispatchOutcome, DispatchRepo
from app.v1.notifications.enums import NotificationChannel
from app.v1.notifications.models import DeliveryAttempt, Notification
from app.v1.notifications.tasks import dispatch as dispatch_module
from app.v1.notifications.tasks.dispatch import (
    dispatch_notification,
    redispatch_undispatched_notifications,
)
from app.v1.stream.models import StreamCursor
from app.v1.stream.tasks.maintenance import sweep_stale_stream_cursors
from tests.integration.conftest import seed


@pytest.fixture
def task_env(monkeypatch, database_url):
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("JWT_PUBLIC_KEY_PATH", "")
    monkeypatch.setenv("DISPATCH_BACKOFF_BASE_SECONDS", "0")
    get_settings.cache_clear()
    yield monkeypatch
    get_settings.cache_clear()


def run(task, *args):
    return asyncio.to_thread(lambda: task.apply(args=args).get())


async def email_row(app, user_id: str) -> uuid.UUID:
    async with app.state.db.sessionmaker() as session:
        found = await session.scalar(
            select(Notification.id).where(
                Notification.user_id == uuid.UUID(user_id), Notification.channel == "email"
            )
        )
    assert found is not None
    return found


async def attempts(app, notification_id) -> list[tuple[int, str, str | None]]:
    async with app.state.db.sessionmaker() as session:
        rows = await session.scalars(
            select(DeliveryAttempt)
            .where(DeliveryAttempt.notification_id == notification_id)
            .order_by(DeliveryAttempt.attempt)
        )
        return [(a.attempt, str(a.status), a.error) for a in rows]


def test_tasks_are_routed_to_the_one_notification_queue() -> None:
    conf = celery_app.conf
    assert conf.task_default_queue == "notification.notifications"
    assert conf.task_acks_late and conf.task_reject_on_worker_lost
    schedule = {e["task"] for e in conf.beat_schedule.values()}
    assert schedule == {
        "smartretail.notification.redispatch_undispatched_notifications",
        "smartretail.notification.sweep_processed_events",
        "smartretail.notification.sweep_stale_stream_cursors",
    }
    assert dispatch_notification.name == "smartretail.notification.dispatch_notification"


async def test_dispatch_sends_once_and_is_idempotent(app, callers, task_env) -> None:
    await seed(app, callers.customer_id, 1)
    email = await email_row(app, callers.customer_id)
    assert await run(dispatch_notification, str(email)) == "sent"
    assert await attempts(app, email) == [(1, "sent", None)]
    # A redelivered or re-enqueued task finds the sent attempt.
    assert await run(dispatch_notification, str(email)) == "already_sent"
    assert await attempts(app, email) == [(1, "sent", None)]


async def test_dispatch_retries_with_backoff_until_exhausted(app, callers, task_env) -> None:
    task_env.setenv("NOTIFICATION_FAKE_EMAIL_FAILURE_RATE", "1.0")
    get_settings.cache_clear()
    await seed(app, callers.customer_id, 1)
    email = await email_row(app, callers.customer_id)
    assert await run(dispatch_notification, str(email)) == "exhausted"
    history = await attempts(app, email)
    assert [(n, s) for n, s, _ in history] == [(i, "failed") for i in range(1, 6)]
    assert history[0][2] == "fake email provider failure"
    # The fifth failure was the last row written; the admin view shows it as failed.
    assert await run(dispatch_notification, str(email)) == "exhausted"
    assert len(await attempts(app, email)) == 5


async def test_dispatch_recovers_after_a_transient_failure(app, callers, task_env) -> None:
    results = iter([False, False, True])

    class Flaky:
        channel = NotificationChannel.EMAIL

        async def send(self, view):
            if next(results):
                return Sent("ok")
            raise RuntimeError("smtp timeout")

    task_env.setattr(dispatch_module, "registry", lambda s: {NotificationChannel.EMAIL: Flaky()})
    await seed(app, callers.customer_id, 1)
    email = await email_row(app, callers.customer_id)
    assert await run(dispatch_notification, str(email)) == "sent"
    history = await attempts(app, email)
    assert [(n, s) for n, s, _ in history] == [(1, "failed"), (2, "failed"), (3, "sent")]
    assert history[0][2] == "RuntimeError: smtp timeout"


async def test_dispatch_skips_in_app_unknown_and_unregistered_rows(app, callers, task_env) -> None:
    [in_app] = await seed(app, callers.customer_id, 1)
    assert await run(dispatch_notification, str(in_app)) == "skipped"
    assert await run(dispatch_notification, str(uuid.uuid4())) == "skipped"
    email = await email_row(app, callers.customer_id)
    async with app.state.db.sessionmaker() as session:
        result = await DispatchRepo(session).dispatch_once(email, {}, max_attempts=5)
    assert result.outcome == DispatchOutcome.SKIPPED
    assert await attempts(app, in_app) == []


async def test_redispatch_sweep_recovers_orphans_and_crashed_attempts(
    app, callers, task_env
) -> None:
    """NTF-6: a crash between commit and enqueue leaves an email row with no
    attempt; a crashed worker leaves a stale pending one."""
    await seed(app, callers.customer_id, 1)
    await seed(app, callers.other_id, 1)
    await seed(app, callers.merchant_id, 1)
    orphan = await email_row(app, callers.customer_id)
    crashed = await email_row(app, callers.other_id)
    fresh_sent = await email_row(app, callers.merchant_id)
    async with app.state.db.engine.begin() as conn:
        await conn.execute(
            text("UPDATE notifications SET created_at = now() - interval '10 minutes'")
        )
        await conn.execute(
            text(
                "INSERT INTO delivery_attempts"
                " (notification_id, channel, status, attempt, created_at)"
                " VALUES (:c, 'email', 'pending', 1, now() - interval '5 minutes'),"
                "        (:s, 'email', 'sent', 1, now())"
            ),
            {"c": crashed, "s": fresh_sent},
        )

    sent: list[str] = []
    task_env.setattr(dispatch_notification, "delay", lambda nid: sent.append(nid))
    assert await run(redispatch_undispatched_notifications) == 2
    assert sorted(sent) == sorted([str(orphan), str(crashed)])

    # Too young (inside the 2-minute grace) or too old (past 24 h): left alone.
    async with app.state.db.engine.begin() as conn:
        await conn.execute(text("UPDATE notifications SET created_at = now()"))
    sent.clear()
    assert await run(redispatch_undispatched_notifications) == 0
    async with app.state.db.engine.begin() as conn:
        await conn.execute(text("UPDATE notifications SET created_at = now() - interval '2 days'"))
    assert await run(redispatch_undispatched_notifications) == 0


async def test_the_dedupe_sweep_task_runs(app, task_env) -> None:
    async with app.state.db.engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO processed_events (consumer_group, event_id, event_type, processed_at)"
                " VALUES ('notification', gen_random_uuid(), 'x', now() - interval '40 days'),"
                "        ('notification', gen_random_uuid(), 'x', now())"
            )
        )
    assert await run(sweep_processed_events) == 1


async def test_the_cursor_sweep_drops_idle_cursors_and_caps_each_user(app, task_env) -> None:
    user, other = uuid.uuid4(), uuid.uuid4()
    async with app.state.db.engine.begin() as conn:
        # 30 devices for one user, newest last; one of them idle for 100 days.
        for i in range(30):
            await conn.execute(
                text(
                    "INSERT INTO stream_cursors (user_id, client_id, last_seen_at)"
                    " VALUES (:u, :c, now() - make_interval(mins => :m))"
                ),
                {"u": user, "c": f"device-{i:04d}", "m": 30 - i},
            )
        await conn.execute(
            text(
                "INSERT INTO stream_cursors (user_id, client_id, last_seen_at)"
                " VALUES (:u, 'device-old1', now() - interval '100 days'),"
                "        (:o, 'device-keep', now())"
            ),
            {"u": user, "o": other},
        )
    removed = await run(sweep_stale_stream_cursors)
    assert removed == 11  # 1 idle + 10 beyond the newest 20
    async with app.state.db.sessionmaker() as session:
        kept = list(
            await session.scalars(
                select(StreamCursor.client_id)
                .where(StreamCursor.user_id == user)
                .order_by(StreamCursor.client_id)
            )
        )
        others = list(
            await session.scalars(
                select(StreamCursor.client_id).where(StreamCursor.user_id == other)
            )
        )
    assert kept == [f"device-{i:04d}" for i in range(10, 30)]
    assert others == ["device-keep"]
    assert await run(sweep_stale_stream_cursors) == 0  # safe to run twice
