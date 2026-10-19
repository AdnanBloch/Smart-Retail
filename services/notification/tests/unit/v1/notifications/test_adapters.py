import uuid

from app.core.config import Settings
from app.v1.notifications.adapters import registry
from app.v1.notifications.adapters.base import Failed, NotificationView, Sent
from app.v1.notifications.adapters.email_log import EmailLogAdapter
from app.v1.notifications.adapters.sms_log import SmsLogAdapter
from app.v1.notifications.enums import NotificationChannel

VIEW = NotificationView(
    id=uuid.uuid4(),
    user_id=uuid.uuid4(),
    channel="email",
    type="order_confirmed",
    title="Order confirmed",
    body="x" * 300,
    correlation_id="corr",
)


async def test_log_adapters_send_and_fail_on_demand(caplog) -> None:
    caplog.set_level("INFO")
    assert isinstance(await EmailLogAdapter().send(VIEW), Sent)
    sms = await SmsLogAdapter().send(VIEW)
    assert isinstance(sms, Sent) and sms.provider_ref == f"log-sms-{VIEW.id}"
    record = next(r for r in caplog.records if r.getMessage() == "sms sent")
    assert len(record.body) == 160  # type: ignore[attr-defined]

    assert isinstance(await EmailLogAdapter(1.0, rand=lambda: 0.5).send(VIEW), Failed)
    assert isinstance(await SmsLogAdapter(0.4, rand=lambda: 0.5).send(VIEW), Sent)
    failed = await SmsLogAdapter(0.6, rand=lambda: 0.5).send(VIEW)
    assert isinstance(failed, Failed) and failed.retryable


def test_registry_has_no_push_adapter() -> None:
    adapters = registry(Settings(database_url="postgresql+asyncpg://x/y"))
    assert set(adapters) == {NotificationChannel.EMAIL, NotificationChannel.SMS}
