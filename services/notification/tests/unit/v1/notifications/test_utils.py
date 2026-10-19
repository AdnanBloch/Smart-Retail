"""The event → recipe table (NTF-3, NTF-7), scope derivation (I2), backoff (NTF-5)."""

import uuid

import pytest

from app.v1.notifications.enums import (
    NotificationChannel,
    NotificationDeliveryScope,
    NotificationType,
)
from app.v1.notifications.utils import DEFAULT_SCOPE, backoff_seconds, derive_scope, resolve
from tests.integration.conftest import (
    cancelled,
    confirmed,
    failed,
    payment_failed,
    payment_succeeded,
    registered,
    shipment,
    stockout,
)

U = str(uuid.uuid4())
IN_APP, EMAIL = NotificationChannel.IN_APP, NotificationChannel.EMAIL


@pytest.mark.parametrize(
    ("event_type", "payload", "kind", "channels"),
    [
        ("identity.user.registered", registered(U), NotificationType.WELCOME, (IN_APP, EMAIL)),
        ("order.confirmed", confirmed(U), NotificationType.ORDER_CONFIRMED, (IN_APP, EMAIL)),
        ("order.failed", failed(U), NotificationType.ORDER_FAILED, (IN_APP, EMAIL)),
        ("order.cancelled", cancelled(U), NotificationType.ORDER_CANCELLED, (IN_APP, EMAIL)),
        (
            "shipment.status_changed",
            shipment(U, "shipped"),
            NotificationType.SHIPMENT_UPDATE,
            (IN_APP, EMAIL),
        ),
        (
            "shipment.status_changed",
            shipment(U, "in_transit"),
            NotificationType.SHIPMENT_UPDATE,
            (IN_APP, EMAIL),
        ),
        (
            "shipment.status_changed",
            shipment(U, "delivered"),
            NotificationType.SHIPMENT_UPDATE,
            (IN_APP, EMAIL),
        ),
        ("inventory.stockout", stockout(U), NotificationType.STOCKOUT, (IN_APP,)),
    ],
)
def test_notifying_events(event_type, payload, kind, channels) -> None:
    recipe = resolve(event_type, payload)
    assert recipe is not None
    assert recipe.user_id == uuid.UUID(U)
    assert recipe.type == kind
    assert recipe.channels == channels


@pytest.mark.parametrize(
    ("event_type", "payload"),
    [
        ("payment.succeeded", payment_succeeded(U)),
        ("payment.failed", payment_failed(U)),
        ("shipment.status_changed", shipment(U, "created")),
        ("catalog.product.published", {}),
    ],
)
def test_events_that_notify_nobody(event_type, payload) -> None:
    assert resolve(event_type, payload) is None


def test_stockout_goes_to_the_merchant_not_the_customer() -> None:
    merchant = str(uuid.uuid4())
    recipe = resolve("inventory.stockout", stockout(merchant))
    assert recipe is not None and recipe.user_id == uuid.UUID(merchant)


def test_a_missing_or_malformed_recipient_is_poison() -> None:
    payload = confirmed(U)
    del payload["customer_id"]
    with pytest.raises(KeyError):
        resolve("order.confirmed", payload)
    with pytest.raises(ValueError):
        resolve("order.confirmed", {**confirmed(U), "customer_id": "not-a-uuid"})


@pytest.mark.parametrize("kind", list(NotificationType))
@pytest.mark.parametrize("origin", [None, "client-abc-123"])
def test_scope_over_every_type_and_origin(kind, origin) -> None:
    """I2: twelve combinations; no origin always broadcasts."""
    scope = derive_scope(kind, origin)
    if origin is None:
        assert scope == NotificationDeliveryScope.ALL_DEVICES
    else:
        assert scope == DEFAULT_SCOPE[kind]


def test_default_scopes() -> None:
    assert DEFAULT_SCOPE[NotificationType.WELCOME] == NotificationDeliveryScope.ORIGIN_ONLY
    assert DEFAULT_SCOPE[NotificationType.ORDER_FAILED] == NotificationDeliveryScope.ORIGIN_FIRST
    assert DEFAULT_SCOPE[NotificationType.SHIPMENT_UPDATE] == NotificationDeliveryScope.ALL_DEVICES


def test_backoff_is_exponential_capped_with_full_jitter() -> None:
    top = lambda lo, hi: hi  # noqa: E731
    assert [backoff_seconds(n, base=2, cap=300, rand=top) for n in range(1, 11)] == [
        2,
        4,
        8,
        16,
        32,
        64,
        128,
        256,
        300,
        300,
    ]
    assert backoff_seconds(0, base=2, cap=300, rand=top) == 2
    for n in range(1, 6):
        assert 0 <= backoff_seconds(n, base=2, cap=300) <= 2 * 2 ** (n - 1)
