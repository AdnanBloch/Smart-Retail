from decimal import Decimal
from functools import lru_cache

from smartretail_common.config import BaseServiceSettings, DatabaseSettings


class Settings(BaseServiceSettings, DatabaseSettings):
    """RFC 0005 §3.1.1.6 (ORD-14)."""

    service_name: str = "order"
    port: int = 8003

    # The one synchronous call in the system (A13): the snapshot at checkout.
    catalog_base_url: str = "http://catalog:8002"
    catalog_timeout_seconds: float = 2.0

    # Workflow-side calls to inventory use a client-credentials service token (ORD-11).
    inventory_base_url: str = "http://inventory:8004"
    identity_token_url: str = "http://identity:8001/api/v1/auth/token"
    service_client_id: str = "temporal-worker"
    service_client_secret: str = ""

    temporal_address: str = "temporal:7233"
    temporal_namespace: str = "smartretail"
    # PLT-D13: `order-lifecycle` (RFC 0005 said `orders`; RFC 0001 owns the name).
    order_task_queue: str = "order-lifecycle"
    order_workflow_execution_timeout_seconds: int = 86_400

    idempotency_ttl_hours: int = 24
    idempotency_in_progress_lease_seconds: int = 30

    order_shipping_fee: Decimal = Decimal("0.00")
    order_max_lines: int = 50
    order_max_quantity_per_line: int = 999

    # Deterministic provider fakes (A9, B10): every saga branch reachable by configuration.
    fake_payment_decline_token: str = "tok_decline"
    fake_payment_timeout_token: str = "tok_timeout"
    fake_payment_flaky_token: str = "tok_flaky_2"
    fake_payment_refund_fail_token: str = "tok_refund_fail"
    fake_provider_timeout_seconds: float = 30
    fake_shipping_reject_postcode: str = "00000"
    fake_shipping_timeout_postcode: str = "99999"


@lru_cache
def get_settings() -> Settings:
    return Settings()
