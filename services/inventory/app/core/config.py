from functools import lru_cache

from smartretail_common.config import BaseServiceSettings, DatabaseSettings


class Settings(BaseServiceSettings, DatabaseSettings):
    """RFC 0006 §3.1.1.6. Names keep the RFC's ``INVENTORY_`` prefix."""

    service_name: str = "inventory"
    port: int = 8004

    inventory_reservation_ttl_seconds: int = 900  # B5: 15 minutes
    inventory_default_low_stock_threshold: int = 5
    inventory_lock_timeout_ms: int = 2000
    inventory_statement_timeout_ms: int = 5000
    inventory_lock_retry_attempts: int = 3
    inventory_lock_retry_base_ms: int = 25
    inventory_max_lines_per_reservation: int = 100
    inventory_sweep_batch_size: int = 500
    inventory_sweeper_inprocess: bool = False
    inventory_sweeper_interval_seconds: float = 60


@lru_cache
def get_settings() -> Settings:
    return Settings()
