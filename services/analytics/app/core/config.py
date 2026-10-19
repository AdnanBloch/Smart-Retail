from functools import lru_cache

from pydantic import model_validator

from smartretail_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    """RFC 0008 §3.1.1.6. No Postgres: ``analytics_db`` is MongoDB (ANL-D1)."""

    service_name: str = "analytics"
    port: int = 8006

    analytics_mongo_uri: str = "mongodb://mongo:27017/?replicaSet=rs0"
    analytics_mongo_db: str = "analytics_db"
    # A read must fail fast (503) rather than hang when Mongo is down.
    analytics_mongo_timeout_ms: int = 3000

    analytics_consumer_group: str = "analytics"
    analytics_dlq_group: str = "analytics-dlq-observer"
    analytics_dlq_pattern: str = r"^smartretail\..*\.dlq$"

    raw_events_ttl_days: int = 7
    processed_events_ttl_days: int = 30
    reconcile_lookback_days: int = 6
    analytics_reporting_currency: str = "USD"
    analytics_raw_window_max_days: int = 7

    # The event pipeline (RFC 0001 §3.1.1.6); used by the consumer process.
    kafka_bootstrap_servers: str = "kafka:9092"
    schema_registry_url: str = "http://schema-registry:8081"

    @model_validator(mode="after")
    def _lookback_inside_raw_window(self) -> "Settings":
        # The reconciliation re-derives whole days from raw_events, so every
        # day it touches must still be fully inside raw_events' TTL.
        if self.reconcile_lookback_days >= self.raw_events_ttl_days:
            raise ValueError("RECONCILE_LOOKBACK_DAYS must be < RAW_EVENTS_TTL_DAYS")
        if self.processed_events_ttl_days < 2:
            raise ValueError("PROCESSED_EVENTS_TTL_DAYS must be at least 2")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
