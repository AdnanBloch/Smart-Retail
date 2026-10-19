from functools import lru_cache

from smartretail_common.config import BaseServiceSettings, DatabaseSettings


class Settings(BaseServiceSettings, DatabaseSettings):
    """RFC 0007 §3.1.1.6. The database URL is the platform's ``DATABASE_URL``."""

    service_name: str = "notification"
    port: int = 8005

    # Tickets, fan-out, connection caps, the ack rate limit.
    redis_url: str = "redis://redis:6379/0"

    # The event pipeline (RFC 0001 §3.1.1.6); used by the consumer process.
    kafka_bootstrap_servers: str = "kafka:9092"
    schema_registry_url: str = "http://schema-registry:8081"
    consumer_max_attempts: int = 3  # C6

    # The SSE stream (C15-C22).
    stream_ticket_ttl_seconds: int = 60
    stream_max_per_user: int = 3
    stream_conn_count_ttl_seconds: int = 60
    stream_heartbeat_seconds: float = 20
    stream_queue_max: int = 100
    stream_retry_base_ms: int = 3000
    stream_retry_jitter_ms: int = 2000
    stream_subscribe_timeout_seconds: float = 2.0
    catchup_max_frames: int = 50
    catchup_max_age_hours: int = 24
    seq_visibility_window_seconds: int = 5

    # The ack (C23, NTF-15).
    ack_rate_limit_per_minute: int = 12
    ack_max_ids: int = 200

    # Cursor retention (stream_cursors).
    stream_cursor_retention_days: int = 90
    stream_cursor_max_per_user: int = 20

    # Dispatch (NTF-5, NTF-6). DISPATCH_MAX_RETRIES is the attempt ceiling: the
    # fifth failed attempt is the last one written.
    dispatch_max_retries: int = 5
    dispatch_backoff_base_seconds: float = 2
    dispatch_backoff_max_seconds: float = 300
    redispatch_min_age_seconds: int = 120
    redispatch_max_age_hours: int = 24
    dispatch_stale_pending_seconds: int = 60  # the task's hard time limit
    notification_fake_email_failure_rate: float = 0.0
    notification_fake_sms_failure_rate: float = 0.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
