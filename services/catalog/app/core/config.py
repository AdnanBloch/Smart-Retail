from functools import lru_cache

from smartretail_common.config import BaseServiceSettings, DatabaseSettings


class Settings(BaseServiceSettings, DatabaseSettings):
    """RFC 0004 §3.1.1.6 (CAT-10)."""

    service_name: str = "catalog"
    port: int = 8002

    # Publishing workflow (CAT-7, CAT-8).
    temporal_address: str = "temporal:7233"
    temporal_namespace: str = "smartretail"
    publish_task_queue: str = "catalog-publishing"
    publish_workflow_execution_timeout_seconds: int = 3600
    publish_media_max_attempts: int = 3
    publish_sync_max_attempts: int = 8

    # sync_downstream calls inventory with a client-credentials service token.
    inventory_base_url: str = "http://inventory:8004"
    identity_token_url: str = "http://identity:8001/api/v1/auth/token"
    service_client_id: str = "temporal-worker"
    service_client_secret: str = ""

    # Object storage for uploaded media (B12).
    minio_endpoint: str = "minio:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_secure: bool = False
    minio_media_bucket: str = "product-media"
    # Presigned GET urls are signed for this host, when set (§3.3.1).
    minio_public_endpoint: str | None = None
    media_max_bytes: int = 10_485_760
    media_allowed_content_types: str = "image/jpeg,image/png,image/webp"
    media_max_pixels: int = 40_000_000
    media_derived_sizes: str = "thumb:150,medium:600,large:1200"
    media_url_ttl_seconds: int = 3600
    # process_media on Celery's catalog.media queue (0002-Q7); inline when false.
    catalog_media_via_celery: bool = False
    # Under the activity's 5-minute start-to-close, so a lost task is retried in time.
    media_job_timeout_seconds: float = 240

    @property
    def allowed_content_types(self) -> frozenset[str]:
        return frozenset(t.strip() for t in self.media_allowed_content_types.split(",") if t)

    @property
    def derived_sizes(self) -> dict[str, int]:
        sizes: dict[str, int] = {}
        for item in self.media_derived_sizes.split(","):
            name, _, edge = item.strip().partition(":")
            sizes[name] = int(edge)
        return sizes


@lru_cache
def get_settings() -> Settings:
    return Settings()
