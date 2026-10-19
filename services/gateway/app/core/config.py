from functools import lru_cache

from pydantic import field_validator

from smartretail_common.config import BaseServiceSettings


class GatewaySettings(BaseServiceSettings):
    service_name: str = "gateway"
    port: int = 8000

    upstream_identity_url: str = "http://identity:8001"
    upstream_catalog_url: str = "http://catalog:8002"
    upstream_order_url: str = "http://order:8003"
    upstream_inventory_url: str = "http://inventory:8004"
    upstream_notification_url: str = "http://notification:8005"
    upstream_analytics_url: str = "http://analytics:8006"
    upstream_assistant_url: str = "http://assistant:8007"

    gateway_cors_allow_origins: str = "http://localhost:3000,http://localhost:5173"
    gateway_cors_max_age_s: int = 600
    gateway_public_url: str = "http://localhost:8000"
    gateway_max_body_bytes: int = 1_048_576
    # GW-14: the media upload route alone accepts larger bodies, and more time.
    gateway_max_upload_bytes: int = 10_485_760
    gateway_upload_read_timeout_s: float = 30.0
    gateway_upstream_connect_timeout_s: float = 2.0
    gateway_upstream_read_timeout_s: float = 10.0
    # The `stream` timeout class (SSE: notifications, assistant chat). Must exceed
    # the longest silent gap a stream may have between keepalives (GW-D30).
    gateway_stream_read_timeout_s: float = 120.0
    gateway_upstream_max_connections: int = 100
    gateway_upstream_max_keepalive: int = 20
    gateway_openapi_cache_ttl_s: int = 60

    @field_validator("gateway_cors_allow_origins")
    @classmethod
    def _no_wildcard(cls, value: str) -> str:
        if "*" in value:
            raise ValueError("GATEWAY_CORS_ALLOW_ORIGINS must list explicit origins, not '*'")
        return value

    @property
    def cors_origins(self) -> frozenset[str]:
        return frozenset(o.strip() for o in self.gateway_cors_allow_origins.split(",") if o.strip())

    @property
    def upstreams(self) -> dict[str, str]:
        return {
            "identity": self.upstream_identity_url,
            "catalog": self.upstream_catalog_url,
            "order": self.upstream_order_url,
            "inventory": self.upstream_inventory_url,
            "notification": self.upstream_notification_url,
            "analytics": self.upstream_analytics_url,
            "assistant": self.upstream_assistant_url,
        }


@lru_cache
def get_settings() -> GatewaySettings:
    return GatewaySettings()
