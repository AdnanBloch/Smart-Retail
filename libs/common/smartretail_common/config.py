"""Base settings shared by every service (RFC 0001 §3.1.1.1).

Settings are read from the environment only. Validation happens when the
settings object is built at import time, so a missing variable fails the
container at start-up rather than on first use.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)

    service_name: str
    environment: str = "local"
    log_level: str = "INFO"
    port: int = 8000

    # JWT verification. Every verifier pins RS256 and checks iss/aud/exp/nbf.
    jwt_public_key_path: str | None = "/run/keys/jwt_public.pem"
    jwks_url: str | None = None
    jwks_min_refetch_seconds: int = 30
    jwt_issuer: str = "smartretail-identity"
    jwt_audience: str = "smartretail"
    jwt_leeway_seconds: int = 10

    # Observability arrives in Milestone 3.
    otel_sdk_disabled: bool = True


class DatabaseSettings(BaseSettings):
    """Mixed into the settings of services that own a store."""

    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)

    database_url: str
    db_pool_size: int = 10
    db_max_overflow: int = 20
