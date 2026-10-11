from functools import lru_cache

from pydantic import BaseModel, Field

from smartretail_common.config import BaseServiceSettings, DatabaseSettings


class ServiceClient(BaseModel):
    """One client-credentials registration (RFC 0003 IDN-T3). Hashes, never secrets."""

    secret_hash: str
    scopes: list[str] = Field(default_factory=list)


class Settings(BaseServiceSettings, DatabaseSettings):
    service_name: str = "identity"
    port: int = 8001

    # Signing keys (RFC 0003 IDN-5): every PEM in the directory is loaded and
    # published; the active kid signs. Startup fails if it is missing.
    jwt_keys_dir: str = "/run/secrets/jwt"
    jwt_active_kid: str

    access_token_ttl_seconds: int = 900
    refresh_token_ttl_seconds: int = 604_800
    jwks_max_age_seconds: int = 300

    # Client-credentials registry, as JSON in SERVICE_CLIENTS (IDN-T3, IDN-4).
    service_clients: dict[str, ServiceClient] = Field(default_factory=dict)
    service_token_ttl_seconds: int = 300

    argon2_time_cost: int = 3
    argon2_memory_cost_kib: int = 65_536
    argon2_parallelism: int = 4
    argon2_hash_len: int = 32
    argon2_salt_len: int = 16

    password_min_length: int = 12
    password_max_length: int = 128

    seed_admin_email: str = "admin@smartretail.local"
    seed_admin_password: str | None = None
    seed_demo_password: str | None = None


@lru_cache
def get_settings() -> Settings:
    return Settings()
