from functools import lru_cache

from smartretail_common.config import BaseServiceSettings, DatabaseSettings


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
