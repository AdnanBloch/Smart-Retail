from functools import lru_cache

from smartretail_common.config import BaseServiceSettings, DatabaseSettings


class Settings(BaseServiceSettings, DatabaseSettings):
    service_name: str = "catalog"
    port: int = 8002


@lru_cache
def get_settings() -> Settings:
    return Settings()
