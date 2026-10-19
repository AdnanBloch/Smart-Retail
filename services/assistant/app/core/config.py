"""RFC 0009 §3.1.1.6. Every value here is configuration, read from the environment.

Providers are selected by name and default to the deterministic, key-less
implementations, so the local stack and CI need no API key (AST-D12). A real
provider without its key refuses to start rather than failing on first use.
"""

from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, SecretStr, model_validator

from smartretail_common.config import BaseServiceSettings, DatabaseSettings

# The vector columns are vector(1024) (AST-D31): a different dimension is a migration.
EMBEDDING_DIMENSION = 1024

DEFAULT_PRICES: dict[str, tuple[str, str]] = {
    # USD per million tokens (input, output), from the provider's published table.
    "claude-sonnet-5-5": ("2.00", "10.00"),
    "claude-haiku-5-5": ("0.10", "0.50"),
    "voyage-3.5": ("0.06", "0"),
}


def _default_prompts_dir() -> str:
    # services/assistant/app/core/config.py → repo root /ops/prompts (local runs);
    # the image sets PROMPTS_DIR=/app/prompts.
    return str(Path(__file__).resolve().parents[4] / "ops" / "prompts")


class Settings(BaseServiceSettings, DatabaseSettings):
    service_name: str = "assistant"
    port: int = 8007

    # --- live lookups (R5 / A13, AST-2) ---------------------------------
    catalog_base_url: str = "http://catalog:8002"
    inventory_base_url: str = "http://inventory:8004"
    hydration_timeout_seconds: float = 2.0
    hydration_concurrency: int = 10

    # --- embeddings (D10, AST-D31) --------------------------------------
    embedding_provider: Literal["hashing", "voyage"] = "hashing"
    embedding_model: str | None = None  # derived from the provider when unset
    embedding_dimension: int = EMBEDDING_DIMENSION
    embedding_batch_size: int = 96
    voyage_api_key: SecretStr | None = None
    voyage_base_url: str = "https://api.voyageai.com/v1"
    voyage_timeout_seconds: float = 10.0

    # --- chunking (§3.1.1.2) --------------------------------------------
    chunk_target_tokens: int = 256
    chunk_max_tokens: int = 512
    chunk_overlap_tokens: int = 80
    chunk_max_per_product: int = 12

    # --- search (D7, D8) ------------------------------------------------
    search_candidates_per_mode: int = 50
    rrf_k: int = 60
    rrf_weight_dense: float = 1.0
    rrf_weight_keyword: float = 1.0
    hnsw_ef_search: int = 40
    search_default_limit: int = 10
    search_max_limit: int = 50
    query_max_chars: int = 500
    query_log_retention_days: int = 90

    # --- generation (E10, E11, AST-D30) -----------------------------------
    llm_provider: Literal["scripted", "anthropic"] = "scripted"
    llm_model: str = "claude-sonnet-5-5"
    llm_classifier_model: str = "claude-haiku-5-5"
    anthropic_api_key: SecretStr | None = None
    # Sonnet 5.5 and Haiku 5.5 think adaptively by default and thinking counts
    # against max_tokens, so the bounds leave room for it (AST-D30).
    llm_max_tokens: int = 4096
    llm_classifier_max_tokens: int = 1024
    llm_context_token_budget: int = 6000
    llm_request_timeout_seconds: float = 60.0
    # model → (input, output) USD per million tokens. Unknown models cost 0.
    llm_price_table: dict[str, tuple[Decimal, Decimal]] = Field(
        default_factory=lambda: {
            k: (Decimal(i), Decimal(o)) for k, (i, o) in DEFAULT_PRICES.items()
        }
    )

    # --- budgets (E10, AST-D34) -----------------------------------------
    daily_token_budget_user: int = 50_000
    daily_token_budget_global: int = 2_000_000

    # --- conversations and streams ------------------------------------
    memory_window_turns: int = 6
    anon_conversation_ttl_hours: int = 24
    max_concurrent_streams: int = 100
    message_max_chars: int = 2000
    sse_keepalive_seconds: float = 5.0
    bulk_max_products: int = 200

    prompts_dir: str = Field(default_factory=_default_prompts_dir)

    # The event pipeline (RFC 0001 §3.1.1.6), used by indexer-worker.
    kafka_bootstrap_servers: str = "kafka:9092"
    schema_registry_url: str = "http://schema-registry:8081"

    @model_validator(mode="after")
    def _providers(self) -> Self:
        if not (self.embedding_model or "").strip():
            self.embedding_model = (
                "voyage-3.5" if self.embedding_provider == "voyage" else ("hashing-v1")
            )
        if self.embedding_dimension != EMBEDDING_DIMENSION:
            raise ValueError(
                f"EMBEDDING_DIMENSION must be {EMBEDDING_DIMENSION}: the vector columns are "
                f"vector({EMBEDDING_DIMENSION}) and a change is a migration plus a reindex"
            )
        if self.embedding_provider == "voyage" and not _present(self.voyage_api_key):
            raise ValueError("EMBEDDING_PROVIDER=voyage requires VOYAGE_API_KEY")
        if self.llm_provider == "anthropic" and not _present(self.anthropic_api_key):
            raise ValueError("LLM_PROVIDER=anthropic requires ANTHROPIC_API_KEY")
        if not 0 < self.chunk_overlap_tokens < self.chunk_target_tokens <= self.chunk_max_tokens:
            raise ValueError("chunk sizes must satisfy 0 < overlap < target <= max")
        return self

    @property
    def active_embedding_model(self) -> str:
        assert self.embedding_model is not None  # set by the validator
        return self.embedding_model

    def price(self, model: str) -> tuple[Decimal, Decimal]:
        return self.llm_price_table.get(model, (Decimal(0), Decimal(0)))


def _present(secret: SecretStr | None) -> bool:
    return secret is not None and bool(secret.get_secret_value().strip())


@lru_cache
def get_settings() -> Settings:
    return Settings()
