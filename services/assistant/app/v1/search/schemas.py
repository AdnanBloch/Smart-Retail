"""``/api/v1/search/*`` request and response bodies (RFC 0009 §3.1.2.1)."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import Field, model_validator

from app.core.schemas import OutModel, StrictModel
from app.v1.search.enums import SearchMode
from app.v1.search.repo import SearchFilters
from app.v1.search.service import Result


class FiltersIn(StrictModel):
    category_id: uuid.UUID | None = None
    merchant_id: uuid.UUID | None = None
    min_price: Annotated[Decimal, Field(ge=0)] | None = None
    max_price: Annotated[Decimal, Field(ge=0)] | None = None

    @model_validator(mode="after")
    def _range(self) -> "FiltersIn":
        if (
            self.min_price is not None
            and self.max_price is not None
            and self.min_price > self.max_price
        ):
            raise ValueError("min_price must not exceed max_price")
        return self

    def to_filters(self) -> SearchFilters:
        return SearchFilters(self.category_id, self.merchant_id, self.min_price, self.max_price)


class SemanticSearchIn(StrictModel):
    query: Annotated[str, Field(min_length=1, max_length=2000)]
    limit: Annotated[int, Field(ge=1)] | None = None
    filters: FiltersIn | None = None
    mode: SearchMode = SearchMode.HYBRID


class MatchedChunk(OutModel):
    id: uuid.UUID
    chunk_type: str
    content: str


class SearchItem(OutModel):
    product_id: uuid.UUID
    name: str
    score: float
    matched_chunk: MatchedChunk
    match_reason: str
    price: Decimal | None
    currency: str | None
    in_stock: bool | None
    availability: Literal["in_stock", "out_of_stock", "unknown"]
    hydrated: bool

    @classmethod
    def from_result(cls, result: Result) -> "SearchItem":
        hit, live = result.hit, result.live
        availability: Literal["in_stock", "out_of_stock", "unknown"] = (
            "unknown" if live.in_stock is None else "in_stock" if live.in_stock else "out_of_stock"
        )
        return cls(
            product_id=hit.product_id,
            name=live.name or hit.name,
            score=round(hit.score, 6),
            matched_chunk=MatchedChunk(
                id=hit.chunk_id, chunk_type=hit.chunk_type, content=hit.content
            ),
            match_reason=hit.match_reason,
            price=live.price,
            currency=live.currency,
            in_stock=live.in_stock,
            availability=availability,
            hydrated=live.hydrated,
        )


class SearchOut(OutModel):
    items: list[SearchItem]
    mode: SearchMode
    degraded: str | None = None


class ProductScope(StrictModel):
    product_ids: Annotated[list[uuid.UUID], Field(min_length=1, max_length=1000)]


class MerchantScope(StrictModel):
    merchant_id: uuid.UUID


class ReindexIn(StrictModel):
    scope: Literal["full"] | ProductScope | MerchantScope = "full"


class ReindexAccepted(OutModel):
    job_id: uuid.UUID
    status_url: str


class IndexStatusOut(OutModel):
    embedding_model: str
    indexed_products: int
    chunk_count: int
    products_by_status: dict[str, int]
    pending_lag_seconds: float | None
    last_error: dict[str, Any] | None
    cache_entries: int
    cache_hits: int
    reindex_running: bool


class ChunkOut(OutModel):
    id: uuid.UUID
    chunk_index: int
    chunk_type: str
    content: str
    content_hash: str
    token_count: int
    indexed_at: datetime


class IndexDetailOut(OutModel):
    product_id: uuid.UUID
    merchant_id: uuid.UUID | None
    status: str
    catalog_version: int
    chunk_count: int
    embedding_model: str
    error: str | None
    indexed_at: datetime | None
    updated_at: datetime
    chunks: list[ChunkOut]
