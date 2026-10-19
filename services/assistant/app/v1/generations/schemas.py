"""``/api/v1/assistant/generate*``, ``/generations*``, ``/usage*`` bodies."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import Field

from app.core.enums import ContentType
from app.core.schemas import OutModel, StrictModel
from app.v1.generations.repo import GenerationParams

Keyword = Annotated[str, Field(min_length=1, max_length=50)]


class GenerateIn(StrictModel):
    product_id: uuid.UUID
    content_type: ContentType
    tone: Annotated[str, Field(min_length=1, max_length=40)] | None = None
    max_length: Annotated[int, Field(ge=20, le=5000)] | None = None
    keywords: Annotated[list[Keyword], Field(max_length=10)] = Field(default_factory=list)

    def params(self) -> GenerationParams:
        return GenerationParams(self.content_type, self.tone, self.max_length, self.keywords)


class BulkGenerateIn(StrictModel):
    product_ids: Annotated[list[uuid.UUID], Field(min_length=1)]
    content_type: ContentType
    tone: Annotated[str, Field(min_length=1, max_length=40)] | None = None
    max_length: Annotated[int, Field(ge=20, le=5000)] | None = None
    keywords: Annotated[list[Keyword], Field(max_length=10)] = Field(default_factory=list)

    def params(self) -> GenerationParams:
        return GenerationParams(self.content_type, self.tone, self.max_length, self.keywords)


class GenerateSyncOut(OutModel):
    generation_id: uuid.UUID
    content_type: ContentType
    content: str
    filtered: bool
    finish_reason: str
    usage: dict[str, Any]


class GenerationOut(OutModel):
    id: uuid.UUID
    product_id: uuid.UUID
    content_type: str
    parameters: dict[str, Any]
    content: str
    prompt_tokens: int
    completion_tokens: int
    cost_usd: Decimal
    model: str
    applied: bool
    bulk_job_id: uuid.UUID | None
    created_at: datetime


class BulkAccepted(OutModel):
    job_id: uuid.UUID
    status: str
    status_url: str


class BulkJobOut(OutModel):
    id: uuid.UUID
    status: str
    content_type: str
    completed: int
    failed: int
    total: int
    errors: dict[str, str]
    created_at: datetime
    finished_at: datetime | None
    generations: list[GenerationOut]


class UsageOut(OutModel):
    scope: str
    tokens_used: int
    limit: int
    requests: int
    cost_usd: Decimal
    resets_at: datetime


class AdminUsageOut(OutModel):
    global_usage: UsageOut
    scopes: list[UsageOut]
