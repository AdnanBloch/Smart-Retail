"""Offset pagination (RFC 0000 §3.1.2.2).

``limit`` above the ceiling is clamped, not rejected; negative values are 422.
"""

from dataclasses import dataclass
from typing import Generic, TypeVar

from fastapi import Query
from pydantic import BaseModel

LIMIT_CEILING = 100
DEFAULT_LIMIT = 20

T = TypeVar("T")


@dataclass(frozen=True)
class PageParams:
    limit: int = DEFAULT_LIMIT
    offset: int = 0


def page_params(
    limit: int = Query(DEFAULT_LIMIT, ge=1, description=f"Clamped to {LIMIT_CEILING}"),
    offset: int = Query(0, ge=0),
) -> PageParams:
    return PageParams(limit=min(limit, LIMIT_CEILING), offset=offset)


class Page(BaseModel, Generic[T]):
    items: list[T]
    total: int
    limit: int
    offset: int
