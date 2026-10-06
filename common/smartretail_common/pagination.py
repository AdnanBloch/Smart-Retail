"""Offset pagination (RFC 0000 §3.1.2.2).

``limit`` above the ceiling is clamped, not rejected; negative values are 422.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Generic, TypeVar

from fastapi import Query
from pydantic import BaseModel

LIMIT_CEILING = 100
DEFAULT_LIMIT = 20

T = TypeVar("T")
U = TypeVar("U")


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


@dataclass(frozen=True)
class Slice(Generic[T]):
    """One page of domain objects, as a repo returns it.

    Repos are unversioned, so they never build API models. The versioned API
    layer turns a slice into its own ``Page[...]`` with :meth:`to_page`.
    """

    items: list[T]
    total: int
    params: PageParams

    def to_page(self, convert: Callable[[T], U]) -> Page[U]:
        return Page[U](
            items=[convert(item) for item in self.items],
            total=self.total,
            limit=self.params.limit,
            offset=self.params.offset,
        )
