from decimal import Decimal
from typing import Annotated, Any

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

MAX_KEYS = 50
MAX_KEY_LENGTH = 64
MAX_STRING_LENGTH = 500
MAX_ARRAY_LENGTH = 50


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class OutModel(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


def _scalar_ok(value: Any) -> bool:
    if isinstance(value, str):
        return len(value) <= MAX_STRING_LENGTH
    return value is None or isinstance(value, bool | int | float)


def _check_open_map(value: dict[str, Any]) -> dict[str, Any]:
    """Open-ended jsonb (S8): string keys, scalar or array-of-scalar values, bounded."""
    if len(value) > MAX_KEYS:
        raise ValueError(f"at most {MAX_KEYS} keys")
    for key, item in value.items():
        if not key or len(key) > MAX_KEY_LENGTH:
            raise ValueError(f"keys must be 1-{MAX_KEY_LENGTH} characters")
        if isinstance(item, list):
            if len(item) > MAX_ARRAY_LENGTH or not all(_scalar_ok(v) for v in item):
                raise ValueError(f"{key!r}: arrays must hold at most {MAX_ARRAY_LENGTH} scalars")
        elif not _scalar_ok(item):
            raise ValueError(f"{key!r}: values must be scalars or arrays of scalars")
    return value


OpenMap = Annotated[dict[str, Any], AfterValidator(_check_open_map)]
Money = Annotated[Decimal, Field(ge=0, max_digits=12, decimal_places=2)]
Currency = Annotated[str, Field(pattern=r"^[A-Z]{3}$")]
Sku = Annotated[str, Field(min_length=1, max_length=64)]
Version = Annotated[int, Field(ge=1, description="The product's current version (CAT-12)")]
