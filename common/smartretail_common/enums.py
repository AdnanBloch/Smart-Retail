"""Domain enum base (RFC 0000 S4).

Values live in each service's ``app/domain/enums.py``; this module only
provides the mechanism.
"""

from enum import StrEnum
from typing import Any, Self

from pydantic import BeforeValidator


class UnknownEnumValueError(ValueError):
    def __init__(self, enum_class: type[StrEnum], value: object) -> None:
        super().__init__(f"{value!r} is not a valid {enum_class.__name__}")
        self.enum_class = enum_class
        self.value = value


class DomainEnum(StrEnum):
    @classmethod
    def values(cls) -> list[str]:
        return [m.value for m in cls]

    @classmethod
    def has(cls, raw: object) -> bool:
        return isinstance(raw, str) and raw in cls._value2member_map_

    @classmethod
    def parse(cls, raw: object, *, default: Self | None = None) -> Self:
        if isinstance(raw, str) and cls.has(raw):
            return cls(raw)
        if default is not None:
            return default
        raise UnknownEnumValueError(cls, raw)


def widen_for_response(enum_class: type[DomainEnum]) -> BeforeValidator:
    """Response fields typed ``Enum | str`` render unknown raw values as-is.

    Request schemas stay strict; this is only for reading ``passthrough``
    columns that may hold a value written outside the application.
    """

    def _widen(value: Any) -> Any:
        return enum_class(value) if enum_class.has(value) else value

    return BeforeValidator(_widen)
