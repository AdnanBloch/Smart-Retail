"""``DomainEnumText``: enum values stored as plain ``text`` (RFC 0000 S4).

No PostgreSQL ENUM and no CHECK: adding a value is a code change, not a
migration. Writes are strict, reads are tolerant per column policy.
"""

from typing import Any, Literal

from sqlalchemy import Text
from sqlalchemy.engine import Dialect
from sqlalchemy.types import TypeDecorator

from smartretail_common.enums import DomainEnum, UnknownEnumValueError

OnUnknown = Literal["passthrough", "fallback", "raise"]


class DomainEnumText(TypeDecorator[Any]):
    impl = Text
    cache_ok = True

    def __init__(
        self,
        enum_class: type[DomainEnum],
        *,
        on_unknown: OnUnknown = "passthrough",
        fallback: DomainEnum | None = None,
    ) -> None:
        super().__init__()
        if on_unknown == "fallback" and fallback is None:
            raise ValueError("on_unknown='fallback' requires a fallback member")
        self.enum_class = enum_class
        self.on_unknown = on_unknown
        self.fallback = fallback

    def process_bind_param(self, value: Any, dialect: Dialect) -> str | None:
        if value is None:
            return None
        if isinstance(value, self.enum_class):
            return value.value
        if self.enum_class.has(value):
            return str(value)
        raise UnknownEnumValueError(self.enum_class, value)

    def process_result_value(self, value: Any, dialect: Dialect) -> Any:
        if value is None:
            return None
        if self.enum_class.has(value):
            return self.enum_class(value)
        if self.on_unknown == "passthrough":
            return value
        if self.on_unknown == "fallback":
            return self.fallback
        raise UnknownEnumValueError(self.enum_class, value)

    @property
    def python_type(self) -> type:
        return self.enum_class


def render_item(type_: str, obj: Any, autogen_context: Any) -> str | Literal[False]:
    """Alembic hook: render ``DomainEnumText`` columns as ``sa.Text()``."""
    if type_ == "type" and isinstance(obj, DomainEnumText):
        autogen_context.imports.add("import sqlalchemy as sa")
        return "sa.Text()"
    return False
