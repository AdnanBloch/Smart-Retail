from typing import Annotated

import pytest
from pydantic import BaseModel
from sqlalchemy.dialects import postgresql

from smartretail_common.db_types import DomainEnumText, render_item
from smartretail_common.enums import DomainEnum, UnknownEnumValueError, widen_for_response


class Colour(DomainEnum):
    RED = "red"
    BLUE = "blue"


DIALECT = postgresql.dialect()


def test_enum_helpers() -> None:
    assert Colour.values() == ["red", "blue"]
    assert Colour.has("red") and not Colour.has("green") and not Colour.has(1)
    assert Colour.parse("blue") is Colour.BLUE
    assert Colour.parse("green", default=Colour.RED) is Colour.RED
    with pytest.raises(UnknownEnumValueError):
        Colour.parse("green")


def test_write_is_strict() -> None:
    t = DomainEnumText(Colour)
    assert t.process_bind_param(Colour.RED, DIALECT) == "red"
    assert t.process_bind_param("blue", DIALECT) == "blue"
    assert t.process_bind_param(None, DIALECT) is None
    with pytest.raises(UnknownEnumValueError):
        t.process_bind_param("green", DIALECT)


def test_read_policies() -> None:
    assert DomainEnumText(Colour).process_result_value("green", DIALECT) == "green"
    fb = DomainEnumText(Colour, on_unknown="fallback", fallback=Colour.RED)
    assert fb.process_result_value("green", DIALECT) is Colour.RED
    with pytest.raises(UnknownEnumValueError):
        DomainEnumText(Colour, on_unknown="raise").process_result_value("green", DIALECT)
    assert DomainEnumText(Colour).process_result_value("red", DIALECT) is Colour.RED


def test_fallback_requires_member() -> None:
    with pytest.raises(ValueError):
        DomainEnumText(Colour, on_unknown="fallback")


def test_render_item() -> None:
    class Ctx:
        imports: set[str] = set()

    ctx = Ctx()
    assert render_item("type", DomainEnumText(Colour), ctx) == "sa.Text()"
    assert "import sqlalchemy as sa" in ctx.imports
    assert render_item("type", object(), ctx) is False


def test_widen_for_response() -> None:
    class Out(BaseModel):
        colour: Annotated[Colour | str, widen_for_response(Colour)]

    assert Out(colour="red").colour is Colour.RED
    assert Out(colour="green").colour == "green"
