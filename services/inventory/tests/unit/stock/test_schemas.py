import pytest
from pydantic import ValidationError

from app.stock.v1.schemas import StockAdjust


def test_adjust_rules() -> None:
    assert StockAdjust(delta=-2, reason="manual_adjustment").delta == -2
    for body in (
        {"delta": 0, "reason": "restock"},
        {"delta": -1, "reason": "restock"},
        {"delta": -1, "reason": "return"},
        {"delta": 1, "reason": "reserve"},
    ):
        with pytest.raises(ValidationError):
            StockAdjust.model_validate(body)
