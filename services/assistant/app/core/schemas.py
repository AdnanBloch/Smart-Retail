"""Pydantic bases every entity's versioned schemas build on."""

from pydantic import BaseModel, ConfigDict


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class OutModel(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)
