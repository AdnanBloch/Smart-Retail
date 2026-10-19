"""``prompt_templates`` (RFC 0009 §3.1.3, AST-16, AST-D26)."""

from sqlalchemy import Boolean, Index, Integer, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from app.core.enums import ContentType
from smartretail_common.db import Base, CreatedAt, UUIDPrimaryKey
from smartretail_common.db_types import DomainEnumText


class PromptTemplate(UUIDPrimaryKey, CreatedAt, Base):
    """Versioned rows; supersession is a new row, never an update of ``body``.

    The two uniques use ``NULLS NOT DISTINCT`` (null ``content_type`` = system
    prompts) and are created by migration 0004 in SQL.
    """

    __tablename__ = "prompt_templates"
    __table_args__ = (Index("ix_prompt_templates_name", "name"),)

    content_type: Mapped[ContentType | str | None] = mapped_column(
        DomainEnumText(ContentType), nullable=True, comment="DomainEnum ContentType"
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
