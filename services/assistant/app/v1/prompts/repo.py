"""Active-version lookup, rendering and seeding of prompt templates (AST-D26).

Sources live in ``ops/prompts/`` so a prompt change is reviewable in a diff:

* ``system.<name>.md`` → a system prompt (``content_type`` null, ``name``);
* ``<content_type>.md`` → the ``default`` template for a ``ContentType``.

Bodies use ``$placeholders`` (``string.Template``), so Markdown braces are
safe. :func:`seed_templates` inserts a new version only when a body changed,
deactivating the previous one in the same transaction (one active per key).
When the table has no row for a key, the file itself is used (version 0), so
a fresh database works before ``make seed``.
"""

from dataclasses import dataclass
from pathlib import Path
from string import Template as StringTemplate
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import ContentType
from app.v1.prompts.models import PromptTemplate

SYSTEM_PREFIX = "system."
DEFAULT_NAME = "default"


@dataclass(frozen=True)
class Template:
    content_type: ContentType | None
    name: str
    version: int
    body: str

    def render(self, **values: Any) -> str:
        return StringTemplate(self.body).safe_substitute({k: str(v) for k, v in values.items()})


def template_key(path: Path) -> tuple[ContentType | None, str] | None:
    stem = path.stem
    if stem.startswith(SYSTEM_PREFIX):
        return None, stem[len(SYSTEM_PREFIX) :]
    if ContentType.has(stem):
        return ContentType(stem), DEFAULT_NAME
    return None


def template_sources(prompts_dir: str | Path) -> list[tuple[ContentType | None, str, str]]:
    sources = []
    for path in sorted(Path(prompts_dir).glob("*.md")):
        key = template_key(path)
        if key is not None:
            sources.append((key[0], key[1], path.read_text(encoding="utf-8").strip()))
    return sources


class PromptRepo:
    def __init__(self, session: AsyncSession, prompts_dir: str | Path) -> None:
        self.session = session
        self.prompts_dir = Path(prompts_dir)

    async def active(self, content_type: ContentType | None, name: str = DEFAULT_NAME) -> Template:
        row = await self.session.scalar(
            select(PromptTemplate).where(
                PromptTemplate.content_type.is_not_distinct_from(content_type),
                PromptTemplate.name == name,
                PromptTemplate.is_active.is_(True),
            )
        )
        if row is not None:
            return Template(content_type, name, row.version, row.body)
        filename = f"{SYSTEM_PREFIX}{name}.md" if content_type is None else f"{content_type}.md"
        path = self.prompts_dir / filename
        if not path.exists():
            raise LookupError(f"no prompt template for {content_type}/{name}")
        return Template(content_type, name, 0, path.read_text(encoding="utf-8").strip())

    async def system(self, name: str) -> Template:
        return await self.active(None, name)


async def seed_templates(session: AsyncSession, prompts_dir: str | Path) -> int:
    """Inserts a new active version for every changed source. Returns how many."""
    inserted = 0
    for content_type, name, body in template_sources(prompts_dir):
        key = (
            PromptTemplate.content_type.is_not_distinct_from(content_type),
            PromptTemplate.name == name,
        )
        active = await session.scalar(
            select(PromptTemplate).where(*key, PromptTemplate.is_active.is_(True))
        )
        if active is not None and active.body == body:
            continue
        latest = await session.scalar(select(func.max(PromptTemplate.version)).where(*key))
        await session.execute(update(PromptTemplate).where(*key).values(is_active=False))
        session.add(
            PromptTemplate(
                content_type=content_type,
                name=name,
                version=(latest or 0) + 1,
                body=body,
                is_active=True,
            )
        )
        await session.flush()
        inserted += 1
    return inserted
