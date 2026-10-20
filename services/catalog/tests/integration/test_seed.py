"""``python -m app.seed`` (``make seed``): the category tree, idempotent, and never
overwriting a category an admin already changed."""

from sqlalchemy import select

from app.core.config import Settings
from app.models import Category
from app.seed import CATEGORIES, seed
from tests.integration.conftest import create_category


async def _tree(app) -> dict[str, tuple[str, str | None]]:
    async with app.state.db.sessionmaker() as s:
        rows = list(await s.scalars(select(Category)))
    by_id = {c.id: c.slug for c in rows}
    return {c.slug: (c.name, by_id.get(c.parent_id)) for c in rows}


async def test_seed_builds_the_tree_and_is_idempotent(app, database_url) -> None:
    settings = Settings(database_url=database_url)
    await seed(settings)
    first = await _tree(app)
    assert first == {slug: (name, parent) for name, slug, parent in CATEGORIES}
    await seed(settings)
    assert await _tree(app) == first


async def test_seed_leaves_existing_slugs_alone(app, client, callers, database_url) -> None:
    await create_category(client, callers, "apparel")  # named "Apparel" by the helper
    async with app.state.db.sessionmaker() as s:
        existing = await s.scalar(select(Category).where(Category.slug == "apparel"))
        existing.name = "Clothing"
        await s.commit()
    await seed(Settings(database_url=database_url))
    tree = await _tree(app)
    assert tree["apparel"] == ("Clothing", None)
    # Children attach to the existing row, not a duplicate.
    assert tree["apparel-men"] == ("Men", "apparel")
    assert len(tree) == len(CATEGORIES)
