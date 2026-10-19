"""Database wiring over ``smartretail_common.db``: one async engine per process.

Vector columns use ``pgvector.sqlalchemy.Vector``, which converts to and from
the text form, so asyncpg needs no extra codec.
"""

from app.core.config import Settings
from smartretail_common.db import Database, get_session

# Request-scoped session dependency (reads the Database from app.state.db).
get_db = get_session


def create_database(settings: Settings) -> Database:
    return Database(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
    )
