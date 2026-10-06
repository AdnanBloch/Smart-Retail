"""Every revision has a working downgrade (S14), and models match head."""

import os
import subprocess
import sys

from tests.integration.conftest import SERVICE_DIR


def _alembic(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        [sys.executable, "-m", "alembic", *args],
        cwd=SERVICE_DIR,
        env={**os.environ, "DATABASE_URL": database_url},
        capture_output=True,
        text=True,
    )


def test_head_base_head_and_no_pending_diff(database_url) -> None:
    for args in (("downgrade", "base"), ("upgrade", "head"), ("check",)):
        result = _alembic(database_url, *args)
        assert result.returncode == 0, result.stderr
