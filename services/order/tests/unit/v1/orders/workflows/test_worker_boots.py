"""The worker process imports only what its activities reach. Every mapper must
still configure, or the first activity fails in production while tests that
build the whole app pass."""

import subprocess
import sys
from pathlib import Path

SERVICE_DIR = Path(__file__).resolve().parents[5]


def test_worker_module_configures_every_mapper() -> None:
    code = (
        "import app.v1.orders.workflows.worker\n"
        "from sqlalchemy.orm import configure_mappers\n"
        "configure_mappers()\n"
    )
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", code], cwd=SERVICE_DIR, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
