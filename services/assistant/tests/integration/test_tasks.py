"""The Celery task bodies, called directly (no broker): Beat sweeps, reindex, bulk."""

import uuid

import pytest

import app.core.tasks.maintenance as core_tasks
import app.v1.chat.tasks.maintenance as chat_tasks
import app.v1.generations.tasks.bulk as bulk_tasks
import app.v1.search.reindex as reindex
import app.v1.search.tasks.maintenance as search_tasks
from app.core.celery_app import app as celery


@pytest.fixture
def patched(settings, monkeypatch):
    for module in (core_tasks, chat_tasks, search_tasks, bulk_tasks):
        monkeypatch.setattr(module, "get_settings", lambda: settings)
    return settings


def test_task_registry_and_schedule():
    names = set(celery.tasks)
    for task in (
        "sweep_processed_events",
        "sweep_anonymous_conversations",
        "anonymise_query_log",
        "run_reindex",
        "generate_bulk_content",
    ):
        assert f"smartretail.assistant.{task}" in names
    assert celery.conf.task_default_queue == "assistant.bulk"
    assert set(celery.conf.beat_schedule) == {
        "sweep-processed-events",
        "sweep-anonymous-conversations",
        "anonymise-query-log",
    }


def test_task_bodies_run(patched):
    assert core_tasks.sweep_processed_events() == 0
    assert chat_tasks.sweep_anonymous_conversations() >= 0
    assert search_tasks.anonymise_query_log() >= 0
    assert bulk_tasks.generate_bulk_content(str(uuid.uuid4())) is None
    report = search_tasks.run_reindex({"product_ids": [], "merchant_id": None})
    assert report == {"indexed": 0, "skipped": 0, "failed": 0}
    assert reindex.ReindexScope.from_dict({}).full
