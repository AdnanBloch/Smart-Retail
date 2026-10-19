"""Builds the metrics repos for a request."""

from fastapi import Request

from app.v1.metrics.repo import MetricsRepo


def get_repo(request: Request) -> MetricsRepo:
    return MetricsRepo(request.app.state.mongo, request.app.state.settings)
