# syntax=docker/dockerfile:1
# Multi-stage build (RFC 0001 §3.1.1.12). Build context is the repo root.
# One image, three processes (RFC 0001 §3.1.1.5): the API (default CMD),
# indexer-worker (`python -m app.v1.search.consumers.worker`) and assistant-celery.

FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

FROM base AS builder
COPY common /src/common
COPY services/assistant/pyproject.toml /src/services/assistant/pyproject.toml
RUN python -m venv /venv \
 && /venv/bin/pip install --no-cache-dir "/src/common[events,celery]" /src/services/assistant

FROM base AS runtime
RUN useradd --uid 10001 --create-home app
COPY --from=builder /venv /venv
WORKDIR /app
COPY services/assistant/main.py /app/main.py
COPY services/assistant/app /app/app
COPY services/assistant/alembic.ini /app/alembic.ini
COPY services/assistant/alembic /app/alembic
# Prompt template sources, seeded into prompt_templates by `python -m app.seed` (AST-D26).
COPY ops/prompts /app/prompts
USER app
ENV PATH=/venv/bin:$PATH \
    PORT=8007 \
    PROMPTS_DIR=/app/prompts
EXPOSE 8007
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=6 \
  CMD python -c "import os,urllib.request; urllib.request.urlopen(f'http://127.0.0.1:{os.environ[\"PORT\"]}/health', timeout=2)"
# Shell form so $PORT expands; exec so uvicorn receives SIGTERM and drains.
CMD exec uvicorn --factory main:create_application --host 0.0.0.0 --port "$PORT" --loop uvloop --timeout-graceful-shutdown 10
