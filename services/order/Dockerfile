# syntax=docker/dockerfile:1
# Multi-stage build (RFC 0001 §3.1.1.12). Build context is the repo root.

FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

FROM base AS builder
COPY common /src/common
COPY services/order/pyproject.toml /src/services/order/pyproject.toml
RUN python -m venv /venv \
 && /venv/bin/pip install --no-cache-dir /src/common /src/services/order

FROM base AS runtime
RUN useradd --uid 10001 --create-home app
COPY --from=builder /venv /venv
WORKDIR /app
COPY services/order/main.py /app/main.py
COPY services/order/app /app/app
COPY services/order/alembic.ini /app/alembic.ini
COPY services/order/alembic /app/alembic
USER app
ENV PATH=/venv/bin:$PATH \
    PORT=8003
EXPOSE 8003
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=6 \
  CMD python -c "import os,urllib.request; urllib.request.urlopen(f'http://127.0.0.1:{os.environ[\"PORT\"]}/health', timeout=2)"
# Shell form so $PORT expands; exec so uvicorn receives SIGTERM and drains.
CMD exec uvicorn --factory main:create_application --host 0.0.0.0 --port "$PORT" --loop uvloop --timeout-graceful-shutdown 10
