# syntax=docker/dockerfile:1
# outbox-relay (RFC 0001 §3.1.1.2). Its code is the platform library
# (smartretail_common.events.relay); this image is that code plus the Kafka
# client. It also runs `make topics` (scripts/ is mounted read-only).

FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

FROM base AS builder
COPY common /src/common
RUN python -m venv /venv \
 && /venv/bin/pip install --no-cache-dir "/src/common[events]"

FROM base AS runtime
RUN useradd --uid 10001 --create-home app
COPY --from=builder /venv /venv
USER app
ENV PATH=/venv/bin:$PATH
CMD ["python", "-m", "smartretail_common.events.relay"]
