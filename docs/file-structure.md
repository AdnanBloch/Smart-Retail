# File Structure

This document is the single map of the SmartRetail repository: what every
top-level folder holds, how `common` is laid out, and the one layout that
every service follows. It describes structure only. Why each piece exists is in
the RFCs, which are linked from each section.

The implementation is being built service by service, so some paths below do
not exist yet. Paths marked **(planned)** are fixed by an RFC but have not been
written. Everything else exists in the repository today.

## Contents

1. [Repository root](#1-repository-root)
2. [docs/](#2-docs)
3. [common](#3-common)
4. [Service layout (every service)](#4-service-layout-every-service)
5. [Per-service differences](#5-per-service-differences)
6. [workers/](#6-workers)
7. [ops/, scripts/ and tests/](#7-ops-scripts-and-tests)
8. [Generated and ignored files](#8-generated-and-ignored-files)

## 1. Repository root

```
SmartRetail/
├── docker-compose.yml     # every container, grouped by Compose profile
├── Makefile               # the single entry point: keys, up, migrate, seed, test, lint …
├── .env.example           # every variable, with safe local defaults
├── .env                   # local copy of .env.example (gitignored)
├── .dockerignore
├── .gitignore
├── pyproject.toml         # workspace-wide ruff, mypy and pytest config
├── README.md              # overview, setup, API tour
├── keys/                  # RS256 key pair from `make keys` (gitignored)
├── docs/                  # specification documents and RFCs (§2)
├── common/                # shared package smartretail_common (§3)
├── services/              # one folder per service, all with the same layout (§4)
│   ├── gateway/
│   ├── identity/
│   ├── catalog/
│   ├── order/             # (planned)
│   ├── inventory/         # (planned)
│   ├── notification/      # (planned)
│   ├── analytics/         # (planned)
│   └── assistant/         # (planned)
├── workers/               # (planned) platform process hosts (§6)
├── ops/                   # infrastructure config (§7)
├── scripts/               # developer scripts (§7)
└── tests/
    └── e2e/               # end-to-end tests through the gateway (§7)
```

Source: [RFC 0001 §3.1.1.12](rfcs/0001-platform-and-shared-infrastructure.md#31112-local-environment--compose-makefile-dockerfiles).

## 2. docs/

```
docs/
├── README.md                  # index of the documents and RFCs
├── file-structure.md          # this document
├── part-a.md                  # assignment specification, part A
├── part-b.md                  # assignment specification, part B
├── milestones.md              # milestone plan
├── execution-guidelines.md    # how the work is carried out
├── learning-path.md
├── RFC template.pdf
└── rfcs/
    ├── 0000-architecture.md                       # whole-platform rules; read first
    ├── 0001-platform-and-shared-infrastructure.md # common, workers, infra, Compose
    ├── 0002-gateway-service.md
    ├── 0003-identity-service.md
    ├── 0004-catalog-service.md
    ├── 0005-order-service.md
    ├── 0006-inventory-service.md
    ├── 0007-notification-service.md
    ├── 0008-analytics-service.md
    ├── 0009-assistant-service.md
    └── archive/               # earlier milestone-based RFCs, kept for reference
```

RFCs are organised by service, not by milestone: 0000 is the architecture,
0001 the shared platform, and 0002–0009 one per service.

## 3. common

The shared library, imported by every service as `smartretail_common`. It holds
code that must behave the same everywhere. It never holds business rules.

```
common/
├── pyproject.toml
├── smartretail_common/
│   ├── __init__.py
│   ├── py.typed          # marks the package as typed for mypy
│   ├── app.py            # wiring every service applies to its app (context middleware, error handlers)
│   ├── auth.py           # JWT verification (RS256), Principal, current_principal
│   ├── config.py         # BaseServiceSettings (pydantic-settings); fail-fast validation
│   ├── context.py        # request contextvars: request/trace id, client id, principal
│   ├── logging.py        # JSON logging with trace ids on every line
│   ├── errors.py         # Problem model (RFC 9457), DomainError hierarchy, handlers
│   ├── pagination.py     # limit/offset params (ceiling 100) + Page[T] envelope
│   ├── db.py             # async engine, session dependency, unit-of-work helper
│   ├── db_types.py       # DomainEnumText — stores a DomainEnum as TEXT
│   ├── enums.py          # DomainEnum base + UnknownEnumValueError
│   ├── health.py         # /health and /health/ready routers
│   ├── testing.py        # test helpers: RSA key pairs and token minting
│   ├── service_token.py  # (planned) client-credentials token client for service→service calls
│   ├── idempotency.py    # (planned) Idempotency-Key dependency
│   ├── outbox.py         # (planned) outbox writer, used in the caller's transaction
│   ├── events/           # (planned) registry, envelope, Avro serde, producer,
│   │                     #           EventConsumer base, dedupe, schemas/*.avsc
│   ├── tracing.py        # (planned) OpenTelemetry setup
│   ├── metrics.py        # (planned) OTel meter provider + Prometheus reader
│   ├── temporal.py       # (planned) Temporal client factory
│   ├── celery_app.py     # (planned) Celery app factory and routing
│   └── redis.py          # (planned) Redis client, Cache, RateLimiter
└── tests/
    ├── test_auth.py
    ├── test_enums.py
    ├── test_errors_pagination.py
    └── test_r8_guard.py
```

Source: [RFC 0001 §3.1.1.1](rfcs/0001-platform-and-shared-infrastructure.md#3111-common).

## 4. Service layout (every service)

Every service under `services/<svc>/` uses the same layout, so moving between
services needs no re-orientation. Code is grouped **by entity**: everything
about products sits in `app/products/`. Only the HTTP contract is versioned, in
`app/<entity>/v<N>/`. The tree below is the full template; a service includes
only the folders it needs (see §5).

```
services/<svc>/
├── Dockerfile
├── pyproject.toml            # the service's package and dependencies
├── main.py                   # create_application(): settings, logging, DB, router, /health
├── alembic.ini               # only for services that own a Postgres database
├── alembic/                  # this service's own migration history
│   ├── env.py
│   ├── script.py.mako
│   └── versions/
│       └── NNNN_<change>.py  # e.g. 0001_users.py, 0002_merchant_profiles.py
├── app/
│   ├── __init__.py
│   ├── router.py             # every URL: includes entity routers, owns /api/v<N> + entity prefixes
│   ├── models.py             # imports every entity's ORM models onto one metadata (Alembic, seed)
│   ├── seed.py               # idempotent seed data (`python -m app.seed`)
│   ├── core/                 # cross-entity infrastructure, no business rules
│   │   ├── config.py         # the service's settings class (extends BaseServiceSettings)
│   │   ├── db.py             # Database factory and the get_db session dependency
│   │   ├── logger.py         # JSON logging setup
│   │   ├── dependencies.py   # app state, role guards, principal
│   │   ├── security.py       # service-specific security (password hashing, key ring)
│   │   ├── errors.py         # this service's DomainError subclasses (problem types)
│   │   ├── enums.py          # value sets shared by several entities (DomainEnum)
│   │   ├── schemas.py        # Pydantic bases every versioned schema builds on
│   │   └── utils.py          # small pure helpers shared by the entities
│   ├── health/
│   │   └── routes.py         # /health and /health/ready
│   ├── <entity>/             # one folder per entity, e.g. products/, users/
│   │   ├── models.py         # SQLAlchemy ORM: one shape per table, never versioned
│   │   ├── repo.py           # <Entity>Repo: business rules; the only layer allowed to hold them
│   │   ├── dependencies.py   # builds the entity's repos for a request
│   │   ├── enums.py          # the entity's own value sets (when it has them)
│   │   ├── utils.py          # state machine (transition table) and pure helpers; no I/O
│   │   └── v1/
│   │       ├── routes.py     # `router = APIRouter()` + decorated handlers: parse, call a repo, serialise
│   │       └── schemas.py    # Pydantic request/response models: the v1 contract
│   ├── clients/              # outbound calls to other services
│   ├── consumers/            # (planned) Kafka event handlers; thin, like routes
│   ├── workflows/            # (planned) Temporal workflows and activities
│   └── tasks/                # (planned) Celery tasks
└── tests/
    ├── conftest.py
    ├── unit/                 # no database; mirrors app/: unit/<entity>/test_<topic>.py
    └── integration/          # real PostgreSQL, migrated to head per suite
        ├── conftest.py
        ├── <entity>/test_<topic>.py
        └── test_migrations.py   # head → base → head and `alembic check`
```

An entity with several repos names them `<topic>_repo.py` (catalog's
`products/listing_repo.py`, identity's `auth/session_repo.py`).

The container runs `uvicorn --factory main:create_application`. Tests build
the app the same way: `from main import create_application`.

**Why entity-first with the version inside.** A table has one shape at a time,
so its ORM model and the rules that guard it cannot be versioned; only the HTTP
contract (paths, request and response bodies) can. v2 is a new `<entity>/v2/`
folder plus one line in `app/router.py`, and both versions call the same repo.
See [the review record](reviews/2026-10-06-mentor-review.md#3-entity-first-service-layout-with-versions)
and decision ARC-D in RFC 0000 §3.2.1.

**Layering rules**

- `v<N>/routes.py` handles HTTP only and never touches the ORM.
- `app/router.py` only includes routers. It owns the version prefix and each
  entity's prefix, so every URL the service serves can be read from one file.
- `repo.py` is the only place business rules live. Routes, event consumers,
  Temporal activities and CLI commands all call the repos, so each rule has one
  implementation and one set of tests.
- Repos never build HTTP responses. They raise `DomainError`s, and the shared
  handlers turn them into problem details.
- `v<N>/schemas.py` is the contract. ORM objects are never returned directly.
- `utils.py` state machines and helpers do no I/O.
- An entity may import another entity's repo or models in the same service; a
  service never imports another service's code. Shared code goes in `common`.

**Routes**

```python
# app/products/v1/routes.py: one router, decorated handlers
router = APIRouter()

@router.get("/me", response_model=Page[ProductSummary])
async def my_products(...): ...

@router.get("/{product_id}", response_model=ProductDetail)
async def get_product(
    product_id: uuid.UUID, repo: ProductRepo = Depends(product_repo)
) -> ProductDetail:
    return await repo.get(product_id)

# app/router.py: includes only
v1 = APIRouter(prefix="/api/v1")
v1.include_router(products_v1.router, prefix="/products", tags=["products"])
router.include_router(v1)
```

- Paths in `routes.py` are relative to the entity prefix. A literal segment
  (`/me`) is declared before `/{id}`.
- Each decorator carries all of that route's HTTP metadata: `response_model`,
  `status_code`, `dependencies=[...]` and `responses={...}`.
- A routes module may hold a small request-parsing dependency its handlers
  share (e.g. `product_filters`). Anything else belongs in `dependencies.py`,
  `schemas.py` or `repo.py`.
- `/health` and `/health/ready` are mounted at the root by `main.py`, outside
  `/api/v1`.

**Naming**

- Entity folders are plural (`products/`, `users/`); ORM classes are singular
  (`Product`, `User`). Repos are classes `<Entity>Repo`.
- Migrations are numbered `NNNN_<change>.py`.
- Tests mirror `app/`: `tests/unit/products/test_state_machine.py`.

`services/catalog/` is the best example of the full layout today.

Source: [RFC 0000 §3.1.1.7](rfcs/0000-architecture.md#3117-module-structure-inside-a-service).

## 5. Per-service differences

Each service uses the §4 template. Only the differences are listed here.

| Service | Store | Entities | Differences from the template | RFC |
| --- | --- | --- | --- | --- |
| `gateway` | none | `health/`, `docs/`, `proxy/` | Owns no API contract, so no `v<N>/` folders, `alembic/`, `models.py` or repos. `docs/` serves the merged OpenAPI and Swagger UI; `proxy/` holds the route table (`routing.py`), forwarding, upstream errors and JWT checks (`auth.py`) and the catch-all route. `core/cors.py` holds CORS (later correlation, size limit, rate limit). | [0002](rfcs/0002-gateway-service.md) |
| `identity` | Postgres | `auth/`, `users/`, `merchants/` | `auth/` has several repos (`authentication_repo.py`, `registration_repo.py`, `session_repo.py`, `token_repo.py`). `merchants/utils.py` holds the merchant transition table. | [0003](rfcs/0003-identity-service.md) |
| `catalog` | Postgres | `products/`, `variants/`, `categories/`, `media/` | `products/` adds `state_machine.py`, `access.py` (visibility and ownership), `listing_repo.py` and `audit_repo.py`. Later adds `workflows/` (publishing on Temporal), `tasks/` (media processing on Celery) and `consumers/`. | [0004](rfcs/0004-catalog-service.md#3114-workflows-and-async-jobs) |
| `order` | Postgres | (planned) `orders/` (orders, items, status history); `idempotency_keys` in `core/` | Adds `workflows/` (order saga). Consumes no events, so no `consumers/`; the carrier webhook is its own `webhooks/v1/`. | [0005](rfcs/0005-order-service.md#3114-workflows-and-async-jobs) |
| `inventory` | Postgres | (planned) `stock/` (stock items, movements), `reservations/` | Adds `consumers/` and the activities the order saga calls. | [0006](rfcs/0006-inventory-service.md#3114-workflows-and-async-jobs) |
| `notification` | Postgres | (planned) `notifications/` (notifications, delivery attempts), `subscriptions/` (push) | Adds `consumers/`, `tasks/` (delivery) and `stream/` (SSE). | [0007](rfcs/0007-notification-service.md#3114-workflows-and-async-jobs) |
| `analytics` | MongoDB | (planned) `metrics/` (counters, rollups, product and failure stats) | No `alembic/` (an idempotent init command creates the indexes). Built around `consumers/`. | [0008](rfcs/0008-analytics-service.md#3114-workflows-and-async-jobs) |
| `assistant` | Postgres + pgvector | (planned) `chat/` (conversations, messages), `search/` (chunks, embedding cache, index state), `generations/`, `prompts/` | Also provides the `indexer-worker` entry point. Adds `consumers/` and `tasks/`. | [0009](rfcs/0009-assistant-service.md#3114-workflows-and-async-jobs) |

For the exact workflows, tasks and consumers of each service, see that
service's RFC §3.1.1.4.

## 6. workers/

**(planned)** These are process hosts for platform code. They contain no domain
logic and import their workflows and tasks from the services.

```
workers/
├── outbox_relay/      # moves outbox rows to Kafka (platform code, no domain imports)
├── temporal_worker/   # worker bootstrap; imports services/*/app/workflows
└── celery_worker/     # worker bootstrap; imports services/*/app/tasks
```

Source: [RFC 0001 §3.1.1.2–3.1.1.5](rfcs/0001-platform-and-shared-infrastructure.md#3112-outbox-relay).

## 7. ops/, scripts/ and tests/

```
ops/
├── postgres/init/
│   └── 01-databases.sh        # databases, roles and grants
├── kafka/                     # (planned) topic definitions for `make topics`
├── otel-collector/config.yaml # (planned)
├── prometheus/prometheus.yml  # (planned)
├── grafana/{datasources,dashboards}/  # (planned)
└── prompts/                   # (planned) prompt templates seeded into assistant_db

scripts/
├── keys.sh                    # RS256 key generation (called by `make keys`)
├── topics.py                  # (planned)
├── dlq_replay.py              # (planned)
└── seed.py                    # (planned)

tests/
└── e2e/
    └── test_smoke.py          # Milestone 1 journey through the gateway
```

Unit and integration tests live inside each package (`common/tests/`,
`services/<svc>/tests/`). Only cross-service tests go in `tests/e2e/`.

## 8. Generated and ignored files

None of these are source. Do not edit them or commit them.

| Path | What it is |
| --- | --- |
| `.venv/` | local virtual environment |
| `.env`, `keys/` | local secrets and key pair |
| `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, `__pycache__/` | tool caches (at the root and inside packages) |
| `*.egg-info/` | editable-install metadata |
