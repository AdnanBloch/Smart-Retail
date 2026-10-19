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
│   ├── order/
│   ├── inventory/
│   ├── notification/
│   ├── analytics/
│   └── assistant/
├── workers/               # platform process hosts: outbox_relay (§6)
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
├── messaging-and-consistency.md   # Kafka partitioning, commit/retry, dedupe, atomicity, why each tool
├── business-flows.md              # every business flow end to end, with failure paths
├── architecture-diagrams.md       # top-level diagram + one component diagram per service
├── reviews/
│   ├── mentor-reviews.md          # every mentor review and what it changed
│   └── 2026-10-13-mentor-review.md  # full record of the 2026-10-13 review (version-first layout)
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
│   ├── service_token.py  # client-credentials token client for service→service calls
│   ├── idempotency.py    # Idempotency-Key canonicalisation and request hash
│   ├── outbox.py         # outbox writer (validates against the Avro schema), relay grant SQL
│   ├── events/           # the event pipeline (RFC 0001 §3.1.1.1–§3.1.1.2, PLT-D37–PLT-D41)
│   │   ├── catalogue.json    # every topic: key, cleanup policy, payload schema (data, not code: R8)
│   │   ├── schemas/*.avsc    # envelope.avsc + one payload schema per produced event
│   │   ├── registry.py       # catalogue loader; composes envelope + payload per topic
│   │   ├── serde.py          # Confluent wire format over fastavro; write-time validation
│   │   ├── schema_registry.py # registry REST client (lookup only) + in-memory registry
│   │   ├── envelope.py       # the envelope (RFC 0000 §3.1.2.4)
│   │   ├── producer.py       # idempotent producer config, Publisher protocol
│   │   ├── relay.py          # outbox-relay: SKIP LOCKED batches, ack-only marking, poison holds
│   │   ├── dedupe.py         # processed_events + claim + 30-day sweep
│   │   └── consumer.py       # EventConsumer: dedupe, retry, DLQ, manual offset commit
│   ├── tracing.py        # (planned) OpenTelemetry setup
│   ├── metrics.py        # (planned) OTel meter provider + Prometheus reader
│   ├── temporal.py       # Temporal client factory, retry defaults, activity errors
│   ├── celery_app.py     # Celery app factory: acks_late, prefetch 1, <service>.maintenance queues
│   ├── redis.py          # Redis client and fail-open fixed-window rate limiter
│   ├── migrations.py     # DDL of the shared outbox / processed_events tables
│   └── client_id.py      # X-Client-Id shape check (C21)
└── tests/
    ├── test_auth.py
    ├── test_enums.py
    ├── test_errors_pagination.py
    └── test_r8_guard.py
```

Source: [RFC 0001 §3.1.1.1](rfcs/0001-platform-and-shared-infrastructure.md#3111-common).

## 4. Service layout (every service)

Every service under `services/<svc>/` uses the same layout, so moving between
services needs no re-orientation. The **version comes first**: `app/v1/` holds
every entity, complete (everything about products sits in `app/v1/products/`).
A later version, `app/v<N>/`, holds only the files it changes and imports the
rest from v1 by name. The tree below is the full template; a service includes
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
│   ├── core/                 # shared by two or more entities only; no business rules
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
│   │   └── routes.py         # /health and /health/ready (not versioned)
│   ├── v1/                   # the base version: every entity, complete
│   │   └── <entity>/         # one folder per entity, e.g. products/, users/
│   │       ├── models.py         # SQLAlchemy ORM: one shape per table, only ever in v1
│   │       ├── repo.py           # <Entity>Repo: business rules; the only layer allowed to hold them
│   │       ├── dependencies.py   # builds the entity's repos for a request
│   │       ├── enums.py          # the entity's own value sets (when it has them)
│   │       ├── state_machine.py  # transition table and transition(), when the entity has a status
│   │       ├── utils.py          # pure helpers; no I/O
│   │       ├── clients/          # outbound calls to other services this entity makes
│   │       ├── providers/        # external-provider protocols and fakes this entity uses
│   │       ├── workflows/        # Temporal: types.py, the workflow, activities.py (thin, call
│   │       │                     #   the repos), client.py (start/query/signal), worker.py
│   │       ├── consumers/        # Kafka event handlers for this entity; thin, like routes
│   │       ├── tasks/            # Celery tasks for this entity
│   │       ├── routes.py         # the handlers: `router = APIRouter()` + decorated handlers
│   │       └── schemas.py        # Pydantic request/response models: the v1 contract
│   └── v2/                   # (when one exists) only the files v2 changes
│       └── <entity>/
│           ├── routes.py         # v2's handlers; import what is unchanged from app.v1.<entity>
│           ├── schemas.py        # the v2 contract
│           └── repo.py           # only if v2's business rules differ
└── tests/
    ├── conftest.py
    ├── unit/                 # no database; mirrors app/: unit/v1/<entity>/[workflows/]test_<topic>.py
    └── integration/          # real PostgreSQL, migrated to head per suite
        ├── conftest.py
        ├── v1/<entity>/test_<topic>.py
        └── test_migrations.py   # head → base → head and `alembic check`
```

An entity with several repos names them `<topic>_repo.py` (catalog's
`v1/products/listing_repo.py`, identity's `v1/auth/session_repo.py`).

**Everything one entity uses lives under it** (decision ARC-D27). A workflow,
client, provider, consumer or task sits in the folder of the entity whose state
it changes. It moves up to `core/` only when a second entity starts using it.
See [the 2026-10-12 review record](reviews/mentor-reviews.md#2-every-file-in-its-entity-folder).

The container runs `uvicorn --factory main:create_application`. Tests build
the app the same way: `from main import create_application`.

**Why the version comes first.** A version is the unit that changes: its URLs,
its handlers, its contract, and sometimes its business rules. With the version
first, each version is one folder, and the files in `v2/` are exactly what v2
changes. Nothing is copied: v2 imports what it keeps by name
(`from app.v1.orders.models import Order`). Models are never versioned, because
a table has one shape at a time. Temporal activities, consumers, Celery tasks
and CLIs have no HTTP version, so they live in `v1/<entity>/` and call v1's
repos. A version is retired only after the files later versions import from it
move to `core/` or to the next base version. See
[the 2026-10-13 review record](reviews/2026-10-13-mentor-review.md#1-version-first-layout)
and decision ARC-D23 in RFC 0000 §3.2.1.

**Layering rules**

- `v<N>/<entity>/routes.py` handles HTTP only and never touches the ORM.
- `app/router.py` only includes routers. It owns the version prefix and each
  entity's prefix, so every URL the service serves can be read from one file.
- `repo.py` is the only place business rules live. Routes, event consumers,
  Temporal activities and CLI commands all call the repos, so each rule has one
  implementation and one set of tests.
- Repos never build HTTP responses. They raise `DomainError`s, and the shared
  handlers turn them into problem details.
- `v<N>/<entity>/schemas.py` is the contract. ORM objects are never returned directly.
- `state_machine.py` and `utils.py` do no I/O.
- Workflow code holds sequence and compensation only. Activities are thin
  adapters over the repos: they open a session, call one repo method, and turn
  a 4xx `DomainError` into a non-retryable failure (`common.temporal`).
- An entity may import another entity's repo or models in the same service; a
  service never imports another service's code. Shared code goes in `common`.
- A later version imports from earlier versions only by explicit module path
  (`app.v1.<entity>.<module>`), never with `*`, so its dependencies on v1 can
  be read from its imports.

**Routes**

```python
# app/v1/products/routes.py: one router, decorated handlers
router = APIRouter()

@router.get("/me", response_model=Page[ProductSummary])
async def my_products(...): ...

@router.get("/{product_id}", response_model=ProductDetail)
async def get_product(
    product_id: uuid.UUID, repo: ProductRepo = Depends(product_repo)
) -> ProductDetail:
    return await repo.get(product_id)

# app/router.py: includes only
from app.v1.products import routes as products_v1

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
- Tests mirror `app/`: `tests/unit/v1/products/test_state_machine.py`.

`services/catalog/` is the best example of the full layout today.

Source: [RFC 0000 §3.1.1.7](rfcs/0000-architecture.md#3117-module-structure-inside-a-service).

## 5. Per-service differences

Each service uses the §4 template. Only the differences are listed here. Entity folders
named in the table sit under `app/v1/` (the gateway has no versioned entities).

| Service | Store | Entities | Differences from the template | RFC |
| --- | --- | --- | --- | --- |
| `gateway` | none | `health/`, `docs/`, `proxy/` | Owns no API contract, so no `v<N>/` folders, `alembic/`, `models.py` or repos. `docs/` serves the merged OpenAPI and Swagger UI; `proxy/` holds the route table (`routing.py`), forwarding, upstream errors and JWT checks (`auth.py`) and the catch-all route. `core/cors.py` holds CORS (later correlation, size limit, rate limit). | [0002](rfcs/0002-gateway-service.md) |
| `identity` | Postgres | `auth/`, `users/`, `merchants/` | `auth/` has several repos (`authentication_repo.py`, `registration_repo.py`, `session_repo.py`, `token_repo.py`). `merchants/utils.py` holds the merchant transition table. | [0003](rfcs/0003-identity-service.md) |
| `catalog` | Postgres | `products/`, `variants/`, `categories/`, `media/` | `products/` adds `state_machine.py`, `access.py` (visibility and ownership), `listing_repo.py`, `audit_repo.py` and `publishing_repo.py` (accepting a publish and every write the workflow makes). `products/workflows/` holds `ProductPublishingWorkflow`; `products/clients/inventory.py` serves `sync_downstream`. `media/` adds `storage.py` (MinIO) and `processing.py` (validation, derived sizes). Later adds `media/tasks/` (media processing on Celery) and a `consumers/` folder under the entity each consumer changes. | [0004](rfcs/0004-catalog-service.md#3114-workflows-and-async-jobs) |
| `order` | Postgres | `orders/` (orders, items, status history, `idempotency_keys`) | `orders/` adds `state_machine.py` (order, payment and shipment tables), `snapshot.py`, `events.py`, `shipment_repo.py`, `idempotency_repo.py` (the `Idempotency-Key` claim), `providers/` (payment and shipping protocols and fakes), `clients/` (catalog and inventory calls) and `workflows/` (the order saga). Consumes no events, so no `consumers/`; the carrier webhook (Milestone 3) is its own `v1/webhooks/`. | [0005](rfcs/0005-order-service.md#3114-workflows-and-async-jobs) |
| `inventory` | Postgres | `stock/` (stock items, movements), `reservations/` | `stock/` adds `ledger_repo.py` (the only writer of the ledger), `locking.py` (lock order, timeouts, bounded retry) and `events.py`; `reservations/` adds `state_machine.py` and the in-process `sweeper.py`. Internal routes are a second router per entity, mounted under `/internal/v1/inventory`. Later adds `consumers/`. | [0006](rfcs/0006-inventory-service.md#3114-workflows-and-async-jobs) |
| `notification` | Postgres | `notifications/` (notifications, delivery attempts), `subscriptions/` (push) | Adds `consumers/`, `tasks/` (delivery) and `stream/` (SSE). | [0007](rfcs/0007-notification-service.md#3114-workflows-and-async-jobs) |
| `analytics` | MongoDB | `metrics/` (counters, rollups, product and failure stats) | No `alembic/` (an idempotent init command creates the indexes). Built around `consumers/`. | [0008](rfcs/0008-analytics-service.md#3114-workflows-and-async-jobs) |
| `assistant` | Postgres + pgvector | `chat/` (conversations, messages), `search/` (chunks, embedding cache, index state), `generations/`, `prompts/` | Also provides the `indexer-worker` entry point. Adds `consumers/` and `tasks/`. | [0009](rfcs/0009-assistant-service.md#3114-workflows-and-async-jobs) |

For the exact workflows, tasks and consumers of each service, see that
service's RFC §3.1.1.4.

## 6. workers/

These are process hosts for platform code. They contain no domain logic.

```
workers/
└── outbox_relay/      # Dockerfile only: runs smartretail_common.events.relay; also
                       # runs `make topics` and `make dlq-replay` (scripts/ mounted)
```

Celery workers are not here either (PLT-D36). Each service runs its own from its
own image, `celery -A app.core.celery_app worker -B`: `inventory-celery` on
`inventory.maintenance` and `order-celery` on `order.maintenance`. Kafka
consumers likewise run from the service image: `inventory-consumer` is
`python -m app.core.consumer`.

Temporal workers are not here. Each service that owns a workflow runs its own
worker from its own image, `python -m app.v1.<entity>.workflows.worker`: `catalog-worker`
(`app.v1.products.workflows.worker`) on `catalog-publishing`, `order-worker`
(`app.v1.orders.workflows.worker`) on `order-lifecycle` (decision
PLT-D33). Every service's package is `app`, so one process could not import
two of them, and one role per process is then true by construction (PLT-1).

Source: [RFC 0001 §3.1.1.2–3.1.1.5](rfcs/0001-platform-and-shared-infrastructure.md#3112-outbox-relay).

## 7. ops/, scripts/ and tests/

```
ops/
├── postgres/init/
│   └── 01-databases.sh        # databases, roles and grants
├── mongo/                     # entrypoint, healthcheck and analytics user (make mongo-init)
├── otel-collector/config.yaml # (planned)
├── prometheus/prometheus.yml  # (planned)
├── grafana/{datasources,dashboards}/  # (planned)
└── prompts/                   # prompt templates seeded into assistant_db

scripts/
├── keys.sh                    # RS256 key generation (called by `make keys`)
├── topics.py                  # make topics / schemas-check: topics, DLQs, schema registration
├── dlq_replay.py              # make dlq-replay
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
