# File Structure

This document is the single map of the SmartRetail repository: what every
top-level folder holds, how `libs/common` is laid out, and the one layout that
every service follows. It describes structure only. Why each piece exists is in
the RFCs, which are linked from each section.

The implementation is being built service by service, so some paths below do
not exist yet. Paths marked **(planned)** are fixed by an RFC but have not been
written. Everything else exists in the repository today.

## Contents

1. [Repository root](#1-repository-root)
2. [docs/](#2-docs)
3. [libs/common](#3-libscommon)
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
├── libs/
│   └── common/            # shared library smartretail_common (§3)
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
    ├── 0001-platform-and-shared-infrastructure.md # libs/common, workers, infra, Compose
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

## 3. libs/common

The shared library, imported by every service as `smartretail_common`. It holds
code that must behave the same everywhere. It never holds business rules.

```
libs/common/
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

Source: [RFC 0001 §3.1.1.1](rfcs/0001-platform-and-shared-infrastructure.md#3111-libscommon).

## 4. Service layout (every service)

Every service under `services/<svc>/` uses the same layout, so moving between
services needs no re-orientation. It is the layout of the EVA backends
(`EVA-*-Service-BE`). The tree below is the full template; a service includes
only the folders it needs (see §5).

```
services/<svc>/
├── Dockerfile
├── pyproject.toml            # the service's package and dependencies
├── main.py                   # create_application(): settings, logging, DB, routers, /health
├── alembic.ini               # only for services that own a Postgres database
├── alembic/                  # this service's own migration history
│   ├── env.py
│   ├── script.py.mako
│   └── versions/
│       └── NNNN_<change>.py  # e.g. 0001_users.py, 0002_merchant_profiles.py
├── app/
│   ├── __init__.py
│   ├── api/
│   │   ├── router.py         # every route: path, method, tags, response_model, status_code
│   │   └── handlers/
│   │       ├── <resource>.py # `router = APIRouter()` + handlers: parse, validate, call a repo, serialise
│   │       ├── health.py     # readiness checks behind /health/ready
│   │       └── utils.py      # FastAPI dependencies (session, principal, repos)
│   ├── configs/
│   │   └── env.py            # the service's settings class (extends BaseServiceSettings)
│   ├── infrastructure/
│   │   ├── db.py             # Database factory and the get_db session dependency
│   │   ├── logger.py         # JSON logging setup
│   │   ├── security.py       # service-specific security (e.g. password hashing, key ring)
│   │   └── seed.py           # idempotent seed data (`python -m app.infrastructure.seed`)
│   ├── exception_handlers/
│   │   └── exception_handlers.py  # this service's DomainError subclasses
│   ├── models/
│   │   ├── enums.py          # this service's value sets (DomainEnum)
│   │   ├── response.py       # shared Pydantic bases and field types
│   │   └── <entity>.py       # SQLAlchemy ORM + that entity's Pydantic request/response models
│   ├── services/
│   │   └── <entity>_repo.py  # <Entity>Repo: business rules; the only layer allowed to hold them
│   ├── utils/
│   │   ├── state_machine.py  # the allowed-transition table
│   │   └── helpers.py        # small pure helpers
│   ├── middleware/           # ASGI middleware, when the service has its own
│   ├── clients/              # outbound calls to other services
│   ├── constants/            # fixed values
│   ├── consumers/            # (planned) Kafka event handlers; thin, like handlers
│   ├── workflows/            # (planned) Temporal workflows and activities
│   └── tasks/                # (planned) Celery tasks
└── tests/
    ├── conftest.py
    ├── unit/                 # no database: models, state machines, pure logic
    │   └── test_<topic>.py
    └── integration/          # real PostgreSQL, migrated to head per suite
        ├── conftest.py
        ├── test_<resource>.py
        └── test_migrations.py   # head → base → head and `alembic check`
```

The container runs `uvicorn --factory main:create_application`. Tests build
the app the same way: `from main import create_application`.

**Layering rules**

- The API layer (`api/`) handles HTTP only and never touches the ORM.
- `api/router.py` is the only file that registers routes. Each handler module
  owns one `router = APIRouter()`; its functions carry no decorators.
- `services/` is the only place business rules live. Handlers, event consumers,
  Temporal activities and CLI commands all call the repos, so each rule has one
  implementation and one set of tests.
- The service layer never builds HTTP responses. It raises `DomainError`s, and
  the shared handlers turn them into problem details.
- The Pydantic models in `models/` are the contract. ORM objects are never
  returned directly.
- `utils/` state machines and helpers do no I/O.
- A service never imports another service's code. Shared code goes in
  `libs/common`.

**Routes and handlers**

```python
# app/api/handlers/products.py: one router, plain handler functions
router = APIRouter()

async def get_product(
    product_id: uuid.UUID, repo: ProductRepo = Depends(product_repo)
) -> ProductDetail:
    return await repo.get(product_id)

# app/api/router.py: every route
router = APIRouter(prefix="/api/v1")

# Route: /products/
products.router.get("/me", response_model=Page[ProductSummary])(products.my_products)
products.router.get("/{product_id}", response_model=ProductDetail)(products.get_product)

router.include_router(products.router, prefix="/products", tags=["products"])
```

- `router.py` keeps one block per resource, headed by a `# Route:` comment, in
  registration order. A literal segment (`/me`) is registered before `/{id}`.
- Each registration carries all of that route's HTTP metadata: `response_model`,
  `status_code`, `dependencies=[...]` and `responses={...}`.
- Handler modules may also hold a small request-parsing dependency that their
  handlers share (e.g. `product_filters`). Anything else belongs in
  `api/handlers/utils.py`, `models/` or `services/`.
- `/health` and `/health/ready` are mounted at the root by `main.py`, outside
  `/api/v1`.

**Naming**

- Handler modules are named after the resource in plural (`products.py`).
  Models are singular (`product.py`, `user.py`). Repos are `<entity>_repo.py`
  with a class `<Entity>Repo` (`product_repo.py`, `ProductRepo`).
- Migrations are numbered `NNNN_<change>.py`.
- Test files mirror the area they cover: `test_<resource>.py`.

`services/identity/` is the best example of the full layout today.

Source: [RFC 0000 §3.1.1.7](rfcs/0000-architecture.md#3117-module-structure-inside-a-service).

## 5. Per-service differences

Each service uses the §4 template. Only the differences are listed here.

| Service | Store | Differences from the template | RFC |
| --- | --- | --- | --- |
| `gateway` | none | No `alembic/`, `models/`, `services/` or `exception_handlers/`. `api/handlers/` holds `health.py`, `docs.py` (merged OpenAPI, Swagger UI) and `proxy.py` (the catch-all route). `middleware/` (CORS; later correlation, size limit, rate limit), `clients/upstream/` (forwarding, upstream errors, OpenAPI aggregation, SSE relay) and `utils/routing.py` (route table, matching). | [0002](rfcs/0002-gateway-service.md) |
| `identity` | Postgres | Template as shown. `utils/merchant_status.py` holds the merchant transition table. | [0003](rfcs/0003-identity-service.md) |
| `catalog` | Postgres | `utils/access.py` (visibility and ownership checks). Adds `workflows/` (product publishing on Temporal), `tasks/` (media processing on Celery) and `consumers/`. | [0004](rfcs/0004-catalog-service.md#3114-workflows-and-async-jobs) |
| `order` | Postgres | Adds `workflows/` (order saga) and `consumers/`. | [0005](rfcs/0005-order-service.md#3114-workflows-and-async-jobs) |
| `inventory` | Postgres | Adds `consumers/` and the activities the order saga calls. | [0006](rfcs/0006-inventory-service.md#3114-workflows-and-async-jobs) |
| `notification` | Postgres | Adds `consumers/`, `tasks/` (delivery) and `stream/` (SSE). | [0007](rfcs/0007-notification-service.md#3114-workflows-and-async-jobs) |
| `analytics` | MongoDB | No `alembic/` (an idempotent init command creates the indexes). Built around `consumers/`. | [0008](rfcs/0008-analytics-service.md#3114-workflows-and-async-jobs) |
| `assistant` | Postgres + pgvector | Also provides the `indexer-worker` entry point. Adds `consumers/` and `tasks/`. | [0009](rfcs/0009-assistant-service.md#3114-workflows-and-async-jobs) |

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

Unit and integration tests live inside each package (`libs/common/tests/`,
`services/<svc>/tests/`). Only cross-service tests go in `tests/e2e/`.

## 8. Generated and ignored files

None of these are source. Do not edit them or commit them.

| Path | What it is |
| --- | --- |
| `.venv/` | local virtual environment |
| `.env`, `keys/` | local secrets and key pair |
| `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, `__pycache__/` | tool caches (at the root and inside packages) |
| `*.egg-info/` | editable-install metadata |
