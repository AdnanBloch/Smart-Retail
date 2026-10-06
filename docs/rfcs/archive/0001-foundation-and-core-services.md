# RFC 0001 — Foundation & Core Services

> **SUPERSEDED — archived Sep 29, 2026.** The RFC set was reorganised from milestone-based to architecture- and service-based. This document is kept for history only and is no longer maintained. Its content now lives in: [0001-platform-and-shared-infrastructure](../0001-platform-and-shared-infrastructure.md), [0002-gateway-service](../0002-gateway-service.md), [0003-identity-service](../0003-identity-service.md), [0004-catalog-service](../0004-catalog-service.md). See [docs/README.md](../../README.md).

| | |
| --- | --- |
| **Owner** | @muhammad.adnan |
| **Collaborators** | SarwanAhmed |
| **Status** | **DRAFT** · IN-REVIEW · DONE |
| **Created** | Sep 22, 2026 |
| **Related Epic or story** | Milestone 1 — Foundation & Core Services ([milestones.md](../../milestones.md)), Week 1 ([execution-guidelines.md](../../execution-guidelines.md)) |
| **Builds on** | [RFC 0000 — System Architecture](0000-system-architecture.md) · [RFC 0006 — Service Decomposition & Data Model](0006-service-decomposition-and-data-model.md) |

---

### Milestone source text (verbatim)

> **Week 1 — Foundation + Core Services**
>
> - Initial system design and setup
> - User & Course management (basic CRUD)
> - RBAC handling (student/instructor)
>
> **Expected Outcome**
> - Service structure defined
> - Database schema created
> - Basic APIs working
> - Local setup ready

Domain wording is mapped per [RFC 0000](0000-system-architecture.md) §11 —
*course* reads as *product*, *student / instructor* as *customer / merchant*,
with `admin` as the third role [Part A](../../part-a.md) §1 requires.

---

## Contents

1. [Title](#1-title)
2. [Overview](#2-overview)
   - 2.1. [Goals](#21-goals)
   - 2.2. [Non-goals](#22-non-goals)
3. [Solution Overview](#3-solution-overview)
   - 3.1. [Specification](#31-specification)
     - 3.1.1. [Architectural changes](#311-architectural-changes)
     - 3.1.2. [API changes](#312-api-changes)
     - 3.1.3. [Data/schema changes](#313-dataschema-changes)
     - 3.1.4. [Breaking changes](#314-breaking-changes)
     - 3.1.5. [Action Plan](#315-action-plan)
   - 3.2. [Rationale](#32-rationale)
   - 3.3. [Tradeoffs](#33-tradeoffs)
     - 3.3.1. [Challenges and Open Questions](#331-challenges-and-open-questions)
4. [Dependencies/blockers/known issues](#4-dependenciesblockersknown-issues)
5. [Alternate solutions](#5-alternate-solutions)
6. [Production readiness](#6-production-readiness)
   - 6.1. [Scalability and Fault tolerance](#61-scalability-and-fault-tolerance)
   - 6.2. [Security considerations](#62-security-considerations)
   - 6.3. [Deployment strategy](#63-deployment-strategy)
7. [Test Plan](#7-test-plan)
8. [Impact Analysis](#8-impact-analysis)
9. [Useful Links/References](#9-useful-linksreferences)
10. [Related tickets/issues](#10-related-ticketsissues)
11. [Notes](#11-notes)
12. [Comments](#12-comments)

---

## 1. Title

**Foundation & Core Services — service topology, user and catalog management, and role-based access control for SmartRetail.**

This RFC defines the starting shape of the SmartRetail backend: which services exist, how they are separated, what data each one owns, how a caller is authenticated and authorised, and how the whole thing runs on a developer laptop. It covers Milestone 1 only. The full eight-service target topology is set out in [RFC 0000](0000-system-architecture.md), and the reasoning that produced those boundaries — with the schema conventions every service below follows — in [RFC 0006](0006-service-decomposition-and-data-model.md). Everything the later milestones add — workflow orchestration, event streaming, background workers, observability stack, the GenAI layer — is explicitly out of scope here, but the structure proposed below is chosen so those can be added without a rewrite.

---

## 2. Overview

SmartRetail is a backend commerce platform ([Part A](../../part-a.md)) that has to serve online stores, mobile commerce, warehouse operations, and multi-branch retail from one core. The full system is event-driven and distributed. None of that can be built before there is a place to put it: services with clear ownership, a schema that the rest of the platform reads and writes, and an identity model that every later authorisation decision depends on.

This RFC proposes that foundation. It introduces three deployable services — **gateway**, **identity**, and **catalog** — each with its own database, running under Docker Compose, exposing versioned REST APIs, with JWT-based authentication and a three-role RBAC model (`customer`, `merchant`, `admin`).

### 2.1. Goals

| # | Goal | How it is verified |
| --- | --- | --- |
| G1 | A defined service structure with explicit ownership boundaries | Each service owns its schema; no cross-service database access anywhere in the codebase |
| G2 | User management: registration, login, token refresh, profile read/update | Endpoints in §3.1.2 return the documented responses; covered by tests |
| G3 | Product (catalog) management: CRUD for products, variants, categories | Endpoints in §3.1.2 return the documented responses; covered by tests |
| G4 | RBAC enforced on every non-public endpoint, for `customer` / `merchant` / `admin` | A role matrix test asserts allow/deny for each role against each endpoint |
| G5 | Database schema created and version-controlled | Alembic migrations run from empty database to current head in CI and locally |
| G6 | Local setup ready — one command brings the whole stack up | `make up` produces healthy containers; `make seed` loads demo data; API docs reachable |
| G7 | Ownership enforcement: a merchant can only modify the merchant's own catalog | Tests assert a merchant receives `403` on another merchant's product |
| G8 | A structure that Milestones 2–3 extend rather than replace | Reviewed at milestone 4; see §8 |

### 2.2. Non-goals

These are deliberately excluded from this RFC and this milestone.

| Excluded | Why / when |
| --- | --- |
| Product publishing workflow (Temporal) | Milestone 2. The `status` field and its state machine are defined here; the orchestrated transition to `published` is not implemented |
| Order creation, inventory reservation, payments, shipments | Milestone 2. No `order`, `inventory`, or `payment` service is created in this milestone |
| Kafka, Schema Registry, Celery, RabbitMQ | Milestone 3. No event is produced or consumed in Milestone 1 |
| Prometheus, Grafana, Jaeger, OpenTelemetry | Milestone 3. Milestone 1 ships structured JSON logs and correlation IDs only — the minimum that later tracing plugs into |
| NoSQL store | Milestone 3, when there is an actual workload for it (analytics / event-shaped data). Adding it now would be a technology in search of a requirement |
| Redis caching | Deferred. Introduced when a measured read path needs it, not before |
| Media/image upload and processing | Milestone 2, as part of the publishing workflow. Milestone 1 stores media **URLs** only |
| Promotional and pricing rules engine | Later. Milestone 1 stores a base price and currency |
| Frontend | Nothing in the specification asks for one. The API is the deliverable; OpenAPI docs are the interface |
| Kubernetes / cloud deployment | Out of scope for the whole assignment. Docker Compose is the target environment |
| Full-text / semantic product search | Milestone 3 (indexing) and Part B (semantic). Milestone 1 offers filtering and pagination on indexed columns |

---

## 3. Solution Overview

### 3.1. Specification

#### 3.1.1. Architectural changes

This is a greenfield build — the prior prototype has been removed and every decision in it is re-opened. "Changes" therefore means "what is introduced".

**Service topology.**

```
                    ┌──────────────────────────────────────────┐
   client ─────────▶│  gateway  (FastAPI, :8000)               │
   (HTTP/JSON)      │  • single public entry point             │
                    │  • verifies JWT signature + expiry       │
                    │  • injects X-Request-ID, X-User-*        │
                    │  • routes by path prefix                 │
                    └───────┬──────────────────────┬───────────┘
                            │                      │
              /api/v1/auth  │                      │  /api/v1/catalog
              /api/v1/users │                      │
                            ▼                      ▼
              ┌───────────────────────┐  ┌───────────────────────┐
              │ identity   (:8001)    │  │ catalog    (:8002)    │
              │ • users, credentials  │  │ • products, variants  │
              │ • roles, sessions     │  │ • categories, media   │
              │ • issues + signs JWT  │  │ • audit log           │
              │ • sole key holder     │  │ • verifies JWT only   │
              └──────────┬────────────┘  └──────────┬────────────┘
                         │                          │
                    ┌────▼─────┐               ┌────▼─────┐
                    │ identity │               │ catalog  │
                    │   _db    │               │   _db    │
                    └──────────┘               └──────────┘
                    └──────── one PostgreSQL 16 instance ──────┘
                       (separate logical DBs, separate users,
                        no cross-database grants)
```

**Component responsibilities.**

| Component | Owns | Explicitly does not |
| --- | --- | --- |
| `gateway` | Public surface, routing, JWT verification, correlation-ID generation, CORS, request/response logging, aggregated OpenAPI | Hold business logic or a database |
| `identity` | User records, password hashes, role assignment, refresh-token sessions, JWT signing | Know anything about products |
| `catalog` | Products, variants, categories, media references, catalog audit trail | Read the identity database; it trusts the `merchant_id` in the verified token |
| `libs/common` | Shared, non-domain code: JWT verification, settings base, logging config, error model, pagination helpers, DB session factory, and the `DomainEnum` / `DomainEnumText` pair that implements [RFC 0006](0006-service-decomposition-and-data-model.md) S4 | Contain any business rule belonging to a single service — including any *vocabulary*. `DomainEnum` is the mechanism; the values (`draft`, `confirmed`, …) live in each service's `app/domain/enums.py`. A grep of this package for a domain status literal must return nothing, and that grep is a unit test (R8) |

**Key architectural decisions.**

- **A1 — Separate services with a database each, from day one.** Boundaries are enforced by the deployment topology, not by developer discipline. Milestones 2–3 add `order`, `inventory`, `notification`, and `analytics` alongside these, and Part B adds `assistant` — eight in total ([RFC 0006](0006-service-decomposition-and-data-model.md) §3.1.1). Nothing has to be carved out of a monolith first.
- **A2 — One PostgreSQL *instance*, multiple logical databases.** Each service connects with its own role and can only see its own database. Isolation is real at the schema level; the operational cost of running several Postgres containers on a laptop is not paid. Splitting into separate instances later is a configuration change, not a code change.
- **A3 — No foreign keys across service boundaries.** `catalog.products.merchant_id` is a plain `UUID` with an index, not a reference to `identity.users.id`. Referential integrity across services is handled by the token being trusted and, from Milestone 3, by events.
- **A4 — Asymmetric JWT signing (RS256).** `identity` holds the private key and is the only component that can mint a token. `gateway` and `catalog` hold the public key and verify locally, with no network call to `identity` on the request path. There is no shared secret to leak, and identity is not a runtime dependency of every other service.
- **A5 — Stateless access tokens, stateful refresh tokens.** Access tokens are short-lived (15 min) and never checked against the database. Refresh tokens are long-lived (7 days), stored hashed in `identity_db`, and can be revoked. This is the standard trade of revocation latency for scale, and it is bounded by the access-token TTL.
- **A6 — The gateway verifies; each service re-verifies.** The gateway rejects malformed or expired tokens early, but `catalog` does not trust the gateway's word — it validates the same token itself. A service must be safe to call directly, since Milestone 3 introduces internal callers.
- **A7 — Async all the way down.** FastAPI with `async def` handlers, SQLAlchemy 2.0 async ORM, `asyncpg` driver. Mixing sync ORM calls into an async event loop is a performance failure mode that is far cheaper to avoid now than to unpick later.
- **A8 — Per-service Alembic migration history.** Each service owns and versions its own schema. There is no global migration ordering to coordinate.

**Repository layout.**

```
SmartRetail/
├── docker-compose.yml           # postgres + gateway + identity + catalog
├── Makefile                     # up, down, logs, migrate, seed, test, lint, fmt
├── .env.example                 # every variable, with safe local defaults
├── README.md                    # overview, architecture, setup, API tour
├── docs/
│   ├── rfcs/0001-foundation-and-core-services.md
│   └── … (supplied specification documents)
├── libs/
│   └── common/smartretail_common/
│       ├── auth.py              # JWT verify, current-user + role dependencies
│       ├── config.py            # pydantic-settings base
│       ├── db.py                # async engine + session dependency
│       ├── db_types.py          # DomainEnumText — maps a DomainEnum to TEXT (S4)
│       ├── enums.py             # DomainEnum base + UnknownEnumValueError (S4)
│       ├── errors.py            # problem-detail model, exception handlers
│       ├── logging.py           # JSON logs, request-id contextvar
│       └── pagination.py        # limit/offset params + page envelope
└── services/
    ├── gateway/app/…
    ├── identity/{app,alembic,tests}/…
    └── catalog/{app,alembic,tests}/…
```

Each service uses the same internal layering, so moving between them is not a re-orientation:

```
app/
├── main.py            # app factory, router mounting, handlers, /health
├── api/v1/            # routers — HTTP concerns only
├── core/              # settings, security, dependencies
├── db/                # session, base, seed
├── domain/            # enums.py — this service's value sets (S4); state machines
├── models/            # SQLAlchemy ORM models
├── schemas/           # Pydantic request/response models
└── services/          # business logic — the only layer that is allowed rules
```

Routers do not touch the ORM directly and the service layer does not construct HTTP responses. This is the seam that keeps Milestone 2's workflow code testable: a Temporal activity calls the same service function the router calls.

#### 3.1.2. API changes

All routes are versioned under `/api/v1`, served through the gateway at `http://localhost:8000`. Requests and responses are JSON. Errors follow RFC 9457 problem details (§ below). Every response carries `X-Request-ID`.

**identity — authentication**

| Method | Path | Auth | Role | Purpose |
| --- | --- | --- | --- | --- |
| `POST` | `/api/v1/auth/register` | public | — | Create an account. Body: `{email, password, full_name, role}` where `role ∈ {customer, merchant}`. `admin` cannot be self-assigned |
| `POST` | `/api/v1/auth/login` | public | — | Exchange credentials for tokens. Returns `{access_token, refresh_token, token_type, expires_in}` |
| `POST` | `/api/v1/auth/refresh` | public | — | Body `{refresh_token}`. Rotates the refresh token and returns a new pair |
| `POST` | `/api/v1/auth/logout` | bearer | any | Revokes the presented refresh token |

**identity — users**

| Method | Path | Auth | Role | Purpose |
| --- | --- | --- | --- | --- |
| `GET` | `/api/v1/users/me` | bearer | any | The caller's own profile |
| `PATCH` | `/api/v1/users/me` | bearer | any | Update `full_name`, `phone` |
| `POST` | `/api/v1/users/me/password` | bearer | any | Change password; requires the current one; revokes all other sessions |
| `GET` | `/api/v1/users` | bearer | `admin` | List users. Filters: `role`, `is_active`, `q`. Paginated |
| `GET` | `/api/v1/users/{user_id}` | bearer | `admin` | Read any user |
| `PATCH` | `/api/v1/users/{user_id}` | bearer | `admin` | Update role or `is_active` |
| `GET` | `/api/v1/merchants/{merchant_id}` | public | — | Public merchant profile — the display name the catalog needs |

**catalog — categories**

| Method | Path | Auth | Role | Purpose |
| --- | --- | --- | --- | --- |
| `GET` | `/api/v1/categories` | public | — | Category tree or flat list |
| `GET` | `/api/v1/categories/{id}` | public | — | One category |
| `POST` | `/api/v1/categories` | bearer | `admin` | Create. Body: `{name, slug, parent_id?}` |
| `PATCH` | `/api/v1/categories/{id}` | bearer | `admin` | Update |
| `DELETE` | `/api/v1/categories/{id}` | bearer | `admin` | Delete; rejected with `409` if products reference it |

**catalog — products**

| Method | Path | Auth | Role | Purpose |
| --- | --- | --- | --- | --- |
| `GET` | `/api/v1/products` | public | — | Browse. Only `published` products for anonymous and `customer` callers. Filters: `category_id`, `merchant_id`, `q`, `min_price`, `max_price`, `status` (privileged). Sort: `created_at`, `price`, `name`. Paginated |
| `GET` | `/api/v1/products/{id}` | public | — | One product with variants, media, category. Non-published visible only to the owning merchant or an admin |
| `POST` | `/api/v1/products` | bearer | `merchant`, `admin` | Create in `draft`. Body: `{sku, name, description, category_id, base_price, currency, attributes?}` |
| `PATCH` | `/api/v1/products/{id}` | bearer | owner `merchant`, `admin` | Partial update |
| `DELETE` | `/api/v1/products/{id}` | bearer | owner `merchant`, `admin` | Soft delete → `archived` |
| `POST` | `/api/v1/products/{id}/status` | bearer | owner `merchant`, `admin` | Body `{status}`. **Milestone 1 permits `draft ↔ inactive` and `→ archived` only.** `→ published` returns `501 Not Implemented` with a problem detail pointing at Milestone 2, because publishing is an orchestrated workflow, not a column write |
| `GET` | `/api/v1/products/{id}/history` | bearer | owner `merchant`, `admin` | Audit trail — who changed what, when |
| `GET` | `/api/v1/products/me` | bearer | `merchant` | The caller's own products, any status |

**catalog — variants and media**

| Method | Path | Auth | Role | Purpose |
| --- | --- | --- | --- | --- |
| `POST` | `/api/v1/products/{id}/variants` | bearer | owner `merchant`, `admin` | Add a variant. Body: `{sku, options: {size: "M", color: "red"}, price_override?}` |
| `PATCH` | `/api/v1/variants/{variant_id}` | bearer | owner `merchant`, `admin` | Update |
| `DELETE` | `/api/v1/variants/{variant_id}` | bearer | owner `merchant`, `admin` | Remove |
| `POST` | `/api/v1/products/{id}/media` | bearer | owner `merchant`, `admin` | Attach a media **URL** with `position` and `alt_text`. Binary upload is Milestone 2 |
| `DELETE` | `/api/v1/media/{media_id}` | bearer | owner `merchant`, `admin` | Remove |

**Operational**

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Liveness — on every service |
| `GET` | `/health/ready` | Readiness, including a database round-trip |
| `GET` | `/docs`, `/openapi.json` | Per-service OpenAPI; the gateway also serves an aggregated view |

**Conventions.**

- *Pagination envelope* — `{"items": [...], "total": 128, "limit": 20, "offset": 40}`. Limit/offset is chosen over cursors because catalog listings are filtered and sorted by arbitrary columns, where a cursor buys little; a cursor variant can be added for the high-volume paths in Milestone 3.
- *Errors* — RFC 9457: `{"type": "…/errors/product-not-found", "title": "Product not found", "status": 404, "detail": "…", "instance": "/api/v1/products/…", "request_id": "…"}`. Validation failures add a `violations` array. One error shape across every service means one error handler in `libs/common`.
- *Idempotency* — not introduced here. It belongs to order creation, which is Milestone 2. Catalog writes are either naturally idempotent (`PATCH`) or protected by the unique constraint on `(merchant_id, sku)`.

#### 3.1.3. Data/schema changes

**Every column of every table in the platform is specified in
[RFC 0006 §3.1.3](0006-service-decomposition-and-data-model.md#313-dataschema-changes).**
This section names only what this milestone creates or amends; it does not
restate columns, so a type has exactly one definition in the RFC set.

**Databases created:** `identity_db`, `catalog_db`. Both start from an empty
state; there is no migration of existing data.

| Table | Database | Holds | Columns |
| --- | --- | --- | --- |
| [`users`](0006-service-decomposition-and-data-model.md#users) | `identity_db` | Accounts, credentials, role, active flag | [0006](0006-service-decomposition-and-data-model.md#users) |
| [`merchant_profiles`](0006-service-decomposition-and-data-model.md#merchant_profiles) | `identity_db` | Public merchant identity and approval status, one-to-one with a `merchant` user | [0006](0006-service-decomposition-and-data-model.md#merchant_profiles) |
| [`refresh_sessions`](0006-service-decomposition-and-data-model.md#refresh_sessions) | `identity_db` | Hashed refresh tokens, expiry, revocation | [0006](0006-service-decomposition-and-data-model.md#refresh_sessions) |
| [`categories`](0006-service-decomposition-and-data-model.md#categories) | `catalog_db` | Self-referencing category tree | [0006](0006-service-decomposition-and-data-model.md#categories) |
| [`products`](0006-service-decomposition-and-data-model.md#products) | `catalog_db` | Definition, price, status, attributes, `version` | [0006](0006-service-decomposition-and-data-model.md#products) |
| [`product_variants`](0006-service-decomposition-and-data-model.md#product_variants) | `catalog_db` | Option combinations and price override | [0006](0006-service-decomposition-and-data-model.md#product_variants) |
| [`product_media`](0006-service-decomposition-and-data-model.md#product_media) | `catalog_db` | Image urls and ordering | [0006](0006-service-decomposition-and-data-model.md#product_media) |
| [`product_audit_log`](0006-service-decomposition-and-data-model.md#product_audit_log) | `catalog_db` | Per-change actor, diff, correlation id | [0006](0006-service-decomposition-and-data-model.md#product_audit_log) |

**Vocabularies introduced (S4):** `UserRole`, `MerchantStatus`, `ProductStatus`
— the last without `publishing`, which Milestone 2 adds as a vocabulary change
rather than a migration.

**Why no stock quantity in the catalog.** Inventory has multiple writers —
orders, returns, warehouse sync — and is exactly the consistency problem the
platform is meant to solve. A `quantity` column on `product_variants` would make
`catalog` a second writer on the same fact, which is the inconsistency
[Part A](../../part-a.md) names in its opening challenges. Stock is owned by
`inventory` from Milestone 2 ([RFC 0002](0002-core-business-logic-and-workflows.md)).

`merchant_id` on `products` is a **claim**, not a foreign key — `identity` owns
the user, and no foreign key crosses a service boundary
([RFC 0006](0006-service-decomposition-and-data-model.md#3136-cross-boundary-reference-kinds) R2).

**Product state machine (defined now, completed in Milestone 2).**

```
   create
     │
     ▼
  ┌───────┐  submit (M2: Temporal workflow) ┌───────────┐
  │ draft │ ──────────────────────────────▶ │ published │
  └───┬───┘ ◀────────────────────────────── └─────┬─────┘
      │              unpublish                    │
      │                                           │
      ▼                                           ▼
  ┌──────────┐                              ┌──────────┐
  │ inactive │ ───────────────────────────▶ │ archived │
  └──────────┘                              └──────────┘
```

Milestone 1 implements every transition on this diagram except the two involving `published`, which return `501` with a pointer to this RFC.

#### 3.1.4. Breaking changes

**None in the external sense.** SmartRetail has no released API, no consumers, and no production data. The prior prototype was deleted in full before this RFC, deliberately: it was written before the design was agreed, and keeping it would have meant defending decisions nobody had made on purpose.

Internally, the following are re-set relative to that prototype and should be read as intentional reversals rather than oversights:

| Was | Now | Why |
| --- | --- | --- |
| Eleven services scaffolded up front, most of them empty | Three services, each doing real work | A service with no behaviour is a maintenance cost with no benefit. Services arrive when the milestone that needs them does |
| A frontend service | None | No requirement asks for one; the API and its OpenAPI documentation are the deliverable |
| Full infrastructure stack in Compose from day one | Postgres + three services | Kafka, Temporal, and the observability stack arrive with the milestones that use them, so that each is introduced with a reason attached |
| Symmetric, shared-secret JWTs | RS256, single signer | A shared secret in every service means every service can mint a token |

Once Milestone 2 begins, the `POST /products/{id}/status` behaviour for `published` changes from `501` to an accepted, asynchronous response (`202`). That is the only planned contract change, and it is documented here in advance so no client codes against the `501`.

#### 3.1.5. Action Plan

Sequenced so that each step is reviewable on its own and nothing is blocked waiting on a decision.

| # | Task | Output | Depends on |
| --- | --- | --- | --- |
| T1 | Repository skeleton, tooling, `.env.example`, Makefile, ruff + mypy config, pytest config | `make lint` and `make test` run against an empty suite | — |
| T2 | `libs/common`: settings base, async DB session, JSON logging with request-ID contextvar, problem-detail errors, pagination, `DomainEnum` base and `DomainEnumText` (S4) | Importable package, unit-tested. The S4 pair has tests for strict write, tolerant read, and the R8 vocabulary grep | T1 |
| T3 | Compose: Postgres 16 with a database-per-service init script, healthchecks, named volume | `docker compose up postgres` healthy; both databases and roles exist | T1 |
| T4 | `identity` skeleton + Alembic + `users`, `merchant_profiles`, `refresh_sessions` migrations | `make migrate` takes an empty database to head | T2, T3 |
| T5 | Password hashing (Argon2id), RS256 key loading, token issue/verify in `libs/common` | Unit tests for sign/verify, expiry, tampering, wrong key | T2 |
| T6 | Auth endpoints: register, login, refresh with rotation, logout | Integration tests green | T4, T5 |
| T7 | User endpoints + `require_roles` dependency + admin management routes | Role-matrix tests green | T6 |
| T8 | `catalog` skeleton + Alembic + all five tables | Migration to head | T2, T3 |
| T9 | Category CRUD (admin-gated) | Tests green | T8, T7 |
| T10 | Product CRUD with ownership enforcement, optimistic concurrency, audit-log writes | Tests green, including cross-merchant `403` | T9 |
| T11 | Variants and media endpoints | Tests green | T10 |
| T12 | Product listing: filters, sort, pagination, status visibility rules | Tests green | T10 |
| T13 | Status-transition endpoint with the state machine and the `501` for `published` | Tests green | T10 |
| T14 | `gateway`: routing, JWT pre-verification, request-ID injection, CORS, aggregated docs | End-to-end call through the gateway succeeds | T7, T13 |
| T15 | Seed script: admin, two merchants, two customers, categories, ~20 products with variants | `make seed` is idempotent and re-runnable | T12 |
| T16 | README: overview, architecture diagram, setup, API tour, role matrix | A reader who has never seen the repository can run it | T14, T15 |
| T17 | End-to-end smoke test across both services through the gateway | `make test` green from a cold start | T14, T15 |

**Request flow, once T14 lands.**

```
 client            gateway              catalog            catalog_db
   │                  │                    │                   │
   │ POST /products   │                    │                   │
   │ Bearer <jwt>     │                    │                   │
   ├─────────────────▶│                    │                   │
   │                  │ verify sig+exp     │                   │
   │                  │ (public key, local)│                   │
   │                  │ gen X-Request-ID   │                   │
   │                  ├───────────────────▶│                   │
   │                  │                    │ re-verify token   │
   │                  │                    │ require_roles(    │
   │                  │                    │   merchant,admin) │
   │                  │                    │ validate body     │
   │                  │                    ├── BEGIN ─────────▶│
   │                  │                    │   insert product  │
   │                  │                    │   insert audit    │
   │                  │                    ├── COMMIT ────────▶│
   │                  │◀───────────────────┤ 201 + Location    │
   │◀─────────────────┤ + X-Request-ID     │                   │
```

### 3.2. Rationale

**Why separate services now rather than a monolith that is split later.** The stated purpose of this work ([Learning Path](../../learning-path.md), [Execution Guidelines](../../execution-guidelines.md) §8) is distributed-systems understanding, and the evaluation weighs architecture clarity and correct use of the stack. A modular monolith would reach working endpoints sooner, but the questions this platform exists to answer — how services stay consistent without shared tables, what happens when one is down, how a request is traced across a boundary — only become real once a process boundary exists. Deferring the split also has a reliable failure mode: the shared-table shortcut gets taken under time pressure in Milestone 2, and the split never happens. Milestone 2 adds `order` and `inventory`, which must not share a database with catalog; building that boundary now is cheaper than retrofitting it then.

**Why one Postgres instance with several databases.** Isolation matters; container count does not. Separate logical databases with separate roles make cross-service SQL impossible, which is the property being bought. Running four Postgres containers on a laptop that also has to host Kafka, Temporal, and Grafana by Milestone 3 buys nothing further. If the instance later needs splitting, only connection strings change.

**Why RS256 rather than a shared HMAC secret.** With a shared secret, any service that can verify a token can also forge one, so a single compromised service compromises the identity of every user. With asymmetric keys, only `identity` can mint tokens, and the public key is not a secret at all. Verification is local, so `identity` being down does not stop `catalog` serving authenticated reads — which matters because `identity` is otherwise a single point of failure for the entire platform.

**Why the gateway is thin.** A gateway that accumulates business logic becomes a distributed monolith's centre of gravity. This one does routing, early token rejection, and correlation IDs — cross-cutting concerns that genuinely belong at the edge. Authorisation decisions stay in the service that owns the resource, because only `catalog` knows what "the merchant owns this product" means.

**Why the audit log is a table written by the service layer.** [Part A](../../part-a.md) §1 requires that any change be traceable to who made it and when. A database trigger cannot see the authenticated actor or the request ID. Writing the audit row in the same transaction as the change makes the pair atomic: there is no state where a product changed but the trail did not record it.

**Why `publish` returns `501` instead of just setting the column.** Setting `status = 'published'` in Milestone 1 would be a one-line change, and it would be wrong in a way that is expensive to undo. Publishing is specified as a multi-step workflow — validate, process media, build the search record, synchronise downstream, then mark ready — with an explicit constraint that partial failure must not leave a half-published product. Shipping a fake version of it would train clients on a contract Milestone 2 has to break, and would quietly hide the milestone's actual difficulty. An honest `501` that names the reason is better than a lie that works.

**Why no stock quantity in the catalog.** Inventory has multiple writers — orders, returns, warehouse sync — and is exactly the consistency problem the platform is meant to solve. A `quantity` column on `product_variants` would make catalog a second writer on the same fact, which is the inconsistency [Part A](../../part-a.md) names in its opening challenges.

**Why async SQLAlchemy and asyncpg.** The workloads that follow are I/O-bound and fan out to several downstream systems per request. A synchronous ORM call inside an `async def` handler blocks the event loop, and the resulting latency cliff is hard to diagnose and tedious to unpick once it is spread across a codebase.

### 3.3. Tradeoffs

| Trade being made | The cost | Why it is acceptable |
| --- | --- | --- |
| Services before there is scale to justify them | More scaffolding, more processes, slower first endpoint; local debugging spans containers | Bought once, at the cheapest possible moment. The alternative pays it later with data already in the tables |
| No cross-service foreign keys | The database cannot stop a product referencing a deleted merchant | A real distributed-system property, not an accident. Handled by soft-deleting users and, from Milestone 3, by consuming identity events |
| Stateless access tokens | A revoked or demoted user keeps access for up to 15 minutes | The standard trade for avoiding a database read per request. Bounded and explicit; password change and logout revoke refresh sessions immediately, capping the window |
| One Postgres instance | A single container is a shared blast radius locally | Local-only concern. Logical isolation is what the design depends on, and that holds |
| Limit/offset pagination | Deep offsets scan; concurrent inserts can shift a page | Catalog pages are shallow in practice and heavily filtered. Cursor pagination is additive when a path needs it |
| `merchant_id` denormalised into catalog with no join available | Listing products with merchant names needs a second call or, later, a cached projection | The alternative is a synchronous cross-service call on a public read path — worse for both latency and coupling |
| A shared `libs/common` package | A change there touches every service; it can grow into a hidden monolith | Restricted by rule to non-domain concerns. Any business rule that appears in it is a review failure |
| Publishing deliberately unimplemented | The Milestone 1 demo cannot show a published product without seeding one | Seed data provides published products for browse testing. The scope boundary is explicit, not hidden |
| `jsonb` for variant options and product attributes | No schema validation at the database level; typo'd keys are accepted | Variant dimensions differ per category and are merchant-defined; a rigid schema would be wrong within a week. Validated at the Pydantic layer |

#### 3.3.1. Challenges and Open Questions

| # | Question | Current position | Needs a decision by |
| --- | --- | --- | --- |
| Q1 | Should the gateway be a hand-written FastAPI proxy or an off-the-shelf one (Traefik, Kong, Envoy)? | Hand-written FastAPI, so that the auth and correlation logic is legible and shares `libs/common`. Revisit if routing rules grow | Milestone 3 |
| Q2 | How does `catalog` learn that a merchant was suspended or deleted? | Milestone 1: it does not — the next token refresh fails and the access token expires within 15 minutes. Milestone 3: an identity event consumed from Kafka | Milestone 3 |
| Q3 | Does merchant onboarding need an approval step before the merchant can create products? | `merchant_profiles.status` exists and defaults to `pending`, but Milestone 1 does not gate on it. The column is there so gating is a one-line change | Milestone 2 |
| Q4 | Where do RS256 keys come from outside a laptop? | Local: a generated key pair mounted from a gitignored directory, with the public key in each service's environment. Production key management is out of scope for the assignment | Not required |
| Q5 | Are variant option dimensions free-form or drawn from a category-level definition? | Free-form `jsonb` in Milestone 1 | Milestone 2, if variant synchronisation needs the structure |
| Q6 | Is `citext` the right choice for email, or should normalisation happen in the application? | `citext`, so uniqueness is enforced by the database rather than by every caller remembering to lower-case | Decided |
| Q7 | Should the audit log stay in Postgres or move to the NoSQL store? | Postgres in Milestone 1 — it must be transactional with the change. Revisit if volume becomes a problem | Milestone 3 |
| Q8 | Should product listing responses be cached in Redis? | Not yet. Introduce when there is a measurement showing it is needed | Milestone 3 |

---

## 4. Dependencies/blockers/known issues

**External dependencies.**

| Dependency | Version | Used for |
| --- | --- | --- |
| Python | 3.12 | Runtime |
| FastAPI | 0.115.x | HTTP layer, OpenAPI generation |
| Uvicorn | 0.32.x | ASGI server |
| SQLAlchemy | 2.0.x (async) | ORM |
| asyncpg | 0.30.x | Postgres driver |
| Alembic | 1.14.x | Migrations |
| Pydantic / pydantic-settings | 2.x | Validation, configuration |
| PyJWT | 2.10.x | RS256 sign and verify |
| argon2-cffi | 23.x | Password hashing |
| httpx | 0.28.x | Gateway proxying and test client |
| pytest / pytest-asyncio | 8.x / 0.24.x | Tests |
| ruff, mypy | current | Lint, format, type-check |
| PostgreSQL | 16 | Data store |
| Docker + Compose | v2 | Local environment |

**Blockers.** None. Every dependency is public, and no external account, credential, or third-party service is required for Milestone 1.

**Known issues carried into this milestone.**

- No email delivery, so `email_verified` cannot move to `true` and password reset is unavailable. Registration is immediate.
- No rate limiting on `POST /auth/login`. A brute-force attempt is unthrottled. Noted in §6.2 and scheduled for Milestone 3 alongside Redis.
- Role changes take effect only on the next token issuance (Q2).
- No distributed tracing, so a request crossing the gateway can be followed only by correlating `X-Request-ID` across log streams. Sufficient for two services; Milestone 3 replaces it.
- The seed script writes `published` products directly to the database, bypassing the (unimplemented) workflow. This is acknowledged as a test-data shortcut and is confined to the seed path.

---

## 5. Alternate solutions

**Alternative 1 — Modular monolith, split later.** One FastAPI application, one database, module boundaries enforced by convention and package structure. Materially faster to a working API, simpler to run and debug, and a widely recommended default.

*Rejected because* the explicit objective is distributed-systems experience, and a monolith would defer every interesting question — cross-service consistency, partial failure, per-service schema ownership — past the point where the schema has already been designed around shared tables. The split is also reliably deferred forever once orders and inventory can simply join against catalog. The cost of building boundaries now is highest in Milestone 1 and rises in every later one.

**Alternative 2 — Full microservice set scaffolded up front** (identity, catalog, order, inventory, payment, shipment, notification, analytics). Matches a plausible Part A target and avoids later restructuring.

*Rejected because* this was the prior prototype's approach and it produced eight services with nothing in most of them. Empty services still need Dockerfiles, migrations, health checks, and CI time, and they make it impossible to tell which boundaries were reasoned about and which were guessed. Services arrive with the milestone that gives them behaviour.

**Alternative 3 — Session cookies with server-side session storage instead of JWTs.** Immediate revocation, no token-expiry window, simpler mental model.

*Rejected because* every request would then require a session lookup in a shared store, making that store a hard dependency of every service and reintroducing the single point of failure the asymmetric-key design removes. It also fits API clients and non-browser callers poorly. The revocation window (§3.3) is the cost of that removal, and it is bounded.

**Alternative 4 — Delegate authentication to Keycloak or Auth0.** Production-grade, standards-compliant, less code to write and maintain.

*Rejected because* identity is a core part of what this milestone exists to design and demonstrate; outsourcing it removes the RBAC design work that Milestone 1 is meant to cover, and adds a heavyweight container to a Compose file that has a great deal more to fit by Milestone 3. The token format chosen is standard, so this remains a viable later swap.

**Alternative 5 — A single shared database for all services, with per-service schemas.** Simpler local setup, cross-service joins available when convenient.

*Rejected because* "available when convenient" is precisely the property being designed out. A shared database makes the boundary advisory, and an advisory boundary does not survive the first tight deadline.

**Alternative 6 — Off-the-shelf gateway (Traefik / Kong / Envoy).** Mature routing, rate limiting, and TLS without writing code.

*Deferred rather than rejected* (Q1). For two backend services, a declarative gateway's configuration is roughly the size of the Python it replaces, and a hand-written one keeps the token-verification path readable and shared with the services. Worth revisiting in Milestone 3, when rate limiting and more routes arrive.

---

## 6. Production readiness

"Production" here means the assignment's target environment — Docker Compose on a developer machine, reviewed by a mentor. The properties below are designed to hold in principle and are demonstrated at that scale.

### 6.1. Scalability and Fault tolerance

- **Stateless services.** `gateway`, `identity`, and `catalog` hold no in-process state, so each scales horizontally with `docker compose up --scale`. All state is in Postgres.
- **Connection pooling.** SQLAlchemy async pool, sized per service (`pool_size=10`, `max_overflow=20`) so that a scaled-out service does not exhaust Postgres connections.
- **Local token verification.** No network call on the authorisation path. `identity` being unavailable degrades login and refresh but leaves every already-authenticated read and write working — a deliberate blast-radius reduction.
- **Health probes.** `/health` for liveness, `/health/ready` for readiness including a database round-trip. Compose `depends_on: service_healthy` sequences startup so no service accepts traffic before its database is reachable.
- **Timeouts and failure containment.** The gateway applies a 10-second timeout to upstream calls and returns `504` as a problem detail rather than hanging. A circuit breaker is unnecessary at two upstreams and is revisited in Milestone 3.
- **Graceful shutdown.** SIGTERM drains in-flight requests before the event loop closes, so a redeploy does not sever open connections.
- **Query shapes.** Every filter and sort exposed on `GET /products` is backed by an index (§3.1.3); the default browse path is served by the `(status, created_at desc)` composite. Pagination is mandatory, with a hard `limit` ceiling of 100, so no endpoint can be asked to return an unbounded set.
- **Optimistic concurrency.** `products.version` is checked on update, so two merchants' concurrent edits cannot silently overwrite each other; the loser receives `409`.
- **Known ceiling.** Postgres is a single instance with no replica. Read replicas and partitioning are not Milestone 1 concerns, but no schema decision here prevents them.

### 6.2. Security considerations

- **Passwords** — Argon2id with library defaults, never logged, never returned. The response model for a user has no password field at all, so it cannot leak by accident.
- **Tokens** — RS256; the private key exists only in `identity`. Access tokens 15 minutes, refresh tokens 7 days. Refresh tokens are stored as SHA-256 hashes, rotated on every use, and a reuse of an already-rotated token revokes the whole session family, which is the standard detection for a stolen refresh token.
- **Privilege escalation** — `role` is rejected on `PATCH /users/me`; only an admin may change a role, through the admin route. `admin` cannot be chosen at registration and is created only by the seed script or by another admin.
- **Ownership** — enforced in the service layer against `merchant_id` from the verified token, never from a request body or query parameter. A merchant supplying another merchant's `merchant_id` is ignored, not obeyed.
- **Injection** — all data access is through the ORM with bound parameters; no string-built SQL anywhere.
- **Input validation** — Pydantic models on every request body, with length ceilings on free-text fields and a `numeric(12,2)` plus non-negative check on prices.
- **Secrets** — supplied by environment only. `.env` is gitignored; `.env.example` carries placeholders. Keys are mounted from a gitignored directory and are never committed.
- **CORS** — an explicit allow-list from configuration, not `*`.
- **Error hygiene** — problem details never include stack traces or SQL. Login failures return one indistinguishable message for both unknown-email and wrong-password, so the endpoint cannot be used to enumerate accounts.
- **Transport** — plain HTTP locally. TLS termination is an environment concern outside Compose and is out of scope.
- **Accepted gaps for this milestone** — no login rate limiting (Milestone 3, with Redis); no email verification; no audit log for identity actions, only for catalog. Each is listed here so it is reviewed as a decision rather than discovered as an omission.

### 6.3. Deployment strategy

- **Target** — Docker Compose, one file at the repository root, `postgres` plus the three services. Each service has a multi-stage Dockerfile running as a non-root user.
- **Bring-up** — `make up` builds and starts everything; `make migrate` runs Alembic to head for each service; `make seed` loads demo data idempotently; `make down` stops; `make reset` removes the volume for a genuine cold start. A first-time reader runs three commands.
- **Configuration** — twelve-factor, environment-only, validated at startup by `pydantic-settings`. A missing or malformed variable fails the container immediately and loudly rather than at the first request that needs it.
- **Migrations** — run as an explicit step, never automatically on application startup. Automatic migration means N scaled replicas racing to migrate the same database.
- **Database provisioning** — a Postgres init script creates `identity_db` and `catalog_db` with a dedicated role each, granted only on its own database.
- **Rollback** — Alembic `downgrade` per service; every migration is written with a working `downgrade`. Since services deploy independently, a rollback of one does not require a rollback of the other — which is the point of separate histories.
- **CI (if added)** — lint, type-check, and the full test suite against an ephemeral Postgres service container.

---

## 7. Test Plan

The target is meaningful coverage of the authorisation and ownership paths, not a coverage percentage. Rough aim: ~80% on service layers, 100% on the role matrix.

**Unit tests** — no I/O, fast.

- Password hashing: hash/verify round trip, rejection of a wrong password, distinct salts.
- JWT: sign and verify, expiry rejection, signature-tamper rejection, wrong-key rejection, claim contents.
- `require_roles` dependency: allow, deny, missing token, malformed token.
- Product state machine: every legal transition allowed, every illegal one rejected, `published` raising the not-implemented path.
- Pagination and filter parameter parsing, including the limit ceiling.
- Problem-detail serialisation.

**Integration tests** — real Postgres (a throwaway database per test session), `httpx.ASGITransport` against the app, transaction rolled back per test.

*identity*
- Register: success for `customer` and `merchant`; duplicate email → `409`; weak password → `422`; attempting `role: admin` → `422`.
- Login: success; wrong password → `401`; unknown email → `401` with an identical body; inactive user → `403`.
- Refresh: success and rotation; reuse of a rotated token → `401` and family revoked; expired token → `401`.
- `GET /users/me` with and without a token; `PATCH /users/me` cannot change `role`.
- Admin routes: reachable by `admin`, `403` for `customer` and `merchant`.
- Password change revokes other sessions.

*catalog*
- Categories: admin creates; merchant and customer receive `403`; delete with dependent products → `409`.
- Product create: merchant succeeds and lands in `draft`; customer → `403`; anonymous → `401`; duplicate `(merchant_id, sku)` → `409`.
- Product update: owner succeeds; **a different merchant receives `403`**; admin succeeds; stale `version` → `409`.
- Visibility: anonymous listing returns only `published`; a merchant sees own drafts via `/products/me`; a merchant does not see another merchant's drafts; admin sees everything.
- Status endpoint: `draft → inactive` succeeds; `draft → published` → `501`; `archived → draft` → `409`.
- Variants and media: created, updated, deleted by the owner only; cascade delete with the product.
- Audit log: a create and an update produce rows with the correct actor and a `changes` diff; the log is written in the same transaction (a failed update leaves no audit row).
- Listing: filter by category, merchant, price range, and text; sort by each allowed column; pagination boundaries; `limit` above the ceiling is clamped.

**End-to-end smoke test** — through the gateway, cold start, no fixtures: register a merchant → log in → create a category as admin → create a product → add a variant → list as anonymous (absent, because it is a draft) → move to `inactive` → attempt `published` and assert `501` → assert `X-Request-ID` is present and consistent across the gateway and service logs for one request.

**Manual verification checklist for the milestone review** — `make reset && make up && make migrate && make seed` from a clean clone on a machine that has never run it; `/docs` reachable for the gateway and both services; the Postman/HTTPie collection in the README executes top to bottom.

**Explicitly not tested here** — load and performance (no traffic target is defined for Milestone 1), and anything belonging to the deferred milestones.

---

## 8. Impact Analysis

**Scope of impact.** SmartRetail has no users, no data, and no dependent systems, so there is no external impact to assess. The impact of this RFC is on the work that follows it.

**On later milestones.**

| Milestone | What this RFC makes easy | What it makes harder |
| --- | --- | --- |
| 2 — Orders & publishing | `order` and `inventory` join an existing service pattern: copy the layout, add a database, register a route. Temporal activities call the same service-layer functions the routers call. The `status` state machine and `published_at` already exist for the workflow to drive | Order creation cannot join to products; it must call catalog or carry a product snapshot. This is deliberate, and forces the snapshot decision to be made consciously |
| 3 — Events & observability | Each service is already a separate process with structured logs and a request ID, so tracing and metrics instrument cleanly. The audit log gives a ready source of catalog events | Kafka, Celery, and RabbitMQ containers join a Compose file that is already multi-service; laptop resource pressure is real and should be expected |
| 4 — Part A review | Every decision above is written down with the alternatives that were rejected, which is what the review asks for | — |
| 5–6 — Retrieval & AI | Product text, attributes, and media URLs are in one owned schema, straightforward to chunk and embed. A retrieval service consumes the catalog API rather than its tables | An embedding pipeline must track catalog changes through the API or events rather than by polling a shared database |

**On effort.** The service boundaries and `libs/common` cost roughly two to three days of scaffolding that a monolith would not spend. That cost is recovered in Milestone 2, where two further services are added with no restructuring, and again in Milestone 3, where per-service instrumentation is the natural shape.

**Risks.**

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| Local resource exhaustion once Milestone 3's containers arrive | Medium | Medium | One Postgres instance; the observability stack can be brought up under a Compose profile rather than by default |
| `libs/common` accumulating domain logic and becoming a hidden monolith | Medium | Medium | Stated rule in §3.1.1; checked at each milestone review |
| Milestone 1 scaffolding overruns and squeezes Milestone 2 | Medium | High | T1–T3 are one day; the action plan is ordered so identity and catalog can proceed in parallel after T3 |
| Cross-service data drift (a product owned by a deleted merchant) | Low now, higher later | Medium | Soft-delete users in Milestone 1; identity events in Milestone 3 (Q2) |
| The `501` on publish being read as an incomplete milestone | Medium | Low | Documented here and in the README as a scope boundary with a reason |

**On reviewers.** The review for Milestone 1 should be able to check: the boundaries hold (no cross-database access), the role matrix is enforced rather than merely declared, the schema supports Milestone 2 without alteration, and the setup works from a cold clone.

---

## 9. Useful Links/References

**Project documents**

- [Part A — platform requirements](../../part-a.md)
- [Part B — GenAI layer](../../part-b.md)
- [Milestones](../../milestones.md) · [Execution Guidelines](../../execution-guidelines.md) · [Learning Path](../../learning-path.md)

**Technical references**

- FastAPI — https://fastapi.tiangolo.com/
- SQLAlchemy 2.0 async ORM — https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html
- Alembic — https://alembic.sqlalchemy.org/
- RFC 9457, Problem Details for HTTP APIs — https://www.rfc-editor.org/rfc/rfc9457
- RFC 7519, JSON Web Token — https://www.rfc-editor.org/rfc/rfc7519
- OAuth 2.0 Security Best Current Practice (refresh-token rotation and reuse detection) — https://datatracker.ietf.org/doc/html/draft-ietf-oauth-security-topics
- OWASP Password Storage Cheat Sheet (Argon2id parameters) — https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html
- PostgreSQL `citext` — https://www.postgresql.org/docs/16/citext.html
- The Twelve-Factor App — https://12factor.net/

**RFC style references** (from the template)

- https://github.com/sohaibomr/notification-gateway
- https://github.com/django/deps/blob/main/accepted/0009-async.rst
- https://thanos.io/v0.21/proposals/202004_embedd_cortex_frontend.md/

---

## 10. Related tickets/issues

| ID | Title | Maps to |
| --- | --- | --- |
| *(to be created)* | M1-T1 … M1-T17 | The tasks in §3.1.5, one ticket each |
| — | Milestone 1 — Foundation & Core Services | This RFC |
| — | [RFC 0000 — System Architecture](0000-system-architecture.md) | The umbrella design this RFC implements the first slice of |
| — | [RFC 0006 — Service Decomposition & Data Model](0006-service-decomposition-and-data-model.md) | The boundary rules and schema conventions applied here |
| — | [RFC 0002 — Core Business Logic & Workflows](0002-core-business-logic-and-workflows.md) | Milestone 2; consumes the `501` → `202` change announced in §3.1.4 |
| — | [RFC 0003 — Events & Observability](0003-events-and-observability.md) | Milestone 3; closes Q2 and the login rate-limiting gap in §4 |

---

## 11. Notes

- **On the restart.** The previous prototype and its design documents were deleted in full before this RFC was written, at the owner's direction. Nothing in this document is constrained by what was built before; where a decision differs from the prototype's, §3.1.4 records it as an intentional reversal.
- **On domain wording.** The milestone list arrived phrased as "User & Course management" with "student / instructor" roles. [Part A](../../part-a.md) specifies a retail commerce platform, so this RFC reads *course* as *product* and *student / instructor* as *customer / merchant*, with `admin` as the third role Part A §1 requires. The milestone's structure and expected outcomes are unchanged by that mapping.
- **On the RFC set.** This is one of six. [RFC 0000](0000-system-architecture.md) holds the whole-system design and [RFC 0006](0006-service-decomposition-and-data-model.md) the decomposition and schema reasoning; [RFC 0002](0002-core-business-logic-and-workflows.md)–[RFC 0005](0005-ai-assistant-and-streaming.md) cover Milestones 2, 3, 5, and 6. Milestones 4 and 7 are reviews and have no RFC of their own. Every deferral in §2.2 names the RFC that owns it.
- **On scope discipline.** Several things that would be quick to add — a `quantity` column, a real publish transition, a Redis cache — are deliberately left out. Each one would be a decision made for the wrong reason at the wrong time, and each is recorded above with the milestone that owns it.
- **Definition of done for Milestone 1.** Every goal G1–G8 in §2.1 verified by its stated check; `make up && make migrate && make seed && make test` green from a clean clone; README complete; this RFC moved to **DONE** with any review outcomes folded in.

---

## 12. Comments

*Reviewer comments go here. Please raise objections against the numbered decisions (A1–A8), the open questions (Q1–Q8), or the rejected alternatives in §5, so the discussion attaches to something specific.*

| Reviewer | Section | Comment | Resolution |
| --- | --- | --- | --- |
| | | | |
