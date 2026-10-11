# RFC 0003 — `identity` Service

| | |
| --- | --- |
| **Owner** | @muhammad.adnan |
| **Collaborators** | SarwanAhmed |
| **Status** | **DRAFT** · IN-REVIEW · DONE |
| **Created** | Sep 29, 2026 |
| **Supersedes** | [archive/0001 — Foundation & Core Services](archive/0001-foundation-and-core-services.md) (identity half) · [archive/0000 — System Architecture](archive/0000-system-architecture.md) (authentication split, A4/A5, identity events) · [archive/0002 — Core Business Logic & Workflows](archive/0002-core-business-logic-and-workflows.md) (client-credentials grant) · [archive/0003 — Events & Observability](archive/0003-events-and-observability.md) (identity events, outbox, login rate limit, suspension, Q6) · [archive/0006 — Service Decomposition & Data Model](archive/0006-service-decomposition-and-data-model.md) (`identity_db` tables, ownership-test row) |
| **Depends on** | [RFC 0000 — Architecture](0000-architecture.md) · [RFC 0001 — Platform & Shared Infrastructure](0001-platform-and-shared-infrastructure.md) · [RFC 0002 — `gateway` Service](0002-gateway-service.md) |

> **Reading note.** This RFC specifies the `identity` service completely: its users, credentials, roles, merchant profiles and their approval state, sessions, token issuance, signing keys, the service-token grant, its HTTP surface, its two events, and every column of `identity_db`. It is the only place those tables are defined. Cross-cutting rules that bind every service — the ownership test, boundary rules R1–R8, decisions A1–A14, schema conventions S1–S15, the event envelope, the shared `outbox` DDL, API conventions — live in [RFC 0000](0000-architecture.md) and are linked rather than restated. Token *verification* is a `common` concern hosted by [RFC 0001](0001-platform-and-shared-infrastructure.md); the edge rate limiter is a `gateway` concern in [RFC 0002](0002-gateway-service.md). Both are described here only as far as `identity` depends on them.

---

## Contents

1. [Title](#1-title)
2. [Overview](#2-overview)
   - 2.1. [Goals](#21-goals)
   - 2.2. [Non-goals](#22-non-goals)
3. [Solution Overview](#3-solution-overview)
   - 3.1. [Specification](#31-specification)
     - 3.1.1. [Architectural changes](#311-architectural-changes)
       - 3.1.1.1. [Responsibility and boundaries](#3111-responsibility-and-boundaries)
       - 3.1.1.2. [Domain model](#3112-domain-model)
       - 3.1.1.3. [State machines](#3113-state-machines)
       - 3.1.1.4. [Workflows and async jobs](#3114-workflows-and-async-jobs)
       - 3.1.1.5. [Dependencies and degradation](#3115-dependencies-and-degradation)
       - 3.1.1.6. [Configuration](#3116-configuration)
       - 3.1.1.7. [Observability](#3117-observability)
     - 3.1.2. [API changes](#312-api-changes)
       - 3.1.2.1. [Endpoints](#3121-endpoints)
       - 3.1.2.2. [Role matrix](#3122-role-matrix)
       - 3.1.2.3. [Events produced](#3123-events-produced)
       - 3.1.2.4. [Events consumed](#3124-events-consumed)
     - 3.1.3. [Data/schema changes](#313-dataschema-changes)
     - 3.1.4. [Breaking changes](#314-breaking-changes)
     - 3.1.5. [Action Plan](#315-action-plan)
   - 3.2. [Rationale](#32-rationale)
     - 3.2.1. [Decision register](#321-decision-register)
     - 3.2.2. [Rationale in detail](#322-rationale-in-detail)
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

**`identity` — users, credentials, roles, merchant onboarding and approval, sessions, and the sole signer of every token in SmartRetail.**

`identity` (`:8001`, `identity_db`) answers one question for the rest of the platform: *who is this caller, and what role do they hold?* It is the only component that holds credentials, the only one that holds a private signing key, and the only one that can revoke a session. Every other service verifies its tokens locally with a public key and never calls it on the request path.

---

## 2. Overview

[Part A](../part-a.md) §1 requires "merchant onboarding and user registration with appropriate roles: `customer`, `merchant`, `admin`". Every authorisation decision anywhere in the platform depends on the answer `identity` gives, so it is the highest-security boundary in the system ([archive/0006](archive/0006-service-decomposition-and-data-model.md) ownership test, row 2) and the narrowest: it knows nothing about products, orders or stock.

It issues RS256-signed, short-lived (15-minute) access tokens and long-lived (7-day), rotating, revocable refresh tokens stored hashed in `identity_db`; publishes its public keys as a JWKS document that every verifier caches; issues short-lived service tokens to internal callers that have no user in context; and announces the two facts other services react to — a user registered, a merchant's approval status changed — as events through its transactional outbox.

### 2.1. Goals

Goal IDs `G2` and `G4` are carried from [archive/0001](archive/0001-foundation-and-core-services.md) §2.1; `IDN-G*` are this RFC's.

| # | Goal | How it is verified |
| --- | --- | --- |
| G2 | User management: registration, login, token refresh, profile read/update | Endpoints in §3.1.2.1 return the documented responses; covered by tests |
| G4 | RBAC enforced on every non-public endpoint, for `customer` / `merchant` / `admin` | A role-matrix test asserts allow/deny for each role against each endpoint (§3.1.2.2); 100% coverage of the matrix |
| IDN-G1 | `identity` is the only signer; no other component can mint a token | Private key mounted into `identity` only; a configuration test asserts no other container mounts `secrets/jwt/`; verifiers hold public keys only |
| IDN-G2 | Refresh tokens rotate on every use and a replayed token is detected and contained | Integration test: reuse of a rotated token → `401` and every live session of that user revoked |
| IDN-G3 | No request path in another service depends on `identity` being up | With `identity` stopped, an already-issued access token is still accepted by `catalog` (R7) |
| IDN-G4 | Login cannot be used to enumerate accounts | Unknown email and wrong password return byte-identical bodies and comparable latency |
| IDN-G5 | Merchant approval status is owned here and propagates without a synchronous call | Suspending a merchant emits `identity.merchant.status_changed`; `catalog` hides the merchant's products with no call back ([archive/0003](archive/0003-events-and-observability.md) Q6) |
| IDN-G6 | Signing keys can be rotated without a login outage or a verifier redeploy | Rotation drill in §7: tokens signed by the old and new key both verify through the overlap window |

### 2.2. Non-goals

| Excluded | Why / who owns it |
| --- | --- |
| Email delivery, email verification, password reset | No email channel exists; `email_verified` is carried but cannot move to `true`. Registration is immediate. A reset flow needs a delivery channel that `notification` ([RFC 0007](0007-notification-service.md)) would supply; not scheduled |
| Token **verification** code | `common/smartretail_common/auth.py` ([RFC 0001](0001-platform-and-shared-infrastructure.md)). `identity` defines the claims; the shared library checks them |
| Rate limiting enforcement | At the `gateway` ([RFC 0002](0002-gateway-service.md)) with Redis. The limits that apply to identity routes are recorded here (§6.2) |
| Resource-level authorisation in other services | Owned by the service that owns the resource ([RFC 0000 §3.1.1.4](0000-architecture.md#3114-authentication-and-authorisation-split)). `identity` says "merchant `M` with role `merchant`"; `catalog` decides whether `M` may edit product `P` |
| A central policy service, token introspection on each request | Rejected by A4/A5: verification is local |
| Delegated identity (Keycloak, Auth0), social login, SSO, MFA | Out of scope for the assignment; token format is standard so a swap remains viable (§5) |
| Production key management (KMS, HSM, Vault) | Out of scope ([archive/0001](archive/0001-foundation-and-core-services.md) Q4). Keys are files mounted from a gitignored directory |
| Audit log for identity actions | Accepted gap. `catalog` has `product_audit_log` (S7); `identity` has no audit table ([archive/0001](archive/0001-foundation-and-core-services.md) §6.2). See IDN-Q5 |
| A frontend, session cookies | The API is the deliverable; tokens are bearer tokens, not cookies (§5, Alternative 3) |
| Kubernetes / cloud deployment, TLS | Docker Compose is the target; TLS termination is an environment concern |

---

## 3. Solution Overview

### 3.1. Specification

#### 3.1.1. Architectural changes

##### 3.1.1.1. Responsibility and boundaries

**Owns.** State nothing else writes ([RFC 0000 §3.1.3.6](0000-architecture.md#3136-ownership-map)):

| Fact | Sole writer | Read elsewhere as | Mechanism |
| --- | --- | --- | --- |
| User identity, credentials, role | `identity` | Claim | JWT |
| Merchant profile, approval status | `identity` | Projection (in `catalog`) | `identity.merchant.status_changed` |
| Refresh sessions | `identity` | — | — |
| Signing keys (private) | `identity` | Public keys, cached by every verifier | `GET /api/v1/auth/jwks` |
| Service-client credentials | `identity` (configuration) | — | Client-credentials grant |

**The authentication split, as it applies here.** Authentication is owned by `identity`, alone ([RFC 0000 §3.1.1.4](0000-architecture.md#3114-authentication-and-authorisation-split)):

| Concern | Owner | Detail |
| --- | --- | --- |
| Registration, password hashing, credential storage | `identity` | Argon2id; `identity_db` only |
| Login, token issuance, refresh, logout | `identity` | RS256; 15-minute access token, rotating refresh token with reuse detection |
| Signing key custody and rotation | `identity` | Private key mounted into `identity` only; public key (JWKS) served at `/api/v1/auth/jwks` and cached by every verifier |
| Role assignment, merchant approval | `identity` | Roles and merchant status are claims, set here and nowhere else |
| Token **verification** | every service, plus `gateway` | Local signature and claims check against the cached public key. No network call to `identity` on the request path (A4, A5) |

No other service reads `identity_db`, and no other service has the private key.

**Does not own.** Products, orders, stock, notifications, metrics — anything outside the table above. `identity` holds no `merchant_id`-scoped business data; `products.merchant_id` and `orders.customer_id` elsewhere are **claims** from its tokens, not foreign keys to `users.id` (R2, S15).

**Ownership-test row** (from [RFC 0000 §3.1.1.2](0000-architecture.md#3112-the-ownership-test)):

| # | Candidate | State it would own | Invariant | Independent change | Distinct profile | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| 2 | `identity` | users, credentials, roles, sessions | unique email; only identity mints tokens | yes | auth load; highest security boundary | **Service** |
| 17 | `merchant` | merchant profiles | subset of user invariants | no | no | **Rejected → `identity`** |

Decided and recorded rather than open: `identity` is **not** split into `auth` and `user-profile` — they share the `users` aggregate ([archive/0006](archive/0006-service-decomposition-and-data-model.md) §3.3.1).

**Binding rules and decisions.** Cross-cutting decisions are cited by their [RFC 0000](0000-architecture.md#3115-cross-cutting-decisions-a1a14) IDs. [archive/0001](archive/0001-foundation-and-core-services.md) numbered its own A1–A8 independently; where it is cited below it is written `0001-A*` to keep the two series apart (see §11).

| Rule | What it means for `identity` |
| --- | --- |
| R1 | `identity` connects with its own Postgres role, granted on `identity_db` only. `catalog` cannot read password hashes, by grant, not by review |
| R2 / S15 | No FK leaves `identity_db`; no other service's FK points in |
| R3 | `identity` is the single writer of users, roles, merchant status, sessions |
| R4 | Other services learn identity facts through the token (claim), `GET /merchants/{id}` (API), or events |
| R7 | Local JWT verification is the mechanism by which no other service needs `identity` up to serve reads |
| R8 | `common` holds verification and the claim model, never `UserRole` or `MerchantStatus` values — those are in `services/identity/app/core/enums.py` |
| A2 | `identity_db` is one logical database on the shared PostgreSQL 16 instance |
| A4 | RS256; `identity` is the only signer. Every other component holds the public key and verifies locally, with no network call on the request path |
| A5 | Every service re-verifies the token; the gateway's check is an early rejection, not the boundary |
| A6 | FastAPI `async def`, SQLAlchemy 2.0 async, `asyncpg`. Argon2id hashing is CPU-bound and runs in a thread pool (`anyio.to_thread`) so it never blocks the event loop |
| A7 | RFC 9457 problem details from the `common` handler |
| A8 | `X-Request-ID` / `traceparent` on every log line and every outbox row |
| A10 | Events are written to `identity_db.outbox` in the state-change transaction; `outbox-relay` publishes |
| 0001-A5 | **Stateless access tokens, stateful refresh tokens.** Access tokens are short-lived (15 min) and never checked against the database. Refresh tokens are long-lived (7 days), stored hashed in `identity_db`, and can be revoked. The trade of revocation latency for scale, bounded by the access-token TTL |

**Internal module layout** (the entity-first shape of [RFC 0000 §3.1.1.7](0000-architecture.md#3117-module-structure-inside-a-service) and [file-structure.md](../file-structure.md) §4–§5). Routes handle HTTP only and never touch the ORM; repos hold every rule and never build HTTP responses. Entries marked *(planned)* belong to milestones not yet built (§3.1.5).

```
services/identity/
├── main.py                       create_application(): settings, logger, key ring, DB, /health, app.router
├── app/
│   ├── router.py                 includes only: /api/v1 + /auth, /users, /merchants prefixes
│   ├── models.py                 every ORM model on one metadata (Alembic, seed)
│   ├── seed.py                   admin from SEED_ADMIN_*, demo merchants and customers (T15)
│   ├── core/
│   │   ├── config.py             Settings (pydantic-settings, §3.1.1.6)
│   │   ├── db.py                 Database factory, get_db
│   │   ├── logger.py             JSON logging
│   │   ├── dependencies.py       app state, current_principal, require_admin / require_merchant
│   │   ├── security.py           PasswordHasher (Argon2id, dummy hash), KeyRing (sign, JWKS), refresh-token helpers
│   │   ├── errors.py             problem types: invalid-credentials, email-taken, last-admin, …
│   │   ├── enums.py              UserRole, MerchantStatus (S4) — shared by several entities
│   │   └── schemas.py            StrictModel / OutModel bases, shared field types
│   ├── health/routes.py          /health, /health/ready (DB + active signing key)
│   ├── auth/
│   │   ├── models.py             RefreshSession
│   │   ├── enums.py              TokenType (typ claim; not persisted)
│   │   ├── dependencies.py       builds the auth repos
│   │   ├── registration_repo.py  create user (+ merchant profile)
│   │   ├── authentication_repo.py credential check, dummy-hash equalisation, rehash-on-login
│   │   ├── session_repo.py       rotate, reuse detection, logout, revoke_all_sessions
│   │   ├── token_repo.py         claim assembly and signing (access token, session pair)
│   │   └── v1/{routes,schemas}.py register, login, refresh, logout, jwks (+ token, planned)
│   ├── users/
│   │   ├── models.py             User
│   │   ├── repo.py               profile, password change, admin updates (IDN-7, I10)
│   │   ├── dependencies.py
│   │   ├── utils.py              password policy (IDN-2)
│   │   └── v1/{routes,schemas}.py /users/me, /users/me/password, admin /users
│   └── merchants/
│       ├── models.py             MerchantProfile
│       ├── repo.py               public profile, /me, admin listing (+ status transitions, planned)
│       ├── dependencies.py
│       ├── utils.py              MERCHANT_TRANSITIONS (§3.1.1.3), slug derivation
│       └── v1/{routes,schemas}.py /merchants/me, /merchants/{id}, admin /merchants (+ /status, planned)
├── alembic/                      per-service history (S14)
└── tests/{unit,integration}/<entity>/
```

The client-credentials grant (IDN-T3) and the outbox writes (IDN-T4, IDN-T5) are *(planned)*: they land in `auth/` and `merchants/` as further repos and routes, not as new top-level packages.

**Components.**

```mermaid
flowchart LR
    client["Client"] --> gw["gateway :8000"]
    gw -->|"/api/v1/auth, /users, /merchants"| routes
    tw["temporal-worker"] -->|"POST /api/v1/auth/token (direct, planned)"| routes
    verifiers["Every verifier<br/>(gateway, services, workers)"] -->|"GET /api/v1/auth/jwks"| routes
    subgraph identity["identity :8001"]
        routes["auth / users / merchants<br/>v1/routes.py"] --> repos["Repos<br/>registration, authentication,<br/>session, token, user, merchant"]
        repos --> sec["core/security.py<br/>PasswordHasher, KeyRing"]
        routes --> sec
    end
    keys[("secrets/jwt/*.pem<br/>read-only mount")] --> sec
    repos --> db[("identity_db<br/>users, merchant_profiles,<br/>refresh_sessions, outbox")]
    db -.->|"outbox rows"| relay["outbox-relay"]
    relay -.-> kafka["Kafka<br/>smartretail.identity.*"]
    kafka -.-> catalog["catalog"]
    kafka -.-> analytics["analytics"]
    kafka -.-> notification["notification"]
```

Solid arrows are synchronous calls; dotted arrows are asynchronous. `identity` makes no synchronous call out (§3.1.1.5).

There is no `consumers/` package: `identity` consumes no events (§3.1.2.4).

##### 3.1.1.2. Domain model

**Aggregate: `User`.** Root `users`; children `merchant_profiles` (0..1) and `refresh_sessions` (0..n). All three are in one database, so real foreign keys with `ON DELETE CASCADE` hold them together (R2 permits FKs inside a service).

| Entity | Identity | Holds |
| --- | --- | --- |
| `User` | `users.id` (uuid, S1) | Email (`citext`), Argon2id hash, name, phone, role, active flag, `email_verified` |
| `MerchantProfile` | `merchant_profiles.user_id` — the user's own id, so **a merchant's `merchant_id` everywhere in the platform is its `users.id`** | Business name, slug, description, approval status |
| `RefreshSession` | `refresh_sessions.id` | SHA-256 of an opaque refresh token, expiry, revocation, user agent, IP |
| `ServiceClient` | `client_id` (configuration, not a table) | Argon2id hash of the client secret, granted scopes |
| `SigningKey` | `kid` (configuration, not a table) | RSA key pair; the public half is published in JWKS |

**Invariants.**

| # | Invariant | Enforced by |
| --- | --- | --- |
| I1 | Email is unique, case-insensitively | `citext` + unique index on `users.email` (S11; [archive/0001](archive/0001-foundation-and-core-services.md) Q6, decided) |
| I2 | `role ∈ UserRole` | `DomainEnumText` on write, Pydantic at the edge (S4) |
| I3 | `admin` is never self-assigned | Registration schema restricts `role` to `{customer, merchant}`; `admin` → `422` |
| I4 | A user never changes their own role | `PATCH /users/me` forbids `role` (Pydantic `extra="forbid"`) → `422`; only `PATCH /users/{id}` as `admin` may |
| I5 | Every user with `role = merchant` has exactly one `merchant_profiles` row | Created in the same transaction as the user (registration) or the role change (IDN-7) |
| I6 | `merchant_profiles.status ∈ MerchantStatus`, and changes only along the transition table | `DomainEnumText` + `MERCHANT_TRANSITIONS` in `merchants/utils.py` (S4) |
| I7 | A refresh token's raw value is never stored or logged | Only `token_hash` (SHA-256) is persisted |
| I8 | A refresh session is usable at most once | Rotation sets `revoked_at` on the presented row and inserts a successor, in one transaction, under `SELECT … FOR UPDATE` |
| I9 | No token is issued to an inactive user or a suspended merchant | Checked at login, refresh and role-bearing issuance ([archive/0003](archive/0003-events-and-observability.md) Q6) |
| I10 | The platform always has at least one active `admin` | Admin updates that would demote or deactivate the last active admin → `409` (IDN-8) |
| I11 | Users are never hard-deleted through the API | S5: deactivation is `is_active = false`. The cascades exist for operator and test cleanup only |

**Vocabularies (S4), in `services/identity/app/core/enums.py`** (shared by the `users` and `merchants` entities).

| `DomainEnum` | Values | Column |
| --- | --- | --- |
| `UserRole` | `customer`, `merchant`, `admin` | `users.role` |
| `MerchantStatus` | `pending`, `approved`, `suspended` | `merchant_profiles.status` |

Non-persisted vocabularies (plain `StrEnum`, not columns): `TokenType` — `access`, `service` (the `typ` claim, §3.1.2.1), in `app/auth/enums.py`; `LoginOutcome` — `success`, `invalid_credentials`, `inactive`, `suspended`, `rate_limited` (metric label only).

**Credentials — Argon2id parameters (IDN-1).** [archive/0001](archive/0001-foundation-and-core-services.md) §6.2 specifies "Argon2id with library defaults". They are pinned here explicitly so a library upgrade cannot silently change them; the values are argon2-cffi 23.x's defaults, which are the RFC 9106 low-memory profile:

| Parameter | Value | Setting |
| --- | --- | --- |
| Variant | Argon2id | fixed |
| `time_cost` | 3 | `ARGON2_TIME_COST` |
| `memory_cost` | 65 536 KiB (64 MiB) | `ARGON2_MEMORY_COST_KIB` |
| `parallelism` | 4 | `ARGON2_PARALLELISM` |
| `hash_len` | 32 bytes | `ARGON2_HASH_LEN` |
| `salt_len` | 16 bytes, random per hash | `ARGON2_SALT_LEN` |
| Encoding | PHC string (`$argon2id$v=19$m=65536,t=3,p=4$…`) in `users.password_hash` | — |

Parameters travel inside the PHC string, so raising them needs no migration: on a successful login `check_needs_rehash` is consulted and, if true, the hash is replaced in the same transaction. **Password policy (IDN-2):** 12–128 characters after Unicode NFKC normalisation, not equal to the email; anything else → `422` ("weak password → `422`", [archive/0001](archive/0001-foundation-and-core-services.md) §7). The upper bound caps hashing cost per request.

**Admin bootstrap.** `admin` cannot be chosen at registration and is created only by the seed script or by another admin ([archive/0001](archive/0001-foundation-and-core-services.md) §6.2). `make seed` creates the first admin from `SEED_ADMIN_EMAIL` / `SEED_ADMIN_PASSWORD`, idempotently (upsert on email; an existing admin's password is not overwritten). The seed also creates two merchants (profiles `approved`) and two customers. Every later admin is a promotion via `PATCH /api/v1/users/{user_id}` by an existing admin.

**Tokens.**

| Token | Format | Lifetime | Stored | Revocable |
| --- | --- | --- | --- | --- |
| Access token | JWT, RS256 | 15 minutes (`ACCESS_TOKEN_TTL_SECONDS=900`) | No | No — expires (0001-A5) |
| Refresh token | Opaque, 32 random bytes, base64url (43 chars) (IDN-3) | 7 days (`REFRESH_TOKEN_TTL_SECONDS=604800`), sliding: each rotation issues a successor with a fresh 7-day expiry | SHA-256 in `refresh_sessions.token_hash` | Yes — `revoked_at` |
| Service token | JWT, RS256, `typ=service` | 5 minutes (`SERVICE_TOKEN_TTL_SECONDS=300`) (IDN-4) | No | No — expires |

The refresh token is opaque rather than a JWT (IDN-3): it is only ever read by `identity`, which looks it up anyway, so a signature buys nothing and a self-describing token would tempt a verifier to accept it.

**JWT claim set — access token** (IDN-5). Header: `{"alg": "RS256", "typ": "JWT", "kid": "<active kid>"}`.

| Claim | Type | Value | Consumed by |
| --- | --- | --- | --- |
| `iss` | string | `JWT_ISSUER`, default `smartretail-identity` | every verifier (must match) |
| `aud` | string | `JWT_AUDIENCE`, default `smartretail` | every verifier (must match) |
| `sub` | string (uuid) | `users.id`. For a merchant this is also the `merchant_id` written into `products.merchant_id` etc. | every service — the subject and the claim-kind reference |
| `typ` | string | `access` | verifiers: user-facing routes require `access`; internal routes reject it |
| `role` | string | `UserRole` value | `require_roles`, gateway coarse route checks |
| `merchant_status` | string | `MerchantStatus` value; **present only when `role = merchant`** | `catalog` (e.g. gating product creation if [archive/0001](archive/0001-foundation-and-core-services.md) Q3 is resolved that way) |
| `sid` | string (uuid) | `refresh_sessions.id` of the session that minted this token | `identity` only — logout and "revoke all *other* sessions" on password change |
| `iat` | number | issue time (epoch seconds) | verifiers |
| `nbf` | number | equal to `iat` | verifiers |
| `exp` | number | `iat + 900` | verifiers |
| `jti` | string (uuid) | unique per token | logging and trace correlation; not a revocation list |

Deliberately **absent**: email, name, phone, `is_active` — a token is copied into logs, proxies and client storage, and carries only what authorisation needs (data minimisation, [archive/0006](archive/0006-service-decomposition-and-data-model.md) §6.2).

**JWT claim set — service token** (IDN-4). Header as above.

| Claim | Value |
| --- | --- |
| `iss`, `aud`, `iat`, `nbf`, `jti` | as for access tokens |
| `sub` | `service:<client_id>` — e.g. `service:temporal-worker` ([archive/0002](archive/0002-core-business-logic-and-workflows.md) §3.1.2) |
| `typ` | `service` — the "own claim" [archive/0000](archive/0000-system-architecture.md) §3.1.1 requires |
| `scope` | space-separated scopes granted to the client, e.g. `inventory:internal catalog:internal` |
| `exp` | `iat + 300` |

No `role`, no `sid`, no `merchant_status`. Internal routes require the scope and reject user tokens; user-facing routes reject service tokens. Sharing a role for both would mean a stolen customer token could call `/internal/v1/inventory/reservations` ([archive/0002](archive/0002-core-business-logic-and-workflows.md) §3.1.2).

**Signing keys, custody and rotation (IDN-6).**

- **Algorithm and size.** RSA 2048-bit, RS256. Verifiers accept `RS256` only, pinned in `common`; a token with `alg: none` or `HS256` is rejected before any key lookup, which closes algorithm-confusion attacks.
- **Custody.** Key files live in a gitignored `secrets/jwt/` directory, one PEM per key named `<kid>.pem`, mounted read-only into the `identity` container only. `kid` is `YYYYMMDD-<8 hex>` so ordering is legible. The private key is never logged, never returned, never mounted elsewhere ([archive/0001](archive/0001-foundation-and-core-services.md) Q4: "a generated key pair mounted from a gitignored directory"; production KMS is out of scope).
- **Key ring.** On startup `identity` loads every PEM in `JWT_KEYS_DIR`. `JWT_ACTIVE_KID` names the one used to sign; every loaded key's public half is published in JWKS. Startup fails loudly if the active kid is missing.
- **Rotation procedure.** Four steps, no downtime:
  1. Generate a new key into `secrets/jwt/`; restart `identity`. It is now **published** in JWKS but not used to sign.
  2. Wait at least one JWKS `max-age` (300 s) so every verifier's cache contains it.
  3. Set `JWT_ACTIVE_KID` to the new kid; restart. New tokens are signed with it; old tokens still verify against the old key, which is still published.
  4. After at least `ACCESS_TOKEN_TTL + JWKS max-age` (15 + 5 = 20 minutes), remove the old PEM; restart. No live token references it.
  Refresh tokens are opaque and unaffected by rotation. Service tokens (5 min) are covered by step 4's window.
- **Emergency revocation** of a compromised key is steps 1, 3 and 4 back to back: every access token signed by the compromised key stops verifying as soon as verifiers refresh JWKS (at most `max-age`), which forces every client through refresh — which still works, because refresh tokens are not keyed to the signing key.
- **Bootstrap for verifiers.** [archive/0001](archive/0001-foundation-and-core-services.md) Q4 put the public key in each service's environment; [archive/0000](archive/0000-system-architecture.md) §3.1.1 serves JWKS. Both are kept: `JWT_PUBLIC_KEYS` in a verifier's environment is the **fallback** key set, used when JWKS cannot be fetched at startup, so a verifier never needs `identity` up to boot (R7). JWKS is the rotation path.

Planned key rotation (IDN-6), as a sequence:

```mermaid
sequenceDiagram
    autonumber
    participant Op as Operator
    participant I as identity
    participant V as Verifier (common JWKS cache)
    Op->>Op: generate key B into secrets/jwt/
    Op->>I: restart (B published in JWKS, A still signs)
    V->>I: GET /api/v1/auth/jwks (cache refresh)
    I-->>V: keys A and B
    Note over Op,V: wait at least JWKS max-age (300 s)
    Op->>I: set JWT_ACTIVE_KID = B, restart
    Note over I,V: new tokens signed with B, old tokens still verify with A
    Note over Op,V: wait at least ACCESS_TOKEN_TTL + max-age (20 min)
    Op->>I: remove A, restart
    V->>I: GET /api/v1/auth/jwks
    I-->>V: key B only
```

##### 3.1.1.3. State machines

**`MerchantStatus`** — `merchant_profiles.status`. Initial state `pending` (column default). The transition table `MERCHANT_TRANSITIONS` in `app/merchants/utils.py` is authoritative (S4); any pair not listed → `409` problem `…/errors/invalid-merchant-transition`.

```mermaid
stateDiagram-v2
    [*] --> pending: register as merchant, or role set to merchant
    pending --> approved: approve (admin)
    pending --> suspended: suspend (admin), the rejection path
    approved --> suspended: suspend (admin), or role changed away from merchant
    suspended --> approved: reinstate (admin)
    pending --> suspended: role changed away from merchant
    note right of pending
        pending is only ever initial.
        A repeat of the current status is a no-op, 200, no event.
    end note
```

| From | To | Trigger | Who | Guards | Side effects (same transaction) |
| --- | --- | --- | --- | --- | --- |
| — | `pending` | `POST /auth/register` with `role=merchant`; or `PATCH /users/{id}` setting `role=merchant` on a user with no profile | the registrant; `admin` | email unique | profile row inserted; `identity.user.registered` outbox row (registration only) |
| `pending` | `approved` | `POST /merchants/{id}/status {"status":"approved"}` | `admin` | user `is_active` | `identity.merchant.status_changed` outbox row |
| `pending` | `suspended` | same, `{"status":"suspended"}` — the rejection path; there is no `rejected` value | `admin` | — | outbox row; all live refresh sessions revoked |
| `approved` | `suspended` | same | `admin` | — | outbox row; all live refresh sessions revoked |
| `suspended` | `approved` | same — reinstatement | `admin` | user `is_active` | outbox row |
| `approved` / `pending` | `suspended` | `PATCH /users/{id}` changing `role` away from `merchant` (IDN-7) | `admin` | — | outbox row; sessions revoked. The profile is retained, never deleted (S5) |
| any | same | repeat of the current status | `admin` | — | **no-op**, `200`, no event — idempotent |

Not permitted: `approved → pending`, `suspended → pending` (`pending` is only ever initial).

**Effect of each state on token issuance** ([archive/0003](archive/0003-events-and-observability.md) Q6: "`identity` refuses to issue tokens; `catalog` consumes `merchant.status_changed` and hides the merchant's products"):

| Status | Login | Refresh | Claim |
| --- | --- | --- | --- |
| `pending` | allowed — [archive/0001](archive/0001-foundation-and-core-services.md) Q3 does not gate on it (open, IDN-Q1) | allowed | `merchant_status: "pending"` |
| `approved` | allowed | allowed | `merchant_status: "approved"` |
| `suspended` | `403` `…/errors/merchant-suspended` | `401`, session revoked | — (never issued) |

A suspended merchant's already-issued access token lives out its ≤ 15 minutes; `catalog`'s event-fed projection hides their products independently, so the storefront effect does not wait for the token.

**`RefreshSession` lifecycle** — not a `DomainEnum` (it is derived from two timestamps), but a state machine all the same:

| From | To | Trigger | Condition |
| --- | --- | --- | --- |
| — | `live` | login; successful rotation (as the successor) | — |
| `live` | `rotated` (`revoked_at` set) | `POST /auth/refresh` presenting it | `expires_at > now()`, user active, merchant not suspended |
| `live` | `revoked` (`revoked_at` set) | logout; password change (other sessions); admin deactivation; role change; merchant suspension; reuse detection | — |
| `live` | `expired` | time passes `expires_at` | — (no write; checked on read) |
| `rotated` / `revoked` | — (terminal) | presented again → **reuse detected** | → revoke every live session of the user, `401` |

```mermaid
stateDiagram-v2
    [*] --> live: login, or successor of a rotation
    live --> rotated: POST /auth/refresh presents it
    live --> revoked: logout, password change, deactivation,<br/>role change, suspension, reuse detection
    live --> expired: now() passes expires_at (no write)
    rotated --> [*]
    revoked --> [*]
    expired --> [*]
    note right of revoked
        rotated and revoked are terminal. Presenting either again
        is reuse: every live session of the user is revoked, 401.
    end note
```

`rotated` and `revoked` are indistinguishable in the exact schema (both are `revoked_at IS NOT NULL`), so **any** presentation of a revoked token is treated as reuse. That is the fail-safe reading; see §3.3.1 IDN-Q2 for narrowing it to a family.

**`users.is_active`** — boolean, not a machine: `true → false` (admin deactivate: revoke all sessions) and `false → true` (admin reactivate), both via `PATCH /users/{user_id}`.

##### 3.1.1.4. Workflows and async jobs

`identity` runs **no Temporal workflows** and hosts no activities. Registration, login, rotation and status changes are each one database transaction.

| Job | Kind | Owner | Behaviour |
| --- | --- | --- | --- |
| Event emission | Transactional outbox (A10, S12) | `identity` writes; `outbox-relay` publishes ([RFC 0001](0001-platform-and-shared-infrastructure.md)) | The user/profile change and its `outbox` row commit together. The relay polls `identity_db.outbox` (500 ms, `FOR UPDATE SKIP LOCKED`, archive/0003 C1/C2), produces with key `aggregate_id` (C3), sets `published_at`. Failure mode is a duplicate, never a loss |
| Published-outbox sweep | Celery Beat `sweep_published_outbox`, `maintenance` queue, daily | Platform task, runs against every producer's outbox including `identity_db` | Deletes published rows past retention |
| Expired-session sweep (IDN-9, proposed) | Celery Beat `sweep_expired_refresh_sessions`, `maintenance` queue, daily | `identity` repo function (`auth/session_repo.py`), hosted by `celery-worker` | Deletes `refresh_sessions` rows with `expires_at < now() - interval '30 days'` or `revoked_at < now() - interval '30 days'`, in batches of 1 000. The 30-day tail keeps reuse detection working for a replayed token well past its expiry |
| Rehash on login | Inline, same request | `auth/authentication_repo.py` | Replaces an outdated Argon2 hash after a successful verify |

**Outbox rows `identity` writes.** `aggregate_type` = `user`, `aggregate_id` = `users.id` (so the Kafka key is `user_id`, matching the catalogue), `event_type` from §3.1.2.3, `correlation_id` from the request context, `causation_id` null, `client_id` from `X-Client-Id` if the gateway passed one.

```mermaid
sequenceDiagram
    autonumber
    participant I as identity
    participant DB as identity_db
    participant RL as outbox-relay
    participant K as Kafka
    participant Cs as catalog / analytics / notification
    I->>DB: BEGIN, state change + INSERT outbox row, COMMIT
    loop every 500 ms
        RL->>DB: SELECT unpublished rows FOR UPDATE SKIP LOCKED
        RL->>K: produce, key = aggregate_id (user_id)
        RL->>DB: SET published_at
    end
    K->>Cs: deliver (at least once, consumers dedupe)
```

##### 3.1.1.5. Dependencies and degradation

**Synchronous calls in.**

| Caller | Calls | Why |
| --- | --- | --- |
| `gateway` | every public `/api/v1/auth/*`, `/api/v1/users/*`, `/api/v1/merchants/*` route | Routing by prefix ([RFC 0000 §3.1.2.1](0000-architecture.md#3121-surface-map)) |
| Every verifier (`gateway`, all services, workers) | `GET /api/v1/auth/jwks` | Key-cache refresh — off the request path |
| `temporal-worker` | `POST /api/v1/auth/token` (direct, `http://identity:8001`) | Service token for activities calling `inventory` and `catalog` |

**Synchronous calls out.** **None.** `identity` calls no other service. This is why it can sit at the bottom of the dependency graph.

**Infrastructure.** PostgreSQL (`identity_db`) — required. Kafka, `outbox-relay` — not required to serve requests. Redis — not used by `identity` directly (the gateway's rate limiter uses it). OTel Collector — optional (export failures are dropped).

**When a dependency is down.**

| Down | Effect on `identity` | Effect on the platform |
| --- | --- | --- |
| `identity` itself | — | Login, register, refresh, logout, profile routes fail (`504`/`503` problem from the gateway). **Every already-authenticated read and write elsewhere keeps working** (local verification, R7) until access tokens expire; clients then cannot refresh. Verifiers keep their cached JWKS and keep verifying. `temporal-worker` cannot obtain a new service token once its cached one expires; activities fail and Temporal retries them with backoff, so workflows stall rather than fail |
| PostgreSQL | `/health/ready` fails; every route except `/health` and `/api/v1/auth/jwks` returns `503`. JWKS is served from memory and keeps working | As above |
| Kafka / `outbox-relay` | None. Outbox rows accumulate in `identity_db` and publish on recovery | `analytics` counts and `catalog`'s merchant projection lag; no user-visible failure on identity routes |
| Redis (gateway limiter) | None directly. Rate limiting **fails open** ([archive/0003](archive/0003-events-and-observability.md) §6.1) | Login brute-force protection is absent for the outage — accepted, recorded in §6.2 |
| JWKS unreachable at a verifier's startup | — | Verifier falls back to `JWT_PUBLIC_KEYS` from its environment |
| JWKS unreachable during rotation | — | A token with an unknown `kid` fails verification (`401`) at that verifier until JWKS is reachable; step 2 of the rotation procedure exists so this never happens in a planned rotation |

##### 3.1.1.6. Configuration

Environment only, validated at startup by `pydantic-settings`; a missing or malformed variable fails the container immediately ([archive/0001](archive/0001-foundation-and-core-services.md) §6.3).

**`identity` service.**

| Variable | Default (local) | Meaning |
| --- | --- | --- |
| `IDENTITY_DATABASE_URL` | `postgresql+asyncpg://identity:identity@postgres:5432/identity_db` | Own role, own database (R1) |
| `DB_POOL_SIZE` / `DB_MAX_OVERFLOW` | `10` / `20` | SQLAlchemy async pool ([archive/0001](archive/0001-foundation-and-core-services.md) §6.1) |
| `JWT_KEYS_DIR` | `/run/secrets/jwt` | Directory of `<kid>.pem` private keys |
| `JWT_ACTIVE_KID` | *(required)* | Signing key |
| `JWT_ISSUER` | `smartretail-identity` | `iss` |
| `JWT_AUDIENCE` | `smartretail` | `aud` |
| `ACCESS_TOKEN_TTL_SECONDS` | `900` | 15 minutes |
| `REFRESH_TOKEN_TTL_SECONDS` | `604800` | 7 days |
| `SERVICE_TOKEN_TTL_SECONDS` | `300` | 5 minutes (IDN-4) |
| `JWKS_MAX_AGE_SECONDS` | `300` | `Cache-Control: max-age` on JWKS |
| `ARGON2_TIME_COST` | `3` | IDN-1 |
| `ARGON2_MEMORY_COST_KIB` | `65536` | IDN-1 |
| `ARGON2_PARALLELISM` | `4` | IDN-1 |
| `ARGON2_HASH_LEN` | `32` | IDN-1 |
| `ARGON2_SALT_LEN` | `16` | IDN-1 |
| `PASSWORD_MIN_LENGTH` / `PASSWORD_MAX_LENGTH` | `12` / `128` | IDN-2 |
| `SERVICE_CLIENTS` | `{"temporal-worker": {"secret_hash": "$argon2id$…", "scopes": ["inventory:internal", "catalog:internal"]}}` | Client-credentials registry (JSON). Secret hashes, never secrets |
| `SEED_ADMIN_EMAIL` / `SEED_ADMIN_PASSWORD` | `admin@smartretail.local` / *(from `.env`)* | Read by `make seed` only |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://otel-collector:4317` | Tracing/metrics export |
| `OTEL_SERVICE_NAME` | `identity` | |
| `LOG_LEVEL` | `INFO` | JSON logs |

**Verifier-side settings** (in `common`, set in every other service; listed because `identity` defines their contract): `JWT_JWKS_URL` (`http://identity:8001/api/v1/auth/jwks`), `JWT_PUBLIC_KEYS` (fallback PEM set), `JWT_ISSUER`, `JWT_AUDIENCE`, `JWT_LEEWAY_SECONDS` (`30`), `JWKS_MIN_REFETCH_SECONDS` (`30` — the unknown-`kid` refetch throttle). `temporal-worker` additionally sets `SERVICE_CLIENT_ID` and `SERVICE_CLIENT_SECRET`.

**Gateway-side settings affecting identity routes** (owned by [RFC 0002](0002-gateway-service.md)): the login limit of 5 per minute per IP and per email, the 100 per minute per IP default, the 10-second upstream timeout.

##### 3.1.1.7. Observability

RED metrics, traces and logs follow the platform stack ([RFC 0001](0001-platform-and-shared-infrastructure.md); archive/0003 C10–C12, G8). `/metrics` is exposed on `:8001`.

**Metrics.** The standard HTTP set (`http_server_requests_total{route,method,status}`, `http_server_request_duration_seconds{route,method}`) from the shared instrumentation, plus (IDN-10):

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `identity_registrations_total` | counter | `role` | Successful registrations |
| `identity_login_attempts_total` | counter | `outcome` = `success` \| `invalid_credentials` \| `inactive` \| `suspended` | Login results. The `invalid_credentials` rate is the brute-force signal |
| `identity_refresh_total` | counter | `outcome` = `rotated` \| `reuse_detected` \| `expired` \| `unknown` \| `refused` | Refresh results |
| `identity_refresh_reuse_detected_total` | counter | — | Replayed refresh tokens; each one revoked a user's sessions. Non-zero is worth reading |
| `identity_sessions_revoked_total` | counter | `reason` = `logout` \| `password_change` \| `deactivated` \| `role_change` \| `suspended` \| `reuse` | Revocations |
| `identity_tokens_issued_total` | counter | `typ` = `access` \| `service`, `role` | Token issuance |
| `identity_service_token_denied_total` | counter | `client_id` | Bad client credentials or unknown client |
| `identity_password_hash_seconds` | histogram | `op` = `hash` \| `verify` \| `dummy` | Argon2id cost; drift here is a capacity signal |
| `identity_merchant_status_changes_total` | counter | `from`, `to` | Approval activity |
| `identity_signing_key_info` | gauge (=1) | `kid`, `active` | Which keys are loaded and which signs — a rotation is visible on the dashboard |
| `identity_jwks_requests_total` | counter | `status` = `200` \| `304` | Verifier cache behaviour |

The outbox backlog for `identity_db` is exported by `outbox-relay` (`outbox_backlog{service="identity"}`), not by `identity`.

**Spans.** Auto-instrumented FastAPI, SQLAlchemy and httpx spans, plus manual spans `identity.password.hash`, `identity.password.verify`, `identity.token.sign`, `identity.session.rotate`. Attributes: `enduser.id` (user id), `identity.outcome`. Never: email, password, token, token hash.

**Log fields.** `request_id`/`trace_id`, `user_id`, `session_id`, `event` (`login`, `refresh`, `reuse_detected`, …), `outcome`, `client_ip`, `kid`. Emails are logged only as the first 12 hex characters of `SHA-256(lower(email))`, which is enough to correlate an attack across lines and useless for harvesting. Passwords, raw tokens and `Authorization` headers are never logged; the logging filter in `common` drops those keys.

**Dashboard.** An "Identity" panel group on the per-service RED dashboard: login outcomes stacked, refresh outcomes with reuse highlighted, hash latency p50/p99, active signing key, merchant status changes.

**Alerts.** None configured — alerting and SLOs are out of scope for the platform ([archive/0003](archive/0003-events-and-observability.md) §11). The conditions that would become alerts are named so they are ready: `identity_refresh_reuse_detected_total` increase > 0 in 5 min; `invalid_credentials` > 50/min; `identity_password_hash_seconds` p99 > 1 s.

#### 3.1.2. API changes

Conventions (problem details, pagination envelope with `limit` ceiling 100, `X-Request-ID`, versioning under `/api/v1`) are fixed in [RFC 0000 §3.1.2.2](0000-architecture.md#3122-api-conventions). All routes are reached through the gateway at `:8000` except `POST /api/v1/auth/token`, which is internal.

##### 3.1.2.1. Endpoints

**identity — authentication**

| Method | Path | Auth | Request | Success | Errors |
| --- | --- | --- | --- | --- | --- |
| `POST` | `/api/v1/auth/register` | public | `{email, password, full_name, role, business_name?}`; `role ∈ {customer, merchant}` — `admin` cannot be self-assigned. `business_name` is used only when `role=merchant` and defaults to `full_name` (IDN-11) | `201` + `Location: /api/v1/users/{id}`, body: user resource. No tokens — the client logs in next | `409` `…/errors/email-taken` duplicate email; `422` weak password, invalid email, `role: admin`, unknown field |
| `POST` | `/api/v1/auth/login` | public | `{email, password}` | `200` `{access_token, refresh_token, token_type: "Bearer", expires_in: 900}` | `401` `…/errors/invalid-credentials` — **one identical body** for unknown email and wrong password; `403` `…/errors/account-inactive` (only after the password verifies); `403` `…/errors/merchant-suspended`; `422`; `429` from the gateway with `Retry-After` |
| `POST` | `/api/v1/auth/refresh` | public (the refresh token is the credential) | `{refresh_token}` | `200` — same shape as login; a **new** refresh token, the presented one is dead | `401` `…/errors/invalid-refresh-token` for unknown, expired, revoked, or reused (reuse additionally revokes every live session of the user); `401` if the user is inactive or the merchant suspended (session revoked) |
| `POST` | `/api/v1/auth/logout` | bearer (`access`) | `{refresh_token}` | `204`. Revokes the presented refresh token if it belongs to `sub`; otherwise still `204` (idempotent, reveals nothing) | `401` no/invalid bearer |
| `GET` | `/api/v1/auth/jwks` | public | — | `200` `{"keys":[{"kty":"RSA","use":"sig","alg":"RS256","kid","n","e"}, …]}` with caching headers below; `304` on a matching `If-None-Match` | — |
| `POST` | `/api/v1/auth/token` | **internal** — client credentials; not routed by the gateway (IDN-12) | `application/x-www-form-urlencoded`: `grant_type=client_credentials`, `client_id`, `client_secret`, optional `scope` (subset of granted) — RFC 6749 §4.4 | `200` `{access_token, token_type: "Bearer", expires_in: 300, scope}`. No refresh token | `400` `{"error":"unsupported_grant_type"}` / `invalid_scope`; `401` `{"error":"invalid_client"}` (RFC 6749 error body, not a problem detail, because this is an OAuth endpoint) |

**JWKS caching headers (IDN-6).**

```
HTTP/1.1 200 OK
Content-Type: application/jwk-set+json
Cache-Control: public, max-age=300, stale-while-revalidate=60, stale-if-error=86400
ETag: "<sha256 of the serialised key set, first 16 hex>"
```

`max-age=300` bounds how long a newly published key takes to reach every verifier (rotation step 2). `stale-if-error=86400` tells caches — including the `common` in-process cache, which honours the same directives — to keep verifying with the last good set for a day if `identity` is unreachable, which is what makes IDN-G3 hold across an `identity` outage longer than five minutes. On a token with an unknown `kid` a verifier refetches immediately, at most once per `JWKS_MIN_REFETCH_SECONDS` (30), so a flood of forged `kid`s cannot turn verifiers into a JWKS amplifier.

How a verifier in `common` resolves the key for a token (IDN-6, R7):

```mermaid
flowchart TD
    T["Bearer token"] --> A{"alg is RS256?"}
    A -- no --> R1["401, before any key lookup"]
    A -- yes --> K{"kid in cached key set?"}
    K -- yes --> V["Verify signature, iss, aud, exp, nbf, typ"]
    K -- no --> Th{"Last refetch older than<br/>JWKS_MIN_REFETCH_SECONDS?"}
    Th -- no --> R2["401"]
    Th -- yes --> F["Refetch JWKS"]
    F --> Ok{"Fetched?"}
    Ok -- yes --> K2{"kid now present?"}
    K2 -- yes --> V
    K2 -- no --> R2
    Ok -- no --> St["Keep last good set<br/>(stale-if-error, 24 h)"]
    St --> R2
    Boot["Verifier start-up"] --> BJ{"JWKS reachable?"}
    BJ -- yes --> Cache["Cache JWKS"]
    BJ -- no --> Env["Use JWT_PUBLIC_KEYS<br/>from the environment"]
```

**Registration.** One transaction writes the user, the merchant profile when `role=merchant`, and (from M3, IDN-T4) the `identity.user.registered` outbox row.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant G as gateway
    participant R as identity<br/>registration_repo
    participant H as PasswordHasher<br/>(thread pool)
    participant DB as identity_db
    C->>G: POST /api/v1/auth/register
    G->>R: forward (100/min per IP default limit)
    R->>R: validate body: role in customer, merchant, password policy (IDN-2)
    alt invalid body, role admin, weak password
        R-->>C: 422 validation
    end
    R->>H: hash password (Argon2id, IDN-1)
    H-->>R: PHC string
    R->>DB: BEGIN, INSERT users
    alt unique email violated (citext index)
        DB-->>R: unique violation, ROLLBACK
        R-->>C: 409 email-taken
    else inserted
        opt role is merchant
            R->>DB: INSERT merchant_profiles (status pending, unique slug)
        end
        R->>DB: INSERT outbox identity.user.registered (M3)
        R->>DB: COMMIT
        R-->>C: 201, Location /api/v1/users/{id}, user resource (no tokens)
    end
```

**Login.**

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant G as gateway
    participant I as identity<br/>authentication_repo
    participant DB as identity_db
    C->>G: POST /api/v1/auth/login {email, password}
    G->>G: rate limit 5/min per IP and per email
    alt over the limit
        G-->>C: 429 + Retry-After
    end
    G->>I: forward
    I->>DB: SELECT user BY email
    alt unknown email
        I->>I: verify against the dummy hash (IDN-17)
        I-->>C: 401 invalid-credentials
    else wrong password
        I->>I: verify Argon2id fails
        I-->>C: 401 invalid-credentials (identical body)
    else password verifies
        alt user inactive
            I-->>C: 403 account-inactive
        else merchant suspended
            I-->>C: 403 merchant-suspended
        else allowed
            I->>DB: BEGIN, INSERT refresh_sessions, rehash if needed, COMMIT
            I->>I: sign access JWT (kid, sid, role, merchant_status)
            I-->>C: 200 {access_token, refresh_token, expires_in 900}
        end
    end
```

**Refresh.**

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant G as gateway
    participant S as identity<br/>session_repo
    participant DB as identity_db
    C->>G: POST /api/v1/auth/refresh {refresh_token}
    G->>S: forward
    S->>S: h = sha256(token)
    S->>DB: BEGIN, SELECT refresh_sessions WHERE token_hash = h FOR UPDATE
    alt no row, or expired
        S-->>C: 401 invalid-refresh-token
    else revoked_at set (reuse)
        S->>DB: UPDATE refresh_sessions SET revoked_at = now()<br/>WHERE user_id = u AND revoked_at IS NULL, COMMIT
        S-->>C: 401 invalid-refresh-token
    else user inactive or merchant suspended
        S->>DB: revoke this session, COMMIT
        S-->>C: 401 invalid-refresh-token
    else live
        S->>DB: UPDATE old row SET revoked_at = now()
        S->>DB: INSERT successor (fresh 7-day expiry), COMMIT
        S-->>C: 200 {new access_token, new refresh_token}
    end
```

The decision the refresh handler makes, in order:

```mermaid
flowchart TD
    A["POST /auth/refresh"] --> B["h = sha256(token)<br/>SELECT ... FOR UPDATE"]
    B --> C{"Row found?"}
    C -- no --> X1["401"]
    C -- yes --> D{"revoked_at IS NOT NULL?"}
    D -- yes --> X2["Reuse: revoke every live session<br/>of the user, COMMIT, 401"]
    D -- no --> E{"Expired?<br/>expires_at not in the future"}
    E -- yes --> X3["401"]
    E -- no --> F{"User inactive or<br/>merchant suspended?"}
    F -- yes --> X4["Revoke session, COMMIT, 401"]
    F -- no --> G["Revoke old row, INSERT successor,<br/>COMMIT, 200 new pair"]
```

The `FOR UPDATE` row lock serialises two concurrent refreshes with the same token: the second sees `revoked_at` set and is treated as reuse — the correct reading, because two clients holding one refresh token is exactly what theft looks like.

**identity — users**

| Method | Path | Auth | Request | Success | Errors |
| --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/users/me` | bearer | — | `200` user resource | `401` |
| `PATCH` | `/api/v1/users/me` | bearer | `{full_name?, phone?}` — nothing else | `200` user resource | `422` on `role`, `email`, `is_active` or any other field |
| `POST` | `/api/v1/users/me/password` | bearer | `{current_password, new_password}` | `204`. Revokes **all other** sessions (every live row except `sid`) | `401` `…/errors/invalid-credentials` wrong current password; `422` weak new password |
| `GET` | `/api/v1/users` | bearer | Query: `role`, `is_active`, `q` (email/name substring), `limit`, `offset` | `200` page envelope of user resources | `403` |
| `GET` | `/api/v1/users/{user_id}` | bearer | — | `200` user resource (with `merchant_profile` embedded when present) | `403`; `404` |
| `PATCH` | `/api/v1/users/{user_id}` | bearer | `{role?, is_active?}` | `200` user resource. `is_active=false` or any `role` change revokes all the user's sessions; `role → merchant` creates a `pending` profile if none; `role` away from `merchant` suspends the profile (IDN-7) | `403`; `404`; `409` `…/errors/last-admin` (I10); `422` |

**User resource.** `{id, email, full_name, phone, role, is_active, email_verified, created_at, updated_at}`. There is no password field on any response model, so it cannot leak by accident.

`PATCH /api/v1/users/{user_id}` carries the most rules of any route (I10, IDN-7):

```mermaid
flowchart TD
    P["PATCH /users/{user_id}<br/>role?, is_active?"] --> A{"Caller is admin?"}
    A -- no --> E403["403"]
    A -- yes --> U{"User exists?"}
    U -- no --> E404["404"]
    U -- yes --> L{"Would demote or deactivate<br/>the last active admin?"}
    L -- yes --> E409["409 last-admin"]
    L -- no --> R{"role changed?"}
    R -- "to merchant" --> M1["Create pending profile if none"]
    R -- "away from merchant" --> M2["Suspend profile (kept),<br/>status_changed outbox row"]
    R -- "other change" --> S["Revoke all the user's sessions"]
    R -- no --> D{"is_active set to false?"}
    M1 --> S
    M2 --> S
    D -- yes --> S
    D -- no --> OK["200 user resource"]
    S --> OK
```

**identity — merchants**

| Method | Path | Auth | Request | Success | Errors |
| --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/merchants/{merchant_id}` | public | — | `200` `{merchant_id, business_name, slug, description, status}` — the display name clients need next to a product | `404` if absent **or** not `approved`, for callers other than the merchant themselves and `admin` (IDN-13) |
| `GET` | `/api/v1/merchants/me` | bearer | — | `200` the caller's own profile, any status (IDN-14) | `403` if `role ≠ merchant` |
| `PATCH` | `/api/v1/merchants/me` | bearer | `{business_name?, description?}`. `slug` and `status` are not writable here | `200` profile | `403`; `422` |
| `GET` | `/api/v1/merchants` | bearer | Query: `status`, `q`, `limit`, `offset` — the approval queue is `?status=pending` (IDN-14) | `200` page envelope of profiles | `403` |
| `POST` | `/api/v1/merchants/{merchant_id}/status` | bearer | `{status, reason?}` | `200` profile. Emits `identity.merchant.status_changed` unless a no-op | `403`; `404`; `409` `…/errors/invalid-merchant-transition`; `422` |

`/me` is declared before `/{merchant_id}` in `merchants/v1/routes.py` so `me` is never parsed as an id. `slug` is derived from `business_name` at creation (lower-case, ASCII-folded, hyphenated) with a `-2`, `-3`… suffix on collision; unique by index.

`catalog` does **not** call `GET /merchants/{merchant_id}` on its read path — that would be a second synchronous cross-service call, and A13/R5 allow exactly one (`order → catalog`). Clients call it, or `catalog` relies on its event-fed projection.

**Merchant status change (M3).** The admin endpoint, its transaction, and the downstream reaction that needs no call back to `identity`:

```mermaid
sequenceDiagram
    autonumber
    participant A as Admin
    participant G as gateway
    participant I as identity<br/>merchant repo
    participant DB as identity_db
    participant K as Kafka (via outbox-relay)
    participant Cat as catalog
    A->>G: POST /api/v1/merchants/{id}/status {status, reason}
    G->>I: forward (admin token)
    I->>DB: BEGIN, load merchant_profiles row
    alt status unchanged
        I-->>A: 200 profile, no event (no-op)
    else not in MERCHANT_TRANSITIONS
        I-->>A: 409 invalid-merchant-transition
    else allowed
        I->>DB: UPDATE status, INSERT outbox status_changed
        opt new status is suspended
            I->>DB: revoke all live refresh sessions of the merchant
        end
        I->>DB: COMMIT
        I-->>A: 200 profile
        DB-->>K: relay publishes identity.merchant.status_changed
        K->>Cat: consume, suspended hides products, approved restores
    end
```

**Service token (M2, IDN-12).** Not routed by the gateway; `temporal-worker` calls `identity` directly.

```mermaid
sequenceDiagram
    autonumber
    participant W as temporal-worker
    participant I as identity
    participant Inv as inventory
    W->>I: POST /api/v1/auth/token<br/>grant_type=client_credentials, client_id, client_secret, scope
    I->>I: look up SERVICE_CLIENTS, verify secret hash (Argon2id)
    alt unknown client or wrong secret
        I-->>W: 401 invalid_client
    else scope not granted, or wrong grant_type
        I-->>W: 400 invalid_scope / unsupported_grant_type
    else ok
        I-->>W: 200 {access_token typ=service, expires_in 300, scope}
    end
    W->>Inv: /internal/v1/inventory/reservations, Bearer service token
    Inv->>Inv: verify locally, require typ=service and scope
```

**Operational**

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Liveness |
| `GET` | `/health/ready` | Readiness, including a `identity_db` round-trip and a loaded active signing key |
| `GET` | `/metrics` | Prometheus exposition |
| `GET` | `/docs`, `/openapi.json` | Per-service OpenAPI; `UserRole` and `MerchantStatus` value sets are published here (S4) |

**Idempotency and `202`.** No identity endpoint uses `Idempotency-Key` — the mechanism belongs to business-object creation in `order` ([RFC 0005](0005-order-service.md)). Registration is protected by the unique email (a retry gets `409`); `PATCH` and the status endpoint are naturally idempotent; logout is idempotent by design. `POST /auth/refresh` is **deliberately not** idempotent: a retried refresh whose first response was lost is indistinguishable from replay and is treated as reuse (IDN-Q3). No identity endpoint starts a workflow, so none returns `202`.

**Problem types** (all under the `common` base URI): `invalid-credentials` (401), `invalid-refresh-token` (401), `account-inactive` (403), `merchant-suspended` (403), `forbidden` (403), `user-not-found` / `merchant-not-found` (404), `email-taken` (409), `last-admin` (409), `invalid-merchant-transition` (409), `validation` (422, with `violations[]`), `rate-limited` (429, from the gateway).

##### 3.1.2.2. Role matrix

`✓` allowed · `✗` `403` · `401` no or invalid token · `—` not applicable. **internal** = a service token (`typ=service`). Every user-facing route rejects service tokens with `401`; the internal route accepts only client credentials.

| Endpoint | anonymous | customer | merchant | admin | internal | Scoping rule |
| --- | --- | --- | --- | --- | --- | --- |
| `POST /auth/register` | ✓ | ✓ | ✓ | ✓ | ✗ | `role ∈ {customer, merchant}` regardless of caller |
| `POST /auth/login` | ✓ | ✓ | ✓ | ✓ | ✗ | — |
| `POST /auth/refresh` | ✓ | ✓ | ✓ | ✓ | ✗ | The refresh token is the credential |
| `POST /auth/logout` | 401 | ✓ | ✓ | ✓ | 401 | Revokes only a session whose `user_id = sub` |
| `GET /auth/jwks` | ✓ | ✓ | ✓ | ✓ | ✓ | Public keys only |
| `POST /auth/token` | client credentials only; not routed by the gateway | — | — | — | ✓ | Scope ⊆ client's granted scopes |
| `GET /users/me` | 401 | ✓ | ✓ | ✓ | 401 | `sub` |
| `PATCH /users/me` | 401 | ✓ | ✓ | ✓ | 401 | `sub`; `full_name`, `phone` only |
| `POST /users/me/password` | 401 | ✓ | ✓ | ✓ | 401 | `sub` |
| `GET /users` | 401 | ✗ | ✗ | ✓ | 401 | All users |
| `GET /users/{user_id}` | 401 | ✗ | ✗ | ✓ | 401 | Any user |
| `PATCH /users/{user_id}` | 401 | ✗ | ✗ | ✓ | 401 | Any user; I10 guard |
| `GET /merchants/{merchant_id}` | ✓ (approved only) | ✓ (approved only) | ✓ (approved, or own in any status) | ✓ (any status) | 401 | Non-approved → `404` for non-owners |
| `GET /merchants/me` | 401 | ✗ | ✓ | ✗ | 401 | `sub` |
| `PATCH /merchants/me` | 401 | ✗ | ✓ | ✗ | 401 | `sub`; `business_name`, `description` only |
| `GET /merchants` | 401 | ✗ | ✗ | ✓ | 401 | All profiles |
| `POST /merchants/{merchant_id}/status` | 401 | ✗ | ✗ | ✓ | 401 | Transition table (§3.1.1.3) |

The gateway performs the coarse check (valid, unexpired token; admin-only prefixes) as an early rejection; `identity` re-checks every row above itself with `require_roles` (A5). Ownership is always derived from `sub` in the verified token, never from a path, body or query value — a merchant cannot address another merchant's profile through `/merchants/me` because there is no id to supply.

##### 3.1.2.3. Events produced

Both are in the platform catalogue ([RFC 0000 §3.1.2.3](0000-architecture.md#3123-event-catalogue)), wrapped in the standard envelope ([RFC 0000 §3.1.2.4](0000-architecture.md#3124-event-envelope)), Avro-encoded with `BACKWARD` compatibility (archive/0003 C5), written through `identity_db.outbox` (A10). Payloads carry identifiers and the minimum needed to render — no hashes, no email, no phone (archive/0003 §6.2).

| Topic | Key | Trigger | Consumers | Topic config |
| --- | --- | --- | --- | --- |
| `smartretail.identity.user.registered` | `user_id` | `POST /auth/register` commits | `analytics`, `notification` | 3 partitions, RF 1, 7-day retention, `<topic>.dlq` |
| `smartretail.identity.merchant.status_changed` | `user_id` | a non-no-op merchant status transition commits (§3.1.1.3) | `catalog`, `analytics` | as above, plus `cleanup.policy=compact` — the latest status per merchant is what a new consumer needs |

**Payloads (IDN-15).** No archived RFC fixes these field lists; they are specified here from what each consumer demonstrably needs.

`identity.user.registered` v1:

| Field | Avro type | Meaning | Needed by |
| --- | --- | --- | --- |
| `user_id` | `string` (uuid) | The new user | all |
| `role` | `string` (`UserRole`) | `customer` or `merchant` — never `admin` | `analytics`: Total Customers / Total Merchants are counters filtered on `role` |
| `full_name` | `string` | Display name for the `welcome` notification | `notification` (type `welcome`, `origin_only` scope) |
| `merchant_status` | `["null","string"]`, default `null` | `pending` for a merchant, `null` for a customer | `analytics` |
| `registered_at` | `string` (RFC 3339) | `users.created_at` | `analytics` daily bucketing |

`identity.merchant.status_changed` v1:

| Field | Avro type | Meaning | Needed by |
| --- | --- | --- | --- |
| `user_id` | `string` (uuid) | The merchant — equal to `merchant_id` everywhere else | all |
| `previous_status` | `string` (`MerchantStatus`) | Before | `analytics` |
| `status` | `string` (`MerchantStatus`) | After — the fact a compacted consumer rebuilds from | `catalog`: `suspended` hides (unpublishes) the merchant's products; `approved` restores eligibility |
| `business_name` | `string` | So a projection can display the merchant without a call back | `catalog` |
| `reason` | `["null","string"]`, default `null` | Admin-supplied note | human diagnosis only |
| `changed_at` | `string` (RFC 3339) | `merchant_profiles.updated_at` | ordering sanity within a key |

The envelope's `actor_id` is the admin who made the change (or the registrant for `user.registered`); `correlation_id` is the request's trace id; `client_id` is the `X-Client-Id` if present — which is what lets `notification` scope the `welcome` toast to the device that registered.

Downstream effects owned elsewhere: `catalog` unpublishing a suspended merchant's products ([RFC 0004](0004-catalog-service.md#3124-events-consumed)) — which in turn removes their chunks from the assistant's index ([RFC 0009](0009-assistant-service.md)) — and the analytics counters ([RFC 0008](0008-analytics-service.md#3124-events-consumed)).

##### 3.1.2.4. Events consumed

**None.** `identity` is a pure producer; it carries no `processed_events` table ([RFC 0000 §3.1.3.2](0000-architecture.md#3132-shared-tables--outbox-and-processed_events), carriage matrix: `identity_db` — outbox yes, processed_events no).

#### 3.1.3. Data/schema changes

###### Store overview — `identity_db`

Owned by `identity` · PostgreSQL 16 · one logical database on the shared instance (A2), reached with a dedicated role granted on this database only (R1). The highest-security boundary in the platform: the only store holding credentials, and the only one whose service holds the JWT signing key. Extension required: `citext`. `gen_random_uuid()` is built in on PostgreSQL 16.

| Table | Purpose |
| --- | --- |
| [`users`](#users) | Accounts, credentials, role |
| [`merchant_profiles`](#merchant_profiles) | Public merchant identity, approval status |
| [`refresh_sessions`](#refresh_sessions) | Refresh-token sessions, revocable |
| `outbox` `‡` | S12 shared DDL — [defined in RFC 0000](0000-architecture.md#3132-shared-tables--outbox-and-processed_events) |

**Markers** (as in [archive/0006](archive/0006-service-decomposition-and-data-model.md) §3.1.3): *(none)* — stated explicitly in a source RFC; `†` — the column is named in a source RFC but its type was not stated, and the type shown is inferred from S1–S15; `‡` — the column or table is not named in any source RFC and exists because a convention requires it (S1 a primary key, S3 timestamps, S12 an outbox for a producer, S13 a dedupe table for a consumer). The three `identity_db` tables carry no `†` or `‡` cells; the `identity_db.outbox` **table** is `‡`. Row counts: `users` 10 columns, `merchant_profiles` 7, `refresh_sessions` 8.

```mermaid
flowchart LR
    subgraph idb["identity_db"]
        users["users"]
        mp["merchant_profiles (1:0..1)"]
        rs["refresh_sessions (1:n)"]
        ob["outbox ‡"]
        users --- mp
        users --- rs
    end
    users -. "sub claim (no FK, R2)" .-> claims["products.merchant_id,<br/>orders.customer_id, …"]
    ob -. "identity.* events" .-> proj["catalog merchant projection<br/>(event-fed)"]
```

```mermaid
erDiagram
    users ||--o| merchant_profiles : "is a merchant with"
    users ||--o{ refresh_sessions : "holds"
    users {
        uuid id PK "gen_random_uuid()"
        citext email "unique, not null"
        text password_hash "Argon2id PHC string"
        text full_name "not null"
        text phone "nullable"
        text role "UserRole: customer, merchant, admin"
        bool is_active "default true"
        bool email_verified "default false"
        timestamptz created_at
        timestamptz updated_at
    }
    merchant_profiles {
        uuid user_id PK, FK "users.id, cascade delete"
        text business_name "not null"
        text slug "unique"
        text description "nullable"
        text status "MerchantStatus, default pending"
        timestamptz created_at
        timestamptz updated_at
    }
    refresh_sessions {
        uuid id PK
        uuid user_id FK "users.id, cascade delete, indexed"
        text token_hash "SHA-256, raw value never stored"
        timestamptz expires_at "indexed"
        timestamptz revoked_at "nullable"
        text user_agent "nullable"
        text ip "nullable"
        timestamptz created_at
    }
```

The shared `outbox` table (S12) is defined in [RFC 0000](0000-architecture.md#3132-shared-tables--outbox-and-processed_events) and has no FK to these tables, so it is not drawn.

###### `users`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK | `gen_random_uuid()` |
| `email` | `citext` | unique, not null |
| `password_hash` | `text` | Argon2id, not null |
| `full_name` | `text` | not null |
| `phone` | `text` | nullable |
| `role` | `text` | `customer` \| `merchant` \| `admin`, not null. `UserRole` (S4) — validated in the model, not by a Postgres type |
| `is_active` | `boolean` | default `true` |
| `email_verified` | `boolean` | default `false`; verification flow is later |
| `created_at`, `updated_at` | `timestamptz` | not null, defaults |

Indexes: unique on `email`; btree on `role`; partial on `is_active` where `false`.

###### `merchant_profiles`

One-to-one with a `merchant` user, so `catalog` has something public to show without joining across services.

| Column | Type | Notes |
| --- | --- | --- |
| `user_id` | `uuid` PK/FK → `users.id` | cascade delete |
| `business_name` | `text` | not null |
| `slug` | `text` | unique |
| `description` | `text` | nullable |
| `status` | `text` | `pending` \| `approved` \| `suspended`, default `pending`. `MerchantStatus` (S4) |
| `created_at`, `updated_at` | `timestamptz` | |

###### `refresh_sessions`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK | |
| `user_id` | `uuid` FK → `users.id` | cascade delete, indexed |
| `token_hash` | `text` | SHA-256 of the token; the raw value is never stored |
| `expires_at` | `timestamptz` | indexed |
| `revoked_at` | `timestamptz` | nullable |
| `user_agent`, `ip` | `text` | nullable, for session listing |
| `created_at` | `timestamptz` | |

**Constraints and indexes, collected.**

| Table | Constraint / index | Source |
| --- | --- | --- |
| `users` | PK `id`; unique `email`; btree `role`; partial btree `is_active` `WHERE is_active = false` | archive/0006 |
| `merchant_profiles` | PK `user_id`; FK `user_id → users.id ON DELETE CASCADE`; unique `slug` | archive/0006 |
| `refresh_sessions` | PK `id`; FK `user_id → users.id ON DELETE CASCADE`; btree `user_id`; btree `expires_at` | archive/0006 |
| `refresh_sessions` | **Proposed (IDN-16):** unique btree on `token_hash`. Every refresh and logout looks a session up by hash, and 0006 lists no index for it; uniqueness also makes a 256-bit collision a constraint error rather than a silent ambiguity | this RFC — not in archive/0006 |
| `users.role`, `merchant_profiles.status` | No `CHECK`, no native enum (S4). `COMMENT ON COLUMN … IS 'DomainEnum <Name> — see services/identity/app/core/enums.py'` | S4 / archive/0006 §3.3 |

Defaults the notes imply but do not spell out — `id` default on `refresh_sessions`, `now()` defaults on `merchant_profiles`/`refresh_sessions` timestamps, nullability of `slug`, `created_at`, `updated_at` — follow S1, S3 and S10: `gen_random_uuid()`, `NOT NULL DEFAULT now()`, and `NOT NULL` (a merchant always has a slug) respectively. These are applications of the conventions, not new decisions.

**Shared tables carried.** `outbox` `‡` (S12) — `identity` produces `identity.user.registered` and `identity.merchant.status_changed`. DDL (including `client_id` and the partial index on `published_at IS NULL`) is defined once in [RFC 0000 §3.1.3.2](0000-architecture.md#3132-shared-tables--outbox-and-processed_events). `identity_db` carries **no** `processed_events`. The `‡` on this outbox — a table required by S12 and the event catalogue but never enumerated in a source RFC — was flagged for review in [archive/0006](archive/0006-service-decomposition-and-data-model.md) §3.3.1 Q3; `identity` adopts it, since without it the two events would have to be dual-written, which A10 forbids.

**Migrations** (per-service Alembic history; every revision has a working `downgrade`, S14; run as an explicit step, never on startup; `alembic/env.py` carries the S4 `render_item` hook so `DomainEnumText` renders as `sa.Text`):

| # | Revision | Upgrade | Downgrade |
| --- | --- | --- | --- |
| 1 | `0001_users` | `CREATE EXTENSION IF NOT EXISTS citext`; `users` with its three indexes; column comment on `role` | drop table (extension left in place — it may be shared) |
| 2 | `0002_merchant_profiles` | `merchant_profiles`; column comment on `status` | drop table |
| 3 | `0003_refresh_sessions` | `refresh_sessions` with `user_id` and `expires_at` indexes | drop table |
| 4 | `0004_outbox` | shared `outbox` DDL from RFC 0000 | drop table |
| 5 | `0005_refresh_sessions_token_hash_uidx` *(proposed, IDN-16)* | `CREATE UNIQUE INDEX CONCURRENTLY` on `token_hash` | drop index |

All revisions are additive; no revision rewrites data.

#### 3.1.4. Breaking changes

**None in the external sense.** SmartRetail has no released API, no consumers and no production data.

Recorded so that no client codes against an interim behaviour:

| Change | From | To | Handling |
| --- | --- | --- | --- |
| Login rate limiting | Unthrottled | 5 attempts per minute per IP and per email at the gateway; `429` with `Retry-After` | Documented; `Retry-After` always present ([archive/0003](archive/0003-events-and-observability.md) §3.1.4) |
| Suspended merchants | Could log in and refresh | Login `403`, refresh `401` (archive/0003 Q6) | New behaviour with the status endpoint; no client relied on the old |
| Verifier key source | Public key from each service's environment (archive/0001 Q4) | JWKS, with the environment key set as fallback | Additive; no verifier loses its fallback |
| Service tokens | Internal callers present the end user's token (archive/0000 §3.1.1) | Temporal activities present a client-credentials service token; user-facing routes reject service tokens (archive/0002) | Additive — user tokens on user routes behave as before |

Internal reversal relative to the deleted prototype, carried from [archive/0001](archive/0001-foundation-and-core-services.md) §3.1.4:

| Was | Now | Why |
| --- | --- | --- |
| Symmetric, shared-secret JWTs | RS256, single signer | A shared secret in every service means every service can mint a token |

#### 3.1.5. Action Plan

Task IDs `T4`–`T7` and `T15` are carried from [archive/0001](archive/0001-foundation-and-core-services.md) §3.1.5 (its T1–T3 — skeleton, `common`, Compose — are Platform, [RFC 0001](0001-platform-and-shared-infrastructure.md)); `IDN-T*` are added.

| # | Task | Output | Depends on |
| --- | --- | --- | --- |
| T4 | `identity` skeleton + Alembic + `users`, `merchant_profiles`, `refresh_sessions` migrations | `make migrate` takes an empty database to head | Platform: `common`, Compose, Postgres init with `identity_db` role |
| T5 | Password hashing (Argon2id, IDN-1 parameters), RS256 key loading, token issue in `identity` and verify in `common` | Unit tests for sign/verify, expiry, tampering, wrong key, `alg` pinning | T4 |
| T6 | Auth endpoints: register (with merchant profile), login, refresh with rotation and reuse detection, logout | Integration tests green | T4, T5 |
| T7 | User endpoints + `require_roles` dependency + admin management routes (incl. I10 guard, session revocation on role/active change) | Role-matrix tests green | T6 |
| IDN-T1 | Key ring and `GET /api/v1/auth/jwks` with `ETag`/`Cache-Control`; `common` JWKS cache with env fallback and unknown-`kid` throttle | Rotation drill passes (§7) | T5 |
| IDN-T2 | Merchant routes: public profile, `/merchants/me`, admin listing | Tests green | T7 |
| T15 | Seed: admin from `SEED_ADMIN_*`, two merchants (`approved`), two customers | `make seed` idempotent and re-runnable | T7, IDN-T2 |
| IDN-T3 | Client-credentials grant: `SERVICE_CLIENTS` registry, `POST /api/v1/auth/token`, `typ=service` tokens; gateway excludes the route; `require_scope` in `common` | `temporal-worker` obtains a token; user routes reject it; internal routes reject user tokens | IDN-T1 |
| IDN-T4 | `outbox` migration; outbox rows for `identity.user.registered` in the registration transaction | Relay publishes to the topic; Avro schema registered | Platform: relay, Schema Registry |
| IDN-T5 | Merchant status endpoint and transition table; `identity.merchant.status_changed` outbox row; login/refresh refusal and session revocation on suspension | E2E step: suspend → `catalog` hides products (archive/0003 Q6) | IDN-T2, IDN-T4 |
| IDN-T6 | Metrics (§3.1.1.7), manual spans, log filter for secrets; identity panels on the RED dashboard | `/metrics` scraped; dashboard panels return data | Platform: OTel, Prometheus |
| IDN-T7 | Gateway login limits for `/auth/login` (5/min per IP and per hashed email) — implemented in [RFC 0002](0002-gateway-service.md), acceptance-tested here | `429` + `Retry-After` at the threshold; recovery after the window | Gateway limiter |
| IDN-T8 | *(proposed)* `sweep_expired_refresh_sessions` Beat task; `token_hash` unique index | Sweep deletes only the 30-day tail; index present | IDN-T4 |

**Delivery phasing**

| Component | Milestone | Source |
| --- | --- | --- |
| `identity` service, `identity_db`, `users`, `merchant_profiles`, `refresh_sessions` | M1 | archive/0001 T4 |
| `UserRole`, `MerchantStatus` vocabularies | M1 | archive/0006 §3.1.5 |
| Argon2id hashing, RS256 signing, 15-min access / 7-day rotating refresh tokens, reuse detection | M1 | archive/0001 T5–T6, §6.2 |
| Register, login, refresh, logout; `/users/*` including admin routes; `GET /merchants/{merchant_id}` | M1 | archive/0001 §3.1.2 |
| Seed admin and demo accounts | M1 | archive/0001 T15 |
| JWKS endpoint and verifier key cache (env public key as fallback) | M1 | archive/0000 §3.1.1; archive/0001 Q4 |
| `/merchants/me`, admin `GET /merchants` | M1 | proposed here (IDN-14) |
| Client-credentials grant and service tokens | M2 | archive/0002 §3.1.2, T2 |
| `identity_db.outbox` `‡`; `identity.user.registered`; `identity.merchant.status_changed` | M3 | archive/0006 §3.1.5; archive/0003 T5 |
| Merchant status endpoint; token refusal for suspended merchants | M3 | archive/0003 Q6 (endpoint proposed here, IDN-14) |
| Login rate limiting at the gateway | M3 | archive/0003 §3.1.2 |
| Identity metrics, spans, dashboard panels | M3 | archive/0003 T7, T10, T11 |
| Expired-session sweep, `token_hash` unique index | M3 | proposed here (IDN-9, IDN-16) |

### 3.2. Rationale

#### 3.2.1. Decision register

This register lists every significant `identity` decision. Where a decision already carries an ID in this RFC (IDN-1 … IDN-17, or a platform ID such as A4 or 0001-A5), the row says "= <ID>" and that ID stays canonical; the IDN-D numbers only index the register. Cross-cutting decisions owned by [RFC 0000](0000-architecture.md#3115-cross-cutting-decisions-a1a14) are listed only where this RFC applies them in a specific way.

| ID | Decision | Why | Alternatives rejected (and why) | Revisit if |
| --- | --- | --- | --- | --- |
| IDN-D1 | `identity` is the only signer: RS256, private key in `identity` only, every other component verifies locally (= A4, A5) | A shared secret lets any verifier forge any identity; local verification keeps `identity` off every request path ([§3.2.2](#322-rationale-in-detail)) | HS256 shared secret: every service could mint tokens (Alt 3). Introspection per request: makes `identity` a runtime dependency of everything (Alt 4) — [§5](#5-alternate-solutions) | A KMS/HSM becomes available, or instant revocation becomes a requirement |
| IDN-D2 | Stateless 15-minute access tokens, stateful 7-day refresh tokens (= 0001-A5) | Avoids a database read per request in every service; revocation latency is bounded by the access TTL ([§3.2.2](#322-rationale-in-detail), [§3.3](#33-tradeoffs)) | Server-side session cookies: a shared session store becomes a hard dependency of every service (Alt 1) | The 15-minute revocation window is unacceptable for some route |
| IDN-D3 | Refresh tokens rotate on every use; presenting a rotated or revoked token is reuse and revokes every live session of the user | A stolen token is valid only until either party uses it; the collision is the detection signal (OAuth Security BCP) ([§3.2.2](#322-rationale-in-detail)) | Family-scoped revocation: the `refresh_sessions` schema has no family column (IDN-Q2). No rotation: theft goes undetected | IDN-Q2 adds `family_id` |
| IDN-D4 | Refresh token is opaque, 32 random bytes, stored only as SHA-256 (= IDN-3, I7) | Only `identity` reads it and must look it up anyway; a signature buys nothing ([§3.1.1.2](#3112-domain-model)) | JWT refresh token: needs server state anyway, and another service might accept it (Alt 7) | — |
| IDN-D5 | Refresh is deliberately non-idempotent; no reuse grace window | A retried refresh is indistinguishable from replay; a grace window needs the raw successor stored, which I7 forbids ([§3.1.2.1](#3121-endpoints)) | Short grace window or returned-once cache: weakens detection (IDN-Q3) | Client logs show retry-induced logouts |
| IDN-D6 | Minimal access-token claim set with `sid` and `merchant_status`; no email, name or phone (= IDN-5) | `sid` answers "this session" and "all other sessions" without a lookup; tokens are copied into logs, so they carry only what authorisation needs ([§3.1.1.2](#3112-domain-model)) | Profile data in the token: data-minimisation breach | A consumer needs another claim for authorisation |
| IDN-D7 | Service tokens: client-credentials grant, `typ=service` + `scope`, 5-minute TTL, `/auth/token` internal-only (= IDN-4, IDN-12) | Temporal activities have no user in context and outlive the user's token; distinct `typ` makes user and internal routes mutually exclusive by construction ([§3.2.2](#322-rationale-in-detail)) | Forward the user's token (Alt 5): fails after expiry and opens internal routes to users. A `service` role (Alt 6): one bug turns a user route internal | A non-Temporal internal caller needs different scopes or lifetimes |
| IDN-D8 | Signing keys as PEM files in a gitignored mount, four-step rotation, JWKS with `max-age=300` and `stale-if-error`, env key set as verifier fallback (= IDN-6) | Rotation without a login outage or verifier redeploy; verifiers keep working through an `identity` outage (R7, IDN-G3, IDN-G6) ([§3.1.1.2](#3112-domain-model), [§3.1.2.1](#3121-endpoints)) | KMS/HSM: out of scope (archive/0001 Q4). JWKS fetch per request: reintroduces the dependency A4 removes ([§3.3](#33-tradeoffs)) | Deployment leaves Docker Compose |
| IDN-D9 | Argon2id with parameters pinned in configuration, rehash on login, hashing in a thread pool (= IDN-1) | Memory-hard, OWASP first choice; pinning stops a library upgrade from silently changing cost ([§3.2.2](#322-rationale-in-detail)) | bcrypt / PBKDF2: not memory-hard, bcrypt truncates at 72 bytes (Alt 11). Library defaults: change silently | Login latency or capacity forces a lower cost |
| IDN-D10 | Password policy: 12–128 characters after NFKC, not equal to the email (= IDN-2) | "Weak password → 422" needs a definition; the upper bound caps hashing cost per request ([§3.1.1.2](#3112-domain-model)) | Unbounded length: hashing cost is caller-controlled | — |
| IDN-D11 | Email is `citext` with a unique index | Uniqueness enforced by the database, not by every caller remembering to lower-case ([§3.2.2](#322-rationale-in-detail)) | Application-level normalisation: has a race and a forgetting problem (Alt 10) | — |
| IDN-D12 | Login enumeration defences: one `401` body for unknown email and wrong password, dummy-hash timing equalisation, `403`s only after the password verifies (= IDN-17) | IDN-G4: login cannot be used to enumerate accounts ([§6.2](#62-security-considerations)) | Distinct errors: leak account existence | — |
| IDN-D13 | Registration keeps `409` on a duplicate email | The honest error a user needs; without email delivery there is no "check your email" alternative ([§3.3](#33-tradeoffs)) | Always-`202` flow: needs email delivery (IDN-Q4) | Email delivery exists |
| IDN-D14 | `admin` is never self-assignable: seed or promotion by an admin only; the last active admin cannot be demoted or deactivated (= I3, IDN-8) | Every other privilege boundary is only as strong as this one ([§3.2.2](#322-rationale-in-detail)) | Self-chosen role at registration: anonymous path to `admin` | — |
| IDN-D15 | `MerchantStatus` = `pending` → `approved` / `suspended`, with suspension as the rejection path and no `rejected` value; transitions only via `MERCHANT_TRANSITIONS`; a repeat is a no-op | `pending` is only ever initial; one table is the single source of truth (S4) ([§3.1.1.3](#3113-state-machines)) | Native Postgres `ENUM` or `CHECK`: no inverse for `ADD VALUE`, and weaker than the table (Alt 9) | A real rejection state is needed by the product |
| IDN-D16 | Suspension refuses token issuance (login `403`, refresh `401`) and revokes sessions; the status is also a claim and an event | Two freshness needs: the claim gates writes for ≤ 15 min, the event-fed `catalog` projection hides products at once (archive/0003 Q6) ([§3.2.2](#322-rationale-in-detail)) | `catalog` calling `GET /merchants/<built-in function id>`: a second synchronous cross-service call, forbidden by A13/R5 | — |
| IDN-D17 | A role change creates a `pending` profile (to merchant) or suspends it (away from merchant), and always revokes the user's sessions (= IDN-7) | Keeps I5 (every merchant has a profile) and makes the new role take effect at the next login ([§3.1.2.1](#3121-endpoints)) | Deleting the profile: violates S5 soft-delete | A role-change event gets a consumer (IDN-Q6) |
| IDN-D18 | Non-approved merchants return `404` on the public profile route to non-owners (= IDN-13) | The profile route must not reveal pending or suspended merchants ([§3.1.2.1](#3121-endpoints)) | `200` with status: exposes unapproved merchants publicly | — |
| IDN-D19 | Two events, `user.registered` and `merchant.status_changed`, written through `identity_db.outbox`; `identity` consumes nothing (= A10, IDN-15) | The event is part of the state-change transaction, so it cannot be lost ([§3.1.2.3](#3123-events-produced)) | Publish from the handler after commit: a crash loses the event (Alt 12) | A consumer needs a new identity fact (IDN-Q6) |
| IDN-D20 | `identity` makes no synchronous call out; JWKS is served from memory | It sits at the bottom of the dependency graph and keeps serving JWKS when Postgres is down ([§3.1.1.5](#3115-dependencies-and-degradation)) | — | — |
| IDN-D21 | `merchant` is not its own service, and `identity` is not split into `auth` and `user-profile` | Profiles share the `users` aggregate and its invariants; a split puts a distributed transaction inside registration ([§3.1.1.1](#3111-responsibility-and-boundaries)) | Split services (Alt 8, ownership-test row 17) | Merchant data gains its own invariants or load profile |
| IDN-D22 | Login rate limits (5/min per IP and per email) are enforced at the gateway and fail open | Edge limiting belongs to the gateway; availability over protection is a platform decision; Argon2id cost is the floor ([§6.2](#62-security-considerations)) | Limiting inside `identity` only: kept as the fallback for the per-email half (IDN-Q8) | IDN-Q8 is resolved the other way |
| IDN-D23 | Expired-session sweep deletes rows 30 days past expiry or revocation; unique index on `token_hash` (= IDN-9, IDN-16) | Bounds `refresh_sessions` growth while keeping reuse detection for late replays; every refresh looks up by hash ([§3.1.1.4](#3114-workflows-and-async-jobs), [§3.1.3](#313-dataschema-changes)) | No sweep: unbounded table. Immediate delete: replays become "unknown" instead of reuse | — |
| IDN-D24 | Entity-first layout: `auth/`, `users/`, `merchants/` each with models, repos and `v1/{routes,schemas}.py`; shared enums in `core/enums.py` | Only the HTTP contract can be versioned; one folder per entity keeps its rules together ([§3.1.1.1](#3111-responsibility-and-boundaries); [review §3](../reviews/2026-10-06-mentor-review.md#3-entity-first-service-layout-with-versions)) | Version-first `app/v1/<entity>/`: v2 would duplicate the ORM. Hybrid split tree: an entity in two places | — |

#### 3.2.2. Rationale in detail

**Why RS256 rather than a shared HMAC secret.** With a shared secret, any service that can verify a token can also forge one, so a single compromised service compromises the identity of every user. With asymmetric keys only `identity` can mint tokens, and the public key is not a secret at all. Verification is local, so `identity` being down does not stop `catalog` serving authenticated reads — which matters because `identity` is otherwise a single point of failure for the entire platform.

**Why stateless access tokens and stateful refresh tokens (0001-A5).** A database read per request, in every service, against `identity_db` would make `identity` a runtime dependency of every request path and violate R1 and R7. The price is a revocation latency bounded by the 15-minute access-token TTL; logout, password change, deactivation, role change and suspension all revoke refresh sessions immediately, capping that window.

**Why rotation with reuse detection.** A refresh token lives for days in client storage, which is where tokens get stolen. Rotating on every use means a stolen token is valid only until either party uses it next; when the second party uses the now-rotated token, the collision is the detection signal, and revoking the user's sessions evicts the thief at the cost of one legitimate re-login. This is the OAuth 2.0 Security BCP's recommendation for public clients.

**Why reuse revokes every session of the user rather than a "family".** The source intent ([archive/0001](archive/0001-foundation-and-core-services.md) §6.2) is to revoke "the whole session family"; the exact `refresh_sessions` schema has no family or parent column, so the family cannot be identified. Revoking all of the user's live sessions is a strict superset: never less safe, occasionally a second device's re-login. IDN-Q2 proposes the column that narrows it.

**Why the refresh token is opaque.** Only `identity` reads it, and it must look it up to check revocation anyway. A JWT refresh token adds a signature nobody needs and a self-describing artefact another service might be tempted to accept.

**Why `sid` in the access token.** "Revoke all *other* sessions" on password change and "revoke *this* session" on logout both need to know which session minted the calling token. One uuid claim answers both without a lookup.

**Why merchant status is a claim and an event.** It is needed in two different places with two different freshness needs. At write time in `catalog` (may this merchant create products?) the token's claim is authoritative for its 15 minutes — strong, signed, local. For storefront visibility, a suspension must hide products even while the merchant's own token is still valid, so `catalog` keeps an event-fed projection. Both are fed from the single writer; neither calls `identity` ([archive/0003](archive/0003-events-and-observability.md) Q6).

**Why `merchant` is not its own service.** Profiles hold a subset of the user's invariants, change for the same reasons and have no distinct operational profile — it fails tests 2–4 (ownership-test row 17). Splitting it would put a distributed transaction inside registration.

**Why admin is never self-assignable.** Every other privilege boundary in the platform is only as strong as this one. Bootstrapping through the seed and promoting only through an authenticated admin route means there is no request an anonymous caller can make that yields `role: admin`.

**Why a client-credentials grant rather than forwarding the user's token.** [archive/0000](archive/0000-system-architecture.md) §3.1.1 anticipated this: "If a future path needs a call with no user in context, it gets a dedicated client-credentials token issued by `identity` with its own claim." Temporal activities are that path — a workflow outlives the request and the 15-minute token, and `/internal/v1/inventory/reservations` must not be callable by any user token ([archive/0002](archive/0002-core-business-logic-and-workflows.md) §3.1.2). A distinct `typ` and `scope`, rather than a special role, keeps user routes and internal routes mutually exclusive by construction.

**Why `citext` for email.** Uniqueness is enforced by the database rather than by every caller remembering to lower-case ([archive/0001](archive/0001-foundation-and-core-services.md) Q6, decided). An application-level normalisation has a race and a forgetting problem; a unique index on `citext` has neither.

**Why Argon2id with pinned parameters.** Argon2id is OWASP's first recommendation and memory-hard, which is what defeats GPU cracking. Pinning the parameters in configuration, and rehashing on login when they change, means an upgrade of `argon2-cffi` can neither silently weaken nor silently strengthen (and slow) every login.

**Why JWKS with `stale-if-error`.** R7 says no service may need another up to serve reads it already has data for. A verifier's key set is exactly such data; `stale-if-error` makes "keep verifying with the last good keys" the specified behaviour rather than an accident of implementation.

### 3.3. Tradeoffs

| Trade being made | The cost | Why it is acceptable |
| --- | --- | --- |
| Stateless access tokens | A revoked, demoted, deactivated or suspended user keeps access for up to 15 minutes | The standard trade for avoiding a database read per request. Bounded and explicit; every revocation event also kills refresh sessions immediately |
| Role changes take effect at next issuance | An admin's demotion of a user is not instant | Same bound (≤ 15 min). Sessions are revoked on role change, so the next refresh fails and the user must log in and receive the new role |
| Reuse revokes all of a user's sessions | A legitimate second device is logged out when one device's token is replayed | Fail-safe; the exact schema cannot identify a family (IDN-Q2) |
| Refresh is non-idempotent | A client whose refresh response is lost in transit is logged out on retry | Indistinguishable from theft by construction; failing safe is the point (IDN-Q3) |
| No cross-service FK to `users` | The database cannot stop a product or order referencing a deactivated user | A real distributed-system property. Users are soft-deleted (S5), and merchant state reaches `catalog` by event |
| Merchant status replicated as a projection | A suspended merchant's products stay visible for the seconds the event takes | Bounded, and fails safe ([archive/0006](archive/0006-service-decomposition-and-data-model.md) §3.3). The alternative is a synchronous call on a public read path |
| Argon2id at 64 MiB × 3 passes | ~50–100 ms CPU and 64 MiB per login or registration; a login burst is CPU-bound | Thread-pooled, rate-limited at the gateway, and what makes a leaked hash expensive. Capacity is scaled with replicas (§6.1) |
| Key files on disk, not a KMS | A host compromise yields the private key | Local-only environment; production key management is out of scope ([archive/0001](archive/0001-foundation-and-core-services.md) Q4). Rotation (IDN-6) is the containment |
| JWKS cached for 5 minutes | A planned rotation takes ≥ 20 minutes end to end; an emergency key revocation takes up to 5 minutes to bite | Rotation is rare; the alternative is a JWKS fetch per request, which reintroduces the dependency A4 removes |
| Registration returns `409` on a duplicate email | Registration can be used to test whether an email has an account | The honest error a user needs; bounded by the gateway's per-IP limit. Listed as IDN-Q4 |
| Rate limits fail open | A Redis outage removes brute-force protection for its duration | Availability over protection, by platform decision ([archive/0003](archive/0003-events-and-observability.md) §6.1); Argon2id cost remains a floor |
| No identity audit log | Admin role changes and suspensions are traceable only through logs and the status-change event | Accepted gap ([archive/0001](archive/0001-foundation-and-core-services.md) §6.2); `merchant.status_changed` carries `actor_id`. IDN-Q5 |

#### 3.3.1. Challenges and Open Questions

Carried questions keep their archive IDs with the source RFC named, because Q numbers collide across the archive.

| # | Question | Current position | Needs a decision by |
| --- | --- | --- | --- |
| Q2 (archive/0001) | How does `catalog` learn that a merchant was suspended or deleted? | **Closed** by Q6 (archive/0003): `identity` emits `identity.merchant.status_changed`; `catalog` consumes it. Before events exist, the next token refresh fails and the access token expires within 15 minutes | Decided |
| Q3 (archive/0001) | Does merchant onboarding need an approval step before the merchant can create products? | `merchant_profiles.status` exists and defaults to `pending`, but nothing gates on it. `identity` now carries `merchant_status` in the token, so gating is a one-line check in `catalog`'s product-create path. **Still open**; tracked as IDN-Q1 | Architecture review |
| Q4 (archive/0001) | Where do RS256 keys come from outside a laptop? | Local: generated key pairs mounted from a gitignored directory; public keys via JWKS with the environment as fallback. Production key management is out of scope for the assignment | Not required |
| Q6 (archive/0001) | Is `citext` the right choice for email, or should normalisation happen in the application? | `citext`, so uniqueness is enforced by the database | Decided |
| Q6 (archive/0003) | Where does merchant suspension take effect — token issuance, or a catalog reaction? | Both. `identity` refuses to issue tokens; `catalog` consumes `merchant.status_changed` and hides the merchant's products | Decided |
| Q3 (archive/0006) | Is the `‡` `identity_db.outbox` genuinely missing, or is the blanket "all producers" rule enough? | Carried here as a table, because the two events need it and A10 forbids dual-writing | Architecture review (bookkeeping only) |
| IDN-Q1 | Should `pending` merchants be blocked from creating products, and if so, where? | Proposal: `catalog` rejects product creation with `403` when `merchant_status ≠ approved` in the token. `identity` needs no change — the claim exists | Architecture review; owned by [RFC 0004](0004-catalog-service.md) |
| IDN-Q2 | Reuse detection revokes all of a user's sessions because `refresh_sessions` has no family column. Narrow it? | Proposal: add `family_id uuid` (not null; the first session's `id` for a new login, inherited on rotation) and `replaced_by uuid` (nullable) — both `‡`, with a btree on `family_id`. Reuse then revokes one family, and an absolute family lifetime (e.g. 30 days) becomes expressible. Not added silently: it changes the archive/0006 table | Architecture review |
| IDN-Q3 | Should refresh tolerate a network retry (a short reuse grace window)? | No for now. A grace window needs either the raw successor token stored (forbidden, I7) or a returned-once cache; both weaken the detection. Clients are told to serialise refreshes | Revisit if client logs show retry-induced logouts |
| IDN-Q4 | Registration's `409` on duplicate email enumerates accounts. Replace with an always-`202` "check your email" flow? | Not possible without email delivery. Kept as `409`; the gateway's 100/min per-IP default applies. A tighter `/auth/register` limit (e.g. 10/min per IP) is proposed to [RFC 0002](0002-gateway-service.md) | When email delivery exists |
| IDN-Q5 | Should `identity` gain an audit table (S7) for admin actions — role changes, deactivation, merchant status? | Proposal: `identity_audit_log` (`bigserial`, actor, target, action, diff, correlation id) written in the same transaction. Not in scope today; accepted gap | Architecture review |
| IDN-Q6 | Should a role change emit an event (e.g. `identity.user.role_changed`)? | No consumer needs it today: role reaches services via the token, and the merchant case is already covered by `merchant.status_changed` (IDN-7 suspends the profile on demotion). Adding a topic is an amendment to the [RFC 0000 catalogue](0000-architecture.md#3123-event-catalogue) | When a consumer needs it |
| IDN-Q7 | Session listing: `refresh_sessions.user_agent` and `ip` exist "for session listing", but no route lists sessions | Proposal: `GET /api/v1/users/me/sessions` and `DELETE /api/v1/users/me/sessions/{id}` (bearer, own sessions only). Unscheduled | Architecture review |
| IDN-Q8 | Per-email login limiting needs the email, which is in the request body; the gateway otherwise never parses bodies | The gateway parses the JSON body for `POST /auth/login` only and keys the counter on `SHA-256(lower(email))` so Redis never holds an email. Alternatively `identity` enforces the per-email half itself against Redis. Owned by [RFC 0002](0002-gateway-service.md) | Before the limiter is implemented |

---

## 4. Dependencies/blockers/known issues

**External dependencies.**

| Dependency | Version | Used for |
| --- | --- | --- |
| Python | 3.12 | Runtime |
| FastAPI / Uvicorn | 0.115.x / 0.32.x | HTTP layer, OpenAPI |
| SQLAlchemy (async) / asyncpg / Alembic | 2.0.x / 0.30.x / 1.14.x | ORM, driver, migrations |
| Pydantic / pydantic-settings | 2.x | Validation, configuration |
| PyJWT (with `cryptography`) | 2.10.x | RS256 sign and verify, JWK serialisation |
| argon2-cffi | 23.x | Argon2id password and client-secret hashing |
| httpx | 0.28.x | Test client; verifier JWKS fetch in `common` |
| pytest / pytest-asyncio | 8.x / 0.24.x | Tests |
| PostgreSQL | 16 | `identity_db` (`citext`) |

**Internal dependencies.** `common` (settings, DB session, logging, errors, pagination, `DomainEnum`/`DomainEnumText`, verification, outbox helper) and the Postgres init script that creates `identity_db` and its role — [RFC 0001](0001-platform-and-shared-infrastructure.md). `outbox-relay`, Kafka and Schema Registry for event delivery — [RFC 0001](0001-platform-and-shared-infrastructure.md). The gateway's routing, route exclusion of `/auth/token`, and rate limiter — [RFC 0002](0002-gateway-service.md).

**Blockers.** None. No external account, credential or third-party service is required.

**Known issues.**

- No email delivery, so `email_verified` cannot move to `true` and password reset is unavailable. Registration is immediate.
- Role changes take effect only on the next token issuance, within 15 minutes.
- Before the gateway limiter exists, `POST /auth/login` is unthrottled; a brute-force attempt is bounded only by Argon2id cost.
- The seed script writes users (and, for `catalog`, published products) directly, bypassing the API. Acknowledged as a test-data shortcut confined to the seed path.
- Reuse detection is user-wide rather than family-wide (IDN-Q2).
- A lost refresh response followed by a retry logs the client out (IDN-Q3).
- No audit trail for identity administrative actions (IDN-Q5).
- Before tracing exists, a request can be followed only by correlating `X-Request-ID` across log streams.

---

## 5. Alternate solutions

**Alternative 1 — Session cookies with server-side session storage instead of JWTs.** Immediate revocation, no token-expiry window, simpler mental model.
*Rejected because* every request would then require a session lookup in a shared store, making that store a hard dependency of every service and reintroducing the single point of failure the asymmetric-key design removes. It also fits API clients and non-browser callers poorly. The revocation window (§3.3) is the cost of that removal, and it is bounded.

**Alternative 2 — Delegate authentication to Keycloak or Auth0.** Production-grade, standards-compliant, less code to write and maintain.
*Rejected because* identity is a core part of what the platform exists to design and demonstrate; outsourcing it removes the RBAC design work and adds a heavyweight container to a Compose file with a great deal more to fit. The token format chosen is standard (JWT, JWKS, RFC 6749 client credentials), so this remains a viable later swap.

**Alternative 3 — Symmetric (HS256) tokens with a shared secret.** One secret, simpler key handling.
*Rejected* (the prototype's approach, reversed in §3.1.4): a shared secret in every service means every service can mint a token, and one compromised service forges any identity.

**Alternative 4 — Token introspection: every service asks `identity` whether a token is valid.** Instant revocation.
*Rejected* by A4 and R7: it makes `identity` a runtime dependency of every request path in the platform, which is precisely the coupling local verification exists to remove.

**Alternative 5 — Forward the end user's token on internal calls, with no service credential** ([archive/0000](archive/0000-system-architecture.md) §3.1.1's initial position, before any caller without a user in context existed).
*Superseded* for callers with no user in context. A Temporal activity may run after the user's token has expired, and an internal route that accepts user tokens is reachable by any user. Kept for synchronous request-scoped calls (e.g. `order → catalog` at creation), which still present the user's token so `catalog` applies its edge rule.

**Alternative 6 — A special `service` role inside ordinary access tokens.** One token type, one verification path.
*Rejected* because it lets a bug in a role check turn a user route into an internal one or vice versa. A distinct `typ` with `scope` makes the two surfaces mutually exclusive by construction ([archive/0002](archive/0002-core-business-logic-and-workflows.md) §3.1.2).

**Alternative 7 — JWT refresh tokens.** Stateless refresh.
*Rejected*: a refresh token must be revocable and rotation must be detectable, both of which need server state anyway. Opaque + hashed is simpler and gives a verifier nothing it might mistakenly accept.

**Alternative 8 — Split `identity` into `auth` and `user-profile`, or carve out `merchant`.**
*Rejected* ([archive/0006](archive/0006-service-decomposition-and-data-model.md) §3.1.1 row 17, §3.3.1): they share the `users` aggregate and its invariants; a split puts a distributed transaction inside registration.

**Alternative 9 — Native Postgres `ENUM` (or `CHECK`) for `role` and `status`.**
*Rejected* by S4: `ALTER TYPE … ADD VALUE` has no inverse, so S14's mandatory `downgrade` cannot be written; and the merchant transition table is stricter than any type. `CHECK (status IN (…))` remains the escape hatch if a non-Python writer to `identity_db` is ever introduced.

**Alternative 10 — Normalise email in the application instead of `citext`.**
*Rejected* (archive/0001 Q6): uniqueness enforced by every caller remembering to lower-case is uniqueness with a race.

**Alternative 11 — bcrypt or PBKDF2 for passwords.**
*Rejected*: neither is memory-hard, and bcrypt truncates at 72 bytes. Argon2id is the current OWASP first choice and `argon2-cffi` is mature.

**Alternative 12 — Publish events directly from the handler after commit.**
*Rejected* by A10: a crash between commit and produce loses the event with no retry that can fix it. The outbox makes the event part of the state-change transaction.

---

## 6. Production readiness

"Production" means the assignment's target environment — Docker Compose on a developer machine, reviewed by a mentor. The properties below are designed to hold in principle and are demonstrated at that scale.

### 6.1. Scalability and Fault tolerance

- **Stateless service.** No in-process state beyond the loaded key ring (identical on every replica, read from the same mount). Scales horizontally with `docker compose up --scale identity=N`; sessions live in Postgres.
- **CPU-bound hot path.** Argon2id dominates login and registration cost. Hashing runs in a thread pool so the event loop keeps serving refresh, JWKS and profile reads during a login burst. Capacity is added with replicas; the gateway's login limit caps the burst per IP and per email.
- **Local verification everywhere else.** `identity` being unavailable degrades login and refresh but leaves every already-authenticated read and write in other services working — a deliberate blast-radius reduction (R7, IDN-G3). JWKS `stale-if-error` extends that across outages longer than the cache TTL.
- **Connection pooling.** `pool_size=10`, `max_overflow=20` per replica, so a scaled-out `identity` does not exhaust Postgres connections.
- **Concurrency safety.** Refresh rotation takes a row lock (`FOR UPDATE`) on the presented session, so concurrent refreshes cannot both succeed. Registration races on an email resolve at the unique index — the loser gets `409`, never a duplicate.
- **Event delivery.** Kafka or relay outages accumulate rows in `identity_db.outbox`; nothing on the identity request path waits for Kafka. Duplicates from the relay are absorbed by consumers (archive/0003 C4).
- **Health probes.** `/health` for liveness; `/health/ready` for readiness including a database round-trip and a loaded active key. Compose `depends_on: service_healthy` keeps `identity` from accepting traffic before `identity_db` is reachable.
- **Graceful shutdown.** SIGTERM drains in-flight requests before the event loop closes.
- **Bounded reads.** Admin listings are paginated with a hard `limit` ceiling of 100, backed by the `role` and partial `is_active` indexes.
- **Known ceiling.** Single Postgres instance, no replica. `refresh_sessions` grows with logins; the sweep (IDN-9) bounds it.

### 6.2. Security considerations

- **Passwords** — Argon2id with pinned parameters (IDN-1), never logged, never returned. The response model for a user has no password field at all. Rehash-on-login keeps stored hashes at current parameters.
- **Tokens** — RS256; the private key exists only in `identity`. Access tokens 15 minutes, refresh tokens 7 days, service tokens 5 minutes. Refresh tokens are stored as SHA-256 hashes, rotated on every use, and a reuse of an already-rotated token revokes the user's sessions — the standard detection for a stolen refresh token. Verifiers pin `RS256`, check `iss`, `aud`, `exp`, `nbf` with 30 s leeway, and check `typ` per route.
- **Key custody** — PEMs in a gitignored directory, mounted read-only into `identity` only; never committed, never logged; rotation and emergency revocation per IDN-6.
- **Privilege escalation** — `role` is rejected on `PATCH /users/me`; only an admin may change a role, through the admin route. `admin` cannot be chosen at registration and is created only by the seed script or by another admin. The last active admin cannot be demoted or deactivated (I10).
- **Service tokens** — client secrets are stored as Argon2id hashes in configuration; tokens are short-lived, scoped, and `typ=service`, rejected on every user-facing route. `POST /auth/token` is not routed by the gateway, so it is unreachable from outside the Compose network.
- **Ownership** — every `/me` route derives its subject from `sub` in the verified token, never from a request body or query parameter.
- **Account enumeration defences** —
  - Login returns one indistinguishable `401` body for unknown email and wrong password.
  - **Timing equalisation (IDN-17):** for an unknown email, `identity` verifies the password against a fixed dummy Argon2id hash computed at startup with the live parameters, so both paths cost one Argon2id verification.
  - `403 account-inactive` and `403 merchant-suspended` are returned only after the password has verified, so they disclose nothing to a caller without the password.
  - Logout returns `204` whether or not the presented refresh token exists or belongs to the caller.
  - `GET /merchants/{merchant_id}` returns `404` for non-approved merchants to non-owners, and never exposes email or phone.
  - Residual channel: registration's `409` (IDN-Q4).
- **Login rate limits** — at the gateway: **5 attempts per minute per IP and per email**, `429` with `Retry-After`; **100 requests per minute per IP** as the default on every other route ([archive/0003](archive/0003-events-and-observability.md) §6.2). The per-email counter key is a hash of the normalised email (IDN-Q8). Limits fail open on a Redis outage (accepted, §3.3).
- **Injection** — ORM with bound parameters only; `q` filters use parameterised `ILIKE`.
- **Input validation** — Pydantic models on every body with `extra="forbid"`, length ceilings on free-text fields (`full_name` ≤ 200, `business_name` ≤ 200, `description` ≤ 2 000, `phone` ≤ 32), email syntax validation, the password policy (IDN-2).
- **Data minimisation** — tokens carry no email or name; event payloads carry ids, role, display names and statuses, never hashes or contact details; logs carry a truncated email hash only.
- **Secrets** — environment only; `.env` gitignored, `.env.example` with placeholders.
- **Error hygiene** — problem details never include stack traces, SQL, or which of email/password was wrong.
- **Transport** — plain HTTP locally; TLS is an environment concern outside Compose.
- **Long-lived connections elsewhere** — SSE streams in `notification` carry the token's expiry and close when it passes, so a logout or suspension is not outlived indefinitely ([archive/0003](archive/0003-events-and-observability.md) §6.2). `identity` supplies the `exp`; `notification` enforces it.
- **Accepted gaps** — no email verification or reset; no MFA; no identity audit log (IDN-Q5); key files rather than a KMS; rate limits fail open. Each is listed so it is reviewed as a decision rather than discovered as an omission.

### 6.3. Deployment strategy

- **Target** — one `identity` container in the root Compose file, `core` profile, multi-stage Dockerfile running as a non-root user ([RFC 0001](0001-platform-and-shared-infrastructure.md)).
- **Bring-up** — `make up`, then `make migrate` (Alembic to head, explicit step, never on startup — N replicas must not race to migrate), then `make seed`. Keys must exist in `secrets/jwt/` before `identity` starts; the platform's key-generation step creates a first pair if the directory is empty.
- **Database provisioning** — the Postgres init script creates `identity_db` and a dedicated `identity` role granted only on it.
- **Configuration** — twelve-factor, validated at startup; a missing `JWT_ACTIVE_KID` or unreadable key fails the container loudly.
- **Key rotation** — the four-step procedure in §3.1.1.2 (IDN-6), each step a restart; no verifier redeploys.
- **Rollback** — Alembic `downgrade` per revision; independent of every other service's history. Rolling back past `0004_outbox` is safe only after the relay has drained `identity_db.outbox`.
- **Event schemas** — the two Avro schemas live in `common/smartretail_common/events/schemas/` and are registered by the idempotent `make topics` step; compatibility `BACKWARD`.

---

## 7. Test Plan

Target: 100% of the role matrix, ~80% on the repos (`*_repo.py`, `repo.py`). Integration tests use a real Postgres (throwaway database per session), `httpx.ASGITransport`, a transaction rolled back per test.

**Unit** — no I/O.

- Password hashing: hash/verify round trip; wrong password rejected; distinct salts for the same password; parameters in the PHC string match settings; `check_needs_rehash` true after a parameter change.
- Password policy: 11 chars → reject; 12 → accept; 129 → reject; equal to email → reject.
- JWT: sign and verify; expiry rejection; `nbf` in the future rejected; signature-tamper rejection; wrong-key rejection; `alg: none` and `alg: HS256` rejected; wrong `iss` / `aud` rejected; claim contents exactly as §3.1.1.2 (and `merchant_status` present iff `role = merchant`; no email).
- Service token: `typ=service`, `sub=service:<id>`, `scope` subset; no `role`.
- `require_roles`: allow, deny, missing token, malformed token, service token on a user route → `401`.
- `require_scope`: user token on an internal route → `401`; wrong scope → `403`.
- Merchant transition table: every listed transition allowed; every unlisted pair rejected; repeat is a no-op.
- JWKS: document contains every loaded key, only public components (`n`, `e`), `alg=RS256`, `use=sig`; `ETag` stable for the same key set and changes when a key is added.
- Slug derivation and collision suffixing.
- Log filter: `password`, `refresh_token`, `access_token`, `client_secret`, `Authorization` never appear in a formatted record.
- R8 grep: no `UserRole`/`MerchantStatus` literal in `common`.

**Integration — identity.**

- Register: success for `customer` and `merchant` (merchant gets a `pending` profile, slug set); duplicate email → `409`; duplicate differing only in case → `409` (`citext`); weak password → `422`; `role: admin` → `422`; unknown field → `422`.
- Login: success; wrong password → `401`; unknown email → `401` with a **byte-identical body**; unknown-email and wrong-password latencies within 25% of each other over 20 samples (IDN-17); inactive user → `403`; suspended merchant → `403`; pending merchant → `200` with `merchant_status: "pending"`.
- Refresh: success and rotation (old row `revoked_at` set, new row inserted, new token differs); **reuse of a rotated token → `401` and every live session of that user revoked**; expired token → `401`; unknown token → `401`; two concurrent refreshes with one token → exactly one `200`; refresh for a user deactivated after login → `401`.
- Logout: revokes the presented session; a second logout → `204`; logout with another user's refresh token → `204` and that session untouched.
- `GET /users/me` with and without a token; `PATCH /users/me` cannot change `role`, `email` or `is_active` (`422`).
- Password change: wrong current → `401`; success revokes **all other** sessions and leaves the calling `sid` usable.
- Admin routes: reachable by `admin`; `403` for `customer` and `merchant`; `401` anonymous. Role change revokes the target's sessions; `role → merchant` creates a `pending` profile; `merchant → customer` suspends the profile and writes a `status_changed` outbox row; demoting or deactivating the last admin → `409`.
- Merchants: public profile of an approved merchant → `200` without email; of a pending/suspended merchant → `404` anonymous, `200` for the owner and admin; `/merchants/me` as customer → `403`; admin status transitions per the table; invalid transition → `409`; suspension revokes sessions and blocks login.
- Outbox: registration writes exactly one `identity.user.registered` row in the same transaction (a forced failure after the user insert leaves neither row); a status transition writes one `identity.merchant.status_changed` row with `aggregate_id = user_id`; a no-op writes none; payloads match the Avro schemas.
- Client credentials: valid client → token with requested scope; wrong secret → `401 invalid_client`; unknown scope → `400 invalid_scope`; wrong `grant_type` → `400`; the route is not reachable through the gateway (`404`).
- JWKS: `200` with `Cache-Control` and `ETag`; `If-None-Match` → `304`; served while Postgres is stopped.
- Migrations: empty database → head → base → head, on every CI run (S14).

**Role-matrix test.** One parametrised test per row of §3.1.2.2 × {anonymous, customer, merchant, admin, service token}, asserting the exact status. This is the G4 check.

**Rotation drill (IDN-G6).** Start with key A active. Issue token T1. Add key B (published, not active); a verifier with a cold cache fetches JWKS and verifies T1. Activate B; issue T2; the verifier verifies both T1 and T2. Remove A; T1 → `401`, T2 → `200`. A token signed by an unknown kid triggers exactly one JWKS refetch within the 30 s throttle.

**Resilience.**

- Stop `identity`; an access token issued before the stop is still accepted by `catalog` (IDN-G3, R7).
- Stop `identity` for longer than `max-age`; verifiers keep verifying under `stale-if-error`.
- Start a verifier with `identity` down; it boots on `JWT_PUBLIC_KEYS`.
- Stop Kafka; register succeeds; the outbox row publishes after Kafka returns; `analytics` counts it once.

**Gateway rate limit (acceptance, implemented in [RFC 0002](0002-gateway-service.md)).** Six logins within a minute from one IP → the sixth is `429` with `Retry-After`; six logins for one email from six IPs → the sixth is `429`; recovery after the window; Redis down → limits fail open and logins still succeed.

**Contract.** Both produced events validate against their registered schemas; a deliberately incompatible change to either is rejected by the registry checker in CI; `analytics`, `notification` and `catalog` consumer tests run against the registered schemas.

**End-to-end** (through the gateway, cold start, no fixtures).

1. Register a merchant → log in → admin creates a category → merchant creates a product → list as anonymous → status transitions. (The platform's first scenario; the identity steps are the first two.)
2. Register a customer and a merchant → assert `user.registered` consumed and `total_customers` incremented.
3. Suspend a merchant → `merchant.status_changed` consumed by `catalog`; the merchant's products disappear from browse without any synchronous call; the merchant's next login → `403`.
4. `temporal-worker` obtains a service token and reserves stock during an order; a customer token on `/internal/v1/inventory/reservations` → `401`.
5. Every response carries `X-Request-ID`; the login request's trace contains the `identity.password.verify` span.

**Explicitly not tested here** — load and performance (no traffic target is defined), and token verification internals beyond the contract (owned by `common` tests).

---

## 8. Impact Analysis

**Scope of impact.** No users, data or dependent external systems exist, so the impact is on the other services and RFCs.

| Consumer | What this RFC gives it | What it constrains |
| --- | --- | --- |
| `gateway` ([RFC 0002](0002-gateway-service.md)) | A JWKS to verify against; the claim set for coarse route checks; the login limits to enforce | Must not route `POST /auth/token`; must parse the login body for the per-email limit (IDN-Q8) |
| Every service (via `common`) | `sub`, `role`, `merchant_status`, `typ`, `scope` as the only authorisation inputs | Must pin RS256, check `typ` per route, never trust ids from bodies |
| `catalog` ([RFC 0004](0004-catalog-service.md)) | `merchant_id` = `sub` as a claim; `merchant.status_changed` to project merchant state; `merchant_status` claim for gating (IDN-Q1) | Must not call `identity` on its read path |
| `order`, `inventory` ([RFC 0005](0005-order-service.md), [RFC 0006](0006-inventory-service.md)) | `customer_id` = `sub`; service tokens with scopes for internal routes | Internal routes must require `typ=service` and a scope |
| `notification` ([RFC 0007](0007-notification-service.md)) | `user.registered` with `full_name` for the `welcome` notification; token `exp` for stream re-authorisation | — |
| `analytics` ([RFC 0008](0008-analytics-service.md)) | `user.registered.role` for Total Customers / Total Merchants; status changes | — |
| `assistant` ([RFC 0009](0009-assistant-service.md)) | Suspended merchants' products leave the index via `catalog`'s reaction | — |
| Platform ([RFC 0001](0001-platform-and-shared-infrastructure.md)) | — | Key directory mount, key-generation step, relay coverage of `identity_db.outbox`, the session sweep on Beat |

**Risks.**

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| Private key committed or copied into another container | Low | High | Gitignored directory, read-only mount on `identity` only, a configuration test (IDN-G1); rotation procedure for containment |
| Verifier accepts the wrong `alg` or a service token on a user route | Low | High | Pinned algorithm and `typ` checks in one shared library with unit tests; role-matrix test includes the service-token column |
| Refresh reuse false positives (client retries, multi-tab races) log users out | Medium | Low | Fail-safe by design; metric makes the rate visible; IDN-Q2/Q3 are the narrowing options |
| Argon2id cost under a login burst starves the service | Medium | Medium | Thread pool, gateway limits, horizontal scaling; hash-latency histogram |
| Cross-service drift — a product owned by a deleted merchant | Low | Medium | Users are soft-deleted (S5); merchant state propagates by event |
| Rate limiter failing open during a Redis outage invites brute force | Low | Medium | Accepted; Argon2id is the floor; outage is visible in platform health |

---

## 9. Useful Links/References

**Project documents**

- [Part A — platform requirements](../part-a.md) · [Part B — GenAI layer](../part-b.md)
- [Milestones](../milestones.md) · [Execution Guidelines](../execution-guidelines.md)
- [RFC 0000 — Architecture](0000-architecture.md) — [authentication/authorisation split](0000-architecture.md#3114-authentication-and-authorisation-split), [A1–A14](0000-architecture.md#3115-cross-cutting-decisions-a1a14), [event catalogue](0000-architecture.md#3123-event-catalogue), [envelope](0000-architecture.md#3124-event-envelope), [shared tables](0000-architecture.md#3132-shared-tables--outbox-and-processed_events), [S1–S15](0000-architecture.md#3131-schema-conventions-s1s15)
- [RFC 0001 — Platform](0001-platform-and-shared-infrastructure.md) · [RFC 0002 — gateway](0002-gateway-service.md) · [RFC 0004 — catalog](0004-catalog-service.md) · [RFC 0007 — notification](0007-notification-service.md) · [RFC 0008 — analytics](0008-analytics-service.md)
- Archive: [0000](archive/0000-system-architecture.md) · [0001](archive/0001-foundation-and-core-services.md) · [0002](archive/0002-core-business-logic-and-workflows.md) · [0003](archive/0003-events-and-observability.md) · [0006](archive/0006-service-decomposition-and-data-model.md)

**Technical references**

- RFC 7519, JSON Web Token — https://www.rfc-editor.org/rfc/rfc7519
- RFC 7517, JSON Web Key — https://www.rfc-editor.org/rfc/rfc7517
- RFC 6749 §4.4, Client Credentials Grant — https://www.rfc-editor.org/rfc/rfc6749#section-4.4
- RFC 9106, Argon2 — https://www.rfc-editor.org/rfc/rfc9106
- RFC 9457, Problem Details for HTTP APIs — https://www.rfc-editor.org/rfc/rfc9457
- RFC 5861, `stale-while-revalidate` / `stale-if-error` — https://www.rfc-editor.org/rfc/rfc5861
- OAuth 2.0 Security Best Current Practice (refresh-token rotation and reuse detection) — https://datatracker.ietf.org/doc/html/draft-ietf-oauth-security-topics
- OWASP Password Storage Cheat Sheet — https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html
- OWASP Authentication Cheat Sheet (enumeration, rate limiting) — https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- PostgreSQL `citext` — https://www.postgresql.org/docs/16/citext.html
- PyJWT — https://pyjwt.readthedocs.io/ · argon2-cffi — https://argon2-cffi.readthedocs.io/

---

## 10. Related tickets/issues

| ID | Title | Maps to |
| --- | --- | --- |
| *(to be created)* | T4–T7, T15, IDN-T1 … IDN-T8 | §3.1.5, one ticket each |
| — | archive/0001 §4 "no login rate limiting" | Closed by the gateway limiter (§6.2) |
| — | archive/0001 Q2 "how does catalog learn of suspension" | Closed by archive/0003 Q6 and §3.1.2.3 |
| — | archive/0001 Q3 "merchant approval gating" | Open as IDN-Q1 |
| — | archive/0006 Q3 "`‡` outbox tables" | `identity_db.outbox` carried (§3.1.3) |

---

## 11. Notes

- **On decision numbering.** [archive/0001](archive/0001-foundation-and-core-services.md) numbered its architectural decisions A1–A8 independently of [archive/0000](archive/0000-system-architecture.md)'s A1–A14, and the two series disagree (0001's A5 is "stateless access, stateful refresh"; 0000's A5 is "every service re-verifies"). This RFC uses the RFC 0000 series as canonical and writes the one 0001 decision with no 0000 equivalent as `0001-A5`. Mapping: 0001-A1 → A1, 0001-A2 → A2, 0001-A3 → A3, 0001-A4 → A4, 0001-A6 → A5, 0001-A7 → A6, 0001-A8 → S14.
- **New decisions in this RFC.** IDN-1 Argon2id parameters pinned; IDN-2 password policy; IDN-3 opaque refresh token; IDN-4 service-token lifetime and claims; IDN-5 access-token claim set including `sid` and `merchant_status`; IDN-6 key custody, rotation and JWKS caching; IDN-7 role changes create/suspend merchant profiles and revoke sessions; IDN-8 last-admin guard; IDN-9 expired-session sweep; IDN-10 identity metrics; IDN-11 `business_name` at registration; IDN-12 `/auth/token` internal-only; IDN-13 non-approved merchants hidden from the public profile route; IDN-14 `/merchants/me`, admin merchant listing and the status endpoint; IDN-15 event payloads; IDN-16 unique index on `token_hash`; IDN-17 dummy-hash timing equalisation. Each is a proposal for review, not a ratified decision.
- **On sources that disagreed.** (1) Internal callers: archive/0000 said user tokens are forwarded and no separate service trust model is needed; archive/0002 and archive/0006 §3.1.2 introduced client-credentials service tokens. Resolved in favour of the latter, which is the amendment archive/0000 itself anticipated; forwarding the user's token is kept for request-scoped synchronous calls. (2) Verifier keys: archive/0001 Q4 put the public key in each service's environment, archive/0000 serves JWKS; both are kept, environment as fallback. (3) Reuse detection: archive/0001 says "revokes the whole session family", but archive/0006's `refresh_sessions` has no family column; the table is copied exactly and reuse revokes all of the user's sessions, with the family column proposed as IDN-Q2.
- **On what is not specified by any source and is proposed here.** Event payload fields, the service-token endpoint path and scopes, the merchant status endpoint, JWKS cache headers, key rotation procedure, Argon2id numeric parameters (the source said "library defaults"), and the identity metric names. Each is marked with an IDN ID so a reviewer can object to it specifically.
- **On domain wording.** The source task list arrived phrased as "User & Course management" with "student / instructor" roles; Part A specifies retail commerce, so *student / instructor* read as *customer / merchant*, with `admin` as the third role Part A §1 requires.

---

## 12. Comments

*Reviewer comments go here. Please raise objections against a numbered item — a decision (A*, 0001-A5, IDN-*, or a register row IDN-D*), an invariant (I1–I11), an open question (Q* with its source, IDN-Q*), a row of the role matrix, or a rejected alternative in §5 — so the discussion attaches to something specific.*

| Reviewer | Section | Comment | Resolution |
| --- | --- | --- | --- |
| | | | |
