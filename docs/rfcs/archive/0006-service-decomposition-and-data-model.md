# RFC 0006 — Service Decomposition & Data Model

> **SUPERSEDED — archived Sep 29, 2026.** The RFC set was reorganised from milestone-based to architecture- and service-based. This document is kept for history only and is no longer maintained. Its content now lives in: [0000-architecture](../0000-architecture.md), and the service RFCs 0002–0009 (each owns its tables). See [docs/README.md](../../README.md).

| | |
| --- | --- |
| **Owner** | @muhammad.adnan |
| **Collaborators** | SarwanAhmed |
| **Status** | **DRAFT** · IN-REVIEW · DONE |
| **Created** | Sep 22, 2026 |
| **Related Epic or story** | All milestones ([milestones.md](../../milestones.md)) — the decomposition and schema decisions underlying every one |
| **Companion to** | [RFC 0000 — System Architecture](0000-system-architecture.md) |

> **Read this alongside [RFC 0000](0000-system-architecture.md).** RFC 0000 answers
> *how the system runs* — topology, flows, events, cross-cutting conventions.
> This RFC answers the two questions underneath it: **where the boundaries fall,
> and what the data looks like.** It is the **complete specification of the
> platform's data model**: every database, every table, every column.
> Neither is a milestone; both are implemented through RFCs 0001–0005, and those
> RFCs name which tables their milestone creates without restating a column.

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
       - 3.1.3.1. [Schema conventions (S1–S15)](#3131-schema-conventions-s1s15)
       - 3.1.3.2. [Shared tables — `outbox` and `processed_events`](#3132-shared-tables--outbox-and-processed_events)
       - 3.1.3.3. [The cross-service entity model](#3133-the-cross-service-entity-model)
       - 3.1.3.4. [Table index — every table in the platform](#3134-table-index--every-table-in-the-platform)
       - 3.1.3.5. [The catalogue](#3135-the-catalogue)
       - 3.1.3.6. [Cross-boundary reference kinds](#3136-cross-boundary-reference-kinds)
       - 3.1.3.7. [The ownership map](#3137-the-ownership-map)
       - 3.1.3.8. [Inferred and added cells](#3138-inferred-and-added-cells)
     - 3.1.4. [Breaking changes](#314-breaking-changes)
     - 3.1.5. [Action Plan — the milestone/schema matrix](#315-action-plan--the-milestoneschema-matrix)
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

**The platform data model — the eight-service inventory with the method that produced it, and the complete schema: every database, every table, every column, with the conventions they all follow.**

---

## 2. Overview

Two questions determine more about this platform than any other: **what is a service**, and **who owns which fact**. They are the same question asked twice, because a service boundary that does not coincide with a data-ownership boundary is not a boundary at all — it is a shared database with extra network hops.

Neither [Part A](../../part-a.md) nor [Part B](../../part-b.md) answers them, and neither specifies a table. Both describe capabilities. How those capabilities are grouped into deployable units, and how their data is modelled, is our design work — and it is the work the milestone reviews will examine hardest, because everything else follows from it.

This RFC states the test a candidate service must pass, applies it to every plausible candidate including the ones we rejected, and then specifies **the whole data model in one place**: the entity relationships across service boundaries, every table and column in all eight stores, and the conventions every schema follows.

It is deliberately the *only* place a column is defined. A type that appears twice is a type that will eventually disagree with itself.

### 2.1. Goals

| # | Goal | How it is verified |
| --- | --- | --- |
| G1 | A stated, repeatable test for whether something should be a service | §3.1.1; applied to every candidate, including rejections |
| G2 | The complete service inventory with per-service justification | §3.1.1; every row names the state it owns and the invariant it protects |
| G3 | Every rejected candidate recorded with its reason | §3.1.1; a reviewer can disagree with a specific row |
| G4 | **Every table and every column in the platform, defined exactly once** | §3.1.3.5; the table index in §3.1.3.4 is the completeness check |
| G5 | A cross-service entity model showing every reference and its kind | §3.1.3.3, §3.1.3.6 |
| G6 | Schema conventions fixed once, applied by every service | §3.1.3.1; deviations are review failures |
| G7 | Zero shared tables and zero cross-service foreign keys, at every milestone | §7; enforced by database grants, not convention |

### 2.2. Non-goals

| Excluded | Why |
| --- | --- |
| Runtime topology, request flows, event catalogue, cross-cutting conventions | [RFC 0000](0000-system-architecture.md) |
| Kafka topic configuration, Redis keyspaces, object-storage layout | Not database schema. [RFC 0003](0003-events-and-observability.md) |
| Query-plan tuning and index sizing against real data | Per-milestone, against real query shapes. Indexes that are part of the *design* — the HNSW parameters, every unique constraint, the outbox partial index — are specified here |
| Physical sharding, partitioning, read replicas | No requirement, and nothing here prevents them |
| ORM implementation detail | [RFC 0001](0001-foundation-and-core-services.md) |
| State-machine transition tables | The permitted *values* are here (S4); the permitted *transitions* live with the behaviour, in the milestone RFC that implements them |

---

## 3. Solution Overview

### 3.1. Specification

#### 3.1.1. Architectural changes

**The decomposition method.**

Three candidate methods were available. The one used, and why:

| Method | What it splits by | Why not / why |
| --- | --- | --- |
| By technical layer | api / logic / data | Produces a distributed monolith. Every feature touches every service; nothing can be deployed alone |
| By team | Conway's law, made deliberate | There is one developer. Meaningless here |
| **By bounded context and data ownership** | A model boundary where terms have one unambiguous meaning, and one writer per fact | **Used.** It is the only one of the three that produces boundaries which survive a change in requirements, because it follows the domain rather than the org chart or the stack |

**The ownership test.** A candidate becomes a service only if it passes **all four**:

1. **Owned state.** There is a set of facts it alone writes. If two candidates would write the same fact, they are one service, or the fact is misplaced.
2. **An invariant worth protecting.** It enforces a rule that must hold transactionally — "reserved never exceeds on-hand", "one order per idempotency key". A service with no invariant is a table with an HTTP interface.
3. **An independent reason to change.** Its requirements move for reasons unrelated to its neighbours'.
4. **A distinct operational profile** — different load shape, consistency need, store type, or availability requirement — or a boundary that meaningfully contains failure.

Failing any one means it belongs inside another service, as a module.

**Applying the test.**

| # | Candidate | State it would own | Invariant | Independent change | Distinct profile | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `gateway` | none | — | yes | edge concerns | **Service, by exception** — see below |
| 2 | `identity` | users, credentials, roles, sessions | unique email; only identity mints tokens | yes | auth load; highest security boundary | **Service** |
| 3 | `catalog` | products, variants, categories, media | SKU unique per merchant; status machine | yes | read-heavy, high cardinality | **Service** |
| 4 | `order` | orders, lines, payment and shipment state, idempotency keys | one order per key; valid state transitions | yes | write-heavy bursts; strongest consistency need | **Service** |
| 5 | `inventory` | stock, reservations, movement ledger | `on_hand ≥ reserved ≥ 0` | yes | hot-row contention; the highest-contention writes in the system | **Service** |
| 6 | `notification` | notifications, delivery attempts | at-most-one per (event, user, channel) | yes | bursty, fully async, failure-tolerant | **Service** |
| 7 | `analytics` | rollups, series | counters monotonic per event | yes | append-heavy, document store, must never affect checkout | **Service** |
| 8 | `assistant` | conversations, chunks, embeddings, generations | chunks match indexed catalog version | yes | GPU/provider-bound, unbounded latency, external cost | **Service** |
| 9 | `payment` | payment intents | — (would be order state) | no | no | **Rejected → `order`** |
| 10 | `shipment` | shipment records | — (would be order state) | no | no | **Rejected → `order`** |
| 11 | `cart` | carts | would have one | no requirement | no | **Rejected — not built** |
| 12 | `search` | none — index is derived | none | no | no | **Rejected → worker + `assistant`** |
| 13 | `media` | none — `catalog` owns metadata | none | no | no | **Rejected → Celery task** |
| 14 | `pricing` | none — price is a product attribute | none | no | no | **Rejected → `catalog`** |
| 15 | `review` | reviews | would have one | not in scope | yes | **Rejected — no requirement** |
| 16 | `recommendation` | none | none | no | no | **Rejected → generation in `assistant`** |
| 17 | `merchant` | merchant profiles | subset of user invariants | no | no | **Rejected → `identity`** |
| 18 | `audit` | audit rows | must be transactional with the change it records | no | no | **Rejected — see below** |

**The three rejections most worth defending**, because they are where a reviewer is most likely to push back:

- **`payment` and `shipment`.** In a real system, a payment service exists for a *compliance* boundary — PCI scope isolation — and to own a provider's reconciliation, refund, and dispute lifecycle. Neither exists here: the provider is simulated ([RFC 0000](0000-system-architecture.md) A9) and no cardholder data enters the system at any point. What would remain is a table of payment intents keyed one-to-one to orders, updated only by the order's own workflow. Splitting it would create a distributed transaction across two databases to record one fact, which is strictly worse than a column — more failure modes, no isolation gained. The *interface* is kept separate so a real provider, or a real service, is a later extraction rather than a rewrite.
- **`audit`.** Tempting, because audit data is append-only and grows differently from the data it describes. Rejected because an audit row must be written in the *same transaction* as the change it records — otherwise there exists a state where the change happened and the trail did not, which defeats the purpose. Cross-service transactions do not exist here. So each service owns its own audit trail, and centralised querying, if ever needed, is a consumer of events.
- **`gateway`.** It fails test 1 — it owns no state — and is a deliberate exception. Cross-cutting edge concerns (routing, early token rejection, correlation IDs, CORS, rate limiting, aggregated OpenAPI) must live in exactly one place; the alternatives are duplicating them into eight services or hiding them in a proxy configuration nobody reviews. The exception is bounded by a rule: **the gateway holds no business logic and no database**, and any authorisation decision requiring domain knowledge stays in the service that owns the resource.

**Boundary rules.** These hold at every milestone and are what the reviews check.

| # | Rule | Enforced by |
| --- | --- | --- |
| R1 | A service reads and writes only its own database | Separate Postgres roles with grants on one database each — it fails at connection time, not at review time |
| R2 | No foreign key crosses a service boundary | Schema review; cross-service references are plain indexed UUIDs |
| R3 | A fact has exactly one writer | This document's ownership map (§3.1.3) |
| R4 | Cross-service reads go through an API or an event, never a join | Code review; R1 makes the alternative impossible |
| R5 | Synchronous cross-service calls only where the caller cannot proceed without the answer | One such call exists: `order → catalog` at creation |
| R6 | Data crossing a boundary is snapshotted or projected, never referenced live | §3.1.3 reference kinds |
| R7 | A service may not require another to be up in order to serve reads it already has data for | Local JWT verification; events over calls |
| R8 | `libs/common` contains no business rule | Review check at every milestone |

**Module structure inside a service.** Boundaries are recursive: the same reasoning applies below the service line, which is what keeps a service from becoming a small monolith.

```
  app/
  ├── api/v1/      HTTP only — parse, validate, call, serialise. No rules.
  ├── services/    The only layer permitted business rules. Called by
  │                routers, Temporal activities, event consumers, and CLI.
  ├── models/      SQLAlchemy ORM. No behaviour beyond persistence.
  ├── schemas/     Pydantic. The contract; never the ORM model.
  ├── consumers/   Event handlers — thin, like routers.
  └── core/        Settings, security, dependencies.
```

The rule that matters: **routers do not touch the ORM, and the service layer does not construct HTTP responses.** That single seam is why a Temporal activity ([RFC 0002](0002-core-business-logic-and-workflows.md) B1) and an event consumer can reuse the same business rule as an endpoint, with one implementation and one set of tests.

#### 3.1.2. API changes

**None.** This RFC introduces no endpoint. It constrains how endpoints are grouped, which prefix belongs to which service, and — through R4 and R5 — when one service may call another.

The one API-shaped rule it fixes: **a service's API is its only public interface.** There is no "internal" access path to another service's data that bypasses it. Where an internal caller needs privileged access (the Temporal activities calling `inventory`), it authenticates as a service and calls the same HTTP surface with a scoped token ([RFC 0002](0002-core-business-logic-and-workflows.md) §3.1.2) — not a back door.

#### 3.1.3. Data/schema changes

**This section is the complete data model.** Eight stores, 40 tables. Every
column of every table is defined here and nowhere else; RFCs 0001–0005 name
which tables their milestone creates and link back to this section.

**How to read it.** §3.1.3.1 fixes the conventions that make every type below
legible, and must be read first. §3.1.3.2 defines the two tables that appear in
several databases, once. §3.1.3.3 is the whole-system picture and §3.1.3.4 the
index; §3.1.3.5 is the catalogue itself.

**Every table is shown in its final, end-of-Milestone-6 form.** Where a column
arrived after its table was created, a `Since` column names the milestone.
§3.1.5 gives the same information ordered by milestone instead of by table.

**Markers.** Two, and they mean different things. Both are **proposals for
review**, not decisions already taken — a reviewer who disagrees with a marked
cell is disagreeing with this consolidation, not with a milestone RFC.

| Mark | Meaning |
| --- | --- |
| *(none)* | Stated explicitly in the milestone RFC named in the Notes cell |
| `†` | The column **is named** in a milestone RFC, but its **type was not stated** there. The type shown is inferred from the conventions in §3.1.3.1 |
| `‡` | The column or table is **not named in any RFC**. It exists because a convention requires it — S1 a primary key, S3 timestamps, S12 an outbox for a producer, S13 a dedupe table for a consumer |

Every marked cell is listed in §3.1.3.9, with a count, so a reviewer can find
them all without scrolling the catalogue.

##### 3.1.3.1. Schema conventions (S1–S15)

**Schema conventions.** Fixed once, followed by every service. A deviation is a review failure, not a style preference.

| # | Convention | Rule | Why |
| --- | --- | --- | --- |
| S1 | **Primary keys** | `uuid` v4, generated by the database (`gen_random_uuid()`) | Ids cross service boundaries and appear in event payloads. A sequential integer leaks volume, collides across services, and cannot be generated client-side for idempotency. Exception: high-volume append-only logs (`*_audit_log`, `stock_movements`, `outbox`) use `bigserial` — they never cross a boundary and benefit from index locality |
| S2 | **Money** | `numeric(12,2)` plus a `char(3)` ISO 4217 currency column, always adjacent. Never `float` | Binary floating point cannot represent 0.10. A stored total that disagrees with the sum of its lines by a cent is a defect with no defence |
| S3 | **Timestamps** | `timestamptz`, UTC, never naive. `created_at` and `updated_at` on every mutable table | A naive timestamp is ambiguous the first time anything runs in another zone, and the ambiguity is unrecoverable |
| S4 | **Enums** | A plain `text` column. No Postgres `ENUM` type and no `CHECK` constraint. The permitted set is a `DomainEnum` (a `StrEnum`) in the owning service's `app/domain/enums.py`, enforced on write by a `DomainEnumText` type decorator and at the API edge by Pydantic — which also publishes the set into OpenAPI. A state machine is additionally constrained by an explicit transition table in the service layer | Adding a value becomes a one-line edit rather than an Alembic revision, and — the decisive reason — `ALTER TYPE … ADD VALUE` is irreversible, so a native enum cannot satisfy S14's mandatory `downgrade`. `text` with a `CHECK` is a migration too, so it solves nothing. Self-documentation moves from `\d` to OpenAPI, which is where clients actually read it. For a state machine the transition table is *stricter* than the type it replaces: a Postgres enum accepts `delivered → pending`, a transition table does not |
| S5 | **Soft delete** | A terminal status value (`archived`, `is_active = false`), never a hard `DELETE`, on anything referenced across a boundary | A hard delete breaks snapshots and projections that no foreign key protects. Exception: derived data (`product_chunks`) is hard-deleted, because it is rebuildable |
| S6 | **Optimistic concurrency** | `version integer`, checked and incremented on update, on every aggregate root with concurrent writers | Lost-update prevention without holding locks across a request. The loser gets `409` |
| S7 | **Audit** | Per-service audit table, written by the service layer in the same transaction as the change, carrying actor and correlation id | A trigger cannot see the authenticated actor or the request id. Atomicity means no change can exist without its trail |
| S8 | **`jsonb` use** | Permitted for genuinely open-ended data (`attributes`, `variant_options`, `shipping_address` snapshots, event payloads). Never for anything filtered, joined, or constrained across rows | Rigid columns would be wrong within a week for merchant-defined attributes; but a `jsonb` field that gets filtered is an index and a validation problem waiting to happen |
| S9 | **Naming** | `snake_case`; plural tables; singular columns; `<entity>_id` for references; `is_`/`has_` for booleans; no abbreviations | Eight services written over five weeks; consistency is the only thing that makes them read as one system |
| S10 | **Nullability** | `NOT NULL` by default. A nullable column must have a documented meaning for null | "Unknown", "not yet", and "not applicable" are three different things and should not share a representation |
| S11 | **Constraints in the database** | Business invariants that can be expressed as constraints are (`CHECK (on_hand >= reserved)`, unique `(merchant_id, sku)`, unique `(user_id, idempotency_key)`) | Application-level checks have races. The database is the only thing that can arbitrate concurrent writers |
| S12 | **Outbox** | Identical DDL in every producing service | One relay implementation, one schema to reason about ([RFC 0002](0002-core-business-logic-and-workflows.md) B11) |
| S13 | **Dedupe** | `processed_events (consumer_group, event_id)` in every consuming service, written in the same transaction as the effect | At-least-once delivery makes this the correctness mechanism, not an optimisation |
| S14 | **Migrations** | Per-service Alembic history, explicit step, every migration has a working `downgrade`, additive wherever possible | Services deploy independently; a rollback of one must not require a rollback of another. This rule is why S4 forbids native Postgres enums: `ALTER TYPE … ADD VALUE` has no inverse, so such a revision cannot have a `downgrade` |
| S15 | **No cross-service FK** | R2, restated as a schema rule | The database cannot enforce integrity it cannot see |

**Where the conventions are deliberately broken**, with the reason, so each reads as a decision:

| Break | Where | Why |
| --- | --- | --- |
| `bigserial` instead of `uuid` (S1) | `product_audit_log`, `stock_movements`, `outbox` | Never cross a boundary; index locality matters at their volume |
| Duplicated data (S-normal-form) | `order_items` snapshots, `product_chunks` metadata | Snapshots are correct domain modelling; projections are rebuildable derived data |
| Hard delete (S5) | `product_chunks`, `embedding_cache`, `raw_events` | Derived, rebuildable, and TTL-bounded |
| Schemaless (S8) | `analytics_db` collections | A document store with genuinely varying per-metric shape — the workload that justifies the NoSQL requirement |
| No `updated_at` (S3) | Append-only tables | Nothing is ever updated |
| Validation outside the database (S11) | Every enum-valued column (S4) | S11 exists because application checks have races. An enum value is not a concurrent-write invariant like `on_hand >= reserved` — it is a spelling check, and a spelling check has no race to lose. R1 already gives each database one service and one role, so the application is the only writer there is to constrain |


###### S4 — enums as text

**How S4 is implemented.** The mechanism is small and belongs in `libs/common`; the vocabularies belong to each service. That split is what keeps S4 compatible with R8 — the library knows *how* a closed value set behaves and never *which* values exist.

`DomainEnum` is a `StrEnum`, so a member compares equal to its own string, serialises to JSON and Avro with no encoder hook, and is interchangeable with the raw string as a dict key.

```python
# libs/common/smartretail_common/enums.py
class DomainEnum(StrEnum):
    """A closed set of string values, enforced by the application, not by Postgres."""

    @classmethod
    def values(cls) -> tuple[str, ...]:
        return tuple(m.value for m in cls)

    @classmethod
    def has(cls, raw: object) -> bool:
        return isinstance(raw, str) and raw in cls._value2member_map_

    @classmethod
    def parse(cls, raw: object, *, default: "Self | None" = None) -> "Self | None":
        if isinstance(raw, cls):
            return raw
        if isinstance(raw, str):
            return cls._value2member_map_.get(raw, default)
        return default


class UnknownEnumValueError(ValueError):
    """A value outside the permitted set. Names the set, so the error is actionable."""
```

`DomainEnumText` maps such a set onto a `TEXT` column. Its two paths are deliberately asymmetric:

```python
# libs/common/smartretail_common/db_types.py
class DomainEnumText(TypeDecorator[Any]):
    impl = Text
    cache_ok = True          # the cache key reflects enum_class; two instances over
                             # the same enum compare equal, so this is safe

    def __init__(self, enum_class, *, on_unknown="passthrough", fallback=None): ...

    def process_bind_param(self, value, dialect):      # WRITE — strict
        if value is None:
            return None
        if isinstance(value, self.enum_class) or self.enum_class.has(value):
            return str(value)
        raise UnknownEnumValueError(self.enum_class, value)

    def process_result_value(self, value, dialect):    # READ — tolerant
        if value is None:
            return None
        member = self.enum_class.parse(value)
        if member is not None:
            return member
        if self.on_unknown == "raise":
            raise UnknownEnumValueError(self.enum_class, value)
        if self.on_unknown == "fallback":
            return self.fallback
        return value                                   # passthrough: the raw string
```

**Why the write path is strict and the read path is not.** A value this build does not understand may legitimately *exist*; this build has no business *creating* one. Concretely: `catalog` v1.5 writes four hundred rows with `status = 'publishing'`, then is rolled back to v1.4 for an unrelated defect, and v1.4 now reads them. Under a strict read that is four hundred uncaught 500s on a *list* endpoint — the rollback that was meant to restore service widens the incident instead. S14 says a rollback of one service must not require a rollback of another; the read path extends that to *a rollback of a service must not require a rollback of its own data*. This is the one property a native Postgres enum had for free, and giving it up deliberately, at the least damaging point, is part of the price of S4.

`on_unknown` is chosen per column, and the choice follows from what the value is used for:

| Policy | Use for | Why |
| --- | --- | --- |
| `passthrough` (default) | Display and filter columns — `products.status`, `notifications.channel` | The value is rendered or compared, never branched on. A raw string is harmless |
| `fallback` | Anything a state machine reads — `orders.status`, `payment_status`, `shipment_status` | A state machine must never branch on a bare string. Failing closed to the initial state freezes the row loudly instead of corrupting it quietly |
| `raise` | Nothing, currently | Reserved for a column where reading an unknown value is worse than failing |

**Two failure modes that are easy to get wrong**, both verified against this project's pinned SQLAlchemy 2.0 and Pydantic 2.10:

1. `process_bind_param` runs inside the flush, so SQLAlchemy wraps its exception: the caller sees `sqlalchemy.exc.StatementError` with the `UnknownEnumValueError` on `.orig`. The type decorator is therefore a **backstop, not the primary gate** — it fires late, inside the transaction, with a poor error shape. The primary gates are Pydantic at the edge and `transition()` in the service layer.
2. A tolerant ORM read is cancelled out by a strict Pydantic *response* schema — `model_validate` raises `ValidationError` on the unknown value and the 500 reappears one layer later. Both layers must agree. Request schemas stay strict always: the API never accepts a value this build does not know. Response schemas for `passthrough` columns widen to `ProductStatus | str` via a `BeforeValidator`; response schemas for `fallback` columns stay strict, because the ORM already guarantees a member.

**The API contract does not weaken.** Pydantic v2 renders a `DomainEnum` field as a `$defs` entry carrying the full value list, and FastAPI lifts it into OpenAPI unchanged. The published enum is generated from `app/domain/enums.py`, so — unlike a hand-maintained `CHECK` or a `\d` listing — it cannot drift from what the service actually accepts.

**Alembic.** Columns are declared `sa.Column("status", sa.Text(), nullable=False, server_default="draft")` — an ordinary, trivially reversible revision. One hook is mandatory in every service's `alembic/env.py`, because autogenerate otherwise renders `DomainEnumText()` **without its required `enum_class` argument**, producing a migration that raises `TypeError` when run and that imports domain types into a layer where they do not belong:

```python
# services/<svc>/alembic/env.py
def render_item(type_, obj, autogen_context):
    """A migration describes the database. A DomainEnumText column is TEXT."""
    if type_ == "type" and isinstance(obj, DomainEnumText):
        autogen_context.imports.add("import sqlalchemy as sa")
        return "sa.Text()"
    return False

context.configure(..., render_item=render_item, compare_type=True)
```

**One documentation affordance, since `\d` no longer carries the values.** The creating migration adds a comment that points at the source of truth rather than duplicating it, so it cannot go stale:

```python
op.execute("COMMENT ON COLUMN orders.status IS "
           "'DomainEnum OrderStatus — see services/order/app/domain/enums.py; "
           "transitions in app/domain/state_machine.py'")
```

**What adding a value costs, end to end.** One line in `app/domain/enums.py`, plus an edge in the transition table if the column has one. No Alembic revision, no `alembic upgrade head`, no deploy ordering constraint — the column already accepts the string. The OpenAPI enum, the round-trip test, and the seed script all follow from the same source. CI confirms it was genuinely migration-free: `alembic check` must report no pending diff.

**What is measurably given up.** Storage: an enum label is a fixed 4 bytes, a short text value is its length plus a one-byte header, so the delta is roughly 2–13 bytes per row on the status columns — invisible next to the `uuid`s, `numeric`s and `jsonb` snapshots already in those rows. Index size is about 1.5–2× per entry. Equality filtering is unaffected in practice: for a deterministic collation Postgres short-circuits `texteq` to a length check plus `memcmp` and never consults the collation library, so the familiar worry about expensive text comparison applies to ordering and pattern matching, not to `=`. The one real difference is ordering — `ORDER BY status` on a native enum sorted in declaration order, which was often meaningful; on `text` it sorts alphabetically, which is not. Any query relying on that must sort on a timestamp or an explicit `CASE`. The platform has no such query today, and the indexes S4's columns carry are all equality or composite-prefix indexes, which are unaffected.


##### 3.1.3.2. Shared tables — `outbox` and `processed_events`

Two tables appear in more than one database. They are **not** `order_db`'s
tables that other services copied — they are artifacts of conventions S12 and
S13, so they are defined here, once, at the same level as the conventions
themselves. In the catalogue each database lists them as a pointer back to this
section rather than restating the DDL.

That placement is not cosmetic. Their DDL previously lived inside `order_db`'s
section in [RFC 0002](0002-core-business-logic-and-workflows.md), and the direct
consequence is the four `‡` rows in the matrix below: databases that need one of
these tables and were never given it, because the table's home was somebody
else's database.

###### `outbox`

Identical DDL in every service that produces events (S12, [RFC 0002](0002-core-business-logic-and-workflows.md) B11).

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `bigserial` PK | S1 exception — never crosses a boundary, index locality matters at this volume |
| `event_id` | `uuid` | unique — the consumer dedupe key |
| `event_type` | `text` | from the [RFC 0000](0000-system-architecture.md) catalogue |
| `aggregate_type`, `aggregate_id` | `text`, `uuid` | the Kafka partition key |
| `payload` | `jsonb` | |
| `correlation_id`, `causation_id` | `text` | |
| `client_id` | `text` | nullable — the `X-Client-Id` of the request that caused the change, or null for an event produced by a workflow, a scheduler, or a webhook. Carried into the envelope and used by `notification` for origin-scoped delivery ([RFC 0003](0003-events-and-observability.md) C21) |
| `created_at` | `timestamptz` | |
| `published_at` | `timestamptz` | nullable — null means unpublished; M3's relay sets it |

Index: partial on `published_at IS NULL`, ordered by `id`, which is the relay's only query.

###### `processed_events`

The consumer dedupe table (S13, [RFC 0003](0003-events-and-observability.md) C4). Written in the same transaction as the effect it guards — at-least-once delivery makes this a correctness mechanism, not an optimisation.

| Column | Type | Notes |
| --- | --- | --- |
| `consumer_group` | `text` | part of PK |
| `event_id` | `uuid` | part of PK |
| `event_type` | `text` | |
| `processed_at` | `timestamptz` | indexed, for the retention sweep |

`analytics_db` carries a MongoDB variant of the same idea — `{_id: {group, event_id}, processed_at}` with a 30-day TTL index. Note the key is `group`, not `consumer_group`; the shapes are deliberately not identical because the stores are not.

###### Which database carries which

Derived from the event catalogue in [RFC 0000](0000-system-architecture.md) §3.1.3: a **producer** needs an `outbox`, a **consumer** needs `processed_events`.

| Database | `outbox` (S12) | Produces | `processed_events` (S13) | Consumes |
| --- | --- | --- | --- | --- |
| `identity_db` | **yes** `‡` | `identity.user.registered`, `identity.merchant.status_changed` | no | — |
| `catalog_db` | yes | `catalog.product.published` / `.updated` / `.unpublished` | **yes** `‡` | `identity.merchant.status_changed` |
| `order_db` | yes | `order.created` / `.confirmed` / `.failed` / `.cancelled`, `payment.*`, `shipment.status_changed` | no | — |
| `inventory_db` | yes | `inventory.reserved` / `.released` / `.stockout` | **yes** `‡` | `catalog.product.published`, `order.cancelled` |
| `notification_db` | no | — | yes | `order.*`, `payment.*`, `shipment.*`, `inventory.stockout`, `identity.user.registered` |
| `analytics_db` | no | — | yes (Mongo variant) | every topic |
| `assistant_db` | **yes** `‡` | `assistant.interaction` | yes | `catalog.product.published` / `.updated` / `.unpublished` |

**On the four `‡` rows.** [RFC 0003](0003-events-and-observability.md) §3.1.3 mandates these generically — "all producers", "all consumers" — but no RFC ever enumerates them per database, so each was absent from every table list in the RFC set. They are added here because the conventions require them and the event catalogue proves the requirement. **Flagged for the Milestone 4 review** (§3.3.1 Q3): the alternative reading is that the blanket statement was always sufficient, in which case nothing here is new and only the bookkeeping changes.


##### 3.1.3.3. The cross-service entity model

Boxes are aggregates; solid lines are real foreign keys inside one database;
dashed lines cross a boundary and are *not* foreign keys. The diagram shows
shape — §3.1.3.4 is the complete list, and only that list is exhaustive.

```
 ┌── identity_db ────────────┐        ┌── catalog_db ──────────────────────┐
 │                           │        │                                    │
 │  users ──┬── merchant_    │        │  categories ──┐ (self-ref tree)     │
 │    │     │   profiles     │        │      │        │                     │
 │    └── refresh_sessions   │        │      ▼        │                     │
 │  outbox ‡                 │        │  products ────┼── product_variants  │
 └───────┬───────────────────┘        │      │        ├── product_media     │
         ┊ merchant_id                │      │        ├── product_audit_log │
         ┊ (projected via event)      │      │        └── product_publish_  │
         └┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄▶ │      │             failures         │
         ┊ customer_id                │  outbox · processed_events ‡        │
         ┊ (claim in token)           └──────┼──────────────────────────────┘
         ┊                                   ┊ product_id / variant_id
 ┌───────▼─── order_db ──────┐               ┊ (snapshot at write)
 │                           │               ├┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┐
 │  orders ──┬── order_items ┼┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┘                     ┊
 │           ├── order_status_history         ┌── inventory_db ─────▼────┐
 │           │                                │                          │
 │  idempotency_keys                          │  stock_items ──┬── reser-│
 │  outbox                                    │       │        │   vations│
 │                                            │       └── stock_movements │
 └───────────┬───────────────┘                │  outbox · processed_ev. ‡│
             ┊ order_id                       └──────────┬───────────────┘
             ┊ (event payload)                           ┊ order_id
             ├┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┘ (event payload)
             ▼
 ┌── notification_db ────┐  ┌── analytics_db (Mongo) ──┐  ┌── assistant_db ────┐
 │                       │  │                          │  │  (+ pgvector)      │
 │  notifications ──┬──  │  │  counters                │  │                    │
 │   delivery_      │    │  │  daily_metrics           │  │  product_chunks ┄┄┼┄▶
 │   attempts       │    │  │  product_stats           │  │  embedding_cache   │
 │  push_subscriptions   │  │  failure_stats           │  │  index_state       │
 │  processed_events     │  │  raw_events              │  │  query_log         │
 └───────────────────────┘  │  assistant_conversions   │  │  conversations ──┬─│
                            │  processed_events        │  │   messages       │ │
                            └──────────────────────────┘  │  generations       │
                                                          │  token_budgets     │
        ── solid: FK inside one database                  │  bulk_generation_  │
        ┄┄ dashed: cross-boundary reference, NOT a FK     │    jobs            │
        ‡  added by this consolidation (§3.1.3.2)         │  prompt_templates †│
                                                          │  outbox ‡          │
                                                          │  processed_events  │
                                                          └────────────────────┘
```

##### 3.1.3.4. Table index — every table in the platform

**38 distinct table definitions, 46 instances across 7 stores.** The two numbers
differ because `outbox` appears in five databases and `processed_events` in
five; both are defined once in §3.1.3.2. The `Cols` column is the completeness
check — a column lost in transcription shows up as arithmetic.

| Service | Store | Table | Key | Cols | What it holds | Created | Amended |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `gateway` | — | *none* | — | — | Stateless edge — the deliberate exception to the ownership test (§3.1.1) | — | — |
| `identity` | `identity_db` | [`users`](#users) | `uuid` | 10 | Accounts, credentials, role, active flag | M1 | — |
| | | [`merchant_profiles`](#merchant_profiles) | `uuid` → `users.id` | 7 | Business name, slug, approval status | M1 | — |
| | | [`refresh_sessions`](#refresh_sessions) | `uuid` | 8 | Hashed refresh tokens, expiry, revocation | M1 | — |
| | | [`outbox`](#outbox) `‡` | `bigserial` | 11 | S12 — shared DDL | M3 | — |
| `catalog` | `catalog_db` | [`categories`](#categories) | `uuid` | 6 | Self-referencing category tree | M1 | — |
| | | [`products`](#products) | `uuid` | 16 | Definition, price, status, attributes, `version` | M1 | M2, M3 |
| | | [`product_variants`](#product_variants) | `uuid` | 8 | Option combinations, price override. **No stock column** — that is `inventory`'s fact | M1 | — |
| | | [`product_media`](#product_media) | `uuid` | 11 | Image urls, ordering, storage and processing state | M1 | M2 |
| | | [`product_audit_log`](#product_audit_log) | `bigserial` | 6 | Per-change actor, diff, correlation id (S7) | M1 | — |
| | | [`product_publish_failures`](#product_publish_failures) | `uuid` | 6 | Failed publish workflows as records, not log lines | M2 | — |
| | | [`outbox`](#outbox) | `bigserial` | 11 | S12 | M2 | — |
| | | [`processed_events`](#processed_events) `‡` | `(group, event_id)` | 4 | S13 — consumes `identity.merchant.status_changed` | M3 | — |
| `order` | `order_db` | [`orders`](#orders) | `uuid` | 24 | Order, totals, payment and shipment sub-state, Temporal ids, `version` | M2 | M3 |
| | | [`order_items`](#order_items) | `uuid` | 11 | Line snapshots — name, sku, `unit_price`, variant options | M2 | — |
| | | [`order_status_history`](#order_status_history) | `bigserial` | 8 | Append-only timeline; the order audit trail | M2 | — |
| | | [`idempotency_keys`](#idempotency_keys) | `(key, user_id)` | 9 | One order per key; stored reply replayed verbatim | M2 | — |
| | | [`outbox`](#outbox) | `bigserial` | 11 | S12 | M2 | — |
| `inventory` | `inventory_db` | [`stock_items`](#stock_items) | `uuid`, unique `variant_id` | 12 | `on_hand`, `reserved`, generated `available`; the lock target | M2 | — |
| | | [`reservations`](#reservations) | `uuid` | 8 | Held stock per order, with expiry for the sweeper | M2 | — |
| | | [`stock_movements`](#stock_movements) | `bigserial` | 10 | Append-only signed ledger of every delta | M2 | — |
| | | [`outbox`](#outbox) | `bigserial` | 11 | S12 | M2 | — |
| | | [`processed_events`](#processed_events) `‡` | `(group, event_id)` | 4 | S13 — consumes `catalog.product.published`, `order.cancelled` | M3 | — |
| `notification` | `notification_db` | [`notifications`](#notifications) | `uuid` + `seq` | 16 | Rendered notification, read state, stream cursor, origin client and delivery scope, delivery report | M3 | — |
| | | [`delivery_attempts`](#delivery_attempts) | `uuid` `‡` | 7 | Per-channel send attempts and errors | M3 | — |
| | | [`push_subscriptions`](#push_subscriptions) | `uuid` `‡` | 8 | Specified, unused — makes the offline channel an adapter | M3 | — |
| | | [`stream_cursors`](#stream_cursors) | `uuid`, unique `(user_id, client_id)` | 8 | Per-device SSE replay floor — what stops a fresh session replaying everything | M3 | — |
| | | [`processed_events`](#processed_events) | `(group, event_id)` | 4 | S13 | M3 | — |
| `analytics` | `analytics_db` (Mongo) | [`counters`](#counters) | `_id` string | — | Monotonic totals | M3 | M6 |
| | | [`daily_metrics`](#daily_metrics) | `{date, metric, scope}` | — | Time series per day per metric | M3 | M6 |
| | | [`product_stats`](#product_stats) | `product_id` | — | Popularity and revenue ranking source | M3 | — |
| | | [`failure_stats`](#failure_stats) | `{date, source, reason}` | — | Failed events and workflow issues | M3 | — |
| | | [`raw_events`](#raw_events) | envelope, TTL 7d | — | Backfill escape hatch for a new metric | M3 | — |
| | | [`assistant_conversions`](#assistant_conversions) | `uuid` | — | Assistant interaction → order correlation | M6 | — |
| | | [`processed_events`](#processed_events-mongo-variant) | `{group, event_id}` | 3 | S13, TTL 30d | M3 | — |
| `assistant` | `assistant_db` + pgvector | [`product_chunks`](#product_chunks) | `uuid` | 16 | Chunk text, `vector(1536)`, `tsvector`, pre-filter snapshots | M5 | — |
| | | [`embedding_cache`](#embedding_cache) | `content_hash` | 7 | Embeddings keyed by hash; survives a reindex | M5 | — |
| | | [`index_state`](#index_state) | `product_id` | 8 | Indexed catalog version, model, status — staleness detection | M5 | — |
| | | [`query_log`](#query_log) | `uuid` | 11 | Every search, for the eval set and latency metrics | M5 | — |
| | | [`conversations`](#conversations) | `uuid` | 9 | Thread, rolling summary, token totals | M5 (empty) | M6 |
| | | [`messages`](#messages) | `uuid` | 17 | Turn, intent, grounding `sources`, tokens, cost, latency | M5 (empty) | M6 |
| | | [`generations`](#generations) | `uuid` | 13 | Merchant content drafts; the AI-authored-copy audit trail | M6 | — |
| | | [`token_budgets`](#token_budgets) | `(scope, date)` | 6 | Spend caps; Redis is the fast path, this is the durable record | M6 | — |
| | | [`bulk_generation_jobs`](#bulk_generation_jobs) | `uuid` `‡` | 9 | Batch generation with per-item completed/failed counters | M6 | — |
| | | [`prompt_templates`](#prompt_templates) | `uuid` `‡` | 7 | Versioned prompts as rows, so a change is attributable | M6 | — |
| | | [`outbox`](#outbox) `‡` | `bigserial` | 11 | S12 — produces `assistant.interaction` | M6 | — |
| | | [`processed_events`](#processed_events) | `(group, event_id)` | 4 | S13 | M5 | — |

**Non-database state**, listed so the inventory is complete and so nothing here
is mistaken for a gap: Redis keyspaces (rate limits, the idempotency fast path,
notification pub/sub and SSE tickets) are specified in
[RFC 0003](0003-events-and-observability.md); object storage (MinIO) holds media
binaries keyed by `product_media.storage_key`; Temporal owns workflow execution
state, referenced from `order` and `catalog` by `workflow_id` only.

**Reading the index against the rules.** Three things in it are worth checking
directly, because this is where the conventions show up as facts rather than prose:

- **Every `bigserial` is in S1's exception list** — `product_audit_log`, `order_status_history`, `stock_movements`, `outbox`, and `notifications.seq`. All append-only, none crossing a boundary.
- **`outbox` and `processed_events` recur verbatim.** That repetition is the design: one relay implementation and one dedupe mechanism, not per-service variants.
- **No table appears under two services.** That is G7 read off the index — the shared-table count is zero by inspection, not by assertion.

##### 3.1.3.5. The catalogue

Databases in milestone order. Within a database: root aggregate first, then its
children, then append-only logs, then the shared infrastructure tables.

---

###### `identity_db`

Owned by `identity` · PostgreSQL 16 · arrives M1. The highest-security boundary
in the platform: the only store holding credentials, and the only one whose
service holds the JWT signing key.

| Table | Purpose |
| --- | --- |
| [`users`](#users) | Accounts, credentials, role |
| [`merchant_profiles`](#merchant_profiles) | Public merchant identity, approval status |
| [`refresh_sessions`](#refresh_sessions) | Refresh-token sessions, revocable |
| [`outbox`](#outbox) `‡` | S12 shared DDL |

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

**Shared tables.** [`outbox`](#outbox) `‡` (S12) — `identity` produces `identity.user.registered` and `identity.merchant.status_changed`.

---

###### `catalog_db`

Owned by `catalog` · PostgreSQL 16 · arrives M1, amended M2 and M3. Read-heavy
and the highest-cardinality store in the platform.

| Table | Purpose |
| --- | --- |
| [`categories`](#categories) | Self-referencing category tree |
| [`products`](#products) | The product aggregate root |
| [`product_variants`](#product_variants) | Option combinations |
| [`product_media`](#product_media) | Images and their processing state |
| [`product_audit_log`](#product_audit_log) | Append-only change trail (S7) |
| [`product_publish_failures`](#product_publish_failures) | Failed publish workflows |
| [`outbox`](#outbox) · [`processed_events`](#processed_events) `‡` | S12 / S13 |

###### `categories`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK | |
| `name` | `text` | not null |
| `slug` | `text` | unique, not null |
| `parent_id` | `uuid` FK → `categories.id` | nullable, self-referencing tree |
| `created_at`, `updated_at` | `timestamptz` | |

###### `products`

| Column | Type | Since | Notes |
| --- | --- | --- | --- |
| `id` | `uuid` PK | M1 | |
| `merchant_id` | `uuid` | M1 | **no FK** — owned by `identity`; indexed. Reference kind: claim (§3.1.3.6) |
| `sku` | `text` | M1 | not null; unique together with `merchant_id` |
| `name` | `text` | M1 | not null |
| `description` | `text` | M1 | nullable |
| `category_id` | `uuid` FK → `categories.id` | M1 | nullable, restrict on delete |
| `base_price` | `numeric(12,2)` | M1 | not null, `>= 0` check (S2) |
| `currency` | `char(3)` | M1 | ISO 4217, default `USD` (S2) |
| `status` | `text` | M1 | `draft` \| `publishing` \| `published` \| `inactive` \| `archived`, default `draft`. `ProductStatus` (S4). **`publishing` is reachable only from M2** — written by the publish workflow ([0002](0002-core-business-logic-and-workflows.md) B2). Adding it was a vocabulary change, not a migration (S4) |
| `attributes` | `jsonb` | M1 | free-form merchant attributes, default `{}` (S8) |
| `published_at` | `timestamptz` | M1 | nullable; written by the M2 workflow |
| `version` | `integer` | M1 | default 1, incremented per update — the S6 optimistic-concurrency token |
| `created_at`, `updated_at` | `timestamptz` | M1 | |
| `publish_workflow_id` | `text` | **M2** | nullable — the running or last publish workflow |
| `search_document` | `jsonb` | **M3** | the projection built by the publishing workflow, persisted so M5 can backfill without re-running workflows |

Indexes: unique `(merchant_id, sku)`; btree `merchant_id`, `category_id`, `status`; composite `(status, created_at desc)` for the default browse query; GIN on `attributes`.

###### `product_variants`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK | |
| `product_id` | `uuid` FK → `products.id` | cascade delete, indexed |
| `sku` | `text` | unique within the product |
| `options` | `jsonb` | e.g. `{"size": "M", "color": "red"}`, not null (S8) |
| `price_override` | `numeric(12,2)` | nullable (S2) |
| `is_active` | `boolean` | default `true` |
| `created_at`, `updated_at` | `timestamptz` | |

**Stock deliberately lives nowhere in this table.** A `quantity` column here would make `catalog` a second writer on a fact `inventory` owns, which is the multi-writer inconsistency [Part A](../../part-a.md) exists to solve. See [`stock_items`](#stock_items) and R3.

###### `product_media`

| Column | Type | Since | Notes |
| --- | --- | --- | --- |
| `id` | `uuid` PK | M1 | |
| `product_id` | `uuid` FK → `products.id` | M1 | cascade delete, indexed |
| `url` | `text` | M1 | not null |
| `alt_text` | `text` | M1 | nullable |
| `position` | `integer` | M1 | default 0, ordering |
| `created_at` | `timestamptz` | M1 | |
| `storage_key` | `text` † | **M2** | the MinIO object key for the original |
| `content_type` | `text` † | **M2** | MIME type as uploaded |
| `size_bytes` | `bigint` † | **M2** | of the original |
| `processing_status` | `text` | **M2** | `pending` \| `ready` \| `failed`. `MediaProcessingStatus` (S4), read policy `fallback` → `pending` |
| `derived` | `jsonb` | **M2** | map of size → storage key, written by the media activity |

###### `product_audit_log`

Satisfies the auditability requirement in [Part A](../../part-a.md) §1. Written in the same transaction as the change it records, by the service layer — not by a trigger, so the actor and request id are available (S7).

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `bigserial` PK | S1 exception — append-only, never crosses a boundary |
| `product_id` | `uuid` | indexed |
| `actor_id` | `uuid` | the user from the token |
| `action` | `text` | `created` \| `updated` \| `status_changed` \| `deleted` \| `variant_added` … |
| `changes` | `jsonb` | before/after of changed fields only |
| `created_at` | `timestamptz` | indexed |

###### `product_publish_failures`

So a failed publish is a record, not a log line. Named in [RFC 0002](0002-core-business-logic-and-workflows.md) §3.1.3 as a bare column list; types below are inferred.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK `‡` | Not named in the source; S1 requires a key |
| `product_id` | `uuid` † | indexed; no FK needed — same database, but the row must survive the product being archived |
| `workflow_id` | `text` † | the Temporal workflow that failed; matches `products.publish_workflow_id` |
| `failed_step` | `text` † | the activity name — `validate_product`, `process_media`, `build_search_document`, `sync_downstream` |
| `reason` | `text` † | the non-retryable failure message |
| `created_at` | `timestamptz` † | indexed (S3) |

**Shared tables.** [`outbox`](#outbox) (S12) — produces the three `catalog.product.*` topics. [`processed_events`](#processed_events) `‡` (S13) — consumes `identity.merchant.status_changed` to project merchant approval state.

---

###### `order_db`

Owned by `order` · PostgreSQL 16 · arrives M2, amended M3. Write-heavy in
bursts, and the strongest consistency requirement in the platform.

| Table | Purpose |
| --- | --- |
| [`orders`](#orders) | The order aggregate root, incl. payment and shipment sub-state |
| [`order_items`](#order_items) | Line items, fully snapshotted |
| [`order_status_history`](#order_status_history) | Append-only timeline (S7) |
| [`idempotency_keys`](#idempotency_keys) | One order per key |
| [`outbox`](#outbox) | S12 |

###### `orders`

Payment and shipment are **sub-state on this aggregate**, not separate services — see the rejections in §3.1.1.

| Column | Type | Since | Notes |
| --- | --- | --- | --- |
| `id` | `uuid` PK | M2 | |
| `order_number` | `text` | M2 | unique, human-readable, e.g. `SR-2026-0001042` |
| `customer_id` | `uuid` | M2 | no FK — owned by `identity`; indexed. Reference kind: claim |
| `status` | `text` | M2 | `pending` \| `awaiting_payment` \| `confirmed` \| `shipped` \| `delivered` \| `cancelled` \| `failed` \| `refunding` \| `refunded`. `OrderStatus` (S4), read policy `fallback` → `pending`. Transitions are constrained by `ORDER_TRANSITIONS` in the service layer, specified in [RFC 0002](0002-core-business-logic-and-workflows.md) §3.1.1 — 14 of the 81 ordered pairs are legal |
| `failure_reason` | `text` | M2 | nullable; set with any terminal failure |
| `subtotal`, `shipping_fee`, `total` | `numeric(12,2)` | M2 | non-negative checks (S2) |
| `currency` | `char(3)` | M2 | all lines must agree (S2) |
| `shipping_address` | `jsonb` | M2 | snapshot, not a reference (S8) |
| `payment_status` | `text` | M2 | `none` \| `pending` \| `succeeded` \| `failed` \| `refunded`. `PaymentStatus` (S4), read policy `fallback` → `none` |
| `payment_reference` | `text` | M2 | nullable; the simulated provider's id |
| `shipment_status` | `text` | M2 | `none` \| `created` \| `shipped` \| `in_transit` \| `delivered`. `ShipmentStatus` (S4), read policy `fallback` → `none` |
| `shipment_reference`, `tracking_number` | `text` | M2 | nullable |
| `workflow_id`, `workflow_run_id` | `text` | M2 | nullable, indexed — the link to Temporal |
| `version` | `integer` | M2 | S6 optimistic concurrency |
| `placed_at`, `confirmed_at`, `created_at`, `updated_at` | `timestamptz` | M2 | |
| `notified_at` | `timestamptz` | **M3** | nullable — so a notification failure is visible on the order |
| `origin_client_id` | `text` | **M3** | nullable — the `X-Client-Id` of the checkout request, persisted because `order.confirmed` and `order.failed` are emitted by the Temporal workflow after the request has ended and would otherwise have no origin ([RFC 0003](0003-events-and-observability.md) C21) |

Indexes: unique `order_number`; `(customer_id, created_at desc)`; `status`; `(status, created_at)` for operational sweeps.

###### `order_items`

Every product fact here is a **snapshot** taken at purchase ([0002](0002-core-business-logic-and-workflows.md) B8). A later price or name change must not reach a historical order — that is the domain behaviour, not a limitation.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK | |
| `order_id` | `uuid` FK → `orders.id` | cascade, indexed |
| `product_id`, `variant_id`, `merchant_id` | `uuid` | **no FK** — all snapshots; `merchant_id` indexed for the merchant view |
| `product_name`, `sku` | `text` | snapshot at purchase |
| `variant_options` | `jsonb` | snapshot |
| `unit_price` | `numeric(12,2)` | snapshot (S2) |
| `quantity` | `integer` | `> 0` check |
| `line_total` | `numeric(12,2)` | `unit_price * quantity`, stored |

###### `order_status_history`

The `timeline[]` the API returns, and the audit trail for orders (S7).

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `bigserial` PK | S1 exception |
| `order_id` | `uuid` | indexed |
| `from_status`, `to_status` | `text` | `OrderStatus` values. **Plain `text` with no type decorator** — an append-only historical record must stay readable after a value is retired from the enum |
| `reason` | `text` | nullable |
| `actor` | `text` | `customer:<id>` \| `workflow` \| `admin:<id>` \| `system`. Free `text`, deliberately **not** a `DomainEnum` — a `<kind>:<id>` composite is not a closed set (S4) |
| `correlation_id` | `text` | |
| `created_at` | `timestamptz` | |

###### `idempotency_keys`

Claimed by a unique insert, never a read-then-write ([0002](0002-core-business-logic-and-workflows.md) B7).

| Column | Type | Notes |
| --- | --- | --- |
| `key` | `text` | part of PK |
| `user_id` | `uuid` | part of PK — a key is scoped to its caller, so one user cannot collide with or probe another's |
| `request_hash` | `text` | SHA-256 of the canonicalised body; same key + different body is a client bug → `422` |
| `order_id` | `uuid` | nullable until the order is created |
| `response_status`, `response_body` | `integer`, `jsonb` | the stored reply, replayed verbatim on retry |
| `state` | `text` | `in_progress` \| `completed` — an in-flight duplicate receives `409 Retry-Later`, not a second order. `IdempotencyState` (S4) |
| `created_at`, `expires_at` | `timestamptz` | 24-hour retention, swept |

Constraint: unique `(user_id, key)` — S11, and the mechanism behind "one order per key".

**Shared tables.** [`outbox`](#outbox) (S12) — produces `order.*`, `payment.*`, `shipment.status_changed`.

---

###### `inventory_db`

Owned by `inventory` · PostgreSQL 16 · arrives M2. The highest-contention writes
in the platform: a single row per variant is the lock target under a flash sale.

| Table | Purpose |
| --- | --- |
| [`stock_items`](#stock_items) | Current quantities — the lock target |
| [`reservations`](#reservations) | Stock held for in-flight orders |
| [`stock_movements`](#stock_movements) | Append-only signed ledger |
| [`outbox`](#outbox) · [`processed_events`](#processed_events) `‡` | S12 / S13 |

###### `stock_items`

One row per variant. `on_hand` is what physically exists; `reserved` is what is held for in-flight orders; `available` is what a customer may buy.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK | |
| `variant_id` | `uuid` | **unique** — one row per variant, the lock target |
| `product_id`, `merchant_id` | `uuid` | snapshots, indexed |
| `sku` | `text` | snapshot, for operator legibility |
| `on_hand` | `integer` | `>= 0` |
| `reserved` | `integer` | `>= 0` |
| `available` | `integer` GENERATED `‡` | `on_hand - reserved`, stored generated column. Described in prose in [RFC 0002](0002-core-business-logic-and-workflows.md) but never listed as a column; it is listed here because callers read it. Generated rather than computed so no caller can define it differently |
| `low_stock_threshold` | `integer` | default 5; drives the stockout signal |
| `version` | `integer` | S6 |
| `created_at`, `updated_at` | `timestamptz` | |

Constraint: `CHECK (on_hand >= reserved AND reserved >= 0)` — S11, and the last line of defence against oversell.

###### `reservations`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK | |
| `order_id` | `uuid` | indexed; **unique together with `variant_id`** — the idempotency guarantee for retried reserve activities ([0002](0002-core-business-logic-and-workflows.md) B3) |
| `variant_id` | `uuid` | indexed |
| `quantity` | `integer` | `> 0` |
| `status` | `text` | `held` \| `committed` \| `released` \| `expired`. `ReservationStatus` (S4), read policy `fallback` → `held`; a `committed` reservation can never return to `held` |
| `expires_at` | `timestamptz` | indexed — the expiry sweeper's query ([0002](0002-core-business-logic-and-workflows.md) B5), 15 minutes |
| `created_at`, `updated_at` | `timestamptz` | |

###### `stock_movements`

Append-only ledger ([0002](0002-core-business-logic-and-workflows.md) B6). Quantity is never computed — it is stored in `stock_items` and ledgered here, so a disagreement between the two is detectable.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `bigserial` PK | S1 exception |
| `variant_id` | `uuid` | indexed |
| `delta_on_hand`, `delta_reserved` | `integer` | signed. `reserve` is `(0, +q)`; `commit` is `(-q, -q)`; `release` is `(0, -q)`; `restock` is `(+q, 0)` |
| `reason` | `text` | `initial` \| `manual_adjustment` \| `reserve` \| `release` \| `commit` \| `expire` \| `restock` \| `return`. `StockMovementReason` (S4) — a ledger vocabulary, expected to grow |
| `reference_type`, `reference_id` | `text`, `uuid` | e.g. `order`, the order id |
| `actor`, `correlation_id` | `text` | |
| `created_at` | `timestamptz` | indexed |

**Shared tables.** [`outbox`](#outbox) (S12) — produces `inventory.reserved` / `.released` / `.stockout`. [`processed_events`](#processed_events) `‡` (S13) — consumes `catalog.product.published` (to initialise stock rows) and `order.cancelled`.

---

###### `notification_db`

Owned by `notification` · PostgreSQL 16 · arrives M3. Bursty, fully async, and
failure-tolerant — a pure consumer that never sits on a checkout path.

| Table | Purpose |
| --- | --- |
| [`notifications`](#notifications) | The rendered notification and its read state |
| [`delivery_attempts`](#delivery_attempts) | Per-channel send attempts |
| [`push_subscriptions`](#push_subscriptions) | Specified, unused this milestone |
| [`stream_cursors`](#stream_cursors) | The server-side SSE replay floor, one row per `(user, device)` |
| [`processed_events`](#processed_events) | S13 |

###### `notifications`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK | |
| `seq` | `bigserial` | The stream cursor. Indexed `(user_id, seq)`. Global rather than per-user because a per-user sequence needs a lock or a second table for no benefit — the catch-up query filters by `user_id` anyway. S1 exception |
| `user_id` | `uuid` | no FK; indexed with `created_at desc` |
| `type` | `text` | `order_confirmed` \| `order_failed` \| `order_cancelled` \| `shipment_update` \| `stockout` \| `welcome`. `NotificationType` (S4) — the vocabulary most likely to grow in this platform |
| `channel` | `text` | `in_app` \| `email` \| `sms` — only `in_app` is delivered; the column exists so preferences are additive. `NotificationChannel` (S4) |
| `title`, `body` | `text` | rendered at consume time from a template |
| `payload` | `jsonb` | the source event, for rendering and for support (S8) |
| `read_at` | `timestamptz` | nullable |
| `correlation_id` | `text` | indexed — the link back to the trace |
| `origin_client_id` | `text` | nullable — the `X-Client-Id` of the request that caused the event, or null when there was no originating client (a carrier webhook, a scheduler, a workflow with no captured client), in which case `delivery_scope` is coerced to `all_devices`. An opaque client-minted token of at most 64 characters, validated for shape only, never an identity ([RFC 0003](0003-events-and-observability.md) C21 and §6.2). Deliberately not `uuid`: the server does not get to assume the client mints one |
| `delivery_scope` | `text` | `origin_only` \| `origin_first` \| `all_devices`. `NotificationDeliveryScope` (S4), derived at consume time from `type` and the presence of `origin_client_id`. `NOT NULL`, default `all_devices` — the safe value, because scope narrows delivery and the default must not |
| `delivered_at` | `timestamptz` | nullable — null means no client has reported receiving a frame. Stamped once by the ack endpoint, first writer wins. A delivery *report*, never a guarantee ([RFC 0003](0003-events-and-observability.md) C23) |
| `delivered_client_id` | `text` | nullable — the client that reported first; null iff `delivered_at` is null |
| `promoted_at` | `timestamptz` | nullable — set when an `origin_first` notification was widened to all devices because it went unacknowledged (C21) |
| `created_at` | `timestamptz` | |

Index: partial on `(created_at)` where `delivered_at IS NULL AND promoted_at IS NULL AND delivery_scope = 'origin_first'` — the promoter's only query, over a set that is normally near-empty, in the same spirit as the relay's partial index on `outbox`.

Constraint: at-most-one per `(event_id, user_id, channel)` — the invariant that makes `notification` a service (§3.1.1).

###### `delivery_attempts`

Named in [RFC 0003](0003-events-and-observability.md) §3.1.3 as a prose list; only `status` carried a type.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK `‡` | Not named in the source; S1 requires a key |
| `notification_id` | `uuid` FK → `notifications.id` † | cascade, indexed. Same database, so a real FK (R2 permits it) |
| `channel` | `text` † | `NotificationChannel` (S4), as on `notifications` |
| `status` | `text` | `pending` \| `sent` \| `failed`. `DeliveryStatus` (S4) |
| `error` | `text` † | nullable — the provider error on a failed attempt |
| `attempt` | `integer` † | 1-based retry counter |
| `created_at` | `timestamptz` † | indexed (S3) |

###### `push_subscriptions`

Specified but unused in M3, so the offline channel is an adapter rather than a migration. Named in [RFC 0003](0003-events-and-observability.md) §3.1.3 as a prose list with **no types at all**; every type below is inferred.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK `‡` | Not named in the source; S1 requires a key |
| `user_id` | `uuid` † | indexed; no FK — owned by `identity` |
| `endpoint` | `text` † | the Web Push endpoint URL |
| `p256dh` | `text` † | client public key, base64url |
| `auth` | `text` † | client auth secret, base64url |
| `user_agent` | `text` † | nullable, for subscription listing |
| `created_at`, `last_seen_at` | `timestamptz` † | S3 |

Constraint: unique `(user_id, endpoint)`.

###### `stream_cursors`

The server-side replay floor, one row per `(user, device)` ([RFC 0003](0003-events-and-observability.md) C22). It doubles as a device registry, but its job is the cursor — naming it `client_devices` would name the side effect. It exists because `EventSource` retains `Last-Event-ID` only across its own automatic reconnects: a page reload or a fresh browser profile sends none, and without a server-side floor the catch-up query replays a lifetime of notifications as live frames.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK | S1 |
| `user_id` | `uuid` | no FK — owned by `identity`; part of the unique key |
| `client_id` | `text` | the client-minted token, at most 64 characters, always scoped under `user_id` and never trusted as an identity |
| `cursor_seq` | `bigint` | `NOT NULL` default `0`. A plain `bigint`, not a sequence — it mirrors `notifications.seq` rather than generating one, so it is not an S1 exception. Advanced monotonically with `greatest()` by the ack endpoint, clamped by the 5-second visibility window ([RFC 0003](0003-events-and-observability.md) §3.1.1a) |
| `last_seen_at` | `timestamptz` | indexed — the retention sweep's query; refreshed on stream open and on each ack |
| `user_agent` | `text` | nullable — for the admin device list only |
| `created_at`, `updated_at` | `timestamptz` | S3 |

Constraint: unique `(user_id, client_id)` — the whole table is an upsert target. Retention: rows untouched for 90 days are deleted by `sweep_stale_stream_cursors` (Celery Beat, daily), which also enforces a cap of **20 rows per user**, evicting by `last_seen_at`, so a client rotating its id cannot grow the table without bound.

*Why Postgres and not Redis.* A cursor that a Redis flush resets turns every device into a cold start — survivable, but wrong. One small row per device is cheap enough that the durable store is the obvious answer. Redis holds only the 45-second presence key, which is genuinely disposable.

**Shared tables.** [`processed_events`](#processed_events) (S13) — consumes `order.*`, `payment.*`, `shipment.status_changed`, `inventory.stockout`, `identity.user.registered`.

---

###### `analytics_db`

Owned by `analytics` · **MongoDB** · arrives M3, extended M6. The one genuinely
schemaless workload in the platform, and the deliberate break from S8: per-metric
document shapes vary, which is what justifies the NoSQL requirement with a real
workload rather than a token one.

`analytics` is a **pure consumer**. It never calls another service, and its
failure is invisible to checkout.

| Collection | Purpose |
| --- | --- |
| [`counters`](#counters) | Monotonic totals |
| [`daily_metrics`](#daily_metrics) | Time series per day per metric per scope |
| [`product_stats`](#product_stats) | Popularity and revenue ranking |
| [`failure_stats`](#failure_stats) | Failed events and workflow issues |
| [`raw_events`](#raw_events) | Backfill escape hatch, TTL 7d |
| [`assistant_conversions`](#assistant_conversions) | Assistant → order correlation (M6) |
| [`processed_events`](#processed_events-mongo-variant) | S13, TTL 30d |

###### `counters`

`{_id: "<counter_name>", value, updated_at}` — monotonic per event (the invariant that makes `analytics` a service).

Counters: `total_customers`, `total_merchants`, `total_products`, `orders_total`, `revenue_total`, and from M6 `assistant_interactions`, `assistant_questions_answered`, `assistant_refusals`.

###### `daily_metrics`

`{_id: {date, metric, merchant_id}, …}` — one document per day per metric per scope. The source lists the value fields with an ellipsis; the set below is the full list as used by the M3 and M6 read APIs.

| Field | Type | Notes |
| --- | --- | --- |
| `_id.date` | `date` | |
| `_id.metric` | `string` | |
| `_id.merchant_id` | `uuid` \| `null` | null is the platform-wide scope |
| `orders_created`, `orders_confirmed`, `orders_failed` | `int` † | |
| `revenue` | `decimal` † | S2 — `Decimal128`, never a float |
| `currency` | `string` † | ISO 4217 (S2) |
| `assistant_messages`, `assistant_tokens` | `int` | M6 |
| `assistant_cost` | `decimal` | M6 |
| `assistant_avg_latency_ms`, `assistant_p95_latency_ms` | `int` | M6 |

Index: `{date, metric}`.

###### `product_stats`

`{_id: product_id, merchant_id, name, quantity_sold, revenue, order_count, last_sold_at}`. Index: `{merchant_id, quantity_sold: -1}`.

`name` is a snapshot; `revenue` is `Decimal128` (S2).

###### `failure_stats`

`{_id: {date, source, reason}, count}`.

###### `raw_events`

The full event envelope as received, TTL 7 days. The escape hatch that lets a new metric be backfilled without replaying Kafka. The source says only "the full envelope"; that envelope is fixed in [RFC 0000](0000-system-architecture.md) §3.1.3 and is reproduced here so the collection is specified:

| Field | Type | Notes |
| --- | --- | --- |
| `event_id` | `uuid` † | unique |
| `event_type` | `string` † | |
| `event_version` | `int` † | |
| `occurred_at` | `date` † | **TTL index, 7 days** — named by the source's index note |
| `producer` | `string` † | |
| `correlation_id`, `causation_id` | `string` † | |
| `actor_id` | `uuid` \| `null` † | |
| `payload` | `object` † | the domain payload, shape per topic |

Hard-deleted by TTL rather than soft-deleted — derived, rebuildable, TTL-bounded (the S5 break).

###### `assistant_conversions`

New in M6. `{user_id, interaction_at, order_id?, ordered_at?, converted}` — populated by correlating `assistant.interaction` with `order.confirmed` within 24 hours.

###### `processed_events` (Mongo variant)

`{_id: {group, event_id}, processed_at}`, TTL index at 30 days on `processed_at`. Note the key is `group`, not `consumer_group` — the Postgres and Mongo shapes are deliberately not identical because the stores are not. See §3.1.3.2.

---

###### `assistant_db`

Owned by `assistant` · PostgreSQL 16 with the `vector` extension · retrieval
half arrives M5, conversational half M6. GPU/provider-bound, unbounded latency,
external cost — the most distinct operational profile in the platform.

Retrieval and generation are **one service**, not two: they share the chunk
store, the embedding client and the conversation store, and splitting them adds
a hop per query for no ownership gain ([RFC 0004](0004-retrieval-layer.md) D12).

| Table | Purpose | Arrives |
| --- | --- | --- |
| [`product_chunks`](#product_chunks) | Embedded chunk text + vector + tsvector | M5 |
| [`embedding_cache`](#embedding_cache) | Embeddings keyed by content hash | M5 |
| [`index_state`](#index_state) | Per-product index freshness | M5 |
| [`query_log`](#query_log) | Every search, for evaluation | M5 |
| [`conversations`](#conversations) | Thread + rolling summary | M5 empty, M6 used |
| [`messages`](#messages) | Turns, grounding, cost, latency | M5 empty, M6 used |
| [`generations`](#generations) | Merchant content drafts | M6 |
| [`token_budgets`](#token_budgets) | Spend caps | M6 |
| [`bulk_generation_jobs`](#bulk_generation_jobs) | Batch generation | M6 |
| [`prompt_templates`](#prompt_templates) | Versioned prompts | M6 |
| [`outbox`](#outbox) `‡` · [`processed_events`](#processed_events) | S12 / S13 | M6 / M5 |

###### `product_chunks`

Derived and rebuildable (S5) — hard-deleted, never soft-deleted, and reconstructible by a reindex. A **projection**, not catalog data.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK | |
| `product_id` | `uuid` | indexed; no FK — owned by `catalog` |
| `merchant_id`, `category_id` | `uuid` | snapshots, indexed, used as pre-filters |
| `chunk_index` | `integer` | ordinal within the product |
| `chunk_type` | `text` | `identity` \| `description` \| `attributes` \| `variants` \| `category`. `ChunkType` (S4) — a new chunk kind costs a reindex, never a migration |
| `content` | `text` | the embedded text, including the identity prefix — returned as M6's context |
| `content_hash` | `text` | `sha256(model, normalised content)`; unique with `product_id` |
| `embedding` | `vector(1536)` | dimension follows the model; a change means a reindex, recorded in `index_state` |
| `token_count` | `integer` | for cost accounting and context budgeting |
| `search_vector` | `tsvector` | generated from `content`; the keyword half of hybrid search |
| `product_status` | `text` | denormalised for pre-filtering without a cross-service call. Reference kind: **projection** |
| `price`, `currency` | `numeric(12,2)`, `char(3)` | snapshot at index time, used only for range pre-filtering; the displayed price is hydrated live (§3.1.3.6) |
| `metadata` | `jsonb` | product name, sku, media url — enough to render a result without another call (S8) |
| `indexed_at` | `timestamptz` | |

Indexes — part of the design, not tuning, so they are specified here:

- **HNSW** on `embedding`, `vector_cosine_ops`, `m = 16`, `ef_construction = 64`. HNSW over IVFFlat for better recall at this corpus size, no training step, and no rebuild as the corpus grows ([RFC 0004](0004-retrieval-layer.md) Q2). `ef_search` is a per-query knob.
- GIN on `search_vector`.
- btree on `product_id`, `merchant_id`, `category_id`, `product_status`.
- Composite `(product_status, category_id)` — the most common pre-filter pair.
- Unique `(product_id, content_hash)`.

###### `embedding_cache`

Survives a reindex deliberately: re-chunking with a changed strategy but unchanged text costs nothing.

| Column | Type | Notes |
| --- | --- | --- |
| `content_hash` | `text` PK | `sha256(model, normalised text)` |
| `model` | `text` | part of the hash input, stored for legibility |
| `embedding` | `vector(1536)` | |
| `token_count` | `integer` | |
| `hit_count` | `integer` | cache effectiveness, exported as a metric |
| `created_at`, `last_used_at` | `timestamptz` | LRU eviction input |

Hard-deleted on eviction (the S5 break) — derived and rebuildable.

###### `index_state`

| Column | Type | Notes |
| --- | --- | --- |
| `product_id` | `uuid` PK | |
| `catalog_version` | `integer` | the `products.version` that was indexed — makes staleness detectable |
| `chunk_count` | `integer` | |
| `embedding_model` | `text` | a mismatch marks the product for reindex |
| `status` | `text` | `indexed` \| `pending` \| `failed` \| `removed`. `IndexStatus` (S4), read policy `fallback` → `pending` — the indexer branches on it, so it must never see a bare string |
| `error` | `text` | nullable |
| `indexed_at`, `updated_at` | `timestamptz` | |

###### `query_log`

Every search, for the evaluation set and for M6's analytics.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK | |
| `query_text` | `text` | |
| `filters` | `jsonb` | S8 |
| `mode` | `text` | `hybrid` \| `dense` \| `keyword`. `SearchMode` (S4) — the same enum backs the `mode` parameter on `/search/semantic`, so the API's published values and the logged values cannot diverge |
| `result_product_ids` | `uuid[]` | |
| `result_count` | `integer` | zero-result queries are the most useful diagnostic in the table |
| `latency_ms`, `embedding_cached` | `integer`, `boolean` | |
| `user_id`, `correlation_id` | `uuid`, `text` | nullable / indexed |
| `created_at` | `timestamptz` | |

###### `conversations`

Created empty in M5 so M6 adds no migration to a running service.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK | |
| `user_id` | `uuid` | nullable for anonymous; indexed with `created_at desc` |
| `session_id` | `text` | for anonymous continuity |
| `title` | `text` | generated from the first message |
| `summary` | `text` | the rolling summary, rewritten every 6 turns |
| `message_count`, `total_tokens` | `integer` | |
| `last_message_at`, `created_at` | `timestamptz` | |

###### `messages`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK | |
| `conversation_id` | `uuid` FK → `conversations.id` | cascade, indexed with `created_at` |
| `role` | `text` | `user` \| `assistant` \| `system`. `MessageRole` (S4) |
| `content` | `text` | |
| `intent` | `text` | `product_search` \| `compare` \| `too_vague` \| `out_of_scope` \| `follow_up` — the classifier's output, which makes the graph's routing auditable after the fact. `Intent` (S4); the vocabulary most likely to grow in the whole platform |
| `sources` | `jsonb` | product ids, scores, and the matched chunk ids that grounded the answer — the grounding record |
| `prompt_tokens`, `completion_tokens` | `integer` | |
| `model`, `cost_usd` | `text`, `numeric(10,6)` | |
| `latency_ms`, `time_to_first_token_ms` | `integer` | latency targets are measured from stored data, not from a stopwatch |
| `finish_reason` | `text` | `stop` \| `length` \| `error` \| `refused` \| `budget_exceeded`. `FinishReason` (S4) — provider-driven, so it will gain values the platform does not control |
| `feedback_helpful`, `feedback_comment` | `boolean`, `text` | nullable |
| `correlation_id` | `text` | indexed — links a message to its trace |
| `created_at` | `timestamptz` | |

> **Open gap — prompt attributability.** [RFC 0005](0005-ai-assistant-and-streaming.md) §3.1.3 says `prompt_templates` exists so a prompt change is "attributable to a version **recorded on every message**", but `messages` has no `prompt_template_id` or `prompt_version` column, in that RFC or here. The column is **not added silently** — see §3.3.1 Q4.

###### `generations`

Merchant content, kept because a merchant will want to compare drafts and because it is the audit trail for AI-authored copy.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK | |
| `merchant_id`, `product_id` | `uuid` | indexed |
| `content_type` | `text` | `description` \| `seo_title` \| `faq` \| `promotional` \| `benefits_summary` \| `cross_sell`. `ContentType` (S4); the same enum backs `bulk_generation_jobs.content_type` and the `prompt_templates` key |
| `parameters` | `jsonb` | tone, length, keywords as requested (S8) |
| `content` | `text` | the generated draft |
| `prompt_tokens`, `completion_tokens` | `integer` † | the source leaves the type cell blank with the note "as above"; resolved against `messages` |
| `cost_usd` | `numeric(10,6)` † | as above |
| `model` | `text` † | as above |
| `applied` | `boolean` | whether the merchant used it — the most honest quality metric available |
| `bulk_job_id` | `uuid` | nullable, indexed. → `bulk_generation_jobs.id` |
| `created_at` | `timestamptz` | |

###### `token_budgets`

Enforced in Redis for speed with Postgres as the durable record, reconciled by a Beat task — the same pattern as the idempotency fast path.

| Column | Type | Notes |
| --- | --- | --- |
| `scope` | `text` | `user:<uuid>` \| `ip:<hash>` \| `global`; part of PK. Free `text`, deliberately **not** a `DomainEnum` — a `<kind>:<id>` composite is not a closed set (S4) |
| `date` | `date` | part of PK |
| `tokens_used`, `requests` | `integer` | |
| `cost_usd` | `numeric(10,6)` | S2 |
| `updated_at` | `timestamptz` | |

###### `bulk_generation_jobs`

Named in [RFC 0005](0005-ai-assistant-and-streaming.md) §3.1.3 as a prose list; only `status` carried a type.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK † | |
| `merchant_id` | `uuid` † | indexed |
| `content_type` | `text` † | `ContentType` (S4) — named in the source, type inferred |
| `product_ids` | `uuid[]` † | the batch |
| `status` | `text` | `queued` \| `running` \| `completed` \| `partially_failed` \| `failed` \| `cancelled`. `BulkJobStatus` (S4), read policy `fallback` → `queued`. `partially_failed` earns its place because the table carries both counters below — collapsing it into `failed` would make them unreadable. Transitions in [RFC 0005](0005-ai-assistant-and-streaming.md) §3.1.3 |
| `completed`, `failed` | `integer` † | per-item counters |
| `created_at`, `finished_at` | `timestamptz` † | `finished_at` nullable until terminal (S3, S10) |

###### `prompt_templates`

> **This table is specified here for the first time.**
> [RFC 0005](0005-ai-assistant-and-streaming.md) §3.1.3 states its purpose —
> "versioned system prompts and per-content-type templates, in the database
> rather than in code, so a prompt change is a row and its effect is
> attributable to a version recorded on every message" — but **names no column**.
> Every column below is `‡`. This is the one table in the platform whose schema
> is *proposed* by this consolidation rather than consolidated by it, and
> **Milestone 4 reviewers should treat it as a new design decision**, not as a
> transcription. Two constraints come from the purpose statement: it must be
> versioned, and it must be keyed by `ContentType`.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK `‡` | |
| `content_type` | `text` `‡` | `ContentType` (S4), or null for the system prompt. Named indirectly by `generations.content_type`, which says this enum "backs the `prompt_templates` key" |
| `name` | `text` `‡` | template identifier within a content type |
| `version` | `integer` `‡` | monotonic per `(content_type, name)`; unique together |
| `body` | `text` `‡` | the template source |
| `is_active` | `boolean` `‡` | exactly one active per `(content_type, name)`; supersession is a new row, never an update (S5) |
| `created_at` | `timestamptz` `‡` | S3 |

**Shared tables.** [`outbox`](#outbox) `‡` (S12) — produces `assistant.interaction`. [`processed_events`](#processed_events) (S13) — the indexer consumes `catalog.product.published` / `.updated` / `.unpublished`.

---

##### 3.1.3.6. Cross-boundary reference kinds

**Reference kinds.** Every dashed line above is one of five kinds. Naming them is what makes "no foreign keys" a design rather than an omission. The fifth arrived with origin-scoped notification delivery and is proposed rather than settled (Q7).

| Kind | What it is | When used | Example | Consistency |
| --- | --- | --- | --- | --- |
| **Claim** | The id arrives in a verified JWT | Caller identity | `orders.customer_id`, `products.merchant_id` on write | Strong at write time — the token is signed |
| **Snapshot** | Values copied at write time and never updated | Facts whose historical value is the correct one | `order_items.product_name`, `unit_price` | Intentionally frozen. Updating it would be the bug |
| **Projection** | A read model kept current by events | Facts needed for filtering that change occasionally | `product_chunks.product_status`, `stock_items.merchant_id` | Eventual, seconds |
| **Live lookup** | An API call at read time | Facts that must be current and cannot be stale | price and stock in a search response; `order → catalog` at creation | Strong, at the cost of a dependency |
| **Opaque client token** `‡` | A client-minted correlator, validated for shape, always stored and queried scoped under an authenticated `user_id` | Correlating an action back to the device that started it | `notifications.origin_client_id`, `outbox.client_id`, `orders.origin_client_id`, `stream_cursors.client_id` | None, and advisory by design. Never an identity, never an authorisation input, never a join to an owned row |

The decision procedure, applied to any new cross-boundary reference:

```
  Does the historical value matter more than the current one?
        yes ──▶ SNAPSHOT
        no
        │
  Would a few seconds of staleness cause a wrong decision or a wrong display?
        no  ──▶ PROJECTION (event-fed)
        yes
        │
  Is the caller able to proceed without it?
        yes ──▶ PROJECTION, with the staleness documented
        no  ──▶ LIVE LOOKUP, and accept the availability coupling (R5)
```

The fifth kind sits outside that procedure, because it is not a reference to a fact another service owns — nobody owns it. It is a token the client made up, which the platform agrees to carry and compare for equality and nothing else. That is exactly why it needs its own row: calling it a snapshot would imply a source of truth it does not have, and calling it a claim would imply it was verified.

Applied: an order line's price is historical → snapshot. A chunk's `product_status` is a filter that may lag seconds → projection. A displayed price must never be stale → live lookup. Order creation cannot proceed without knowing the product is published and its real price → live lookup, the one such call in the system.

##### 3.1.3.7. The ownership map

**The ownership map.** The authoritative answer to "where does this fact live". One writer each; everything else is one of the four kinds above.

| Fact | Sole writer | Read elsewhere as | Mechanism |
| --- | --- | --- | --- |
| User identity, credentials, role | `identity` | Claim | JWT |
| Merchant profile, approval status | `identity` | Projection (in `catalog`) | `identity.merchant.status_changed` |
| Product definition, price, status | `catalog` | Snapshot (`order`), Projection (`assistant`), Live lookup (search display) | API + events |
| Category tree | `catalog` | Projection | Event |
| Stock on hand, reservations | `inventory` | Live lookup | API |
| Stock movement ledger | `inventory` | — | — |
| Order, payment state, shipment state | `order` | Event payload only | Events |
| Idempotency claim | `order` | — | — |
| Notification, delivery record, and per-device stream cursor | `notification` | — | — |
| Metric rollups | `analytics` | — | — |
| Chunks, embeddings, conversations | `assistant` | — | — |
| Workflow execution state | Temporal | Referenced by id from `order` / `catalog` | `workflow_id` column |

##### 3.1.3.8. Inferred and added cells

Every `†` and `‡` in the catalogue, in one place, so a reviewer can audit the
consolidation without scrolling it. **39 rows carry an inferred type; 11 rows
are columns no RFC names.** Counts are rows, not columns — a row such as
`created_at`, `updated_at` covers two columns.

| Table | `†` rows | `‡` rows | What the source actually said |
| --- | --- | --- | --- |
| [`product_media`](#product_media) | 3 | — | M2 additions `storage_key`, `content_type`, `size_bytes` given as bare names ([0002](0002-core-business-logic-and-workflows.md)) |
| [`product_publish_failures`](#product_publish_failures) | 5 | 1 | A five-name prose list, no types, **no primary key** ([0002](0002-core-business-logic-and-workflows.md)) |
| [`stock_items`](#stock_items) | — | 1 | `available` described in prose as a generated column but never listed as one ([0002](0002-core-business-logic-and-workflows.md)) |
| [`delivery_attempts`](#delivery_attempts) | 5 | 1 | Prose list; only `status` typed ([0003](0003-events-and-observability.md)) |
| [`push_subscriptions`](#push_subscriptions) | 6 | 1 | Prose list, **no types at all** ([0003](0003-events-and-observability.md)) |
| [`daily_metrics`](#daily_metrics) | 3 | — | Document shape ends in a literal `…` ([0003](0003-events-and-observability.md)) |
| [`raw_events`](#raw_events) | 8 | — | "The full envelope" with no field list, though its TTL index cites `occurred_at`. Expanded from the envelope fixed in [0000](0000-system-architecture.md) §3.1.3 |
| [`generations`](#generations) | 3 | — | One row's Type cell is blank with the note "as above"; resolved against `messages` ([0005](0005-ai-assistant-and-streaming.md)) |
| [`bulk_generation_jobs`](#bulk_generation_jobs) | 6 | — | Prose list; only `status` typed ([0005](0005-ai-assistant-and-streaming.md)) |
| [`prompt_templates`](#prompt_templates) | — | 7 | **No columns named anywhere.** Purpose only — the whole table is proposed here ([0005](0005-ai-assistant-and-streaming.md)) |

Plus four `‡` **tables** rather than columns — `identity_db.outbox`,
`assistant_db.outbox`, `catalog_db.processed_events`,
`inventory_db.processed_events` — required by S12/S13 and by the event
catalogue, mandated generically by [RFC 0003](0003-events-and-observability.md)
but never enumerated per database. See §3.1.3.2.

**How to read the concentration.** Nine of the ten rows above are tables the
source RFCs wrote as a prose sentence instead of a column table. That is the
single best predictor of an under-specified table in this RFC set, and it is
worth stating as a rule: *a table written as a sentence is a table nobody has
designed yet.* `prompt_templates` is the limiting case — a sentence with no
columns at all.

`stream_cursors` is the counter-example, and it adds **no rows to the table
above**: it arrives from [RFC 0003](0003-events-and-observability.md) §3.1.1a
fully typed, with its key, its constraint, its index, its retention and its cap
already decided, so there is nothing for this section to infer. It is the first
table in the set to do so, which is what the rule above predicts when a
milestone RFC designs a table instead of mentioning one.

---

#### 3.1.4. Breaking changes

**None.** This RFC introduces no code and no running schema. It records and
consolidates decisions that RFCs 0001–0005 implement.

Two editorial changes are worth naming because a reader of an earlier draft will
notice them. First, **column-level DDL moved here** from the five milestone
RFCs; those RFCs now name their tables and link to §3.1.3.5, so a type has
exactly one definition in the RFC set. Second, this document formally supersedes
the prior prototype's decomposition — eleven services and a frontend — as
recorded in [RFC 0001](0001-foundation-and-core-services.md) §3.1.4.

#### 3.1.5. Action Plan — the milestone/schema matrix

This document produces no tasks of its own. Its output is (a) the schema each
milestone builds, and (b) a set of constraints each milestone RFC is checked
against.

**What each milestone creates.** The union of the "Tables created" column is the
catalogue in §3.1.3.5; that is the consistency check between the two views.

| M | Databases created | Tables created | Tables amended | Enum vocabularies added |
| --- | --- | --- | --- | --- |
| **1** | `identity_db`, `catalog_db` | `users`, `merchant_profiles`, `refresh_sessions`, `categories`, `products`, `product_variants`, `product_media`, `product_audit_log` | — | `UserRole`, `MerchantStatus`, `ProductStatus` (less `publishing`) |
| **2** | `order_db`, `inventory_db` | `orders`, `order_items`, `order_status_history`, `idempotency_keys`, `stock_items`, `reservations`, `stock_movements`, `product_publish_failures`, `outbox` ×3 (`catalog`, `order`, `inventory`) | `products` +`publish_workflow_id`; `product_media` +5 columns | `OrderStatus`, `PaymentStatus`, `ShipmentStatus`, `IdempotencyState`, `ReservationStatus`, `StockMovementReason`, `MediaProcessingStatus`; `ProductStatus` +`publishing` |
| **3** | `notification_db`, `analytics_db` | `notifications`, `delivery_attempts`, `push_subscriptions`, `stream_cursors`; the six M3 Mongo collections; `processed_events` ×4 (`catalog` `‡`, `inventory` `‡`, `notification`, `analytics`); `outbox` ×1 (`identity` `‡`) | `products` +`search_document`; `orders` +`notified_at` +`origin_client_id`; `outbox` +`client_id` ×5 | `NotificationType`, `NotificationChannel`, `DeliveryStatus`, `NotificationDeliveryScope` |
| **5** | `assistant_db` | `product_chunks`, `embedding_cache`, `index_state`, `query_log`, `processed_events`; `conversations` and `messages` **created empty** | — | `ChunkType`, `IndexStatus`, `SearchMode` |
| **6** | — | `generations`, `token_budgets`, `bulk_generation_jobs`, `prompt_templates`, `outbox` (`assistant` `‡`); `assistant_conversions` (Mongo) | `conversations`, `messages` now written; `counters` and `daily_metrics` gain assistant fields | `MessageRole`, `Intent`, `FinishReason`, `ContentType`, `BulkJobStatus` |

No milestone drops a table, and every migration is additive — which is what
makes S14's mandatory `downgrade` achievable throughout.

**What each milestone must satisfy from this RFC.**

| Milestone | Constraint |
| --- | --- |
| 1 | R1 enforced by grants; S1–S11 applied to `identity_db` and `catalog_db`; `merchant_id` as a claim, not a FK; S4's `DomainEnum` and `DomainEnumText` land in `libs/common` before the first enum-valued column exists, and every service's `alembic/env.py` carries the `render_item` hook |
| 2 | `order` and `inventory` per §3.1.1; `payment`/`shipment` folded, not split; snapshots in `order_items`; S11 constraints on stock; S12 outbox in all producing services |
| 3 | S13 in every consumer; `analytics` as a pure consumer (never a caller); projection references fed by events, not polling |
| 5 | `assistant` owns its vectors; chunks are a projection, hard-deletable and rebuildable |
| 6 | No new service; `assistant` gains a router, not a sibling |
| 4, 7 | Reviews check G7 directly: no shared table, no cross-service FK, no cross-database grant — plus the `†`/`‡` census in §3.1.3.8, which is this consolidation's proposal surface |

### 3.2. Rationale

The decomposition arguments are compressed here; each is stated in full as a row
of the table it justifies in §3.1.1 or §3.1.3.1.

| Decision | Why |
| --- | --- |
| **Ownership, not capability, as the boundary** | Capability splits reliably produce two services writing one fact — "order management" and "fulfilment" both want order status; "catalog" and "pricing" both want price. That forces either a shared table or a distributed transaction. Asking "who writes this?" *before* drawing the boundary cannot produce that situation |
| **A written four-part test rather than intuition** | Intuition produced the eleven-service prototype. Each of those services was individually plausible; the set was not. A written test means a reviewer disagrees with a row and a column, not with a diagram |
| **No foreign key across a boundary** | Lost: the database can no longer prevent an order line referencing a deleted product. Gained: independent deploy, migrate, restore and scale. The lost property is also less valuable than it looks — an order line *should* survive its product being deleted, because it records what was bought, not what is for sale. S5 and the snapshot reference kind turn the constraint into correct domain behaviour |
| **Four *named* reference kinds** | A prohibition alone produces inconsistency: one developer copies a value, another calls an API, a third polls a table. Naming the four legitimate kinds makes every cross-boundary reference's consistency guarantee a decision rather than an accident |
| **`numeric` for money (S2)** | `float` cannot represent 0.10. Sum a hundred lines and a stored total eventually disagrees with the sum of its parts by a cent, in a system that also emits revenue analytics and processes refunds. No defence once shipped, and free to avoid |
| **UUID primary keys (S1)** | Ids cross boundaries, appear in event payloads and sit in URLs. A sequential integer leaks volume, collides across services that merge ids in analytics, and cannot be client-generated — which idempotency and correlation ids require. Index cost accepted; S1's exception covers the volume cases |
| **Per-service audit tables (S7)** | An audit row must be in the same transaction as the change it records; any other arrangement admits a state where the change happened and the trail did not. Cross-service transactions do not exist here, so the trail lives with the data |
| **Module boundaries mirroring service boundaries** | The layering seam is what makes one business rule reachable from four callers — an HTTP route, a Temporal activity, an event consumer and a CLI — with one implementation and one set of tests. It is the highest-leverage structural rule in the codebase and the first to erode under deadline pressure |
| **One document for boundaries *and* schema** | The two questions have one answer. A boundary that does not coincide with a data-ownership boundary is not a boundary, so specifying them apart is what let the table inventory drift from the services that own it |

#### Why enum values are validated in the model, not the database (S4)

This is the one convention that reverses the platform's usual instinct — S11
puts invariants in the database wherever they can be expressed there — so it
carries more weight than the others. It is also the argument
[RFC 0002](0002-core-business-logic-and-workflows.md) §3.1.3 cites.

**The decisive argument is that the old rule made another rule impossible to
keep.** S14 mandates a working `downgrade` on every Alembic revision. PostgreSQL
has no `ALTER TYPE … DROP VALUE`, in any version, so a revision whose `upgrade`
adds an enum label has no inverse. Removing a label means creating a replacement
type, rewriting every dependent column under `ACCESS EXCLUSIVE`, re-pointing
every dependent function and composite type, dropping the old type and renaming
— which is not an inverse at all, because it fails outright if any row already
holds the value being dropped, and after a deploy that used the value, every
such row does. The honest `downgrade` is therefore either a lie (`pass`) or a
destructive operation that can lose data. **Native enums and S14 are mutually
unsatisfiable for any vocabulary that will ever grow**, and vocabularies grow:
`products.status` gains `publishing` in Milestone 2 (§3.1.5). Under the old rule
that is a revision nobody can roll back.

One qualification, because the usual version of this argument overstates it: it
is often claimed that `ALTER TYPE … ADD VALUE` cannot run inside a transaction
block. That was true before PostgreSQL 12 and is **false on this platform's
PostgreSQL 16**. What remains true is narrower — the new value cannot be *used*
until the adding transaction commits, so adding a label and backfilling to it
cannot be one revision. That is an inconvenience. The irreversibility is the
defect.

`text`-plus-`CHECK` rescues nothing, because `ALTER TABLE … DROP CONSTRAINT /
ADD CONSTRAINT` is a migration too. It has one virtue the native enum lacks — it
is trivially reversible — which is why it is the escape hatch in §5 if this
rule's premise ever fails.

**What is gained is a stronger guarantee, not a weaker one.** A Postgres enum on
`orders.status` constrains a write to a valid label; it accepts all 81 ordered
pairs over the nine order states. It always permitted `delivered → pending`,
`cancelled → confirmed`, and `pending → confirmed` skipping payment entirely —
every write that actually corrupts an order. An explicit transition table
accepts 14 of those 81. The enum checked the label and never the edge, and the
edge is where the money is.

**What is given up** is enforcement against writers that do not go through the
service layer — a hand-run `UPDATE` in `psql`. That is a real loss, recorded in
§3.3 rather than argued away. It is bounded by R1: one service and one role per
database, so the only unconstrained writer is a human with that role and a
terminal, which is why the runbook routes manual status edits through an admin
utility that calls the service layer. The trade is sound only while R1 and R3
hold — **if a non-Python writer to any service database is ever introduced, S4
must be revisited**, and the replacement is `CHECK (status IN (…))`.

### 3.3. Tradeoffs

| Trade | Cost | Why acceptable |
| --- | --- | --- |
| Eight services rather than a modular monolith | Eight schemas, eight migration histories, eight deployments; no cross-service joins | The stated learning objective, and boundaries are cheapest to build now (§5) |
| Ownership-based split | Some boundaries cut across a user-visible feature — the merchant order view spans `order` and `catalog` | Features change; data ownership does not |
| No cross-service referential integrity | Orphan references are possible; the database cannot prevent them | S5, snapshots and events maintain it at the application level. An orphan is usually the correct outcome |
| Snapshots duplicating data | Storage, and a correction never propagates to historical records | That is what a historical record means. The alternative restates history |
| Projections lagging seconds | A suspended merchant's products stay searchable briefly | Bounded, and it fails safe. The alternative is a synchronous call on a read path |
| UUID keys | Larger indexes, worse locality than `bigserial` | Necessary for boundary-crossing ids; S1's exception covers the volume cases |
| Rigid conventions | Occasional local awkwardness | Eight schemas by one developer in five weeks read as one system only if the conventions hold. Deviations are listed (§3.1.3.1), not ad hoc |
| `jsonb` for open-ended fields | No database-level validation; typo'd keys accepted | Variant dimensions are merchant-defined; a fixed schema would be wrong immediately. Validated at the Pydantic layer, never used for filtered fields (S8) |
| Per-service audit tables | Cross-service audit queries need manual correlation | Transactional atomicity with the change is non-negotiable |
| **Enum validity in the model, not the database (S4)** | A direct `psql` write is no longer constrained — `UPDATE orders SET status = 'shippd'` now succeeds. `\d orders` no longer lists permitted values | S14 becomes satisfiable at all, and state machines gain a transition table strictly stricter than the type it replaces. Bounded by R1 (one role per database) and reduced by a `COMMENT ON COLUMN` pointing at `app/domain/enums.py`, a per-service round-trip test, and a runbook routing manual edits through an admin utility. Residual risk is a human with the service role at 2am, accepted knowingly |
| The gateway as a stateless exception | Slight inconsistency in the ownership test | Bounded explicitly — no logic, no database. The alternative duplicates edge concerns eight times |
| **One document holding the whole schema** | ~1,300 lines; a column change is an edit here, not in the milestone RFC being worked on | One definition per column. The alternative is what this consolidation replaced: three places defining `products`, drifting independently |

#### 3.3.1. Challenges and Open Questions

| # | Question | Position | Decide by |
| --- | --- | --- | --- |
| Q1 | Is folding `payment` into `order` defensible at review? | Yes, on §3.1.1 — no compliance boundary, no owned state, no independent change. The interface is separate, so extraction is mechanical if the premise changes | M4 review |
| Q2 | Does `order` keep the synchronous `catalog` call, or move to a projection? | Keeps it (R5). Price and publication status at purchase must be authoritative; a projection puts a staleness window on exactly the wrong fact | Revisit M4 |
| Q3 | **Are the four `‡` tables (§3.1.3.2) genuinely missing, or is the blanket rule enough?** | They are added here, because "all producers"/"all consumers" never survives contact with an implementer reading a per-database table list. The opposite reading is defensible and costs nothing to adopt — delete four rows | **M4 review** |
| Q4 | **`prompt_templates` has no specified columns, and `messages` has no `prompt_version`, so the attributability the table exists to provide is not wired up.** Which is right? | The table is proposed in §3.1.3.5 with every column marked `‡`; the `messages` column is **not** added, because adding it silently would fix the symptom and hide the design gap | **M6, or M4 if reviewed earlier** |
| Q5 | Are eight services too many for one developer in five weeks? | It is the honest count the test produces, and the schedule introduces them two at a time. The alternative — fewer services than the data model implies — means shared tables | M4 review |
| Q6 | Should the `†` inferred types be ratified, or re-decided per milestone? | Ratified at M4 as a block, since each follows mechanically from S1–S15. Any row a reviewer disputes becomes that milestone's decision | M4 review |
| Q7 | **Is `origin_client_id` a legitimate fifth reference kind (§3.1.3.6), or a snapshot of an untrusted hint?** | A fifth kind. Calling it a snapshot implies a source of truth it does not have, and calling it a claim implies verification that never happened. It is a token the client invented, compared only for equality, and always scoped under a verified `user_id` | **M4 review** |

Decided and recorded rather than open: `identity` is not split into `auth` and
`user-profile` (they share the `users` aggregate); one Postgres instance with
eight logical databases is a real boundary (separate roles make a cross-database
query fail at connection time); `assistant` is not split into retrieval and
generation ([RFC 0004](0004-retrieval-layer.md) D12); a projection that
disagrees with its source loses and is rebuilt; categories get no shared
reference-data service; multi-location inventory changes `stock_items`' unique
key, not the decomposition.

---

## 4. Dependencies/blockers/known issues

**Dependencies.** None of its own. It constrains [RFC 0001](0001-foundation-and-core-services.md)–[RFC 0005](0005-ai-assistant-and-streaming.md) and is constrained by [Part A](../../part-a.md)'s named stack (PostgreSQL, a NoSQL store, Redis) and the five-week schedule.

**Blockers.** None.

**Known issues.**

- Eight services is more than a five-week solo project would choose on delivery grounds; the learning objective is what justifies it (Q5).
- No cross-service referential integrity; orphan references are mitigated, not prevented.
- One Postgres instance is a shared local blast radius — an operational choice, reversible by connection string.
- Snapshots mean a corrected product name never reaches historical orders. Intended, and occasionally surprising.
- 39 rows carry an inferred type and 11 are columns no RFC names (§3.1.3.8). Every one is a proposal until M4 ratifies it.
- Schema conventions are enforced by review and by the tests in §7, not by a linter. A linter for S1–S15 would be a genuine improvement and is out of scope.

## 5. Alternate solutions

| # | Alternative | Why rejected |
| --- | --- | --- |
| 1 | **Modular monolith** with enforced internal boundaries | The strongest alternative: it would ship faster, run on less, debug more easily, and model the domain just as well. Rejected because the stated objective is distributed-systems experience, the event backbone becomes ceremony when every module can call every other in-process, and the split is deferred forever once `order` can simply join `catalog`. The cost of boundaries is lowest in Week 1 and rises every week after |
| 2 | **One service per noun** (15+: payment, shipment, cart, review, search, media, pricing, merchant, audit…) | This was the prototype. Fails ownership tests 1, 2 and 4 for most candidates, multiplies operational cost with no isolation gain, and makes it impossible for a reviewer to tell a reasoned boundary from a guessed one |
| 3 | **Split by capability** (order-management, fulfilment, merchandising…) | Sounds closer to the business and produces shared writes: fulfilment and order-management both write order status. Every overlap becomes a shared table or a distributed transaction |
| 4 | **Shared database, schema per service** | Makes the boundary advisory, and an advisory boundary does not survive the first tight deadline. It removes the one property that makes R1 real — a cross-service query failing at connection time |
| 5 | **Event sourcing** as the primary persistence model | A genuinely good fit for orders and inventory and the natural home for the audit requirement, but a large conceptual cost on a five-week schedule, and it makes every simple read harder. The outbox gives most of the integration benefit at a fraction of the cost |
| 6 | **A shared canonical data model** with a shared ORM library | Couples every service's deploy to every schema change and reintroduces the shared-model problem separate databases exist to remove. Services share an *event* contract and an *API* contract, never a persistence model (R8) |
| 7 | **Keep native Postgres enums** (the original S4) | Not rejected for the usual reason (that growing a vocabulary costs a migration), which would not on its own overturn S11. Rejected because `ALTER TYPE … ADD VALUE` is irreversible and cannot satisfy S14's mandatory `downgrade` — the two rules cannot both be kept. Its type safety was also shallower than it appeared: it validated the label and never the transition (§3.2) |
| 8 | **`text` with `CHECK (status IN (…))`** | *Rejected for now, retained as the escape hatch.* Still needs a migration per value, so it fails the goal outright. But dropping and re-adding a `CHECK` is trivially reversible, so it satisfies S14 — making it the correct fallback **if a non-Python writer to a service database is ever introduced** |
| 9 | **A lookup table with a foreign key** per vocabulary | The closest call. It delivers both properties — adding a value is an `INSERT`, and the database still enforces validity. Costs a table and a join for every one of seventeen vocabularies, and S15 forbids a shared one, so each must be duplicated per database and kept in step by hand. That is a standing consistency problem introduced to defend against a writer R1 says does not exist. It is the right answer for a vocabulary that is genuinely *data* — merchant-editable, labelled, ordered, translated — and should be reached for the first time one appears. None of the seventeen is one |
| 10 | **Leave the schema distributed across the milestone RFCs** | The status quo this consolidation replaced. Each milestone RFC read well alone, but `products` was defined in three places, `outbox` in one service's section and inherited by four others by a sentence, and four required tables were missing entirely because no document owned the whole picture (§3.1.3.2) |

## 6. Production readiness

### 6.1. Scalability and Fault tolerance

- **Independent scaling.** Each service scales on its own load profile — `catalog` on read volume, `inventory` on write contention, `assistant` on provider concurrency. A shared schema would force them to scale together.
- **Contained failure.** The ownership map determines blast radius directly: `analytics` and `notification` are pure consumers, so their failure is invisible to checkout. `catalog` is a live-lookup dependency of order creation (R5, Q2) — the one coupling the design accepts and states.
- **Independent schema evolution.** Per-service migration histories (S14) mean one service's migration never blocks another's deploy or rollback.
- **Rebuildable projections.** Every projection — chunks, analytics rollups, catalog's merchant state — is reconstructible from events or a backfill. No derived store is ever the only copy of anything.
- **Constraints as the last line of defence.** S11's database constraints hold under concurrency where application checks race: `on_hand >= reserved` and the idempotency uniqueness are correctness guarantees, not validations. S4's enum columns are the deliberate exception, and the distinction is the point — a value's spelling has no race to lose.
- **Known ceiling.** One Postgres instance is a shared local blast radius. Splitting it is a connection-string change, deliberately.

### 6.2. Security considerations

- **The boundary is a security boundary.** R1's separate roles and grants mean a compromise of one service cannot read another's data — `catalog` cannot read password hashes, `assistant` cannot read orders. The strongest security property the decomposition provides, and it comes from the same rule as the architectural one.
- **Identity is the narrowest boundary.** Only `identity` holds credentials and the private signing key; every other service verifies with a public key. No shared secret whose leak would forge identity.
- **Data minimisation across boundaries.** Snapshots carry only what the consumer needs. `order_items` holds a product name and price, never merchant credentials; event payloads carry ids, never hashes or card data.
- **Claims are the only trusted cross-service assertion.** Ownership checks use the verified token's `merchant_id`, never a request body or query parameter. If broken once, this silently defeats tenant isolation.
- **Audit locality (S7)** means a trail cannot be missing for a change that happened.
- **Soft delete (S5)** preserves referential meaning for snapshots and projections that no foreign key protects.

### 6.3. Deployment strategy

- **One logical database and one role per service**, created by the Postgres init script with grants on its own database only. This is the mechanism behind R1 and G7.
- **Per-service Alembic histories** (S14), run as an explicit step, never on startup, with a working `downgrade` on every migration. S4 is what makes that last clause achievable.
- **Independent deployability is the test**: any single service can be rebuilt, migrated and restarted without touching another. Where that is not true, the coupling is a defect.
- **Additive-first migrations.** Expand, migrate, contract — even with no production data, because the habit is what makes the boundaries hold.
- **The shared instance is configuration.** Moving a service to its own Postgres is a connection string, because nothing in any schema depends on co-location.

## 7. Test Plan

This RFC's correctness is tested by what the codebase *cannot* do. These run at every milestone, cumulatively.

**Boundary tests**

1. **No cross-database grant exists.** A test connects as each service's role and asserts every other service's database is unreachable. This is G7 and the single most important test in the project — it makes R1 enforced rather than believed.
2. **No cross-service foreign key exists.** Query `information_schema` in each database; assert every FK's target is a table in the same database.
3. **No service's configuration contains another service's connection string.** Static check over settings and environment.
4. **One writer per fact.** For each row in the ownership map (§3.1.3.7), assert only the owning service's code writes that table — a static check over ORM model imports.

**Convention tests** — static checks over the schema, so S1–S15 are enforced by CI rather than by memory:

| Check | Asserts |
| --- | --- |
| Primary keys | `uuid` everywhere except the S1 exception list |
| Money columns | `numeric(12,2)`, never `float`/`double`; each has an adjacent currency column |
| Timestamps | `timestamptz`, never `timestamp`; `created_at` on every table; `updated_at` on every mutable one |
| Nullability | Every nullable column appears in a documented allow-list (S10) |
| Naming | `snake_case`, plural tables, `<entity>_id` references |
| Outbox and dedupe | Every producer in §3.1.3.2's matrix has an identical `outbox`; every consumer has `processed_events`. **This test is what would have caught the four `‡` tables** |
| Migrations | Every Alembic revision has a non-trivial `downgrade` |
| Enums (S4) | No column's type is `sa.Enum`; no `CHECK` constraint names a status value; `pg_type` holds no enum type in any service database |
| Enum round trip (S4) | Per service: discover every `DomainEnum` subclass in `app/domain/enums.py`, and for every member of every `DomainEnumText` column, write it to real Postgres and read it back. Adding a value extends this test with no edit |
| Enum write strictness (S4) | An unknown value is rejected on write and leaves no row behind. The error arrives as `sqlalchemy.exc.StatementError` with `UnknownEnumValueError` on `.orig`, because `process_bind_param` runs inside the flush |
| Enum read tolerance (S4) | A value written by a newer deploy is readable by an older one — the rollback case. Write an unknown value through raw SQL, read it through the ORM |
| `libs/common` vocabulary (R8, S4) | A grep of `libs/common` for any domain status literal returns nothing — the library holds the mechanism, services hold the values |
| **Catalogue completeness** | Every table in a service's Alembic head appears in §3.1.3.4's index, and vice versa. The index's `Cols` count matches the model's column count. This is what keeps this document from drifting from the code it specifies |

**Model tests**

- Snapshot immutability: changing a product's name and price leaves every existing `order_item` unchanged.
- Projection convergence: after a source change and event propagation the projection matches; after a deliberate divergence, a rebuild restores it.
- Soft delete: archiving a product leaves order history intact and readable.
- Constraint enforcement: each S11 invariant is tested by attempting to violate it **concurrently**, not just sequentially.
- Reference kinds: for each dashed edge in §3.1.3.3, assert the declared consistency behaviour — a snapshot does not change, a projection converges, a live lookup reflects the current value immediately.
- State machine totality (S4): the transition table's keys are exactly the members of its `DomainEnum`, every target is a known state, every state is reachable from the initial one, and the terminal set is the expected one.
- Illegal transitions (S4): every pair *not* in the transition table is parametrised and asserted to raise, with the entity unmutated. For `orders` that is 67 named assertions — the writes a native Postgres enum always accepted.

**Review checks** — at Milestones 4 and 7, by hand, because no test covers them:

- Does every service still pass all four ownership tests?
- Has any service acquired a second writer for a fact it does not own?
- Does `libs/common` contain a business rule (R8)? For S4 this is mechanical: a grep of the package for any status literal must return nothing, and that grep is itself a unit test.
- Has any router acquired direct ORM access, or any service-layer function an HTTP concern?
- Has any new cross-boundary reference been added without declaring its kind?
- **Has every `†` and `‡` cell in §3.1.3.8 been ratified or re-decided?**

## 8. Impact Analysis

**On every milestone.** This document is the constraint each milestone RFC is checked against (§3.1.5), and now also the schema each one builds. Boundary and schema questions are answered once, in advance, rather than re-litigated in Week 2 while two services are being built.

**On the codebase.** The conventions are why eight schemas written over five weeks read as one system. The layering rule (§3.1.1) is why a business rule has one implementation reachable by four callers.

**On the RFC set.** A column now has exactly one definition. RFCs 0001–0005 name the tables their milestone creates and link here, so a type change is one edit rather than two or three that can disagree.

**On the review.** Milestones 4 and 7 examine architecture and design quality directly. This document gives the reviewer the method, the applied results including rejections, the reference-kind taxonomy, the conventions, the complete schema, and — in §3.1.3.8 — an explicit list of what this consolidation proposed rather than transcribed.

**If a premise changed.**

| If | Then |
| --- | --- |
| A real payment provider is introduced | `payment` becomes a service. The interface boundary makes it an extraction, not a rewrite |
| Multi-warehouse inventory is required | `stock_items` gains `location_id` in its unique key. No boundary moves |
| The corpus grows by two orders of magnitude | `assistant`'s vector store moves behind the `VectorStore` protocol to a dedicated engine. No boundary moves |
| A non-Python writer to a service database appears | S4 is revisited; the replacement is `CHECK (status IN (…))` (§5, alternative 8) |
| Reviews or a cart are required | New services, each tested against the same four criteria |
| The project became a team of four | The boundaries already support it — four owners, eight services, no shared schema |

**Risks.**

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| Boundary erosion under deadline pressure — a cross-service query "just this once" | Medium | **High** | R1's grants make it fail at connection time; the boundary test is in CI |
| A second writer appearing for an owned fact | Medium | High | The ownership map is explicit; the static check in §7 catches it |
| **This document drifting from the code it specifies** | **High** | **High** | The catalogue-completeness test in §7 compares §3.1.3.4 against each service's Alembic head in CI. Without it, one file holding all the schema is a worse failure mode than several |
| Conventions drifting across eight schemas | High | Medium | The convention tests are automated; drift is a CI failure, not a review observation |
| An inferred `†` type turning out wrong in implementation | Medium | Low | Each is marked and listed (§3.1.3.8); the cost is one migration in the milestone that hits it |
| The eight-service count being judged excessive | Medium | Low | The method and every rejection are recorded, so the disagreement is specific and answerable (Q1, Q5) |
| `libs/common` accumulating domain logic | Medium | Medium | R8, checked at every milestone review |
| The layering seam eroding, making rules unreachable from activities and consumers | Medium | High | Reviewed at each milestone; highest cost of erosion, lowest visibility |

## 9. Useful Links/References

**Project documents**

- [RFC 0000 — System Architecture](0000-system-architecture.md) — the companion document
- [RFC 0001](0001-foundation-and-core-services.md) · [RFC 0002](0002-core-business-logic-and-workflows.md) · [RFC 0003](0003-events-and-observability.md) · [RFC 0004](0004-retrieval-layer.md) · [RFC 0005](0005-ai-assistant-and-streaming.md)
- [Part A](../../part-a.md) · [Part B](../../part-b.md) · [Milestones](../../milestones.md)

**Technical references**

- Evans, *Domain-Driven Design* — bounded contexts, aggregates, context mapping
- Vernon, *Implementing Domain-Driven Design* — aggregate design rules
- Newman, *Building Microservices*, 2nd ed. — decomposition and the database-per-service pattern
- Kleppmann, *Designing Data-Intensive Applications* — chapters 11–12, derived data and stream processing
- Database per service — https://microservices.io/patterns/data/database-per-service.html
- Shared database anti-pattern — https://microservices.io/patterns/data/shared-database.html
- Transactional outbox — https://microservices.io/patterns/data/transactional-outbox.html
- Aggregate design rules (Vernon) — https://www.dddcommunity.org/library/vernon_2011/
- PostgreSQL numeric types — https://www.postgresql.org/docs/16/datatype-numeric.html
- PostgreSQL `uuid` and `gen_random_uuid()` — https://www.postgresql.org/docs/16/functions-uuid.html
- pgvector HNSW indexing — https://github.com/pgvector/pgvector#hnsw

## 10. Related tickets/issues

| ID | Title | Maps to |
| --- | --- | --- |
| — | RFC 0006 — Service Decomposition & Data Model | This document |
| — | RFC 0000 §3.1.1 | Service inventory — summarised there, derived here |
| — | RFC 0000 §3.1.3 | Ownership map — summarised there, authoritative here (R3) |
| — | RFCs 0001–0005 §3.1.3 | Table lists per milestone; columns here |
| — | Milestone 4 — Part A Review | Checks G7, the §7 review questions, and the §3.1.3.8 census |
| — | Milestone 7 — Part B Review | Same, for `assistant` |

## 11. Notes

- **On why this document holds the schema.** The service count is the first question anyone asks about this project, and the conventions are what keep eight databases coherent. Both were spread across six RFCs, which meant the reasoning could not be reviewed as a whole and the tables could not be read as a set. Consolidating them is what surfaced the four missing `‡` tables and the ten under-specified ones — none of which was visible while each milestone RFC was internally consistent.
- **On the rejections being the substance.** The accepted services are the easy half. The eight rejected candidates in §3.1.1 are where the decomposition was actually decided, and `payment` is the one most likely to be challenged.
- **On conventions as a force multiplier.** Fifteen rules that take an hour to agree are what let eight schemas written across five weeks look like one system. The cost of not agreeing them shows up in Week 3, as three timestamp conventions and two ways of representing money.
- **On the honest position.** Eight services is more than a five-week solo project would choose on delivery grounds. That is a real tension, stated rather than hidden (Q5).
- **On what this document is not.** It is a specification, not an implementation record. If a table here disagrees with a service's Alembic head, the head is right and this is stale — and the catalogue-completeness test in §7 exists to make that state short-lived.
- **Amendment log.** Changes to the decomposition or the conventions are recorded here with a date and a reason, rather than edited silently.

  | Date | Section | Change | Reason |
  | --- | --- | --- | --- |
  | Sep 27, 2026 | §3.1.3 | Consolidated the complete column-level schema from RFCs 0001–0005 into §3.1.3.5; RFCs 0001–0005 now link here | One definition per column. `products` had been defined in three places |
  | Sep 27, 2026 | §3.1.3.2 | Added `identity_db.outbox`, `assistant_db.outbox`, `catalog_db.processed_events`, `inventory_db.processed_events` | Required by S12/S13 and the event catalogue; mandated generically by RFC 0003 but never enumerated per database (Q3) |
  | Sep 27, 2026 | §3.1.3.5 | Specified `prompt_templates` for the first time; 39 rows given inferred types | The source RFCs wrote ten tables as prose sentences rather than column tables (§3.1.3.8) |
  | Sep 27, 2026 | §2.2 | Removed "column-level DDL" from the non-goals | It is now the point of the document |

- **Definition of done for this RFC.** Goals G1–G7 satisfied; the boundary and convention tests in §7 implemented and green in CI from Milestone 1; the §3.1.3.8 census ratified at Milestone 4; no milestone RFC contradicting it without saying so; reviewed and moved to **DONE** alongside [RFC 0000](0000-system-architecture.md) before Milestone 2 begins.

## 12. Comments

*Reviewer comments go here. Please raise objections against a specific row: the ownership test in §3.1.1, a candidate in its table (particularly the rejections), a boundary rule R1–R8, a schema convention S1–S15, a column in §3.1.3.5, a `†` or `‡` cell in §3.1.3.8, or an open question Q1–Q6 — so the discussion attaches to something specific.*

| Reviewer | Section | Comment | Resolution |
| --- | --- | --- | --- |
| | | | |
