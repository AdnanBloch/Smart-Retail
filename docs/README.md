# Documentation

Two kinds of document live here: the **assignment specification as supplied**,
which defines what SmartRetail is and how the work should proceed, and the
**RFCs we write**, which propose a design before it is built and record why
each decision was made.

The implementation is being restarted from zero. Nothing is built until the
RFC covering it has been reviewed.

## RFCs

The RFC set is organised by **architecture and service**, not by milestone.
One RFC fixes everything that must hold across the platform. One covers the shared
infrastructure. Each of the eight services then has an RFC of its own that
covers every aspect of it: responsibility and boundaries, domain model, full
schema, state machines, API and role matrix, events, workflows, dependencies,
configuration, observability, security, and tests. Read 0000 first.

| # | RFC | Covers | Status |
| --- | --- | --- | --- |
| 0000 | [System Architecture](rfcs/0000-architecture.md) | Whole platform: service inventory and the ownership test, boundary rules R1–R8, decisions A1–A14, critical flows, API conventions, event catalogue and envelope, schema conventions S1–S15, shared `outbox`/`processed_events` tables, table index, ownership map | **DRAFT** |
| 0001 | [Platform & Shared Infrastructure](rfcs/0001-platform-and-shared-infrastructure.md) | `common`, `outbox-relay`, `temporal-worker`, `celery-worker`, `indexer-worker` runtime, Kafka + Schema Registry, RabbitMQ, Redis, MinIO, Temporal, the observability stack, Compose, Makefile | **DRAFT** |
| 0002 | [`gateway` Service](rfcs/0002-gateway-service.md) | Routing, early JWT rejection, correlation IDs, CORS, rate limiting, SSE proxying, aggregated docs | **DRAFT** |
| 0003 | [`identity` Service](rfcs/0003-identity-service.md) | Users, credentials, roles, merchant profiles, tokens and sessions, JWKS | **DRAFT** |
| 0004 | [`catalog` Service](rfcs/0004-catalog-service.md) | Products, variants, categories, media, audit trail, the product publishing workflow | **DRAFT** |
| 0005 | [`order` Service](rfcs/0005-order-service.md) | Orders, line snapshots, idempotency, payment and shipment sub-state, the order saga | **DRAFT** |
| 0006 | [`inventory` Service](rfcs/0006-inventory-service.md) | Stock levels, reservations, movement ledger, contention handling | **DRAFT** |
| 0007 | [`notification` Service](rfcs/0007-notification-service.md) | Notification records, delivery, SSE stream and per-device cursors | **DRAFT** |
| 0008 | [`analytics` Service](rfcs/0008-analytics-service.md) | Event-derived rollups and metric series in MongoDB | **DRAFT** |
| 0009 | [`assistant` Service](rfcs/0009-assistant-service.md) | Retrieval (chunking, embeddings, hybrid search) and generation (agent, merchant content, streaming, budgets) | **DRAFT** |

**Why this split.** A reader, reviewer or implementer working on a service
should find everything about it in one document. Under the earlier
milestone-based split, a service's design was scattered across three or four
RFCs according to the week each piece was built. Now anything that must be
consistent across services lives in 0000 and nowhere else. Anything specific to
one service lives in that service's RFC. Each table's columns are defined exactly
once, in the RFC of the service that owns the table. RFC 0000's
[table index](rfcs/0000-architecture.md#3134-table-index) links to every one.

**Milestones still set the schedule.** [milestones.md](milestones.md) is
unchanged. Each RFC ends its §3.1.5 Action Plan with a short *Delivery phasing*
table, which maps its components to the milestone that builds them. That table is
the only place milestones appear. The Part A and Part B reviews (Milestones 4 and 7)
review the RFCs and code of the services delivered by then.

**Archive.** The earlier milestone-based RFCs (old 0000–0006) are kept, unchanged
apart from a SUPERSEDED banner, in [rfcs/archive/](rfcs/archive/). Decision IDs
from them (A*, B*, C*, Q*, …) are preserved in the new RFCs so they remain
traceable.

RFCs follow [the supplied template](RFC%20template.pdf).
An RFC moves DRAFT → IN-REVIEW → DONE. It is merged into `DONE` only once the
service it covers is built and reviewed.

## Reviews

| Date | Record | Outcome |
| --- | --- | --- |
| 2026-10-06 | [Mentor review](reviews/mentor-reviews.md#review-of-2026-10-06) | `common/` package, entity-first layout with versioned API folders, a decision register in every RFC, Mermaid diagrams, coverage gate, pre-commit and CI |
| 2026-10-12 | [Mentor review](reviews/mentor-reviews.md#review-of-2026-10-12) | Why FastAPI and the rest of the stack (ARC-D28–ARC-D35); everything an entity uses under its folder; review-changed decisions traced in every RFC; diagram index per service |
| 2026-10-13 | [Mentor review](reviews/mentor-reviews.md#review-of-2026-10-13) · [full record](reviews/2026-10-13-mentor-review.md) | Version-first layout (`app/v1/<entity>/`, ARC-D23 changed); Kafka partitioning, commit, retry and dedupe explained and built; outbox relay, inventory consumer and Celery in Milestone 3; why Kafka, RabbitMQ/Celery and Temporal each exist; end-to-end business flows; one diagrams page |

## Cross-cutting guides

These pages gather what the RFCs fix across several documents, for reading in
one sitting.

| Page | Answers |
| --- | --- |
| [messaging-and-consistency.md](messaging-and-consistency.md) | Kafka partitioning, offset commit and retry, duplicates, atomicity, how each queue works, why Kafka, RabbitMQ/Celery and Temporal each exist |
| [business-flows.md](business-flows.md) | Every business flow end to end, with failure and compensation paths |
| [architecture-diagrams.md](architecture-diagrams.md) | The top-level service-communication diagram and one component diagram per service |

## Repository structure

[file-structure.md](file-structure.md) maps every folder in the repository,
including the single layout that all services follow.

## The specification, as supplied

### Reading order

| # | Document | What it covers |
| --- | --- | --- |
| 1 | [Learning Path](learning-path.md) | Why the assignment exists — focus areas, expected outcomes, the system-thinking goal |
| 2 | [Milestones](milestones.md) | The seven milestones, their scope and expected outcomes, with a tracking table |
| 3 | [Execution Guidelines](execution-guidelines.md) | How to work — weekly cycle, mentor reviews, submission rules, evaluation criteria |
| 4 | [Part A](part-a.md) | What to build first: commerce core, distributed systems, observability |
| 5 | [Part B](part-b.md) | What to build second: GenAI and the intelligent commerce layer |

Start with the Learning Path for context, then Milestones for the schedule.
Part A and Part B are the actual requirements and are the documents to return
to while building.

### Structure

- **Parts A and B** are product specifications: business goals, functional
  requirements, analytics, observability expectations, tech stack.
- **Milestones and Execution Guidelines** are process documents: what is due
  when, and how it will be assessed.
- **Learning Path** frames both.

### Editorial note

The milestone list arrived with LMS wording (courses, student/instructor) that
did not match SmartRetail's domain. It is read as products and
customer/merchant throughout; the mapping is stated at the top of
[milestones.md](milestones.md) and again in
[RFC 0000](rfcs/0000-architecture.md) §11. The original wording is preserved
verbatim in the archived milestone RFCs, so the reading can be checked rather than
taken on trust.
