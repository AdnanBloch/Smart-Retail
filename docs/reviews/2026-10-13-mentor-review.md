# Mentor review of 2026-10-13

This file records the mentor's review of 2026-10-13 as it was given. It covers
two topics:

- **API versioning.** This point changed code: every service now uses a
  version-first layout. The change reverses ARC-D23 as it stood after the
  review of 2026-10-06.
- **Messaging and business logic.** The mentor asked that these be understood
  and defended. The answers are written up in the section on this review in
  [mentor-reviews.md](mentor-reviews.md#review-of-2026-10-13) (§1–§8), and
  this file links to them.

## The review, as given

> Versioning: make it the top level, before the entity. In an upcoming vX we
> make only the files that may change; business logic may also change in each
> version. In an upcoming version, the main files contain only the URLs and the
> views (the handlers, as we call them), and we reference a few files from an
> older version, or from v1, explicitly — models, for example.

> Have to understand: Temporal; Kafka partitioning — which method we use; the
> commit solution; retry, on which basis; atomicity; the business logic;
> RabbitMQ, Celery workers and Kafka — why use all of them when we have one.

## Points and where each lands

| # | Mentor's request | Where it lands |
| --- | --- | --- |
| 1 | Version folder at the top level, before the entity. A later version holds only the files that change, and imports the rest from v1 | [§1](#1-version-first-layout) · [RFC 0000 ARC-D23, §3.1.1.7](../rfcs/0000-architecture.md#3117-module-structure-inside-a-service) · [file-structure.md §4](../file-structure.md#4-service-layout-every-service) |
| 2 | Business logic may differ between versions | [§1](#1-version-first-layout): a later version may hold its own `repo.py` |
| 3 | Understand Temporal | [§2](#2-messaging-and-business-logic) · [messaging §4.2](../messaging-and-consistency.md#42-across-services-one-business-operation), [§5.3](../messaging-and-consistency.md#53-temporal-task-queues-are-a-durable-process) |
| 4 | Kafka partitioning: which method | [mentor-reviews §1](mentor-reviews.md#1-kafka-partitioning-method) · [messaging §1](../messaging-and-consistency.md#1-partitioning) |
| 5 | Offset commit solution | [mentor-reviews §2](mentor-reviews.md#2-offset-commit-and-retry-basis) · [messaging §2](../messaging-and-consistency.md#2-offset-commit-and-retry) |
| 6 | Retry: on which basis | [mentor-reviews §2](mentor-reviews.md#2-offset-commit-and-retry-basis) · [messaging §2](../messaging-and-consistency.md#2-offset-commit-and-retry) |
| 7 | Atomicity | [mentor-reviews §4](mentor-reviews.md#4-atomicity-across-database-broker-and-services) · [messaging §4](../messaging-and-consistency.md#4-atomicity) |
| 8 | The complete business logic | [mentor-reviews §5](mentor-reviews.md#5-complete-business-logic) · [business-flows.md](../business-flows.md) |
| 9 | Why RabbitMQ, Celery and Kafka together, when one tool exists | [mentor-reviews §7](mentor-reviews.md#7-why-kafka-rabbitmq-celery-and-temporal), [§8](mentor-reviews.md#8-queue-methodology) · [messaging §5–§6](../messaging-and-consistency.md#5-how-each-queue-works) |

## 1. Version-first layout

**What was missing.** The layout chosen after the review of 2026-10-06
(ARC-D23) put the version *inside* each entity, and versioned only the HTTP
contract:

```
app/orders/models.py, repo.py, workflows/, …   not versioned
app/orders/v1/routes.py, schemas.py            versioned
```

That layout has no place for a business rule that differs between versions.
A v2 rule would need a second repo beside the first, inside an unversioned
folder.

**Options considered**

| Option | For | Against |
| --- | --- | --- |
| Entity-first, version inside the entity (previous ARC-D23) | Everything about an entity sits in one folder | No place for a per-version business rule. A v2 rule needs a second repo beside the first |
| **Version-first; a later version holds only what it changes and imports the rest from v1 by name (chosen)** | Each version is one folder. A version's differences, rules included, are exactly the files in it. Nothing is copied | v1 becomes the permanent base: to read v2, read v2, then fall back to v1 |
| Version-first with a full copy per version | Each version stands alone | Models and rules are duplicated and drift apart. This is why version-first was rejected on 2026-10-06 |

**Why the decision changed.** On 2026-10-06 version-first was rejected because
"v2 would copy the ORM models and business rules". The mentor's form copies
nothing: a later version imports what it keeps and overrides only what it
changes. ARC-D23's own "revisit when" column also named this exact case: *"Two
API versions need different business rules for the same entity."*

**What the layout is now.** In identity, catalog, order, inventory,
notification, analytics and assistant:

```
app/
  router.py          every URL in one file: mounts /api/v1 (and a later /api/v2)
  models.py          imports app.v1.<entity>.models onto one metadata
  core/  health/     not versioned
  v1/                the complete base version
    <entity>/        models, repo, enums, state_machine, workflows, consumers, tasks,
                     clients, providers … and routes.py + schemas.py (the handlers)
tests/{unit,integration}/v1/<entity>/
```

A later version is a folder that holds only the files it changes:

```
app/v2/orders/routes.py     # the handlers and URLs of v2
app/v2/orders/schemas.py    # the v2 contract
app/v2/orders/repo.py       # only if v2's business rules differ
```

```python
# app/v2/orders/routes.py
from app.v1.orders.models import Order      # one table, one model: always from v1
from app.v1.orders.repo import OrderRepo    # or app.v2.orders.repo when the rules change
```

**Rules that come with it**

| Question | Rule |
| --- | --- |
| Where do ORM models live? | Only in `app/v1/<entity>/models.py`. A table has one shape at a time, and Alembic reads them through `app/models.py` |
| Which version do Temporal activities, Kafka consumers, Celery tasks and CLIs call? | They have no HTTP version. They live in `v1/<entity>/` and call v1's repos. If a later version changes a rule they apply, the RFC row for that version says which repo they call |
| How are workers started? | By their module path: `python -m app.v1.products.workflows.worker`, `app.v1.orders.workflows.worker`, `app.v1.metrics.consumers`, `app.v1.search.consumers.worker` |
| How is v1 retired? | A later version can never import from a deleted version. The files it still imports first move to `core/` or into the next base version |
| What about the gateway? | Unchanged. It has no versioned contract of its own (`core/`, `docs/`, `proxy/`, `health/`) |

**What changed in the code**

- Every entity folder moved to `app/v1/<entity>/`. Each old
  `<entity>/v1/{routes,schemas}.py` moved to `app/v1/<entity>/{routes,schemas}.py`,
  because the version is now the outer folder.
- Imports, Celery `include=` lists, the worker commands in `docker-compose.yml`
  and the `Makefile`, and the analytics coverage omits were updated to match.
- Tests now mirror the new layout: `tests/{unit,integration}/v1/<entity>/`.
- Every Celery task and Temporal workflow or activity already carried an
  explicit `name=`. No name stored by RabbitMQ or Temporal changed, so running
  workflows and queued tasks are unaffected.
- ORM column comments that name an enum file now point at `app/v1/…`. Each
  affected database has a comment-only migration:
  - catalog `0006_enum_comment_paths_v1`
  - inventory `0004_enum_comment_paths_v1`
  - order `0005_enum_comment_paths_v1`
  - notification `0004_enum_comment_paths_v1`
- Earlier migrations are history and keep the paths they were written with.

Binding: [RFC 0000](../rfcs/0000-architecture.md) ARC-D23 and §3.1.1.7;
[file-structure.md](../file-structure.md) §4.

## 2. Messaging and business logic

Each point already had a written answer. The table above links to it. These are
the short answers to give in discussion:

| Topic | Answer |
| --- | --- |
| Temporal | It owns multi-step processes: product publishing and the order saga. It stores each workflow's event history, so a crashed workflow resumes at the same step on another worker. Each activity has its own retry policy and timeouts. Business failures are non-retryable and go straight to compensation, which runs in reverse |
| Partitioning | Key-hash partitioning, with key = aggregate id (`murmur2(key) mod 3`). Every event for one order or product lands on one partition, in order. Different entities are processed in parallel |
| Commit | Auto-commit is off. The offset is committed manually, after the effect's database transaction commits or after the message goes to the DLQ. A crash can only cause a redelivery, and `processed_events` dedupe absorbs it |
| Retry basis | Depends on the failure. A poison message goes to the DLQ at once. If a dependency is down, the partition pauses and the attempt is not counted. Any other error gets 3 attempts with backoff, then goes to the DLQ |
| Atomicity | Inside one service: the transactional outbox, so the state change and its event are one local transaction. Across services: a Temporal saga with compensations. An order committed without a started workflow is picked up by `resume_unstarted_orders` |
| Why three tools | Kafka is a log for facts: fan-out and replay, and the producer doesn't know its consumers. RabbitMQ + Celery is a work queue for single and scheduled jobs: per-task ack and retry, plus Beat. Temporal runs durable processes that must complete or compensate. Each tool does the other two's jobs badly |
