# Architecture diagrams

This page puts every architecture diagram in one place, answering
[mentor review of 2026-10-13 §6](reviews/mentor-reviews.md#review-of-2026-10-13):

1. a **top-level** diagram of how the services talk to each other, with the
   protocol on every edge;
2. one **internal component** diagram per service, showing how the parts inside
   a service call each other.

Every diagram is drawn from the code: the boxes are real modules under
`services/<name>/app/`, for all eight services.

Each service RFC keeps its own diagram index (§3.1) with its sequence, state and
data diagrams. The business flows that run across these components are in
[business-flows.md](business-flows.md). The messaging mechanics are in
[messaging-and-consistency.md](messaging-and-consistency.md).

**Legend for edge labels:**

| Label | Meaning |
| --- | --- |
| `HTTP` | Synchronous REST/JSON over HTTP |
| `SSE` | Server-sent events stream |
| `gRPC` | Temporal client and worker protocol |
| `Kafka` | An event through the outbox and relay, asynchronous, key = aggregate id |
| `AMQP` | A Celery task through RabbitMQ |
| `SQL` | asyncpg to the service's own database only |
| `S3` | Object storage API to MinIO |

---

## 1. Top level: how the services communicate

```mermaid
flowchart TB
    client(["Browser / API client<br/>customer · merchant · admin"])

    subgraph edge["Edge"]
        gw["gateway<br/>JWT check · routing · rate limit · CORS"]
    end

    subgraph services["Services (FastAPI, one database each)"]
        idn["identity"]
        cat["catalog"]
        ord["order"]
        inv["inventory"]
        ntf["notification"]
        anl["analytics"]
        ast["assistant"]
    end

    subgraph workers["Workers"]
        cw["catalog-worker<br/>(Temporal activities)"]
        ow["order-worker<br/>(Temporal activities)"]
        relay["outbox-relay"]
        icons["inventory-consumer"]
        icel["inventory-celery<br/>(+ Beat)"]
        ocel["order-celery<br/>(+ Beat)"]
    end

    subgraph infra["Infrastructure"]
        pg[("PostgreSQL<br/>identity_db · catalog_db · order_db · inventory_db …")]
        tmp["Temporal"]
        kafka[["Kafka + Schema Registry"]]
        rmq[["RabbitMQ"]]
        minio[("MinIO")]
        mongo[("MongoDB")]
        redis[("Redis")]
    end

    ext["Simulated payment and shipping providers"]

    client -- "HTTP / SSE" --> gw
    gw -- "HTTP" --> idn & cat & ord & inv & ast
    gw -- "SSE" --> ntf
    gw -. "JWKS (HTTP, cached)" .-> idn

    ord -- "HTTP: price snapshot" --> cat
    ast -- "HTTP: live price and stock" --> cat & inv

    cat -- "gRPC: start ProductPublishingWorkflow" --> tmp
    ord -- "gRPC: start OrderWorkflow, signal cancel" --> tmp
    tmp -- "gRPC: task queue catalog-publishing" --> cw
    tmp -- "gRPC: task queue order-lifecycle" --> ow
    cw -- "HTTP: initialise stock (service token)" --> inv
    ow -- "HTTP: reserve / commit / release (service token)" --> inv
    ow -- "HTTP" --> ext
    cw -- "S3" --> minio
    cw & ow -. "HTTP: client credentials token" .-> idn

    idn & cat & ord & inv -- "SQL (own DB)" --> pg
    relay -- "SQL: poll outbox" --> pg
    relay -- "Kafka: produce" --> kafka
    kafka -- "Kafka: catalog.product.published, order.cancelled" --> icons
    kafka -- "Kafka" --> ntf & anl & ast
    icons -- "SQL" --> pg

    icel & ocel -- "AMQP" --> rmq
    icel -- "SQL" --> pg
    ocel -- "gRPC: resume unstarted orders" --> tmp

    anl -- "MongoDB" --> mongo
    gw & ntf & ast -- "Redis" --> redis
```

The two rules this diagram shows:

- **Synchronous calls are few and point one way.** The gateway calls a service.
  Order asks catalog for a price snapshot. Workers call inventory with a service
  token. No service calls back up that chain, so there is no request cycle.
- **Everything else is asynchronous.** Processes go through Temporal (gRPC).
  Facts go through Kafka, always out of a service's own outbox. One-shot jobs go
  through RabbitMQ.

No service reads another service's database. Each role can only `CONNECT` to its
own database (rule R1).

### 1.1 What runs per Compose profile

```mermaid
flowchart LR
    subgraph core["profile: core"]
        a1["postgres · gateway · identity · catalog · inventory · order · minio"]
    end
    subgraph workflow["profile: workflow"]
        a2["temporal · temporal-ui · catalog-worker · order-worker"]
    end
    subgraph events["profile: events"]
        a3["kafka · schema-registry · rabbitmq · redis · mongo<br/>outbox-relay · inventory-consumer · catalog-celery · inventory-celery · order-celery<br/>notification · notification-consumer · notification-celery<br/>analytics · analytics-consumer · analytics-celery"]
    end
    subgraph ai["profile: ai"]
        a4["assistant · indexer-worker · assistant-celery"]
    end
    core --> workflow --> events --> ai
```

---

## 2. Inside each service

Every service follows the same entity-first layout ([RFC 0000 §3.1.1.7](rfcs/0000-architecture.md#3117-module-structure-inside-a-service)):

```mermaid
flowchart LR
    R["app/router.py<br/>mounts /api/v1/&lt;entity&gt;"] --> RT["&lt;entity&gt;/v1/routes.py<br/>HTTP in/out, schemas.py validates"]
    RT --> DEP["&lt;entity&gt;/dependencies.py<br/>auth principal, repo injection"]
    RT --> REPO["&lt;entity&gt;/*repo.py<br/>business rules + transaction"]
    REPO --> SM["&lt;entity&gt;/state_machine.py<br/>allowed transitions"]
    REPO --> M["&lt;entity&gt;/models.py<br/>SQLAlchemy tables"]
    REPO --> EV["&lt;entity&gt;/events.py<br/>outbox row, same tx"]
    WF["&lt;entity&gt;/workflows/*<br/>Temporal activities"] --> REPO
    CO["&lt;entity&gt;/consumers/*<br/>Kafka handlers"] --> REPO
    TK["&lt;entity&gt;/tasks/*<br/>Celery tasks"] --> REPO
    CL["&lt;entity&gt;/clients/*<br/>HTTP to other services"]
    WF --> CL
    M --> DB[("own database")]
    CORE["core/<br/>config · db · errors · logger"] -.-> REPO
```

The rule is that **routes, activities, consumers and tasks are all thin entry
points into the same repo**. A business rule lives in one place no matter how
the request arrives.

### 2.1 gateway

The gateway owns no database. It is a pipeline in front of the services.

```mermaid
flowchart LR
    IN(["request"]) --> CORS["core/cors.py<br/>CORS"]
    CORS --> RT["proxy/routes.py<br/>catch-all route"]
    RT --> RTG["proxy/routing.py<br/>prefix → upstream service<br/>blocks /internal/*"]
    RT --> AUTH["proxy/auth.py<br/>JWT verified early against cached JWKS"]
    AUTH -. "JWKS (HTTP, cached)" .-> IDN["identity"]
    RT --> FWD["proxy/forward.py<br/>httpx stream, X-Request-ID, timeouts"]
    FWD -- "HTTP / SSE" --> UP["identity · catalog · order · inventory"]
    FWD --> ERR["proxy/errors.py<br/>upstream errors → problem+json"]
    DOCS["docs/routes.py + docs/openapi.py<br/>merged OpenAPI of every service"] -- "HTTP" --> UP
    H["health/routes.py"]
```

See [RFC 0002](rfcs/0002-gateway-service.md) for the request pipeline stages.

### 2.2 identity

```mermaid
flowchart LR
    GW["gateway"] -- "HTTP" --> R["router.py"]
    R --> AR["v1/auth/routes.py<br/>register · login · refresh · logout · token"]
    R --> UR["v1/users/routes.py"]
    R --> MR["v1/merchants/routes.py<br/>approve / suspend"]
    AR --> JW["GET /auth/jwks<br/>public signing keys, ETag"]
    AR --> REG["auth/registration_repo.py"]
    AR --> AUTHN["auth/authentication_repo.py"]
    AR --> SES["auth/session_repo.py<br/>refresh rotation, reuse detection"]
    AR --> TOK["auth/token_repo.py"]
    AR --> CC["auth/client_credentials_repo.py<br/>service tokens for workers"]
    UR --> USR["users/repo.py"]
    MR --> MER["merchants/repo.py<br/>status machine"]
    AUTHN & TOK & CC --> SEC["core/security.py<br/>argon2 · RS256 signing"]
    REG & USR & MER --> OB["outbox<br/>identity.* (M3 relay)"]
    REG & AUTHN & SES & TOK & CC & USR & MER --> DB[("identity_db")]
    OB --> DB
```

See [RFC 0003](rfcs/0003-identity-service.md).

### 2.3 catalog

```mermaid
flowchart LR
    GW["gateway"] -- "HTTP" --> R["router.py"]
    R --> PR["v1/products/routes.py"]
    R --> VR["v1/variants/routes.py"]
    R --> CR["v1/categories/routes.py"]
    R --> MR["v1/media/routes.py"]
    PR --> PREPO["products/repo.py · listing_repo.py · audit_repo.py"]
    PR --> PUB["products/publishing_repo.py<br/>draft → publishing, start workflow"]
    PUB --> WFC["products/workflows/client.py"] -- "gRPC" --> T["Temporal<br/>catalog-publishing"]
    T -- "gRPC" --> WK["products/workflows/worker.py<br/>(catalog-worker)"]
    WK --> WF["publishing.py<br/>ProductPublishingWorkflow"]
    WF --> ACT["activities.py<br/>validate · media · search doc · sync · mark"]
    ACT --> PUB
    ACT --> MP["media/processing.py"] --> ST["media/storage.py"] -- "S3" --> S3[("MinIO")]
    ACT --> SD["products/search_document.py"]
    ACT --> IC["products/clients/inventory.py"] -- "HTTP" --> INV["inventory /internal"]
    PREPO & PUB --> SM["products/state_machine.py"]
    PUB & PREPO --> EV["products/events.py<br/>outbox: catalog.product.*"]
    VR --> VREPO["variants/repo.py"]
    CR --> CREPO["categories/repo.py"]
    MR --> MREPO["media/repo.py"]
    PREPO & PUB & VREPO & CREPO & MREPO & EV --> DB[("catalog_db")]
    DB -- "outbox" --> RELAY["outbox-relay"] -- "Kafka" --> K[["Kafka"]]
```

See [RFC 0004](rfcs/0004-catalog-service.md).

### 2.4 order

```mermaid
flowchart LR
    GW["gateway"] -- "HTTP" --> R["router.py"] --> OR["v1/orders/routes.py<br/>create · get · cancel · workflow state"]
    OR --> IDEM["orders/idempotency_repo.py<br/>Idempotency-Key claim / replay"]
    OR --> REPO["orders/repo.py<br/>create, advance, cancel"]
    REPO --> SNAP["orders/snapshot.py"] --> CC["orders/clients/catalog.py"] -- "HTTP" --> CAT["catalog"]
    REPO --> SM["orders/state_machine.py"]
    REPO --> EV["orders/events.py<br/>outbox: order.*, payment.*, shipment.*"]
    REPO --> WFC["orders/workflows/client.py"] -- "gRPC start / signal / query" --> T["Temporal<br/>order-lifecycle"]
    T -- "gRPC" --> WK["orders/workflows/worker.py<br/>(order-worker)"]
    WK --> WF["order_workflow.py<br/>saga + compensations"]
    WF --> ACT["activities.py"]
    ACT --> REPO
    ACT --> SHR["orders/shipment_repo.py"]
    ACT --> IC["orders/clients/inventory.py"] -- "HTTP" --> INV["inventory /internal"]
    ACT --> PP["orders/providers/payment.py<br/>orders/providers/shipping.py"] -- "HTTP" --> EXT["providers"]
    TASK["orders/tasks/maintenance.py<br/>resume_unstarted_orders (Celery Beat)"] --> REPO
    TASK --> WFC
    IDEM & REPO & SHR & EV --> DB[("order_db")]
    DB -- "outbox" --> RELAY["outbox-relay"] -- "Kafka" --> K[["Kafka"]]
```

See [RFC 0005](rfcs/0005-order-service.md).

### 2.5 inventory

```mermaid
flowchart LR
    GW["gateway"] -- "HTTP" --> R["router.py"]
    WKR["catalog-worker · order-worker"] -- "HTTP /internal/v1 (service token)" --> R
    R --> SR["v1/stock/routes.py<br/>availability · stock · initialise"]
    R --> RR["v1/reservations/routes.py<br/>reserve · commit · release · reverse"]
    SR --> SREPO["stock/repo.py"]
    SR --> LREPO["stock/ledger_repo.py<br/>every movement recorded"]
    RR --> RREPO["reservations/repo.py"]
    RREPO --> SM["reservations/state_machine.py"]
    SREPO & RREPO --> LOCK["stock/locking.py<br/>row locks in variant_id order, timeouts"]
    SREPO & RREPO --> EV["stock/events.py<br/>outbox: inventory.*"]
    K[["Kafka"]] -- "catalog.product.published" --> C1["stock/consumers/<br/>catalog_product_published.py"]
    K -- "order.cancelled" --> C2["reservations/consumers/<br/>order_cancelled.py"]
    C1 --> SREPO
    C2 --> RREPO
    CE["core/consumer.py<br/>EventConsumer, group = inventory"] --> C1 & C2
    BEAT["Celery Beat (60 s)"] -- "AMQP" --> TK["reservations/tasks/maintenance.py<br/>sweep_expired_reservations"]
    TK --> SW["reservations/sweeper.py"] --> RREPO
    SREPO & RREPO & LREPO & EV --> DB[("inventory_db<br/>+ processed_events")]
    CE --> DB
    DB -- "outbox" --> RELAY["outbox-relay"] -- "Kafka" --> K
```

See [RFC 0006](rfcs/0006-inventory-service.md).

### 2.6 notification

```mermaid
flowchart LR
    K[["Kafka: identity.user.registered, order.*, payment.*,<br/>shipment.status_changed, inventory.stockout"]] --> CO["core/consumer.py<br/>group = notification"]
    CO --> H["notifications/consumers/record.py<br/>payment.py (no-op)"]
    H --> U["notifications/utils.py<br/>resolve · derive_scope"]
    H --> T["notifications/templates/*"]
    H --> REC["notifications/recording_repo.py<br/>ON CONFLICT on (event_id, user, channel)"]
    REC --> DB[("notification_db<br/>notifications · delivery_attempts<br/>stream_cursors · processed_events")]
    CO -- "after_commit" --> AC["consumers/after_commit.py"]
    AC -- "Redis PUBLISH nt:user:{id}" --> R[("Redis")]
    AC -- "AMQP: dispatch_notification" --> CEL["notifications/tasks/dispatch.py<br/>(notification-celery)"]
    CEL --> AD["adapters/email_log · sms_log"]
    CEL --> DB
    GW["gateway"] -- "HTTP" --> API["v1/notifications/routes.py<br/>list · read · read-all · unread-count · ack · admin"]
    API --> DB
    GW -- "SSE (ticket)" --> ST["v1/stream/routes.py<br/>stream-ticket · stream"]
    ST --> TK["stream/tickets.py (Redis GETDEL)"]
    ST --> HUB["stream/fanout.py FanoutHub<br/>one PubSub per replica"]
    R --> HUB
    ST --> CU["stream/catchup.py · cursor_repo.py"]
    CU --> DB
```

See [RFC 0007](rfcs/0007-notification-service.md).

### 2.7 analytics

```mermaid
flowchart LR
    K[["Kafka: every topic with a schema"]] --> CO["metrics/consumers/__main__.py<br/>group = analytics"]
    KD[["Kafka: *.dlq"]] --> OBS["consumers/dlq_observer.py<br/>group = analytics-dlq-observer"]
    CO --> ST["consumers/dedupe.py MongoDedupeStore<br/>stale guard · one transaction"]
    ST --> RU["metrics/rollups.py<br/>pure: event → upsert ops"]
    ST --> M[("MongoDB analytics_db<br/>raw_events · processed_events<br/>counters · daily_metrics · product_stats · failure_stats")]
    RU --> M
    OBS --> M
    BEAT["analytics-celery Beat (hourly)"] -- "AMQP" --> RC["metrics/tasks/maintenance.py<br/>reconcile_repo.py: rebuild_daily_rollups"]
    RC --> M
    CLI["metrics/cli: indexes · backfill"] --> M
    GW["gateway"] -- "HTTP" --> API["v1/metrics/routes.py"]
    API --> SC["metrics/scoping.py<br/>merchant → own merchant_id"]
    API --> REPO["metrics/repo.py<br/>the only place formulas live"]
    REPO --> M
```

See [RFC 0008](rfcs/0008-analytics-service.md).

### 2.8 assistant

```mermaid
flowchart LR
    K[["Kafka: catalog.product.*"]] --> IDX["search/consumers/worker.py<br/>group = assistant-indexer"]
    IDX --> IR["search/indexing_repo.py<br/>renderer · chunker · embedding cache"]
    IR --> EMB["EmbeddingProvider<br/>hashing-v1 (default) · voyage-3.5"]
    IR --> VDB[("assistant_db (pgvector)<br/>product_chunks · index_state · conversations<br/>messages · generations · token_budgets · outbox")]
    GW["gateway"] -- "HTTP" --> SR["v1/search/routes.py<br/>semantic · similar · index"]
    SR --> RET["search/repo.py<br/>dense + keyword, RRF"]
    RET --> VDB
    SR --> HYD["search/hydration.py"] -- "HTTP: live price, stock" --> CI["catalog · inventory"]
    GW -- "SSE" --> CH["v1/chat/routes.py"]
    CH --> G["chat graph (LangGraph)<br/>guard · memory · classify · retrieve · hydrate · context"]
    G --> RET
    CH --> LLM["LLMProvider<br/>scripted (default) · Claude"]
    CH -- "one transaction" --> VDB
    GW -- "HTTP / SSE" --> GEN["v1/generations/routes.py"]
    GEN -- "AMQP: generate_bulk_content" --> CEL["generations tasks<br/>(assistant-celery, assistant.bulk)"]
    GEN --> LLM
    CEL --> VDB
    VDB -- "outbox: assistant.interaction" --> RELAY["outbox-relay"] -- "Kafka" --> AN["analytics"]
```

See [RFC 0009](rfcs/0009-assistant-service.md).
