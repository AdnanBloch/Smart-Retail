# SmartRetail — review questions (no answers)

Answer each one out loud, then check yourself in [review-qa.md](review-qa.md), which uses the same numbering.

- ⭐ = very likely to be asked, or asked in an earlier review.
- 🪤 = trick question, or a known gap. Say it's a gap honestly.

Tick a box once you can answer without looking.

## 1. Requirements and scope

- [ ] **Q1.1** ⭐ What problem is SmartRetail solving? Give the Part A challenges in one breath
- [ ] **Q1.2** ⭐ Which of your decisions come from the requirements, and which did you make where the requirements say nothing?
- [ ] **Q1.3** What did you cut from scope on purpose?
- [ ] **Q1.4** 🪤 Which explicit requirements are not met yet?
- [ ] **Q1.5** Why is there no cart?

## 2. Architecture and service boundaries

- [ ] **Q2.1** ⭐ List the services and what each one owns
- [ ] **Q2.2** ⭐ How did you decide what becomes a service?
- [ ] **Q2.3** ⭐ Why are payment and shipment not separate services?
- [ ] **Q2.4** Why is the gateway a service if it fails the ownership test?
- [ ] **Q2.5** Why not a modular monolith?
- [ ] **Q2.6** ⭐ What are the boundary rules R1–R8?
- [ ] **Q2.7** ⭐ How many synchronous cross-service calls are on the write path, and why that one?
- [ ] **Q2.8** One Postgres instance for all services: isn't that a shared database?
- [ ] **Q2.9** What goes in `common` and what never does?
- [ ] **Q2.10** Explain the code layout inside a service
- [ ] **Q2.11** Why do activities, consumers and tasks all call the repo instead of having their own logic?

## 3. Request flow (edge to database)

- [ ] **Q3.1** ⭐ Walk me through what happens when a request hits `POST /api/v1/orders`
- [ ] **Q3.2** What does the gateway do and not do?
- [ ] **Q3.3** ⭐ Why does every service re-verify the JWT if the gateway already did?
- [ ] **Q3.4** Why doesn't the gateway retry upstream calls?
- [ ] **Q3.5** How do you trace one request across services?
- [ ] **Q3.6** What error format do APIs return?
- [ ] **Q3.7** What happens on `GET /orders/{id}` while the saga is still running?

## 4. Identity and auth

- [ ] **Q4.1** ⭐ Describe the token model
- [ ] **Q4.2** Why RS256 and not HS256?
- [ ] **Q4.3** How do workers call internal APIs without a user token?
- [ ] **Q4.4** Can someone register as admin?
- [ ] **Q4.5** What's the merchant lifecycle?
- [ ] **Q4.6** 🪤 Can a pending merchant publish products?
- [ ] **Q4.7** How long can a suspended merchant keep acting?
- [ ] **Q4.8** How are passwords stored, and how do you stop email enumeration?
- [ ] **Q4.9** Who decides authorisation, for example "can this merchant edit this product"?

## 5. Catalog and product publishing

- [ ] **Q5.1** ⭐ What's the product state machine?
- [ ] **Q5.2** ⭐ Walk through publishing a product
- [ ] **Q5.3** Why commit `publishing` before starting the workflow?
- [ ] **Q5.4** Why does a failed publish not need step-by-step undo?
- [ ] **Q5.5** Why is the workflow id versioned (`-v{version}`)?
- [ ] **Q5.6** Can a merchant edit a product while it's publishing?
- [ ] **Q5.7** How do you stop lost updates on product edits?
- [ ] **Q5.8** Where is stock stored for a product?
- [ ] **Q5.9** Why is media processing on Celery and not inside the activity?
- [ ] **Q5.10** 🪤 Any validation gaps?

## 6. Inventory: stock, reservations and the ledger (deep)

- [ ] **Q6.1** ⭐ Describe the inventory tables
- [ ] **Q6.2** ⭐⭐ How does stock decrease when an order is placed? Walk the numbers
- [ ] **Q6.3** ⭐⭐ How is stock given back? Cover every reverse path
- [ ] **Q6.4** ⭐ How do you stop two customers buying the last unit?
- [ ] **Q6.5** Why pessimistic locking and not optimistic (version) or SERIALIZABLE?
- [ ] **Q6.6** ⭐ How do you prevent deadlocks when an order has several variants?
- [ ] **Q6.7** What's the fast-reject read for?
- [ ] **Q6.8** ⭐ Isn't `stock_items.reserved` a duplicate source of truth next to the reservation rows? (asked 2026-10-20)
- [ ] **Q6.9** What does the ledger give you?
- [ ] **Q6.10** Why is the reservation idempotent on `(order_id, variant_id)` and not a separate idempotency key?
- [ ] **Q6.11** ⭐ What's the reservation TTL, and why 15 minutes?
- [ ] **Q6.12** ⭐ How does the expiry sweep work, and is it safe on two workers?
- [ ] **Q6.13** ⭐ The reservation is past `expires_at` but the sweep hasn't run. Can it still commit?
- [ ] **Q6.14** What if a merchant reduces stock while customers are reserving?
- [ ] **Q6.15** When are stock rows created, and with what quantity?
- [ ] **Q6.16** When is `inventory.stockout` emitted?
- [ ] **Q6.17** How do two reverses of the same commit not add stock twice?
- [ ] **Q6.18** Why no `reversed` reservation state?
- [ ] **Q6.19** How is availability exposed to customers?

## 7. Order and checkout

- [ ] **Q7.1** ⭐ How do you prevent duplicate orders?
- [ ] **Q7.2** ⭐ Walk through the idempotency-key rules
- [ ] **Q7.3** Why a unique insert and not "SELECT, then INSERT if missing"?
- [ ] **Q7.4** Why is the key scoped per user?
- [ ] **Q7.5** ⭐ Why does `POST /orders` return 202 and not 201?
- [ ] **Q7.6** ⭐ What is the price snapshot, and why?
- [ ] **Q7.7** How is the total computed? Any money rules?
- [ ] **Q7.8** Why isn't stock checked at order creation?
- [ ] **Q7.9** ⭐ Order state machine: states and legal transitions?
- [ ] **Q7.10** Payment and shipment sub-states?
- [ ] **Q7.11** Why store status as `text` and not a Postgres enum?
- [ ] **Q7.12** How does `OrderRepo.advance` work?
- [ ] **Q7.13** Which events does order emit, and when?
- [ ] **Q7.14** ⭐ What if Temporal is down when the order is placed?
- [ ] **Q7.15** Why not start the workflow before committing the order?
- [ ] **Q7.16** ⭐ How does cancellation work?
- [ ] **Q7.17** 🪤 Is there a race in cancellation?
- [ ] **Q7.18** What's the difference between `failed` and `cancelled`?
- [ ] **Q7.19** Can merchants see orders?

## 8. Temporal and the saga

- [ ] **Q8.1** ⭐ Why Temporal for checkout instead of Kafka choreography or Celery chains?
- [ ] **Q8.2** ⭐ Explain the saga pattern as you use it
- [ ] **Q8.3** ⭐ What are the saga steps in order, and why reserve before charge?
- [ ] **Q8.4** ⭐ Failure matrix: what happens at each step?
- [ ] **Q8.5** ⭐ Give the retry policies and the reasoning
- [ ] **Q8.6** How does an HTTP error become retryable or not inside an activity?
- [ ] **Q8.7** ⭐ Ambiguous payment timeout: how do you avoid charging for a failed order?
- [ ] **Q8.8** ⭐ The worker crashes after charging, before committing. What happens?
- [ ] **Q8.9** ⭐ Two worker replicas run the same charge. Double charge?
- [ ] **Q8.10** ⭐ What does "deterministic workflow" mean, and how do you respect it?
- [ ] **Q8.11** Workflow vs activity: what goes where?
- [ ] **Q8.12** Signal vs query vs update?
- [ ] **Q8.13** What's a task queue here, and where's the "queue" for orders?
- [ ] **Q8.14** Explain `start_to_close`, `schedule_to_close` and heartbeat timeouts
- [ ] **Q8.15** What's `REJECT_DUPLICATE` for?
- [ ] **Q8.16** What happens on an operator cancel (Temporal cancel) of an order workflow?
- [ ] **Q8.17** 🪤 What if you change the workflow code while orders are in flight?
- [ ] **Q8.18** 🪤 Known gaps in the saga?
- [ ] **Q8.19** Why does the notify step exist if notification consumes Kafka events?

## 9. Inter-service communication

- [ ] **Q9.1** ⭐ Table every way services talk to each other
- [ ] **Q9.2** ⭐ Are internal endpoints synchronous or asynchronous? (asked 2026-10-20)
- [ ] **Q9.3** ⭐ Why HTTP and not Kafka for order → inventory reserve?
- [ ] **Q9.4** How is the internal API protected from being called by users?
- [ ] **Q9.5** Is there a circuit breaker?
- [ ] **Q9.6** How does inventory protect itself under load?

## 10. Kafka, the outbox and consumers

- [ ] **Q10.1** ⭐ What is the transactional outbox, and why?
- [ ] **Q10.2** ⭐ How does the relay work?
- [ ] **Q10.3** What's a poison outbox row, and how is it handled?
- [ ] **Q10.4** ⭐ How is Kafka partitioned, and why?
- [ ] **Q10.5** Why 3 partitions and not 1?
- [ ] **Q10.6** ⭐ When do consumers commit offsets?
- [ ] **Q10.7** ⭐ How are duplicates handled?
- [ ] **Q10.8** Compacted topics and 30-day dedupe retention: what's the catch?
- [ ] **Q10.9** ⭐ Retry and DLQ policy for consumers?
- [ ] **Q10.10** Why not Kafka exactly-once (transactions)?
- [ ] **Q10.11** Who consumes what?
- [ ] **Q10.12** Schema governance?
- [ ] **Q10.13** What's in the event envelope?
- [ ] **Q10.14** `order.created` and `order.cancelled` are separate topics. Doesn't that break ordering?
- [ ] **Q10.15** Kafka is down. Can customers still check out?

## 11. Celery and RabbitMQ

- [ ] **Q11.1** ⭐ Why Kafka *and* RabbitMQ *and* Temporal?
- [ ] **Q11.2** What runs on Celery?
- [ ] **Q11.3** Celery reliability settings and why?
- [ ] **Q11.4** Why not Kafka for tasks?
- [ ] **Q11.5** Why not RabbitMQ for events?
- [ ] **Q11.6** Is there a DLQ for Celery?

## 12. Database design

- [ ] **Q12.1** ⭐ Database-per-service: what's in each?
- [ ] **Q12.2** ⭐ How do you reference data across services without FKs?
- [ ] **Q12.3** Why UUID primary keys, and where are there exceptions?
- [ ] **Q12.4** Which constraints protect invariants in the DB itself?
- [ ] **Q12.5** Which indexes matter, and why?
- [ ] **Q12.6** How are audit trails stored?
- [ ] **Q12.7** Why JSONB for variant options and shipping address?
- [ ] **Q12.8** Migrations?
- [ ] **Q12.9** Why MongoDB for analytics?

## 13. Concurrency and race conditions

- [ ] **Q13.1** ⭐ List the stock races you tested, and who wins each
- [ ] **Q13.2** ⭐ How do the three release layers avoid double-releasing?
- [ ] **Q13.3** Two concurrent reserves for the *same* order (an activity retry overlapping the first attempt)?
- [ ] **Q13.4** Ten parallel `POST /orders` with the same key?
- [ ] **Q13.5** Sweep vs commit on the same reservation?

## 14. "What if…" failure questions

- [ ] **Q14.1** ⭐ Inventory is down when the saga reserves
- [ ] **Q14.2** ⭐ Inventory is down during a compensation release
- [ ] **Q14.3** ⭐ Temporal (or the order worker) is down mid-saga
- [ ] **Q14.4** Catalog is down at checkout
- [ ] **Q14.5** Celery Beat is down
- [ ] **Q14.6** Redis is down
- [ ] **Q14.7** Identity is down
- [ ] **Q14.8** Postgres restarts
- [ ] **Q14.9** The network drops the reply from inventory after a successful reserve
- [ ] **Q14.10** The relay crashes after Kafka acked but before marking rows published
- [ ] **Q14.11** The address is undeliverable
- [ ] **Q14.12** A refund keeps failing
- [ ] **Q14.13** Someone runs a manual SQL update that breaks `reserved`
- [ ] **Q14.14** How would you find out who released order X's stock?

## 15. Scalability and performance

- [ ] **Q15.1** ⭐ 500 customers press Buy in the same second. What happens?
- [ ] **Q15.2** ⭐ What are the top bottlenecks, and the next step for each?
- [ ] **Q15.3** How do you scale order-worker, and why was it unsafe before?
- [ ] **Q15.4** How would you handle a real flash sale on one SKU?
- [ ] **Q15.5** What's cached?

## 16. Notifications

- [ ] **Q16.1** Which events produce notifications?
- [ ] **Q16.2** How do you avoid sending a notification twice?
- [ ] **Q16.3** How is in-app delivery done?
- [ ] **Q16.4** Email and SMS?
- [ ] **Q16.5** Can a notification outage affect checkout?

## 17. Analytics

- [ ] **Q17.1** ⭐ How is revenue defined?
- [ ] **Q17.2** How are AOV and the stockout rate computed?
- [ ] **Q17.3** How does analytics stay correct with at-least-once delivery and replays?
- [ ] **Q17.4** Who can see which metrics?

## 18. AI assistant (Part B)

- [ ] **Q18.1** ⭐ How does the product index stay in sync with the catalog?
- [ ] **Q18.2** Chunking and embeddings?
- [ ] **Q18.3** Retrieval?
- [ ] **Q18.4** ⭐ Why are price and stock not taken from the index?
- [ ] **Q18.5** How does the LangGraph flow work, and why not a tool-calling agent?
- [ ] **Q18.6** Guardrails and cost control?
- [ ] **Q18.7** Streaming?

## 19. Security

- [ ] **Q19.1** What stops header spoofing?
- [ ] **Q19.2** What are the rate limits, and why fail open?
- [ ] **Q19.3** Prompt injection: where's the risk?

## 20. Testing

- [ ] **Q20.1** ⭐ What is your test strategy?
- [ ] **Q20.2** How do you test race conditions for real?
- [ ] **Q20.3** How do you prove "exactly one charge"?

## 21. Observability and operations

- [ ] **Q21.1** 🪤 What observability exists today?
- [ ] **Q21.2** What signals would you alert on?
- [ ] **Q21.3** How is the local stack run?

## 22. Curveballs and "why not X"

- [ ] **Q22.1** Why not 2PC/XA across services?
- [ ] **Q22.2** Why not Debezium CDC instead of a polling relay?
- [ ] **Q22.3** Why not event sourcing?
- [ ] **Q22.4** Why not charge first and reserve later?
- [ ] **Q22.5** Why not let the customer retry payment on the same order after a decline?
- [ ] **Q22.6** Why allow cancellation only before commit and not until shipment?
- [ ] **Q22.7** What would you change first if this went to production?
- [ ] **Q22.8** What's the one invariant you'd defend above all?
- [ ] **Q22.9** Where does the system choose consistency over availability?
