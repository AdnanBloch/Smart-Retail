# SmartRetail — single entry point (RFC 0001 §3.1.1.12).
# Profiles: `core` (postgres, identity, catalog, gateway); from Milestone 2,
# `workflow` (temporal, temporal-ui, minio, inventory, order and the workers);
# from Milestone 3, `events` (kafka, schema-registry, rabbitmq, outbox-relay,
# the inventory consumer and the Celery workers).

SHELL := /bin/bash
PROFILES ?= --profile core --profile workflow --profile events --profile ai
COMPOSE := docker compose $(PROFILES)
VENV ?= .venv
PY := $(abspath $(VENV))/bin/python
STORE_SERVICES := identity catalog inventory order notification assistant
PACKAGES := common services/identity services/catalog services/gateway services/inventory services/order \
	services/notification services/analytics services/assistant
TEST_PG := smartretail-test-pg
TEST_PG_PORT := 55432
TEST_REDIS := smartretail-test-redis
TEST_REDIS_PORT := 56379
TEST_MONGO := smartretail-test-mongo
TEST_MONGO_PORT := 57017

.DEFAULT_GOAL := help
.PHONY: test-mongo mongo-init analytics-indexes analytics-backfill reindex
.PHONY: help env keys up db-init topics schemas-check dlq-replay down reset logs ps migrate seed dev-install hooks test-db test cov lint-types test-e2e lint fmt audit

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

.env:
	cp .env.example .env

env: .env

keys: .env ## Generate the RS256 key pair into keys/ if missing
	./scripts/keys.sh

up: keys ## Build and start the stack (PROFILES=--profile core for M1 only), wait until healthy
	$(COMPOSE) up -d --wait postgres
	$(MAKE) --no-print-directory db-init
ifneq (,$(findstring events,$(PROFILES)))
	$(COMPOSE) up -d --wait kafka schema-registry mongo
	$(MAKE) --no-print-directory topics
	$(MAKE) --no-print-directory mongo-init
endif
	$(COMPOSE) up -d --build --wait

topics: ## Create every Kafka topic and DLQ, set BACKWARD, register the Avro schemas (idempotent)
	$(COMPOSE) build -q outbox-relay
	$(COMPOSE) run --rm --no-deps -v $(CURDIR)/scripts:/scripts:ro outbox-relay python /scripts/topics.py

mongo-init: ## Create or update the analytics Mongo user (idempotent; mongo must be healthy)
	$(COMPOSE) exec -T mongo sh -c 'mongosh --quiet -u "$$MONGO_INITDB_ROOT_USERNAME" -p "$$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin --file /ops/mongo/init-analytics.js'

reindex: ## Rebuild the assistant's chunks from stored source documents (P="--product-id <id>" or "--merchant-id <id>")
	$(COMPOSE) run --rm --no-deps indexer-worker python -m app.v1.search.reindex $(P)

analytics-indexes: ## Create analytics_db collections, indexes and TTLs (idempotent)
	$(COMPOSE) run --rm --no-deps analytics python -m app.v1.metrics.cli indexes

analytics-backfill: ## Replay a projection over raw_events: PROJECTION=shipments SINCE=YYYY-MM-DD
	@test -n "$(PROJECTION)" -a -n "$(SINCE)" || (echo "usage: make analytics-backfill PROJECTION=shipments SINCE=YYYY-MM-DD" && exit 1)
	$(COMPOSE) run --rm --no-deps analytics python -m app.v1.metrics.cli backfill --projection $(PROJECTION) --since $(SINCE)

schemas-check: ## Check every Avro schema is BACKWARD-compatible with what the registry holds
	$(COMPOSE) build -q outbox-relay
	$(COMPOSE) run --rm --no-deps -v $(CURDIR)/scripts:/scripts:ro outbox-relay python /scripts/topics.py --check

dlq-replay: ## Re-produce dead-lettered events: TOPIC=smartretail.<event> [EVENT_ID=<uuid>]
	@test -n "$(TOPIC)" || (echo "usage: make dlq-replay TOPIC=smartretail.<event> [EVENT_ID=<uuid>]" && exit 1)
	$(COMPOSE) run --rm --no-deps -v $(CURDIR)/scripts:/scripts:ro outbox-relay \
		python /scripts/dlq_replay.py --topic $(TOPIC) $(if $(EVENT_ID),--event-id $(EVENT_ID))

db-init: ## Create any missing database and role (idempotent; safe on an existing volume)
	$(COMPOSE) exec -T postgres bash /docker-entrypoint-initdb.d/01-databases.sh

down: ## Stop the stack, keep volumes
	$(COMPOSE) down

reset: ## Stop the stack and delete volumes
	$(COMPOSE) down -v --remove-orphans

logs: ## Follow logs (S=<service> for one)
	$(COMPOSE) logs -f $(S)

ps: ## Show container status
	$(COMPOSE) ps

migrate: ## Run every service's migrations to head (never on start-up)
	@for svc in $(STORE_SERVICES); do \
		echo "→ migrating $$svc"; \
		$(COMPOSE) run --rm --no-deps $$svc alembic upgrade head || exit 1; \
	done

seed: ## Seed accounts and categories, then demo products published through the real workflow, stock and orders (idempotent)
	$(COMPOSE) run --rm --no-deps identity python -m app.seed
	$(COMPOSE) run --rm --no-deps catalog python -m app.seed
	$(COMPOSE) run --rm --no-deps assistant python -m app.seed
	$(COMPOSE) run --rm --no-deps -v $(CURDIR)/scripts:/scripts:ro \
		-e GATEWAY_URL=http://gateway:8000 gateway python /scripts/seed_demo.py

dev-install: ## Install every package and dev tool into $(VENV)
	$(PY) -m pip install -q -e "common[temporal,events,celery,redis]" ./services/identity ./services/catalog \
		./services/gateway ./services/inventory ./services/order ./services/notification ./services/analytics ./services/assistant
	$(PY) -m pip install -q "pytest>=8,<9" "pytest-asyncio>=0.24,<0.26" "pytest-cov>=6,<7" \
		"ruff>=0.8,<0.9" "mypy>=1.13" "pip-audit>=2.7" "pre-commit>=4"

hooks: ## Install the git pre-commit hooks
	$(PY) -m pre_commit install

test-db: ## Start throwaway Postgres (127.0.0.1:$(TEST_PG_PORT)) and Redis (:$(TEST_REDIS_PORT)) for integration tests
	@docker ps --format '{{.Names}}' | grep -qx $(TEST_PG) || \
		docker run -d --rm --name $(TEST_PG) -e POSTGRES_PASSWORD=postgres \
			-p 127.0.0.1:$(TEST_PG_PORT):5432 pgvector/pgvector:pg16 -c max_connections=300 >/dev/null
	@docker ps --format '{{.Names}}' | grep -qx $(TEST_REDIS) || \
		docker run -d --rm --name $(TEST_REDIS) -p 127.0.0.1:$(TEST_REDIS_PORT):6379 redis:7.4 >/dev/null
	@until docker exec $(TEST_PG) pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done
	@until docker exec $(TEST_REDIS) redis-cli ping >/dev/null 2>&1; do sleep 1; done

test-mongo: ## Start a throwaway single-node MongoDB replica set (127.0.0.1:$(TEST_MONGO_PORT)) for analytics' tests
	@docker ps --format '{{.Names}}' | grep -qx $(TEST_MONGO) || \
		docker run -d --rm --name $(TEST_MONGO) -p 127.0.0.1:$(TEST_MONGO_PORT):$(TEST_MONGO_PORT) mongo:7.0 \
			--replSet rs0 --port $(TEST_MONGO_PORT) --bind_ip_all >/dev/null
	@until docker exec $(TEST_MONGO) mongosh --port $(TEST_MONGO_PORT) --quiet --eval "db.adminCommand('ping').ok" >/dev/null 2>&1; do sleep 1; done
	@docker exec $(TEST_MONGO) mongosh --port $(TEST_MONGO_PORT) --quiet --eval \
		"try { rs.status() } catch (e) { rs.initiate({_id: 'rs0', members: [{_id: 0, host: '127.0.0.1:$(TEST_MONGO_PORT)'}]}) }" >/dev/null
	@until [ "$$(docker exec $(TEST_MONGO) mongosh --port $(TEST_MONGO_PORT) --quiet --eval 'db.hello().isWritablePrimary')" = "true" ]; do sleep 1; done

test: test-db test-mongo ## Unit and integration suites for every package
	@for pkg in $(PACKAGES); do \
		echo "→ $$pkg"; \
		(cd $$pkg && $(PY) -m pytest -q) || exit 1; \
	done

cov: test-db test-mongo ## Tests with branch coverage; fails below 85% per package (HTML in <pkg>/htmlcov/)
	@for pkg in $(PACKAGES); do \
		echo "→ $$pkg"; \
		(cd $$pkg && $(PY) -m pytest -q --cov --cov-report=term --cov-report=html) || exit 1; \
	done

test-e2e: ## End-to-end through the gateway (needs `make up migrate seed`)
	$(PY) -m pytest -q tests/e2e

lint: lint-types ## ruff (lint + format check) and mypy
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .

lint-types: ## mypy for every package
	@for pkg in $(PACKAGES); do \
		echo "→ mypy $$pkg"; \
		(cd $$pkg && $(PY) -m mypy --config-file $(CURDIR)/pyproject.toml $$(ls -d app main.py smartretail_common 2>/dev/null)) || exit 1; \
	done

fmt: ## Format and auto-fix
	$(PY) -m ruff format .
	$(PY) -m ruff check --fix .

audit: ## Check installed dependencies for known vulnerabilities
	$(PY) -m pip_audit --skip-editable
