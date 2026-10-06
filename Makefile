# SmartRetail — single entry point (RFC 0001 §3.1.1.12).
# Milestone 1: the `core` profile (postgres, identity, catalog, gateway).

SHELL := /bin/bash
COMPOSE := docker compose --profile core
VENV ?= .venv
PY := $(abspath $(VENV))/bin/python
STORE_SERVICES := identity catalog
PACKAGES := common services/identity services/catalog services/gateway
TEST_PG := smartretail-test-pg
TEST_PG_PORT := 55432

.DEFAULT_GOAL := help
.PHONY: help env keys up down reset logs ps migrate seed dev-install test-db test test-e2e lint fmt

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

.env:
	cp .env.example .env

env: .env

keys: .env ## Generate the RS256 key pair into keys/ if missing
	./scripts/keys.sh

up: keys ## Build and start the core stack, wait until healthy
	$(COMPOSE) up -d --build --wait

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

seed: ## Seed admin, demo accounts and categories (idempotent)
	$(COMPOSE) run --rm --no-deps identity python -m app.infrastructure.seed
	$(COMPOSE) run --rm --no-deps catalog python -m app.infrastructure.seed

dev-install: ## Install every package and dev tool into $(VENV)
	$(PY) -m pip install -q -e common ./services/identity ./services/catalog ./services/gateway
	$(PY) -m pip install -q "pytest>=8,<9" "pytest-asyncio>=0.24,<0.26" "ruff>=0.8,<0.9" "mypy>=1.13"

test-db: ## Start a throwaway Postgres for integration tests (127.0.0.1:$(TEST_PG_PORT))
	@docker ps --format '{{.Names}}' | grep -qx $(TEST_PG) || \
		docker run -d --rm --name $(TEST_PG) -e POSTGRES_PASSWORD=postgres \
			-p 127.0.0.1:$(TEST_PG_PORT):5432 pgvector/pgvector:pg16 >/dev/null
	@until docker exec $(TEST_PG) pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done

test: test-db ## Unit and integration suites for every package
	@for pkg in $(PACKAGES); do \
		echo "→ $$pkg"; \
		(cd $$pkg && $(PY) -m pytest -q) || exit 1; \
	done

test-e2e: ## Smoke test through the gateway (needs `make up migrate seed`)
	$(PY) -m pytest -q tests/e2e

lint: ## ruff (lint + format check) and mypy
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .
	@for pkg in $(PACKAGES); do \
		echo "→ mypy $$pkg"; \
		(cd $$pkg && $(PY) -m mypy --config-file $(CURDIR)/pyproject.toml $$(ls -d app main.py smartretail_common 2>/dev/null)) || exit 1; \
	done

fmt: ## Format and auto-fix
	$(PY) -m ruff format .
	$(PY) -m ruff check --fix .
