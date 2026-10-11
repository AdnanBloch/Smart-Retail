# SmartRetail — single entry point (RFC 0001 §3.1.1.12).
# Profiles: `core` (postgres, identity, catalog, gateway) and, from Milestone 2,
# `workflow` (temporal, temporal-ui, minio, inventory, order and the workers).

SHELL := /bin/bash
PROFILES ?= --profile core --profile workflow
COMPOSE := docker compose $(PROFILES)
VENV ?= .venv
PY := $(abspath $(VENV))/bin/python
STORE_SERVICES := identity catalog inventory order
PACKAGES := common services/identity services/catalog services/gateway services/inventory services/order
TEST_PG := smartretail-test-pg
TEST_PG_PORT := 55432

.DEFAULT_GOAL := help
.PHONY: help env keys up db-init down reset logs ps migrate seed dev-install hooks test-db test cov lint-types test-e2e lint fmt audit

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
	$(COMPOSE) up -d --build --wait

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
	$(COMPOSE) run --rm --no-deps -v $(CURDIR)/scripts:/scripts:ro \
		-e GATEWAY_URL=http://gateway:8000 gateway python /scripts/seed_demo.py

dev-install: ## Install every package and dev tool into $(VENV)
	$(PY) -m pip install -q -e "common[temporal]" ./services/identity ./services/catalog \
		./services/gateway ./services/inventory ./services/order
	$(PY) -m pip install -q "pytest>=8,<9" "pytest-asyncio>=0.24,<0.26" "pytest-cov>=6,<7" \
		"ruff>=0.8,<0.9" "mypy>=1.13" "pip-audit>=2.7" "pre-commit>=4"

hooks: ## Install the git pre-commit hooks
	$(PY) -m pre_commit install

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

cov: test-db ## Tests with branch coverage; fails below 85% per package (HTML in <pkg>/htmlcov/)
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
