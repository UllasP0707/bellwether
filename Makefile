VENV := .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

.PHONY: help install up down logs topics seed backfill live demo-incident test lint fmt clean

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

install: ## Create venv and install dependencies
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip -q
	$(PIP) install -r requirements-dev.txt
	@test -f .env || cp .env.example .env
	@echo "ready. next: make up"

up: ## Start local infrastructure
	docker compose up -d
	@echo "waiting for cluster..."
	@docker compose exec -T redpanda rpk cluster health

down: ## Stop infrastructure (keeps volumes)
	docker compose down

logs: ## Tail infrastructure logs
	docker compose logs -f --tail=50

topics: ## Create Kafka topics
	./scripts/create_topics.sh

seed: ## Generate the synthetic employee population
	$(PY) -m bellwether.generator.cli population --size 500

backfill: ## Generate 30 days of historical behavior
	$(PY) -m bellwether.generator.cli backfill --days 30

live: ## Stream events in real time until interrupted
	$(PY) -m bellwether.generator.cli live

demo-incident: ## Inject the scripted phishing chain used in the demo
	$(PY) -m bellwether.generator.cli incident --employee E0042 --scenario phish_credential_chain

test: ## Run tests
	$(VENV)/bin/pytest -q

lint: ## Lint and type-check
	$(VENV)/bin/ruff check bellwether tests
	$(VENV)/bin/mypy bellwether

fmt: ## Format
	$(VENV)/bin/ruff format bellwether tests
	$(VENV)/bin/ruff check --fix bellwether tests

clean: ## Remove venv and local data
	rm -rf $(VENV) data .pytest_cache .ruff_cache .mypy_cache
