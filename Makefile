VENV := .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

CLI := $(PY) -m bellwether.generator.cli

.PHONY: help install up down logs topics seed backfill backfill-kafka live \
        demo-incident score consume test lint fmt clean

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
	$(CLI) population --size 500

backfill: ## Generate 30 days of historical behavior into the local lake
	$(CLI) backfill --days 30 --to lake

backfill-kafka: ## Generate 30 days of history into the lake and the raw topic
	$(CLI) backfill --days 30 --to both

live: ## Stream events to Kafka in real time until interrupted
	$(CLI) live --to kafka

demo-incident: ## Inject the scripted phishing chain used in the demo
	$(CLI) incident --employee E0042 --scenario phish_credential_chain --to both

score: ## Score one employee from the local lake (EMPLOYEE=E0042)
	$(CLI) score --employee $(or $(EMPLOYEE),E0042)

consume: ## Read the raw topic back and verify the round trip
	$(CLI) consume --topic bellwether.events.raw

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
