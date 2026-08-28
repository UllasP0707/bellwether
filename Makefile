VENV := .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

CLI := $(PY) -m bellwether.generator.cli

.PHONY: help install install-dbt up down logs topics seed backfill backfill-kafka live \
        demo-incident score consume serve intervene test test-all lint fmt clean \
        batch parity warehouse dbt dags dag-daily backfill-twice

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

install-dbt: ## dbt gets its own venv: dbt-core pins pathspec<0.13, mypy needs >=1.0
	python3 -m venv $(VENV)-dbt
	$(VENV)-dbt/bin/pip install --upgrade pip -q
	$(VENV)-dbt/bin/pip install -r requirements-transform.txt

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

serve: ## Serve the read API and the dashboard on :8800
	$(PY) -m bellwether.cli serve

intervene: ## Decide interventions from the scores already published
	$(PY) -m bellwether.cli intervene

batch: ## Spark: lake -> Parquet -> rollups -> scores (in a container, JDK 17)
	docker compose --profile batch run --rm spark python -m bellwether.cli batch parquet
	docker compose --profile batch run --rm spark python -m bellwether.cli batch rollup
	docker compose --profile batch run --rm spark python -m bellwether.cli batch score --out data/parquet/scores

parity: ## The one that matters: stream and batch must agree
	docker compose --profile batch run --rm --no-deps spark pytest -m spark -v

warehouse: ## Land Spark's Parquet output in Postgres
	$(PY) -m bellwether.cli warehouse load

dbt: ## Build and test the marts
	cd transform && DBT_PROFILES_DIR=$$PWD ../$(VENV)-dbt/bin/dbt seed
	cd transform && DBT_PROFILES_DIR=$$PWD ../$(VENV)-dbt/bin/dbt run
	cd transform && DBT_PROFILES_DIR=$$PWD ../$(VENV)-dbt/bin/dbt test

dags: ## List the Airflow DAGs and any import errors
	docker compose --profile orchestration run --rm airflow \
	  'airflow db migrate >/dev/null 2>&1; airflow dags list; airflow dags list-import-errors'

dag-daily: ## Execute the daily DAG for one date (DATE=2026-08-25)
	docker compose --profile orchestration run --rm airflow \
	  'airflow db migrate >/dev/null 2>&1; airflow dags test bellwether_daily $(or $(DATE),2026-08-25)'

backfill-twice: ## Run the daily DAG for the same date twice; row counts must not move
	./scripts/backfill_twice.sh $(or $(DATE),2026-08-25)

test: ## Run tests (skips the ones needing a JVM)
	$(VENV)/bin/pytest -q -m "not spark"

test-all: ## Run every test, including the Spark parity comparison
	$(VENV)/bin/pytest -q -m "not spark"
	docker compose --profile batch run --rm --no-deps spark pytest -q -m spark

lint: ## Lint and type-check
	$(VENV)/bin/ruff check bellwether tests
	$(VENV)/bin/mypy bellwether

fmt: ## Format
	$(VENV)/bin/ruff format bellwether tests
	$(VENV)/bin/ruff check --fix bellwether tests

clean: ## Remove venv and local data
	rm -rf $(VENV) data .pytest_cache .ruff_cache .mypy_cache

observe: ## Prometheus, Grafana and Jaeger (profile: observability)
	docker compose --profile observability up -d
	@echo "grafana    http://localhost:3000/d/bellwether-overview"
	@echo "prometheus http://localhost:9090"
	@echo "jaeger     http://localhost:16686"
	@echo "then run any stage with BELLWETHER_OTLP_ENDPOINT=http://localhost:4318"

trace-demo: ## One incident, traced end to end across three topics
	./scripts/trace_demo.sh

contracts: ## Run the data-quality contracts for a day (DATE=2026-08-14)
	$(PY) -m bellwether.cli quality check --as-of $(or $(DATE),2026-08-14)

loadtest: ## Where this breaks, and what breaks first (docs/LOAD_TEST.md)
	$(PY) -m bellwether.cli load all
