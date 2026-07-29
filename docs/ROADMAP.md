# Roadmap

Started 2026-07-29. Target: a demoable end-to-end system in ~11 working days.

Ordering principle: every day ends with something that runs. The stream path
comes before the batch path because the demo lives on the stream path, and the
parity test that justifies the shared catalog can't exist until both are built.

## Day 1 — foundations ✅

- [x] Event contract (`BehaviorEvent`, `Employee`) with event-time/ingest-time split
- [x] Signal catalog: 23 signals, weights, half-lives, risk categories
- [x] Shared pure scoring function with per-signal and per-category attribution
- [x] Synthetic population: 6 behavioral personas, dimension-driven assignment
- [x] Behavior simulator: backfill, live, 4 scripted incident scenarios
- [x] JSONL lake sink, Kafka sink, fanout sink
- [x] CLI: `population`, `backfill`, `live`, `incident`, `score`, `catalog`
- [x] 49 tests; ruff and mypy --strict clean

## Day 2 — ingestion

- [ ] Connector base class: cursor persistence, rate-limit backoff, pagination
- [ ] Four connectors reading the generator as if it were a vendor API
- [ ] Raw payloads to MinIO with `raw_ref` written back onto the event
- [ ] Normalizer consumer: `events.raw` → `events.normalized`, schema-version tolerant

## Day 3 — real-time scoring

- [ ] Stream scorer consumer with per-employee 30-day window
- [ ] Redis online feature store; Postgres employee dimension
- [ ] `risk.scores` compacted topic
- [ ] Consumer group rebalance and restart-from-offset behavior verified

## Day 4 — interventions

- [ ] Decisioning: band crossings and specific-signal triggers
- [ ] Cooldown, weekly cap, escalation ladder, all enforced in Postgres
- [ ] Claude-generated copy from the employee's top factors
- [ ] Guardrail validator + static-template fallback

## Day 5 — serving

- [ ] FastAPI: employee score, timeline, population ranking, department rollup
- [ ] Tenant scoping and read audit log
- [ ] Minimal dashboard: ranked list, one employee drill-down, live updates

## Day 6 — batch

- [ ] PySpark: JSONL → Parquet, daily per-employee rollups
- [ ] Batch scorer over the lake using the same `score_events`
- [ ] **`test_score_parity`**: replay a fixed log through stream and batch, assert equal

## Day 7 — transform and orchestrate

- [ ] dbt models: staging → per-employee daily → department and cohort marts
- [ ] dbt tests on the marts
- [ ] Airflow DAGs: hourly ingest, daily rollup, retention enforcement
- [ ] Backfill a DAG run to prove reprocessing works

## Day 8 — operability

- [ ] OpenTelemetry traces from connector through intervention
- [ ] Consumer lag, scoring latency, intervention send-rate metrics
- [ ] Data-quality contracts: null rates, signal-mix drift, late-arrival rate
- [ ] Grafana dashboard

## Day 9 — load test

- [ ] Drive the generator to saturation; find where it breaks
- [ ] Record sustained events/sec, p50/p95/p99 end-to-end, API p99, cost/M events
- [ ] `docs/LOAD_TEST.md` with the numbers and the bottleneck analysis

## Day 10 — infrastructure and data protection

- [ ] Terraform: MSK/Kinesis, S3, RDS, ElastiCache, EKS, IAM
- [ ] Helm/manifests for the consumers
- [ ] Retention job, employee deletion path, field-level tokenization

## Day 11 — the artifacts that get read

- [ ] Finish `DESIGN.md`: tradeoffs, rejected alternatives, scale limits
- [ ] `docs/RUNBOOK.md`: what breaks and what to do
- [ ] 90-second demo video following the incident narrative
- [ ] README with architecture diagram and the load-test numbers up top

## Deliberately out of scope

Multi-tenancy beyond a tenant column; a learned risk model; real vendor API
credentials; SSO on the dashboard; horizontal autoscaling. Each is a paragraph
in the design doc explaining what it would take — which is more useful than a
half-built version of any of them.
