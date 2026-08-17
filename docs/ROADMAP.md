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
- [x] CLI: `population`, `backfill`, `live`, `incident`, `score`, `catalog`, `consume`
- [x] 49 tests; ruff, ruff format and mypy --strict clean, all enforced in CI
- [x] Local stack verified end to end against a real broker

The last item is what actually closed day 1: the producer path existed but had
never run against Kafka. Verified by producing a 30-day backfill and reading it
all back — 8,606 events, broker high-watermarks matching the producer count
exactly, spread across all 6 partitions (1284–1589 each, as employee-key hashing
should give), every message deserializing back into a valid `BehaviorEvent` with
its key matching `employee_id`.

The demo narrative also runs end to end: E0042 scores 35.94 (moderate, driven by
data handling), the scripted phishing chain lands, and they move to 78.68 (high,
driven by phishing susceptibility).

## Day 2 — ingestion ✅

- [x] Mock vendor API: four endpoints, four pagination idioms, four timestamp
      formats, injectable 429s and 5xx
- [x] Connector base class: cursor persistence, rate-limit backoff, pagination,
      identity resolution, deterministic event ids, per-reason drop counters
- [x] Four connectors — Okta, Google Workspace, MailShield, Sentry Agent
- [x] Raw payload archival with `raw_ref` written back onto the event
- [x] Normalizer: re-key onto `employee_id`, deduplicate, tolerate unknown
      schema versions, dead-letter what it cannot parse
- [x] 58 new tests (107 total), CI green

Verified against a live HTTP vendor: 8,288 events across four connectors, zero
dropped, zero malformed, every event carrying a `raw_ref` that resolves back to
the original payload. Rate limiting exercised for real — 6 × 429, 6 retries,
6.0s of `Retry-After` backoff, source still drained cleanly.

The PII boundary is visible end to end: the archived payload names
`zara.moreau@acme.example`, the event it produced carries only `E0459`.

Then verified against the real stack — MinIO, Postgres, Redpanda and Redis:

| Check | Result |
| --- | --- |
| Raw payloads in MinIO | 8,288 objects under `raw/source=*/dt=*/` |
| `events.raw` | 8,288 across 6 partitions (1113–1649) |
| `events.normalized` | 8,288 across 12 partitions (541–959) |
| Dead letters | 0 |
| Employees split across partitions | 0 of 314 |
| Full replay through a fresh consumer group | 8,288 duplicates, 0 emitted, topic unchanged |
| Second connector run | 0 records re-fetched |

The replay is the one worth pointing at: re-reading the entire raw topic
produced no new normalized events and no growth, which is what makes
at-least-once delivery safe rather than merely tolerated.

**A real bug turned up during that verification** — see `Persist a resume
position when a connector drains`. A drained connector was storing "no next
page" as its cursor, so every subsequent poll re-ingested the vendor's whole
history. Downstream never noticed, because dedup absorbed it, which is exactly
what made it worth catching: the only symptom was wasted vendor quota.

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
