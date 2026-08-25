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

## Day 3 — real-time scoring ✅

- [x] `ScorableEvent` protocol: scoring narrowed to the three fields it reads
- [x] Employee dimension in Postgres, read by connectors and the scorer alike
- [x] Redis online feature store: per-employee window plus last-known band
- [x] Stream scorer consumer, `events.normalized` → `risk.scores`
- [x] Band-transition detection and two latency metrics
- [x] `scores` CLI: rank the population or drill into one employee
- [x] Consumer group rebalance and restart-from-offset verified
- [x] 32 new tests (139 total), CI green

The protocol came first because it is load-bearing for day 6. Scoring took
`Iterable[BehaviorEvent]`, which would have forced Spark executors to
materialise millions of Pydantic models — slow enough that the tempting fix is
to reimplement scoring in Spark, which is exactly the duplication the project
exists to avoid. It now reads `employee_id`, `signal`, `occurred_at` and nothing
else, so a Redis projection, a parsed model and a Spark `Row` all satisfy it.

Verified against the running stack:

| Check | Result |
| --- | --- |
| Pipeline | 7,789 ingested → normalized → scored, 0 unknown, 0 malformed |
| Employees scored | 499 of 500 |
| Distribution | 23 critical, 41 high, 79 elevated, 109 moderate, 247 low |
| Band transitions detected | 440 |
| `ingest → score` p50 / p99 | 33.4s / 50.4s |
| Restart from committed offsets | 2,000 + 5,789 = 7,789 exactly |
| Rebalance with a second consumer | 430 messages reprocessed, **scores unchanged** |

The distribution matches day 1's offline computation over the lake — 4.6%
critical either way — which is the first evidence that two independent paths
over the same catalog agree. The real parity test lands on day 6.

The rebalance number is the useful one. Adding a consumer mid-run redelivered
430 uncommitted messages, and the resulting scores were byte-identical, because
the window is keyed by `event_id`. At-least-once is only safe if reprocessing is
provably inert, and that is what this measures.

**Demo narrative, end to end:** E0042 sits at 67.52 (high). The scripted
phishing chain — delivered, clicked, credentials submitted, 112 seconds — takes
them to 89.19 (critical) with the band change flagged and
`phish_credentials_submitted` named as the driver at +43.77.

**Two real bugs found during verification** — see the commits. The window trimmed
against wall-clock time rather than `as_of`, and the population issued colliding
email addresses that silently merged 185 employees into other people's scores.

## Day 4 — interventions ✅

- [x] Decisioning: band crossings, four critical-signal triggers, recency gate
- [x] Cooldown, minimum spacing, weekly cap, escalation ladder, all in Postgres
- [x] Claude-generated copy from the employee's top factors
- [x] Guardrail validator + static-template fallback, both validated
- [x] Intervention ledger with a uniqueness index that fences replays
- [x] `intervene` and `interventions` CLI
- [x] Postgres round-trip tests, run against a real database in CI
- [x] 146 new tests (285 total), CI green

The gates matter more than the triggers. A human-risk platform whose failure
mode is messaging people too much stops being used, and then it protects nobody.

Verified against the running stack:

| Check | Result |
| --- | --- |
| Scores decided | 7,789, every one accounted for |
| Replaying 30 days of history | **0 messages sent**, 212 stale triggers refused |
| One live incident | 3 events → 1 band change → exactly 1 nudge |
| Replay with every rate gate disabled | 137 `already_sent`, ledger unchanged |
| Interventions below the band threshold | 68 of 137, reached only by signal triggers |
| Templates failing their own guardrails | 0 |

The 68 is the number worth pointing at. Those employees never crossed a band —
they have no accumulated history to push them over one — and a policy built on
crossings alone would never have contacted a single one of them. Four had just
handed credentials to a phishing page.

**Four real bugs, three of them found by running it rather than by testing it.**
The runner crashed committing an offset that did not exist. The PII check
matched surnames as bare substrings, so both employees named Lin got fallback
copy because "Lin" is inside "public link". A `uuid` column read back as a
`UUID` where the contract says `str`. And the one worth reading the commit for:
a 32-day-old credential submission, too old to contribute anything to the score
it was attached to, still told someone to reset their password *now*.

## Day 5 — serving ✅

- [x] Scorer projects each score into Redis: snapshot per employee, sorted set
      for ranking, folded into the pipeline that already wrote the band
- [x] FastAPI: score, timeline, ranking, department rollup, intervention
      history, the signal catalog, and the read audit log itself
- [x] Tenant scoping from the credential, never from the request
- [x] Read audit log in Postgres, written before the response
- [x] Dashboard: ranked list, drill-down, live refresh, no build step
- [x] 39 new tests (324 total), CI green

Verified against the running stack — 499 employees projected, API serving:

| Check | Result |
| --- | --- |
| Ranking | E0208 91.49, E0069 88.21, E0042 86.71 … |
| Emails anywhere in a 200-row ranking | 0 |
| No key / bad key / good key | 401 / 401 / 200 |
| Another tenant's employee | 404, byte-identical to a missing one |
| Path traversal in an employee id | 404 at the edge |
| Reads recorded | one row per drill-down, none for browsing |
| Departments | finance mean 31.5 / p90 75.5, 33 of 33 scored |

Two decisions carry the section. **Tenancy is a property of the credential** —
no endpoint takes a tenant, so there is no parameter to override, and a foreign
employee is indistinguishable from a missing one because a 403 would confirm
they exist. **The privacy gradient** — browsing the population is pseudonymous
and unaudited, looking one person up is named and audited. A tool that ranks
colleagues by how much of a liability they are will be opened for reasons that
have nothing to do with security.

Population analytics stop here on purpose. Departments folds live over the
projection, which is right for one company's headcount and the wrong shape for
a trend or a cohort — an online store that gets scanned stops being fast for the
queries it exists for. Those are the marts, on day 7.

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
