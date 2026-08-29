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

## Day 6 — batch ✅

- [x] PySpark: JSONL → Parquet partitioned by event date, daily rollups
- [x] Batch scorer over the lake using the same `score_events`
- [x] **`test_score_parity`**: one fixed log, both paths, asserted equal
- [x] A portable parity layer that needs no JVM and runs everywhere
- [x] Spark container (JDK 17) and a CI job that fails if parity skipped
- [x] 10 new tests (334 total), CI green

| Check | Result |
| --- | --- |
| Fixture | 120 employees, 1,986 events, 21 distinct signals |
| Scores that disagree | **0** |
| Largest absolute delta | **0.0** — exact, not a tolerance |
| Over the live lake (7,792 events) | 498 employees on both paths, max delta 0.01 |
| Spark ranking vs Redis projection | same top ten, same order |

The parity test paid for itself on its first run, twice.

**It failed immediately** on a bug nothing else would have caught: Spark
materialises `TimestampType` as a *naive* datetime, so events written to the
lake timezone-aware came back without it. Raising was the lucky outcome — had
scoring tolerated naive timestamps, every decay calculation in the batch path
would have silently absorbed the session timezone's offset.

**Then the live comparison found a real semantic divergence.** One employee,
whose only event was 33 days old, was scored by the stream and dropped by the
batch job. The batch job was right: a zero computed from no in-window events
claims somebody is low risk when the truth is that we have no data on them.

A third bug came out of running the suite in the Spark container, which is a
different machine on a different day —
`test_signals_reaching_the_pipeline_cover_the_catalog` had been passing on luck.
The simulator generates a quarter as much activity at weekends, and the vendor
fixture backfilled to *now*, so a seeded generator was still producing different
data every day.

## Day 7 — transform and orchestrate ✅

- [x] Parquet → Postgres loader, delete-then-insert scoped to the days loaded
- [x] Signal catalog generated into a dbt seed from the Python catalog
- [x] dbt: 4 staging models, 6 marts, PII dropped at the staging boundary
- [x] 42 dbt tests, including 5 singular ones
- [x] Retention with stated horizons per store, on its own schedule
- [x] 3 Airflow DAGs, each task shelling into a pinned virtualenv
- [x] Backfill run twice, row counts diffed
- [x] 22 new tests (346 total), CI green

| Check | Result |
| --- | --- |
| dbt build | 10 models, `PASS=10 ERROR=0` |
| dbt test | `PASS=42 ERROR=0` |
| Singular tests vs. corrupted rows | 3 injected faults → exactly 3 failures |
| DAG parse | 3 DAGs, 0 import errors |
| `bellwether_daily` end to end | 8 of 8 tasks succeeded |
| Retention | lake 31 → 22 partitions, audit and scores within horizon |
| **Same date run twice** | **6 tables, all unchanged** |

`assert_marts_do_not_reband` is the test to read. Band thresholds live in one
place — `RiskBand.of()` in Python — and the intervention policy, the API and the
dashboard all depend on that. A mart recomputing the boundary in SQL would make
the warehouse and the product disagree about who is critical, with both looking
right in isolation. The five singular tests were checked against deliberately
corrupted rows rather than assumed to work: a score whose band contradicts it,
an unpriced signal and a score of 140 fail exactly three of them and nothing
else.

The Airflow image carries **three virtualenvs**, which is the design rather than
a workaround: Airflow, PySpark and dbt cannot share dependency pins, so each
gets its own and the DAGs shell into them. Same isolation a
KubernetesPodOperator buys, minus the cluster.

## Day 8 — operability ✅

- [x] OpenTelemetry traces from producer through intervention, propagated in
      Kafka headers
- [x] 24 metrics in one declared surface: consumer lag, scoring latency,
      handler time, send rate, copy generation, API percentiles
- [x] Data-quality contracts: null rate, volume shift, signal-mix drift,
      late arrivals — run as their own task in the daily DAG
- [x] Prometheus, Grafana (27 panels) and Jaeger behind a compose profile
- [x] 9 alert rules saying what "unhealthy" means
- [x] 21 new tests (379 total), CI green

| Check | Result |
| --- | --- |
| One incident through the whole pipeline | **9 of 9 traces span all four services** |
| The chain that mattered | `produce(credentials_submitted)` → `normalizer(emitted)` → `scorer(scored)` → `intervention(sent)` |
| Contracts on a healthy day | all four pass |
| Contracts on a day the pipeline barely wrote | volume 0.99, drift 0.87, naming `file_shared_externally -65.2%` |
| dbt tests on that same day | all green — which is the argument for having these |
| API metric cardinality | route templates only; `E0042` and `E9999` are one series |

The trace is the day's headline. A connector fetching a record and the
intervention it eventually causes are four processes and three topics apart and
never overlap in time, so without context propagation there are four unrelated
traces and no mechanical answer to *why did this person get this message*.

Nothing is labelled by employee. That is a cardinality argument and a privacy
one: a metrics endpoint is the least protected surface a service exposes, and
"who is risky" is what this system is careful about everywhere else.

## Day 9 — load test ✅

- [x] Four scenarios, each isolating one suspect
- [x] Scoring cost curve, online-store cost, full pipeline, read path under
      concurrency
- [x] `docs/LOAD_TEST.md` with the numbers, the attribution and the fix
- [x] 11 new tests (390 total), CI green

| Check | Result |
| --- | --- |
| Ceiling | **736 events/sec** per scorer instance |
| What sets it | **three Redis round trips — 92% of the per-message budget** |
| Same run, in-memory store | 9,282 events/sec |
| Scoring cost per event | flat at 1.35 µs — linear, and not the bottleneck |
| Read path, point lookup | 957 req/s, p99 40 ms |
| Read path, department rollup | 60 req/s, **flat from two clients up** |

**The headline is a correction to DESIGN.md.** The document had carried an open
question since day 3 — that recomputing the whole window per message is
O(window) and would bite first. It does not: a realistic employee carries
fifteen events, which is under 4% of the budget. Changing only the online store
and rerunning settled it, and the remedy everyone would have reached for (an
incremental decay update) would have cost the stream/batch parity guarantee to
buy 4%.

**Two bugs were in the harness rather than the system**, both written up. An
end-to-end p50 of *minus 24 seconds*, because the accelerated simulator dates a
phishing chain up to 90 minutes ahead of the wall clock — the scorer now counts
and clamps future-dated events instead of feeding negatives into a histogram
whose first bucket starts at zero. And five identical runs reporting 807, 260,
783, 737 and 389 events/sec, because the timing included consumer-group join
and the Redis window was never cleared between runs. Fixed, three consecutive
runs gave 804, 845, 834.

## Day 10 — infrastructure and data protection ✅

- [x] Per-person erasure, with verification that re-queries every store
- [x] Field-level tokenization: keyed HMAC, deterministic, shreddable
- [x] Terraform: VPC, MSK, S3, RDS, ElastiCache, EKS, IAM, KMS, alarms
- [x] Kubernetes: 23 objects, KEDA scaling on lag, default-deny networking
- [x] `terraform validate` and manifest checks in CI
- [x] 29 new tests (419 total), CI green

| Check | Result |
| --- | --- |
| Erasing the top-ranked employee | 1 dimension row, 3 Redis keys, the ranking, 29 warehouse rows, 1 ledger row |
| The API afterwards | 404, and absent from the ranking |
| Independent verification | clean |
| A bystander | fully intact, 4 findings |
| Terraform | 51 resource blocks, `validate` **passes** |
| Terraform `apply` | **run later, against a real account** — 102 resources, one real bug found; see below |

Erasure is small only because of a day-1 decision: events carry a token and PII
lives in one table, so everything downstream keeps an identifier that resolves
to nobody.

**The live run found a real gap.** The API returned 404 — but it said "no score
yet" rather than "no such employee", because the dimension is an in-process
snapshot loaded at startup. The row was gone, the score was gone, and a running
process still held the name. The snapshot now expires, and that bound is a
privacy property rather than a cache setting: erasure is complete within five
minutes, not instantly, and saying so is better than claiming otherwise.

## Day 11 — the artifacts that get read ✅

- [x] `DESIGN.md` finished: every section `[built]`, plus operability and a
      scale-limits section written from measurements
- [x] `docs/RUNBOOK.md`: organised by symptom, because that is what you have
      at 3am
- [x] `docs/DEMO.md` and `scripts/demo.sh`: the 90-second narrative, runnable
- [x] README rewritten around what the system does

**The video is not in this repository.** `scripts/demo.sh` exists so recording
it is a screen capture rather than a performance — the script does the typing,
the pacing is a variable, and every number that appears comes from the system.

`scripts/demo_reset.sh` exists because the first rehearsal did not work. Days
of testing had left the demo employee pinned at 100.0 from 56 events, so the
incident moved nothing and "watch the band change" was false, and the
intervention beat reported 192 messages where the story says one. Neither was a
bug — both are what a long-lived environment looks like — and a demo that only
works on a machine nobody has used is one that fails in front of someone.

## After day 11 — the infrastructure, applied ✅

Day 10 shipped Terraform that had never met AWS, and day 11 said so in the
first table of `infra/README.md`. That gap is now closed: an account was
attached, the environment was created, verified, and destroyed.

| Check | Result |
| --- | --- |
| `terraform plan` against a live account | **83 to add, 0 errors** — every data source and AZ lookup resolved |
| `terraform apply` | **102 resources**, `Apply complete!` |
| MSK, EKS, RDS, ElastiCache | `ACTIVE` / `available`; MSK on port 9098, IAM auth, TLS both ways |
| Per-topic Kafka permissions | **11 of 11** correct under `iam simulate-principal-policy`, allows and denies |
| "The scorer cannot read the raw archive" | **denied** — as are the API and the batch role |
| IRSA | bound to the live OIDC issuer, with both `sub` and `aud` conditions |
| Service quotas | sufficient; no increase needed |
| Cost | ~$1.35/hour, about $1.50 for the exercise |

**The first apply failed, which is the point.** Creating the MSK broker log
group was denied against the data KMS key. That key had no `policy` argument,
so it took the AWS default — which delegates authorization to IAM *for
principals in this account*. Five of its six consumers reach it through an IAM
role and were fine. CloudWatch Logs encrypts as a **service principal**, which
that delegation does not cover, and has to be named in the key policy itself.

`terraform validate` had passed on this configuration for two weeks. No static
check finds that one; it needs a real `CreateLogGroup` call. `infra/README.md`
had predicted the *class* — "an IAM condition key could be spelled plausibly
and wrongly" — without being able to find the instance.

Two smaller things came out of the same run. `deletion_protection` was a
literal `true`, correct as a default and wrong as a literal: `terraform
destroy` fails on it partway through, and the documented fix was to go edit
`rds.tf` while holding a half-destroyed environment and a running meter. It is
now a variable with the same default and an explicit override. And the state
bucket cannot bootstrap itself, so `env/example.backend.hcl` is committed and
the real backend configs are gitignored — they embed an account id.

**Still not applied:** the Kubernetes manifests. The EKS API endpoint is
private by design, and opening it to the internet to demonstrate a `kubectl
apply` would undo the argument `eks.tf` makes for closing it.

## Deliberately out of scope

Multi-tenancy beyond a tenant column; a learned risk model; real vendor API
credentials; SSO on the dashboard; horizontal autoscaling. Each is a paragraph
in the design doc explaining what it would take — which is more useful than a
half-built version of any of them.
