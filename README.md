# Bellwether

[![ci](https://github.com/UllasP0707/bellwether/actions/workflows/ci.yml/badge.svg)](https://github.com/UllasP0707/bellwether/actions/workflows/ci.yml)

A human-risk platform. It ingests employee security behaviour from four vendor
APIs, scores every person continuously, and sends at most one carefully worded
message to the people who need one — within seconds of the behaviour rather
than in next quarter's training report.

Most enterprise breaches start with a person, and the standard response —
annual training, quarterly phishing tests — is disconnected in both time and
content from the moment the risk was created. Bellwether is an attempt at the
other approach, built as a working scale model of the production data platform
it would take: real-time vendor integrations, Kafka streaming, Spark and
Airflow batch, a warehouse, a read API, and the operational surface that makes
any of it safe to run.

## The loop

```
   4 vendor APIs                                        Redis
   (pagination, 429s,     ┌──────────────────┐        (window + ranking)
    4 timestamp formats)  │  signal catalog  │              ▲
         │                │  23 signals      │              │
         ▼                │  weight, decay,  │        ┌─────┴──────┐
   ┌───────────┐          │  category        │        │   scorer   │──▶ risk.scores
   │ connector │          └────────┬─────────┘        └─────▲──────┘         │
   └─────┬─────┘                   │                        │                │
         │  events.raw     ┌───────┴────────┐        events.normalized       │
         ▼    (keyed by    │  score_events  │               ▲                ▼
   ┌───────────┐  vendor   │  one function  │        ┌──────┴─────┐   ┌──────────────┐
   │raw archive│   id)     └───────┬────────┘        │ normalizer │   │ intervention │
   │ S3/MinIO  │                   │                 │  re-keys   │   │  5 gates,    │
   └───────────┘                   │                 │  on person │   │  guardrails  │
         │                         │                 └────────────┘   └──────┬───────┘
         ▼                         ▼                                         │
   ┌───────────┐            ┌─────────────┐                                  ▼
   │  Spark    │───Parquet─▶│  Postgres   │──▶ dbt: 4 staging, 6 marts   interventions
   │  rollups  │            │  warehouse  │        42 tests                 (outbox)
   └───────────┘            └─────────────┘
         └──────────── Airflow: 3 DAGs, 3 pinned virtualenvs ────────────┘

   traceparent rides the Kafka headers, so one trace id spans all of it.
```

One signal catalog defines every behaviour's weight, decay half-life and risk
category. The streaming scorer and the Spark batch scorer are two evaluation
strategies over that one catalog and the same pure `score_events()`. That
constraint is the point of the project, and it is measured rather than
asserted.

## What it does, measured

Every number here came from running the thing. The commit that produced each
one says how, and [DESIGN.md](DESIGN.md) says why it is the number that
matters.

| | |
| --- | --- |
| **Stream vs. batch agreement** | **0 disagreements, largest delta 0.0** over 1,986 events — exact equality, not a tolerance |
| **Throughput ceiling** | **736 events/sec** per scorer instance, of which **92% is Redis round trips** |
| **One trace, producer → intervention** | **9 of 9** span all four services and three topics |
| Personas recovered from behaviour alone | 6 of 6, ranked correctly, `shadow_it` 71.5 to `vigilant` 2.1 |
| Pipeline, end to end | 7,789 events ingested → normalized → scored, 0 malformed, 0 dropped |
| Replaying the entire history | **0 messages sent**, 212 stale triggers refused |
| Interventions below the band threshold | 68 of 137 — reached only by signal triggers |
| Rebalance mid-run | 430 messages reprocessed, **scores byte-identical** |
| Another tenant's employee | 404, byte-identical to a genuinely missing one |
| Reprocessing a day | 6 warehouse tables, **all unchanged** |
| Erasing one person | 404 from the API, absent from the ranking, verified independently |
| Read path | 957 req/s point lookup; department rollup **flat at 60 req/s** |

## Five things worth reading the code for

**One catalog, two engines, proven equal.**
[`tests/test_score_parity.py`](tests/test_score_parity.py) replays one fixed
event log through the real stream consumer and the real Spark job at the same
instant and compares them person by person. It failed on its first run, on a
bug nothing else would have caught: Spark materialises `TimestampType` as a
*naive* datetime, so events written to the lake timezone-aware came back
without it. Raising was the lucky outcome — tolerant scoring would have
silently absorbed the session timezone into every decay calculation.

**Most of the intervention code is about not intervening.** Five gates, all
defaulting to silence, and a unique index on `(tenant, employee, trigger)` that
encodes *one behaviour, one message*. The type is deliberately not in that key:
it was, and a redelivered score then climbed a rung and inserted cleanly as a
different type — the same click producing a nudge and then a training
assignment.

**The load test corrected this project's own design doc.** DESIGN.md had
carried an open question since day 3: that rescoring the whole window per
message is O(window) and would bite first. Running the identical pipeline
against an in-memory store instead of Redis settled it — 9,282 events/sec
against 736 — so the bottleneck is three round trips and not the algorithm.
The remedy everyone would have reached for, an incremental decay update, would
have cost the stream/batch parity guarantee to buy 4%. See
[docs/LOAD_TEST.md](docs/LOAD_TEST.md).

**The prompt contains no personal data at all.** Not a token, not a surname,
not a given name: the model writes to a `{name}` placeholder and the desk
substitutes afterwards, before validation. That turned out to have a second
consequence nobody designed for — a brief with no person in it is
*low-cardinality*, so generated copy is cacheable by shape and total model
calls became a function of the signal catalog rather than of traffic. Which
matters, because generation measured 8 to 40 seconds a call.

**A privacy gradient across the read path, and an erasure path that admits its
own bound.** Browsing the population is pseudonymous and unaudited; looking one
person up is named and audited. Erasing someone is one row and a projection
drop — small only because events carry a token — and the first live run showed
it was not instant: the dimension is cached in-process, so the row was gone,
the score was gone, and a running API still held the name. The cache now
expires, and erasure is described as complete within five minutes rather than
immediately.

## Quickstart

```bash
make install        # venv + dependencies
make test           # 424 tests, no infrastructure required
make seed           # generate the employee population
make backfill       # 30 days of behaviour -> local lake
make score          # score one employee and show what drove the number
```

Everything above runs with no Docker. For the streaming path:

```bash
make up && make topics
make demo-reset     # 30 days of history, scored, ready
make demo           # the 90-second narrative, end to end
make serve          # read API + dashboard on :8800
```

Batch, warehouse and orchestration:

```bash
make parity         # the one that matters: stream and batch must agree
make batch          # Spark: lake -> Parquet -> rollups -> scores
make warehouse dbt  # load, then build and test the marts
make backfill-twice # run the same date twice; row counts must not move
```

Operations:

```bash
make observe        # prometheus, grafana, jaeger
make trace-demo     # one incident, followed across three topics
make contracts      # data-quality contracts for a day
make loadtest       # where this breaks, and what breaks first
make infra          # terraform validate + manifest checks
make erase          # dry-run erasure for one employee
```

| Where | What |
| --- | --- |
| http://localhost:8800/?key=localdev | The dashboard |
| http://localhost:3000/d/bellwether-overview | Grafana, 27 panels |
| http://localhost:16686 | Jaeger |
| http://localhost:8080 | Redpanda console |

## Layout

| Path | What lives there |
| --- | --- |
| [`bellwether/events/`](bellwether/events/) | Canonical contracts. The vocabulary everything downstream speaks. |
| [`bellwether/scoring/`](bellwether/scoring/) | The signal catalog and the one scoring function both engines call. |
| [`bellwether/connectors/`](bellwether/connectors/) | Four vendor integrations: cursors, backoff, identity resolution, raw archival. |
| [`bellwether/stream/`](bellwether/stream/) | Normalizer, scorer, the shared Kafka runner, the Redis online store. |
| [`bellwether/interventions/`](bellwether/interventions/) | Policy gates, the ledger, copy generation and the guardrails over it. |
| [`bellwether/api/`](bellwether/api/) | Read API, tenancy, the read audit log, the dashboard. |
| [`bellwether/batch/`](bellwether/batch/) | PySpark: Parquet, rollups, the batch scorer. |
| [`bellwether/warehouse/`](bellwether/warehouse/) | Loader, catalog seed generation, retention. |
| [`bellwether/obs/`](bellwether/obs/) | Metrics, traces, data-quality contracts. |
| [`bellwether/privacy/`](bellwether/privacy/) | Per-person erasure and field-level tokenization. |
| [`bellwether/loadtest/`](bellwether/loadtest/) | The harness behind `docs/LOAD_TEST.md`. |
| [`transform/`](transform/) | dbt: 4 staging models, 6 marts, 42 tests. |
| [`airflow/dags/`](airflow/dags/) | Ingest, daily batch, retention. |
| [`infra/`](infra/) | Terraform and Kubernetes. Validated in CI, never applied. |

## Documents

| | |
| --- | --- |
| [DESIGN.md](DESIGN.md) | Why it is built this way, what was rejected, and where it breaks. The long one. |
| [docs/LOAD_TEST.md](docs/LOAD_TEST.md) | Where the ceiling is and what sets it, including two bugs in the harness. |
| [docs/RUNBOOK.md](docs/RUNBOOK.md) | Organised by symptom, because that is what you have at 3am. |
| [docs/DEMO.md](docs/DEMO.md) | The 90-second narrative, with the script. |
| [docs/ROADMAP.md](docs/ROADMAP.md) | Eleven days, what each one produced and what it measured. |
| [infra/README.md](infra/README.md) | What is verified and what is not, in the first table. |

## Status

**Complete.** 68 commits, 17,000 lines of Python, 1,200 of Terraform, 424
tests. CI is green on lint, formatting, `mypy --strict`, the suite, a real
Postgres, a separate JDK-17 job for the parity comparison, and
`terraform validate`.

Every section of [DESIGN.md](DESIGN.md) is marked `[built]`, which was not true
until the last day and is the whole reason the marker exists.

**Two things are honestly incomplete.** The infrastructure has never been
applied — there is no AWS account attached to this project, and
[infra/README.md](infra/README.md) leads with that rather than leaving it to be
assumed. And the demo video is not in the repository:
[`scripts/demo.sh`](scripts/demo.sh) runs the whole narrative so that recording
it is a screen capture rather than a performance.

**Deliberately out of scope.** Multi-tenancy beyond a tenant column, a learned
risk model, real vendor credentials, SSO on the dashboard, a delivery worker
for the interventions outbox. Each is a paragraph in the design doc explaining
what it would take, which is more useful than a half-built version of any of
them.
