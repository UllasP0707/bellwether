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
asserted — see below and [DESIGN.md](DESIGN.md).

## What it does, measured

Every number here comes from running the thing, and the commit that produced
each one says how.

| | |
| --- | --- |
| **Stream vs. batch agreement** | **0 disagreements, largest delta 0.0** over 1,986 events — exact equality, not a tolerance |
| Personas recovered from behaviour alone | 6 of 6, ranked correctly, `shadow_it` 71.5 to `vigilant` 2.1 |
| Pipeline, end to end | 7,789 events ingested → normalized → scored, 0 malformed, 0 dropped |
| ingest → score | p50 33.4s, p99 50.4s |
| Replaying the entire history | **0 messages sent**, 212 stale triggers refused |
| Interventions below the band threshold | 68 of 137 — reached only by signal triggers |
| Rebalance mid-run | 430 messages reprocessed, **scores byte-identical** |
| Another tenant's employee | 404, byte-identical to a genuinely missing one |
| Reprocessing a day | 6 warehouse tables, **all unchanged** |
| One trace, producer → intervention | **4 services, 3 topics, one trace id** |

## The four things worth reading the code for

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

**The prompt contains no personal data at all.** Not a token, not a surname,
not a given name: the model writes to a `{name}` placeholder and the desk
substitutes afterwards, before validation. That turned out to have a second
consequence nobody designed for — a brief with no person in it is
*low-cardinality*, so generated copy is cacheable by shape, and total model
calls became a function of the signal catalog rather than of traffic. Which
matters, because generation measured 8 to 40 seconds a call.

**A privacy gradient across the read path.** Browsing the population is
pseudonymous and unaudited; looking one person up is named and writes an audit
row before the response is built. A tool that ranks colleagues by how much of a
liability they are will be opened for reasons that have nothing to do with
security.

## Quickstart

```bash
make install        # venv + dependencies
make test           # 379 tests, no infrastructure required
make seed           # generate the employee population
make backfill       # 30 days of behaviour -> local lake
make score          # score one employee and show what drove the number
```

Everything above runs with no Docker. For the streaming path:

```bash
make up             # redpanda, postgres, redis, minio
make topics
make backfill-kafka
make demo-incident  # the scripted phishing chain for E0042
make serve          # read API + dashboard on :8800
```

For the batch, warehouse and orchestration paths:

```bash
make parity         # the one that matters: stream and batch must agree
make batch          # Spark: lake -> Parquet -> rollups -> scores
make warehouse dbt  # load, then build and test the marts
make backfill-twice # run the same date twice; row counts must not move
```

And the operational surface:

```bash
make observe        # prometheus, grafana, jaeger
make trace-demo     # one incident, followed across three topics
make contracts      # data-quality contracts for a day
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
| [`transform/`](transform/) | dbt: 4 staging models, 6 marts, 42 tests. |
| [`airflow/dags/`](airflow/dags/) | Ingest, daily batch, retention. |
| [`docs/`](docs/) | Roadmap, and the design notes worth reading. |

## Status

**Day 8 of 11.** 64 commits, 15,000 lines of Python, 379 tests, CI green on
lint, formatting, `mypy --strict`, the suite, a real Postgres, and a separate
JDK-17 job for the parity comparison.

[DESIGN.md](DESIGN.md) marks every section `[built]`, `[partly built]` or
`[designed]`, so it is always clear which parts of the argument are running and
which are still argument.

**Remaining** ([docs/ROADMAP.md](docs/ROADMAP.md)): the load test and where
this breaks (day 9); Terraform, Kubernetes manifests and the per-person
deletion path (day 10); the runbook and the demo (day 11).

**Deliberately out of scope.** Multi-tenancy beyond a tenant column, a learned
risk model, real vendor credentials, SSO on the dashboard, horizontal
autoscaling. Each is a paragraph in the design doc explaining what it would
take, which is more useful than a half-built version of any of them.
