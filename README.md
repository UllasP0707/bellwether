# Bellwether

A human-risk platform: ingest employee security behavior, score it in real time,
and intervene before the behavior becomes a breach.

Human error drives most enterprise security breaches, and the usual response —
annual training, quarterly phishing tests — is disconnected in both time and
content from the moment the risk actually occurs. Bellwether is an attempt at the
other approach: score behavior continuously, and intervene within seconds of the
behavior that created the exposure.

It is built as a working scale model of a production data platform: real-time API
integrations, Kafka streaming, Spark and Airflow batch pipelines, and the data
services that feed an application layer.

## The loop

```
connectors ──▶ bellwether.events.raw ──▶ normalizer ──▶ bellwether.events.normalized
                     │                                      │
                     │                                      ├──▶ stream scorer ──▶ bellwether.risk.scores
                     │                                      │         │
                     ▼                                      │         ▼
              raw lake (S3/MinIO)                           │   online features (Redis)
                     │                                      │         │
                     ▼                                      │         ▼
              Spark daily rollups ──▶ dbt marts             │   intervention engine ──▶ bellwether.interventions
                     │                                      │
                     └────────────── same signal catalog ───┘
```

One signal catalog defines every behavior's weight, decay, and risk category.
The streaming scorer and the Spark batch scorer both read it, so online and
offline scores cannot silently diverge. That constraint is the point of the
project; see [DESIGN.md](DESIGN.md).

## Quickstart

```bash
make install        # venv + dependencies
make up             # redpanda, postgres, redis, minio
make topics         # create topics with the right partition counts
make seed           # generate the employee population into postgres
make backfill       # 30 days of historical behavior into the lake + topics
make live           # real-time event trickle (leave running)
make demo-incident  # inject the scripted phishing chain for employee E0042
```

Then open the console at http://localhost:8080 (Redpanda) and the dashboard at
http://localhost:8000.

## Layout

| Path | What lives there |
| --- | --- |
| `bellwether/events/` | Canonical event contracts. The vocabulary everything downstream speaks. |
| `bellwether/scoring/` | Signal catalog and the shared scoring function (streaming + batch). |
| `bellwether/generator/` | Synthetic employee population and behavior simulator. |
| `bellwether/connectors/` | Source-specific ingestion (Okta, Google Workspace, Slack, email gateway). |
| `bellwether/stream/` | Kafka consumers: normalizer, scorer, intervention engine. |
| `bellwether/api/` | FastAPI read path serving scores and timelines to the dashboard. |
| `batch/` | PySpark jobs and dbt models. |
| `orchestration/` | Airflow DAGs. |
| `infra/` | Terraform for the AWS deployment. |
| `docs/` | Design doc, load-test results, runbook. |

## Status

Built and running: event contracts, signal catalog, employee population,
behavior simulator.

Not yet built: connectors, normalizer, stream scorer, feature store,
intervention engine, read API, Spark rollups, dbt marts, Airflow DAGs,
Terraform, load test. Tracked in [docs/ROADMAP.md](docs/ROADMAP.md).
