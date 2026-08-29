"""Render the architecture diagrams in `docs/img/`.

Diagram as code, for the same reason everything else here is: a PNG somebody
hand-drew in a browser is stale the first time a component moves and nobody can
tell that it has. This regenerates from source.

    brew install graphviz
    python -m venv .venv-diagrams && .venv-diagrams/bin/pip install diagrams
    .venv-diagrams/bin/python scripts/architecture.py

Two diagrams rather than one. The runtime data flow and the AWS footprint answer
different questions -- "what happens to an event" and "what does this cost and
who can reach what" -- and a single picture that answers both answers neither.
"""

from __future__ import annotations

import pathlib

from diagrams import Cluster, Diagram, Edge
from diagrams.aws.analytics import ManagedStreamingForKafka
from diagrams.aws.compute import EKS
from diagrams.aws.database import RDS, ElasticacheForRedis
from diagrams.aws.management import Cloudwatch
from diagrams.aws.network import NATGateway, VPC
from diagrams.aws.security import KMS, IAMRole, SecretsManager
from diagrams.aws.storage import S3
from diagrams.onprem.analytics import Dbt, Spark
from diagrams.onprem.client import Users
from diagrams.onprem.database import Postgresql
from diagrams.onprem.inmemory import Redis
from diagrams.onprem.monitoring import Grafana, Prometheus
from diagrams.onprem.queue import Kafka
from diagrams.onprem.tracing import Jaeger
from diagrams.onprem.workflow import Airflow
from diagrams.programming.framework import Fastapi
from diagrams.programming.language import Python

OUT = pathlib.Path(__file__).resolve().parent.parent / "docs" / "img"

# Edges are coloured by what travels along them, not by which box they leave,
# because the thing worth tracing through this picture is one event.
STREAM = "#2563eb"  # the hot path: an event on its way to a decision
BATCH = "#7c3aed"  # the cold path: the same events, recomputed in bulk
STATE = "#0891b2"  # reads and writes against a store
OBS = "#94a3b8"  # telemetry, deliberately faint

GRAPH = {
    "fontsize": "15",
    "bgcolor": "white",
    "pad": "0.6",
    "splines": "spline",
    "nodesep": "0.6",
    "ranksep": "1.0",
}


def platform() -> None:
    """The runtime path: vendor API to the message a person reads."""
    with Diagram(
        "Bellwether — how one behaviour becomes one message",
        filename=str(OUT / "architecture"),
        show=False,
        direction="LR",
        outformat="png",
        graph_attr=GRAPH | {"fontsize": "20"},
    ):
        # Topics are deliberately *not* grouped into a Kafka cluster box. They
        # were, and every stage then had to reach back into the same box, which
        # turned a straight pipeline into a knot of crossing edges. Drawn
        # inline, the hot path reads left to right in one line.
        with Cluster("Vendor APIs\ncursors, 429s, 4 timestamp formats"):
            vendors = [
                Users("Okta"),
                Users("Google Workspace"),
                Users("Email gateway"),
                Users("Endpoint agent"),
            ]

        connector = Python("connector\nidentity resolution")
        raw = Kafka("events.raw\nkeyed by vendor id")
        normalizer = Python("normalizer\ndedupe + re-key")
        normalized = Kafka("events.normalized\nre-keyed on the person")
        scorer = Python("scorer\n30d decayed window")
        scores = Kafka("risk.scores\ncompacted")
        intervention = Python("intervention\n5 gates + guardrails")
        outbox = Kafka("interventions\noutbox")

        redis = Redis("online store\nwindow + ranking")
        api = Fastapi("read API\n+ dashboard")

        # The constraint the whole project is organised around: drawn once, and
        # pointed at from both engines rather than duplicated into each.
        with Cluster("The invariant"):
            catalog = Python(
                "signal catalog\n23 signals: weight,\nhalf-life, category\n+ score_events()"
            )

        with Cluster("Batch — the same events, recomputed"):
            lake = S3("lake\nParquet, partitioned")
            spark = Spark("batch scorer\nrollups")
            warehouse = Postgresql("warehouse\n+ intervention ledger")
            dbt = Dbt("dbt — 4 staging,\n6 marts, 42 tests")
            airflow = Airflow("Airflow\n3 DAGs")

        archive = S3("raw archive\nvendor payloads —\nthe only store that\nholds addresses")

        with Cluster("Operability"):
            prom = Prometheus("Prometheus\n25 metrics")
            graf = Grafana("Grafana\n27 panels, 9 alerts")
            jaeger = Jaeger("Jaeger — traceparent\nrides Kafka headers")

        # --- the hot path, one straight line ---
        vendors >> Edge(color=STREAM) >> connector
        connector >> Edge(color=STREAM) >> raw
        raw >> Edge(color=STREAM) >> normalizer
        normalizer >> Edge(color=STREAM) >> normalized
        normalized >> Edge(color=STREAM) >> scorer
        scorer >> Edge(color=STREAM) >> scores
        scores >> Edge(color=STREAM) >> intervention
        intervention >> Edge(color=STREAM, label="  1.8% of scores") >> outbox

        # --- state ---
        connector >> Edge(color=STATE, style="dashed") >> archive
        scorer >> Edge(color=STATE, style="dashed", label="  3 round trips = 92%\n  of the 1.36ms budget") >> redis
        intervention >> Edge(color=STATE, style="dashed", label="  ledger") >> warehouse
        # Tried `constraint=false` on these two to stop them dragging Redis
        # rightward. It was worse: freed from rank, the API drifted to the far
        # left and both edges then crossed the entire diagram. Left constrained.
        redis >> Edge(color=STATE, style="dashed") >> api
        warehouse >> Edge(color=STATE, style="dashed") >> api

        # --- one catalog, two engines: the parity guarantee ---
        catalog >> Edge(color=STREAM, style="dotted", label="  same pure function") >> scorer
        catalog >> Edge(color=BATCH, style="dotted") >> spark

        # --- the cold path ---
        raw >> Edge(color=BATCH, style="dashed", label="  archived") >> lake
        lake >> Edge(color=BATCH) >> spark
        spark >> Edge(color=BATCH) >> warehouse
        warehouse >> Edge(color=BATCH) >> dbt
        airflow >> Edge(color=BATCH, style="dotted") >> spark

        # --- telemetry ---
        scorer >> Edge(color=OBS, style="dotted") >> prom
        prom >> Edge(color=OBS, style="dotted") >> graf
        scorer >> Edge(color=OBS, style="dotted", label="  OTLP") >> jaeger


def aws() -> None:
    """The same system, deployed. Applied once for real, then destroyed."""
    with Diagram(
        "Bellwether on AWS — 102 resources, applied and verified",
        filename=str(OUT / "aws"),
        show=False,
        direction="TB",
        outformat="png",
        graph_attr=GRAPH | {"fontsize": "20"},
    ):
        users = Users("security team")

        with Cluster("VPC — 3 availability zones, private subnets"):
            nat = NATGateway("NAT\nsingle, flagged as\nthe wrong trade for prod")

            with Cluster("EKS — private API endpoint"):
                with Cluster("namespace: bellwether"):
                    pods = [
                        EKS("connector"),
                        EKS("normalizer"),
                        EKS("scorer\nKEDA on lag, max 12"),
                        EKS("intervention\nsingleton, Recreate"),
                    ]

            with Cluster("Managed data plane"):
                msk = ManagedStreamingForKafka("MSK\n3 brokers, IAM auth\nTLS only, :9098")
                rds = RDS("RDS Postgres 16\nencrypted, Multi-AZ in prod")
                cache = ElasticacheForRedis("ElastiCache\nreplicated, no persistence")

        with Cluster("Storage"):
            lake = S3("lake")
            archive = S3("raw archive\nthe only store holding\naddresses, not tokens")

        with Cluster("Keys and secrets"):
            kms_data = KMS("data key\nrotating")
            kms_tok = KMS("token key\nNOT rotating —\ndestroying it\ncrypto-shreds the lake")
            secrets = SecretsManager("tokenization secret\ncontainer only,\nnever the value")

        with Cluster("IAM — one role per component"):
            irsa = IAMRole("IRSA\nsub + aud bound to\nsystem:serviceaccount")

        cw = Cloudwatch("CloudWatch\n9 alarms")
        # Drawn as a node rather than wired to every pod: it applies to all of
        # them, and four more dotted arrows said nothing the label does not.
        VPC("default-deny NetworkPolicy\nso the IAM split is not\nhalf a control")
        api_note = Fastapi("read API\npseudonymous browse,\nnamed lookups audited")

        connector_pod, normalizer_pod, scorer_pod, intervention_pod = pods

        # Each pod is drawn touching only the stores its IAM role actually
        # permits, because that is the claim this diagram exists to make. The
        # first version drew every pod against every store, which is both
        # untrue and the exact opposite of the point: the scorer cannot reach
        # the raw archive, and the archive is the only store holding addresses.
        users >> Edge(color=STREAM) >> api_note

        for i, pod in enumerate(pods):
            pod >> Edge(color=STREAM, label="  per-topic auth" if i == 0 else "") >> msk

        scorer_pod >> Edge(color=STATE, style="dashed", label="  window") >> cache
        intervention_pod >> Edge(color=STATE, style="dashed", label="  ledger") >> rds
        connector_pod >> Edge(color=STATE, style="dashed", label="  vendor payloads") >> archive
        msk >> Edge(color=BATCH, style="dashed", label="  archived") >> lake

        # Labelled once rather than once per pod: `>> [list]` repeats the label
        # on every edge in the fan-out, which printed "11/11 verified" four
        # times across the top of the diagram.
        for i, pod in enumerate(pods):
            irsa >> Edge(color=OBS, style="dotted", label="  11/11 verified" if i == 0 else "") >> pod

        kms_data >> Edge(color=OBS, style="dotted") >> rds
        kms_data >> Edge(color=OBS, style="dotted") >> msk
        kms_tok >> Edge(color=OBS, style="dotted") >> secrets
        msk >> Edge(color=OBS, style="dotted") >> cw


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    platform()
    aws()
    print(f"wrote {OUT}/architecture.png and {OUT}/aws.png")
