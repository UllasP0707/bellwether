"""Hourly ingest: vendors -> raw -> normalized -> scored -> interventions.

The stream stages are long-running services in production. Running them here as
bounded, scheduled tasks is what makes the whole pipeline demonstrable on a
laptop, and it is a real deployment shape for a low-volume tenant — each stage
exits once its topic has been quiet for a few seconds, having committed its
offsets, and the next run resumes exactly where it stopped.

The tasks are strictly sequential even though Kafka would let them overlap. A
score computed from a half-normalized batch is not wrong, only early, and it
would make every run's output depend on timing — which is the opposite of what
an orchestrator is for.
"""

from __future__ import annotations

import pendulum
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from common import DEFAULT_ARGS, cli

with DAG(
    dag_id="bellwether_ingest",
    description="Poll the vendors and drive the stream stages one cycle.",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    # One cycle at a time. Two concurrent runs would both consume the same
    # consumer groups, which is safe — the stages are idempotent — but would
    # make the run logs impossible to read and the durations meaningless.
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bellwether", "stream"],
) as dag:
    ingest = BashOperator(
        task_id="ingest",
        # Resumes from the persisted cursor, so a rerun fetches nothing it has
        # already seen rather than re-ingesting the vendor's history.
        bash_command=cli("ingest --to both --archive s3"),
    )

    normalize = BashOperator(
        task_id="normalize",
        bash_command=cli("normalize --idle-timeout 10"),
    )

    score = BashOperator(
        task_id="score",
        bash_command=cli("score-stream --idle-timeout 10"),
    )

    intervene = BashOperator(
        task_id="intervene",
        # The recency gate means a rerun over already-processed scores sends
        # nothing, and the ledger's uniqueness index means it could not send a
        # duplicate even if the gate were off.
        bash_command=cli("intervene --idle-timeout 10"),
    )

    ingest >> normalize >> score >> intervene
