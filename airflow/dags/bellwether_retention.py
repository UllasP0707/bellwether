"""Daily retention.

Behavioural data about identifiable people should not survive because deleting
it was nobody's job. This is that job, on a schedule, reporting counts.

Separate from the daily batch DAG on purpose. A retention run that only happens
when a rollup succeeds is a retention policy that quietly lapses the week Spark
is broken, and "we kept it because the pipeline was down" is not a defence.
"""

from __future__ import annotations

import pendulum
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from common import DEFAULT_ARGS, cli

with DAG(
    dag_id="bellwether_retention",
    description="Enforce the stated horizons on the lake, the audit log and scores.",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    # No catchup. Deleting things older than a horizon is the same operation
    # whenever it runs, so replaying missed days would do the same work
    # repeatedly to no effect.
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bellwether", "retention"],
) as dag:
    BashOperator(
        task_id="enforce_retention",
        bash_command=cli("warehouse retention"),
    )
