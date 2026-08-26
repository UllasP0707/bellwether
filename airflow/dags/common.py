"""Shared DAG furniture.

Every task is a `BashOperator` into one of the image's three virtualenvs rather
than a `PythonOperator` importing Bellwether into the scheduler. That is not
laziness: Airflow, PySpark and dbt cannot share dependency pins, and importing
the pipeline into the scheduler's own interpreter would couple a dbt upgrade to
whether the scheduler starts. Shelling into a pinned environment is the same
isolation a KubernetesPodOperator buys, without the cluster.
"""

from __future__ import annotations

import os

from airflow.models import DAG  # noqa: F401  (re-exported for the DAG modules)

REPO = os.environ.get("BELLWETHER_REPO", "/opt/bellwether")
BATCH_PYTHON = os.environ.get("BATCH_PYTHON", "/opt/batch/bin/python")
DBT = os.environ.get("DBT", "/opt/dbt/bin/dbt")

# Retries exist for transient failures — a broker rebalancing, a vendor 503 —
# and every task in these DAGs is idempotent, so a retry cannot double anything.
# That is the property that makes retrying safe rather than merely hopeful, and
# it is asserted by tests rather than assumed: connectors resume from a cursor,
# stream stages dedupe on event id, the warehouse loader replaces a day, and dbt
# rebuilds a model.
DEFAULT_ARGS = {
    "owner": "bellwether",
    "retries": 2,
    "retry_delay": __import__("datetime").timedelta(minutes=2),
    "depends_on_past": False,
    "email_on_failure": False,
}


def cli(command: str) -> str:
    """A Bellwether CLI invocation inside the batch environment."""
    return f"cd {REPO} && {BATCH_PYTHON} -m bellwether.cli {command}"


def dbt(command: str, target_date: str | None = None) -> str:
    """A dbt invocation inside the dbt environment.

    `DBT_PROFILES_DIR` points at the repo's own profile, which reads everything
    from the environment, so no credential is written into a DAG or an image.
    """
    variables = f" --vars '{{\"run_date\": \"{target_date}\"}}'" if target_date else ""
    return (
        f"cd {REPO}/transform && DBT_PROFILES_DIR={REPO}/transform "
        f"{DBT} {command}{variables}"
    )
