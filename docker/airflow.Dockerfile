# Orchestration.
#
# Three Python environments in one image, deliberately.
#
# Airflow, PySpark and dbt cannot share a virtualenv: dbt-core pins
# `pathspec<0.13` and `protobuf>=6`, Airflow pins neither the same way, and
# resolving all three together produces either a failure or a set of versions
# nobody tested. Installing each into its own venv and having the DAGs shell
# into them is not a workaround — it is the same isolation a KubernetesPodOperator
# buys, minus the cluster, and it means a dbt upgrade cannot break the scheduler.
#
#   /opt/batch  bellwether + pyspark  (needs the JRE below)
#   /opt/dbt    dbt-core + dbt-postgres
#   airflow's own environment stays untouched
FROM apache/airflow:2.10.5-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    # The airflow user cannot write to /opt, so the venv directories are created
    # and handed over here. Building them as root instead would leave the
    # scheduler unable to `pip install` into its own tooling later.
    && mkdir -p /opt/batch /opt/dbt \
    && chown airflow:root /opt/batch /opt/dbt
USER airflow

COPY requirements.txt requirements-spark.txt requirements-transform.txt /tmp/reqs/

RUN python -m venv /opt/batch \
    && /opt/batch/bin/pip install --no-cache-dir -q -r /tmp/reqs/requirements-spark.txt

RUN python -m venv /opt/dbt \
    && /opt/dbt/bin/pip install --no-cache-dir -q -r /tmp/reqs/requirements-transform.txt

ENV PYTHONPATH=/opt/bellwether \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags \
    BELLWETHER_REPO=/opt/bellwether \
    BATCH_PYTHON=/opt/batch/bin/python \
    DBT=/opt/dbt/bin/dbt
