"""Daily batch: lake -> Parquet -> rollups -> scores -> warehouse -> marts.

Every task is parameterised on `{{ ds }}` and every one of them replaces rather
than appends, so running this DAG for the 3rd of August twice produces exactly
what running it once produced. That is the property the whole backfill story
rests on, and `make backfill-twice` measures it rather than asserting it.

It is also the job that keeps scores from going stale. The stream only rescores
somebody when they do something, so a person who has been quiet for a month
keeps whatever score they last earned; this recomputes the whole population at a
stated instant regardless of who was active.
"""

from __future__ import annotations

import pendulum
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from common import DEFAULT_ARGS, cli, dbt

with DAG(
    dag_id="bellwether_daily",
    description="Recompute the lake in Spark and rebuild the warehouse marts.",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    # Catchup on, unlike the ingest DAG. A gap here is a day of history missing
    # from the marts, and because every task replaces its day rather than
    # appending, filling the gap is safe to do automatically.
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bellwether", "batch"],
) as dag:
    to_parquet = BashOperator(
        task_id="lake_to_parquet",
        bash_command=cli("batch parquet"),
    )

    rollups = BashOperator(
        task_id="daily_rollups",
        bash_command=cli("batch rollup"),
    )

    # `--as-of` is the logical date, not now(). A run recovering last Tuesday
    # has to score last Tuesday's window, or the backfilled row is a copy of
    # today's answer wearing an old date — which is worse than a gap, because a
    # gap is visible.
    score = BashOperator(
        task_id="batch_score",
        bash_command=cli(
            "batch score --as-of '{{ ds }}T23:59:59+00:00' --out data/parquet/scores"
        ),
    )

    load = BashOperator(
        task_id="load_warehouse",
        bash_command=cli("warehouse load"),
    )

    # The seed is generated from the Python catalog, so this refuses to run
    # against a stale copy of the scoring weights rather than silently building
    # marts priced by last month's model.
    check_seed = BashOperator(
        task_id="check_catalog_seed",
        bash_command=cli("warehouse check-seed"),
    )

    seed = BashOperator(task_id="dbt_seed", bash_command=dbt("seed"))
    build = BashOperator(task_id="dbt_run", bash_command=dbt("run"))

    # Tests run after the build and are allowed to fail the DAG. A mart that
    # cannot pass its own assertions should not be the thing a security team
    # opens tomorrow morning.
    test = BashOperator(task_id="dbt_test", bash_command=dbt("test"))

    # Distributional checks, and the only task here that looks at more than one
    # day. It runs against the warehouse rather than the marts, and *after* the
    # load rather than after dbt, because the question is whether what arrived
    # resembles what usually arrives — which is answerable before anything is
    # modelled and stays answerable on a day dbt fails.
    #
    # `--fail` because this is the scheduler. The same command run by hand
    # defaults to reporting without exiting non-zero, since somebody
    # investigating a bad day wants the numbers rather than a shell error.
    contracts = BashOperator(
        task_id="data_contracts",
        bash_command=cli("quality check --as-of '{{ ds }}' --fail"),
    )

    to_parquet >> rollups >> score >> load
    load >> check_seed >> seed >> build >> test
    load >> contracts
