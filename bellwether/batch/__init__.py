"""The batch path.

Two evaluation strategies over one signal catalog, and this is the second one.
Nothing here computes a risk score: `score.py` projects Spark rows onto the two
protocols `bellwether.scoring.score_events` declares and calls it, so a weight
change is one edit that both paths pick up.

`tests/test_score_parity.py` is what keeps that honest — it replays one fixed
event log through the real stream scorer and the real Spark job and asserts they
agree, employee by employee.

Nothing in this package imports PySpark at module scope, so the module is
importable without a JVM and the half of the parity suite that needs no Spark
runs everywhere.
"""

from bellwether.batch.lake import event_schema, read_events, read_parquet, write_parquet
from bellwether.batch.rollup import daily_population_counts, daily_signal_counts
from bellwether.batch.score import BatchEvent, BatchSubject, score_dataframe, score_rows
from bellwether.batch.session import s3a_options, spark_session

__all__ = [
    "BatchEvent",
    "BatchSubject",
    "daily_population_counts",
    "daily_signal_counts",
    "event_schema",
    "read_events",
    "read_parquet",
    "s3a_options",
    "score_dataframe",
    "score_rows",
    "spark_session",
    "write_parquet",
]
