"""Reading the lake, and turning it into Parquet.

The schema is declared rather than inferred. Inference means Spark reads the
whole input once just to guess, and it guesses badly for exactly the field that
matters most here: `occurred_at` comes back as a string, every downstream
comparison becomes lexical, and a job that windows on time silently windows on
text instead. Declaring it also means a source that starts emitting a new field
does not change the shape of yesterday's output.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession


def event_schema() -> object:
    """The subset of `BehaviorEvent` the batch path reads.

    Narrower than the full contract on purpose, and for the same reason
    `ScorableEvent` is narrow: a batch job that materialises fields nobody reads
    pays for them on every scan, and columnar storage makes the saving real.
    """
    from pyspark.sql.types import (
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("event_id", StringType(), nullable=False),
            StructField("tenant_id", StringType(), nullable=False),
            StructField("employee_id", StringType(), nullable=False),
            StructField("signal", StringType(), nullable=False),
            StructField("source", StringType(), nullable=True),
            StructField("occurred_at", TimestampType(), nullable=False),
            StructField("ingested_at", TimestampType(), nullable=True),
        ]
    )


def read_events(session: SparkSession, path: str) -> DataFrame:
    """Read the JSONL lake.

    Deduplicated on `event_id` on the way in. At-least-once delivery means the
    lake can hold the same vendor record more than once, and unlike the stream
    scorer — whose window is keyed by event id and therefore inert to repeats —
    a batch aggregation would happily count a duplicate twice. The two paths
    have to absorb redelivery in their own way for their answers to match.
    """
    return (
        session.read.schema(event_schema())
        .json(path)
        .where("employee_id IS NOT NULL AND signal IS NOT NULL")
        .dropDuplicates(["event_id"])
    )


def read_parquet(session: SparkSession, path: str) -> DataFrame:
    return session.read.parquet(path)


def write_parquet(events: DataFrame, path: str, mode: str = "overwrite") -> int:
    """Write events partitioned by event date.

    Partitioned on `occurred_at`, not on ingest date. A reprocess asking for
    "the 3rd of August" means the day the behaviour happened; partitioning by
    when the pipeline happened to see it would scatter one day's events across
    every partition a late-arriving source touched, and turn a partition prune
    into a full scan.
    """
    from pyspark.sql import functions as F

    dated = events.withColumn("dt", F.to_date("occurred_at"))
    dated.write.mode(mode).partitionBy("dt").parquet(path)
    return int(dated.count())
