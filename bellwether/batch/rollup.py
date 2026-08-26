"""Daily rollups.

What the warehouse and the marts are built on: one row per employee per day per
signal, with the count and the day's contribution. Small enough to keep
indefinitely, detailed enough to reconstruct any aggregate a security team asks
for — by department, by cohort, by signal, by week — without going back to the
event log.

The rollup is **not** a daily score. Scores do not sum: they are a saturating
function of a decayed 30-day window, so a score for the 3rd is not something you
can add to a score for the 4th. Storing counts and letting the marts derive
means the aggregate stays additive, which is the property that makes a
`GROUP BY` over any slice of time correct.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame


def daily_signal_counts(events: DataFrame) -> DataFrame:
    """One row per (employee, day, signal).

    Grain is stated in the name because a rollup whose grain is ambiguous gets
    joined to something at a different grain, and the resulting double-count is
    the single most common way an aggregate becomes quietly wrong.
    """
    from pyspark.sql import functions as F

    return (
        events.withColumn("dt", F.to_date("occurred_at"))
        .groupBy("tenant_id", "employee_id", "dt", "signal")
        .agg(
            F.count("*").cast("int").alias("events"),
            F.min("occurred_at").alias("first_at"),
            F.max("occurred_at").alias("last_at"),
        )
        .orderBy("dt", "employee_id", "signal")
    )


def daily_population_counts(events: DataFrame) -> DataFrame:
    """One row per (day, signal): how much of each behaviour the company did.

    Cheap, and it is the series that makes signal-mix drift visible — a source
    that stops reporting looks exactly like a population that stopped
    misbehaving, and only the trend tells them apart.
    """
    from pyspark.sql import functions as F

    return (
        events.withColumn("dt", F.to_date("occurred_at"))
        .groupBy("tenant_id", "dt", "signal")
        .agg(
            F.count("*").cast("int").alias("events"),
            F.countDistinct("employee_id").cast("int").alias("employees"),
        )
        .orderBy("dt", "signal")
    )
