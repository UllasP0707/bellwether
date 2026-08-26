"""Loading Spark's Parquet output into Postgres.

The lake is where events live and the warehouse is where questions get asked,
and they want different shapes. Spark writes columnar files partitioned by day
because a scan over a month should read a month; dbt wants tables it can join
and test. This is the seam between them, and it is deliberately dumb — no
transformation, no derivation, no business logic. Everything that shapes the
data happens either upstream in Spark or downstream in dbt, so there is never a
third place to look for where a number came from.

**Loads are delete-then-insert, scoped to the days present in the input.** Not
an upsert on the primary key: an upsert cannot remove a row that should no
longer exist, so reprocessing a day after fixing a parser bug would leave the
bad rows sitting next to the good ones and every count would be too high. A day
is the unit of reload because it is the unit Spark partitions by, and both are
`{{ ds }}` in the DAG that drives them.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

DDL = """
CREATE TABLE IF NOT EXISTS raw_daily_employee_signal (
    tenant_id   text        NOT NULL,
    employee_id text        NOT NULL,
    dt          date        NOT NULL,
    signal      text        NOT NULL,
    events      integer     NOT NULL,
    first_at    timestamptz NOT NULL,
    last_at     timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, employee_id, dt, signal)
);
CREATE INDEX IF NOT EXISTS raw_daily_employee_signal_dt_idx
    ON raw_daily_employee_signal (tenant_id, dt);

CREATE TABLE IF NOT EXISTS raw_daily_population_signal (
    tenant_id text    NOT NULL,
    dt        date    NOT NULL,
    signal    text    NOT NULL,
    events    integer NOT NULL,
    employees integer NOT NULL,
    PRIMARY KEY (tenant_id, dt, signal)
);

CREATE TABLE IF NOT EXISTS raw_employee_score (
    tenant_id         text             NOT NULL,
    employee_id       text             NOT NULL,
    dt                date             NOT NULL,
    score             double precision NOT NULL,
    band              text             NOT NULL,
    dominant_category text,
    events_considered integer          NOT NULL,
    as_of             timestamptz      NOT NULL,
    PRIMARY KEY (tenant_id, employee_id, dt)
);
"""


@dataclass(frozen=True)
class Loaded:
    table: str
    rows: int
    days: int

    def __str__(self) -> str:
        return f"{self.table}: {self.rows:,} rows across {self.days} day(s)"


# The column order each table is loaded in. Explicit rather than taken from the
# Parquet file: a Spark job that starts emitting columns in a different order
# would otherwise load them into the wrong fields silently, and every value
# would still be the right type.
COLUMNS: dict[str, tuple[str, ...]] = {
    "raw_daily_employee_signal": (
        "tenant_id",
        "employee_id",
        "dt",
        "signal",
        "events",
        "first_at",
        "last_at",
    ),
    "raw_daily_population_signal": ("tenant_id", "dt", "signal", "events", "employees"),
    "raw_employee_score": (
        "tenant_id",
        "employee_id",
        "dt",
        "score",
        "band",
        "dominant_category",
        "events_considered",
        "as_of",
    ),
}


def read_table(path: str | Path) -> list[dict[str, Any]]:
    """Read a Parquet directory into rows.

    pyarrow rather than Spark. The loader runs wherever the DAG runs and should
    not need a JVM to move a few thousand rows: requiring one would mean the
    orchestration image has to carry Spark purely to copy files into Postgres.
    """
    import pyarrow.parquet as pq

    return list(pq.read_table(str(path)).to_pylist())


def load(dsn: str, table: str, rows: list[dict[str, Any]], date_column: str = "dt") -> Loaded:
    """Replace the days present in `rows`, then insert them.

    One transaction, so a crash mid-load leaves the previous day's data intact
    rather than a half-deleted table. The alternative — delete outside the
    transaction — turns a failed load into missing data that nothing reports.
    """
    import psycopg

    if table not in COLUMNS:
        raise ValueError(f"unknown table {table!r}")
    columns = COLUMNS[table]

    if not rows:
        return Loaded(table, 0, 0)

    missing = [c for c in columns if c not in rows[0]]
    if missing:
        # Loudly, rather than inserting nulls. A Spark job that stops emitting a
        # column would otherwise load a table full of them, and every downstream
        # aggregate would be quietly wrong instead of absent.
        raise ValueError(f"{table}: input is missing {', '.join(missing)}")

    days = sorted({row[date_column] for row in rows})

    placeholders = ", ".join(["%s"] * len(columns))
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cur:
            cur.execute(DDL)
            cur.execute(
                f"DELETE FROM {table} WHERE {date_column} = ANY(%s)",
                (days,),
            )
            cur.executemany(
                f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
                [tuple(row.get(c) for c in columns) for row in rows],
            )
        connection.commit()

    return Loaded(table, len(rows), len(days))


def counts(dsn: str, table: str) -> tuple[int, int]:
    """Row count and distinct days. What a backfill is checked against."""
    import psycopg

    with psycopg.connect(dsn) as connection, connection.cursor() as cur:
        cur.execute(f"SELECT count(*), count(DISTINCT dt) FROM {table}")
        row = cur.fetchone()
    return (0, 0) if row is None else (int(row[0]), int(row[1]))
