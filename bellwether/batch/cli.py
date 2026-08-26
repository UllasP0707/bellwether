"""Batch commands.

A separate Typer app from the main CLI so importing `bellwether.cli` never
imports PySpark. Everything here is run inside the Spark container:

    docker compose run --rm spark python -m bellwether.cli batch score
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(add_completion=False, help="The Spark batch path.")
console = Console()

LAKE = "data/events"
PARQUET = "data/parquet/events"


def _as_of(value: str | None) -> datetime:
    """Parse the evaluation instant, defaulting to now.

    Exposed as an option rather than hardcoded to `now()` for the same reason
    `score_events` takes it as a parameter: recomputing last Tuesday means
    scoring *as of* last Tuesday, and a job that can only score the present
    cannot be used to reprocess anything.
    """
    if value is None:
        return datetime.now(UTC)
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


@app.command()
def parquet(
    lake: Annotated[str, typer.Option(help="JSONL lake root.")] = LAKE,
    out: Annotated[str, typer.Option(help="Parquet output root.")] = PARQUET,
) -> None:
    """Convert the JSONL lake to Parquet, partitioned by event date."""
    from bellwether.batch.lake import read_events, write_parquet
    from bellwether.batch.session import spark_session

    session = spark_session(app="bellwether-parquet")
    try:
        events = read_events(session, f"{lake}/*/*.jsonl")
        raw = events.count()
        written = write_parquet(events, out)
        console.print(
            f"read [bold]{raw:,}[/bold] events, wrote [green]{written:,}[/green] to {out}"
        )
    finally:
        session.stop()


@app.command()
def rollup(
    source: Annotated[str, typer.Option(help="Parquet events root.")] = PARQUET,
    out: Annotated[str, typer.Option(help="Rollup output root.")] = "data/parquet/rollups",
) -> None:
    """Daily per-employee and per-population signal counts."""
    from bellwether.batch.lake import read_parquet
    from bellwether.batch.rollup import daily_population_counts, daily_signal_counts
    from bellwether.batch.session import spark_session

    session = spark_session(app="bellwether-rollup")
    try:
        events = read_parquet(session, source)
        per_employee = daily_signal_counts(events)
        per_population = daily_population_counts(events)

        per_employee.write.mode("overwrite").parquet(f"{out}/daily_employee_signal")
        per_population.write.mode("overwrite").parquet(f"{out}/daily_population_signal")

        console.print(
            f"employee-day-signal rows [bold]{per_employee.count():,}[/bold], "
            f"population-day-signal rows [bold]{per_population.count():,}[/bold] -> {out}"
        )
    finally:
        session.stop()


@app.command()
def score(
    lake: Annotated[str, typer.Option(help="JSONL lake root.")] = LAKE,
    as_of: Annotated[str | None, typer.Option(help="Evaluation instant, ISO-8601.")] = None,
    lookback: Annotated[int, typer.Option(help="Scoring window in days.")] = 30,
    out: Annotated[str | None, typer.Option(help="Write scores here as Parquet.")] = None,
    top: Annotated[int, typer.Option(help="How many to show.")] = 10,
) -> None:
    """Score the whole lake with the same function the stream uses."""
    from bellwether.batch.lake import read_events
    from bellwether.batch.score import score_dataframe
    from bellwether.batch.session import spark_session
    from bellwether.config import settings
    from bellwether.dimension import PostgresEmployeeRepository

    config = settings()
    repo = PostgresEmployeeRepository(config.postgres_dsn, tenant_id=config.tenant_id)
    people = repo.all()
    repo.close()
    if not people:
        raise typer.BadParameter("employee dimension is empty; run load-dimension first")

    instant = _as_of(as_of)
    session = spark_session(app="bellwether-score")
    try:
        events = read_events(session, f"{lake}/*/*.jsonl")
        scored = score_dataframe(session, events, people, as_of=instant, lookback_days=lookback)
        scored.cache()

        rows = scored.collect()
        if out:
            # `dt` is added here rather than in `score_dataframe` because it is
            # a storage concern: the warehouse loads and reloads by day, and the
            # scoring function has no opinion about how its output is filed.
            from pyspark.sql import functions as F

            scored.withColumn("dt", F.to_date("as_of")).write.mode("overwrite").parquet(out)

        console.print(
            f"scored [bold]{len(rows):,}[/bold] of {len(people):,} employees "
            f"as of {instant:%Y-%m-%d %H:%M} UTC over a {lookback}d window"
        )

        table = Table(title="riskiest, computed in Spark", header_style="bold")
        table.add_column("employee")
        table.add_column("score", justify="right")
        table.add_column("band")
        table.add_column("driven by")
        table.add_column("events", justify="right")
        for row in sorted(rows, key=lambda r: -r["score"])[:top]:
            colour = {"critical": "red", "high": "red", "elevated": "yellow"}.get(
                row["band"], "dim"
            )
            table.add_row(
                row["employee_id"],
                f"[{colour}]{row['score']:.1f}[/{colour}]",
                row["band"],
                row["dominant_category"] or "-",
                str(row["events_considered"]),
            )
        console.print(table)

        counts: dict[str, int] = {}
        for row in rows:
            counts[row["band"]] = counts.get(row["band"], 0) + 1
        console.print(
            "  ".join(
                f"{band}: {counts.get(band, 0)}"
                for band in ("critical", "high", "elevated", "moderate", "low")
            )
        )
        if out:
            console.print(f"written to {out}")
    finally:
        session.stop()
