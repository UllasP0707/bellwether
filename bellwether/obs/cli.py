"""Running the data contracts.

python -m bellwether.cli quality check --as-of 2026-08-25
python -m bellwether.cli quality history
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from bellwether.config import settings
from bellwether.obs import quality

app = typer.Typer(add_completion=False, help="Data-quality contracts.")
console = Console()


@app.command("check")
def check(
    as_of: Annotated[str, typer.Option(help="Day to check, YYYY-MM-DD.")] = "",
    baseline_days: Annotated[int, typer.Option(help="Trailing days to compare against.")] = (
        quality.BASELINE_DAYS
    ),
    fail: Annotated[bool, typer.Option(help="Exit non-zero if any check fails.")] = False,
) -> None:
    """Compare one day against the days before it.

    `--fail` is off by default and the daily DAG turns it on. Two different
    jobs: a human asking "what does today look like" wants to see the numbers
    whatever they say, and a scheduler wants the run to go red. Defaulting to
    the scheduler's behaviour would mean the interactive command exits 1 on a
    day somebody is trying to investigate.
    """
    config = settings()
    day = date.fromisoformat(as_of) if as_of else (datetime.now(UTC) - timedelta(days=1)).date()

    counts = quality.collect(config.postgres_dsn, day, config.tenant_id, baseline_days)
    if counts.rows == 0:
        # Distinct from every check passing, and the distinction matters: an
        # empty day passes all four trivially, which is exactly how a silent
        # ingestion failure gets a green run.
        console.print(f"[red]no rows for {day}[/red] — nothing to check, which is itself a finding")
        raise typer.Exit(1 if fail else 0)

    checks = quality.evaluate(counts)
    quality.record(config.postgres_dsn, day, checks)

    table = Table(title=f"data contracts for {day}", header_style="bold")
    table.add_column("check")
    table.add_column("value", justify="right")
    table.add_column("max", justify="right")
    table.add_column("detail")
    for result in checks:
        colour = "red" if result.failing else "green"
        table.add_row(
            result.name,
            f"[{colour}]{result.value:.4f}[/{colour}]",
            f"{result.threshold}",
            result.detail,
        )
    console.print(table)
    console.print(
        f"{counts.rows:,} rows, {counts.employees:,} employees, "
        f"{len(counts.signal_events)} signals, baseline of {len(counts.baseline_rows)} days"
    )

    failing = [c for c in checks if c.failing]
    if failing:
        console.print(
            f"[red]{len(failing)} contract(s) failing[/red]: " + ", ".join(c.name for c in failing)
        )
        if fail:
            raise typer.Exit(1)
    else:
        console.print("[green]all contracts hold[/green]")


@app.command("history")
def history(limit: Annotated[int, typer.Option(help="How many rows.")] = 20) -> None:
    """What the contracts have said recently."""
    import psycopg

    config = settings()
    with psycopg.connect(config.postgres_dsn) as connection, connection.cursor() as cur:
        cur.execute("SELECT to_regclass('data_quality_check')")
        row = cur.fetchone()
        if row is None or row[0] is None:
            console.print("[yellow]no checks recorded yet[/yellow]; run quality check first")
            raise typer.Exit(1)
        cur.execute(
            """
            SELECT dt, check_name, value, threshold, failing, detail
            FROM data_quality_check ORDER BY dt DESC, check_name LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()

    table = Table(title="contract history", header_style="bold")
    for column in ("day", "check", "value", "max", "detail"):
        table.add_column(column)
    for day, name, value, threshold, failing, detail in rows:
        colour = "red" if failing else "dim"
        table.add_row(str(day), name, f"[{colour}]{value:.4f}[/{colour}]", str(threshold), detail)
    console.print(table)
