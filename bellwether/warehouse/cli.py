"""Warehouse commands."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from bellwether.config import settings
from bellwether.warehouse.load import counts, load, read_table
from bellwether.warehouse.seeds import SEED, is_current, write

app = typer.Typer(add_completion=False, help="Land Spark output in Postgres for dbt.")
console = Console()

# Which Parquet directory feeds which table. Explicit rather than derived from
# directory names: a renamed output should fail loudly here rather than quietly
# load nothing.
SOURCES: dict[str, str] = {
    "data/parquet/rollups/daily_employee_signal": "raw_daily_employee_signal",
    "data/parquet/rollups/daily_population_signal": "raw_daily_population_signal",
    "data/parquet/scores": "raw_employee_score",
}


@app.command("load")
def load_all(
    root: Annotated[str, typer.Option(help="Repo-relative root for the Parquet paths.")] = ".",
    only: Annotated[str | None, typer.Option(help="Load a single table.")] = None,
) -> None:
    """Load Spark's Parquet output into the warehouse tables.

    Delete-then-insert per day, so running this twice over the same output is a
    no-op rather than a doubling — which is the property the daily DAG's
    backfill depends on.
    """
    config = settings()
    table = Table(title="warehouse load", header_style="bold")
    table.add_column("table")
    table.add_column("rows", justify="right")
    table.add_column("days", justify="right")
    table.add_column("total after", justify="right")

    for source, target in SOURCES.items():
        if only and target != only:
            continue
        path = Path(root) / source
        if not path.exists():
            console.print(f"[yellow]skipping {target}[/yellow]: nothing at {path}")
            continue

        result = load(config.postgres_dsn, target, read_table(path))
        total, days = counts(config.postgres_dsn, target)
        table.add_row(target, f"{result.rows:,}", f"{result.days:,}", f"{total:,} / {days}d")

    console.print(table)


@app.command()
def seed() -> None:
    """Regenerate the dbt signal-catalog seed from the Python catalog."""
    path = write()
    console.print(f"wrote [green]{path.relative_to(Path.cwd())}[/green] from the signal catalog")


@app.command("check-seed")
def check_seed() -> None:
    """Fail if the committed seed no longer matches the catalog."""
    if is_current():
        console.print("[green]seed matches the catalog[/green]")
        return
    console.print(f"[red]{SEED.name} is stale[/red]; run: warehouse seed")
    raise typer.Exit(1)
