"""Bellwether's entry point.

python -m bellwether.cli vendor                 # serve the mock upstream
python -m bellwether.cli ingest                 # connectors -> events.raw
python -m bellwether.cli normalize              # raw -> normalized
python -m bellwether.cli generate --help        # the synthetic data tools
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from bellwether.config import Topics, settings
from bellwether.generator.cli import app as generator_app

app = typer.Typer(add_completion=False, help="Bellwether: a human-risk platform.")
app.add_typer(generator_app, name="generate", help="Synthetic population and behaviour.")
console = Console()


@app.command()
def vendor(
    port: Annotated[int, typer.Option(help="Port to serve on.")] = 8900,
    size: Annotated[int, typer.Option(help="Population size.")] = 500,
    days: Annotated[int, typer.Option(help="Days of history to serve.")] = 30,
    seed: Annotated[int, typer.Option(help="Population seed.")] = 1337,
) -> None:
    """Serve the mock vendor API that the connectors poll."""
    import uvicorn

    from bellwether.vendor.app import create_app
    from bellwether.vendor.store import build_store

    console.print(f"building {days}d of history for {size} employees...")
    store = build_store(size=size, days=days, seed=seed)
    totals = {source.value: store.total(source) for source in store.pages}
    for source, count in sorted(totals.items()):
        console.print(f"  {source:20s} {count:>6,} records")

    console.print(f"serving on http://localhost:{port} (docs at /_docs)")
    uvicorn.run(create_app(store=store), host="127.0.0.1", port=port, log_level="warning")


@app.command()
def ingest(
    connector: Annotated[str, typer.Option(help="Connector name, or 'all'.")] = "all",
    vendor_url: Annotated[str, typer.Option(help="Mock vendor base URL.")] = (
        "http://localhost:8900"
    ),
    to: Annotated[str, typer.Option(help="Sink: lake, kafka, or both.")] = "kafka",
    archive: Annotated[str, typer.Option(help="Raw archive: s3, file, or none.")] = "s3",
    cursors: Annotated[str, typer.Option(help="Cursor store: postgres or memory.")] = "postgres",
    limit: Annotated[int, typer.Option(help="Page size.")] = 200,
    max_pages: Annotated[int, typer.Option(help="Pages per connector per run.")] = 1000,
    size: Annotated[int, typer.Option(help="Population size.")] = 500,
    seed: Annotated[int, typer.Option(help="Population seed.")] = 1337,
    lake: Annotated[Path, typer.Option(help="Local lake root.")] = Path("data/events"),
) -> None:
    """Poll the vendor APIs and publish canonical events."""
    from bellwether.connectors import CONNECTORS, EmployeeDirectory, VendorClient
    from bellwether.connectors.archive import FileArchive, NullArchive, RawArchive, S3Archive
    from bellwether.connectors.cursors import CursorStore, InMemoryCursorStore, PostgresCursorStore
    from bellwether.generator.cli import _build_sink
    from bellwether.generator.population import build_population

    config = settings()
    names = sorted(CONNECTORS) if connector == "all" else [connector]
    for name in names:
        if name not in CONNECTORS:
            raise typer.BadParameter(f"unknown connector {name!r}; have: {', '.join(CONNECTORS)}")

    directory = EmployeeDirectory(
        [m.employee for m in build_population(size=size, tenant_id=config.tenant_id, seed=seed)]
    )

    raw_archive: RawArchive
    match archive:
        case "s3":
            raw_archive = S3Archive(
                bucket=config.s3_bucket,
                endpoint_url=config.s3_endpoint,
                access_key=config.s3_access_key,
                secret_key=config.s3_secret_key,
            )
        case "file":
            raw_archive = FileArchive("data")
        case "none":
            raw_archive = NullArchive()
        case _:
            raise typer.BadParameter(f"unknown archive {archive!r}")

    cursor_store: CursorStore = (
        PostgresCursorStore(config.postgres_dsn) if cursors == "postgres" else InMemoryCursorStore()
    )

    sink = _build_sink(to, lake)
    table = Table(title="ingest", header_style="bold")
    for column, justify in [
        ("connector", "left"),
        ("pages", "right"),
        ("fetched", "right"),
        ("emitted", "right"),
        ("not relevant", "right"),
        ("unresolved", "right"),
        ("malformed", "right"),
        ("retries", "right"),
    ]:
        table.add_column(column, justify=justify)  # type: ignore[arg-type]

    for name in names:
        client = VendorClient(base_url=vendor_url)
        connector_instance = CONNECTORS[name](
            client=client,
            directory=directory,
            archive=raw_archive,
            sink=sink,
            cursors=cursor_store,
            tenant_id=config.tenant_id,
        )
        result = connector_instance.run(max_pages=max_pages, limit=limit)
        client.close()

        table.add_row(
            name,
            f"{result.pages:,}",
            f"{result.fetched:,}",
            f"[green]{result.emitted:,}[/green]",
            f"{result.unknown_event_type:,}",
            f"{result.unresolved_identity:,}"
            if not result.unresolved_identity
            else f"[yellow]{result.unresolved_identity:,}[/yellow]",
            f"{result.malformed:,}" if not result.malformed else f"[red]{result.malformed:,}[/red]",
            f"{client.stats.retries:,}",
        )

    sink.close()
    console.print(table)
    console.print(f"published to [bold]{Topics.RAW}[/bold] via {to}")


@app.command()
def normalize(
    max_messages: Annotated[int | None, typer.Option(help="Stop after N messages.")] = None,
    group: Annotated[str, typer.Option(help="Consumer group id.")] = "bellwether-normalizer",
    dedup: Annotated[str, typer.Option(help="Dedup store: redis or memory.")] = "redis",
    idle_timeout: Annotated[float, typer.Option(help="Stop after N idle seconds.")] = 5.0,
    from_beginning: Annotated[bool, typer.Option(help="Read from the earliest offset.")] = True,
) -> None:
    """Re-key events.raw onto events.normalized, deduplicating as it goes."""
    from bellwether.stream.dedup import DedupStore, InMemoryDedup, RedisDedup
    from bellwether.stream.normalizer import Normalizer
    from bellwether.stream.runner import RunnerOptions, run_normalizer

    config = settings()
    store: DedupStore = RedisDedup(config.redis_url) if dedup == "redis" else InMemoryDedup()

    console.print(f"normalizing {Topics.RAW} -> {Topics.NORMALIZED} (group {group})")
    stats = run_normalizer(
        bootstrap=config.kafka_bootstrap,
        normalizer=Normalizer(dedup=store),
        options=RunnerOptions(
            group_id=group,
            max_messages=max_messages,
            idle_timeout=idle_timeout,
            from_beginning=from_beginning,
        ),
    )

    table = Table(title="normalize", header_style="bold")
    table.add_column("outcome")
    table.add_column("count", justify="right")
    table.add_row("emitted", f"[green]{stats.emitted:,}[/green]")
    table.add_row("duplicate", f"{stats.duplicate:,}")
    table.add_row(
        "forwarded (unknown version)",
        f"{stats.forwarded_unknown_version:,}",
    )
    colour = "red" if stats.dead_lettered else "dim"
    table.add_row("dead lettered", f"[{colour}]{stats.dead_lettered:,}[/{colour}]")
    console.print(table)

    if stats.dead_lettered:
        console.print(f"inspect them: consume --topic {Topics.DLQ}")


if __name__ == "__main__":
    app()
