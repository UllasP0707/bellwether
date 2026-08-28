"""Running the load test.

python -m bellwether.cli load scoring        # no infrastructure needed
python -m bellwether.cli load window --store redis
python -m bellwether.cli load api
python -m bellwether.cli load all
"""

from __future__ import annotations

import platform
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from bellwether.config import settings
from bellwether.loadtest import scenarios
from bellwether.loadtest.harness import HEADERS, Result

app = typer.Typer(add_completion=False, help="Load test: where this breaks, and what breaks first.")
console = Console()


def _machine() -> str:
    """What the numbers were measured on.

    Printed with every run because a throughput number without a machine is
    not a measurement, it is a mood. Anyone reading `docs/LOAD_TEST.md` needs
    to know whether to expect their own numbers to match.
    """
    import os

    return f"{platform.machine()} {platform.system()} {platform.release()}, {os.cpu_count()} cpus"


def _show(title: str, results: list[Result]) -> None:
    table = Table(title=title, header_style="bold")
    for index, header in enumerate(HEADERS):
        table.add_column(header, justify="left" if index == 0 else "right")
    table.add_column("note")
    for result in results:
        row = list(result.row)
        note = result.note
        if result.errors:
            note = f"[red]{result.errors} errors[/red] {note}".strip()
        table.add_row(*row, note)
    console.print(table)


@app.command("scoring")
def scoring(
    repeats: Annotated[int, typer.Option(help="Rounds per window size.")] = 200,
) -> None:
    """Time the scoring function against growing windows. No infrastructure.

    The measurement that settles the open question in DESIGN.md: rescoring the
    whole window on every event is O(window), and this is the curve.
    """
    console.print(f"[dim]{_machine()}[/dim]")
    results = scenarios.scoring(repeats)
    _show("scoring cost by window size", results)

    # Cost per event should be flat if the work is linear in window size. It
    # is the ratio, not the absolute number, that says whether the algorithm
    # is the problem, and printing it saves the reader doing the division.
    first, last = results[0], results[-1]
    growth = last.timing.percentile(50) / max(first.timing.percentile(50), 1e-9)
    sizes = scenarios.WINDOW_SIZES[-1] / scenarios.WINDOW_SIZES[0]
    console.print(
        f"window grew {sizes:.0f}x, median rescore grew [bold]{growth:.0f}x[/bold] "
        f"({first.timing.percentile(50):.3f}ms -> {last.timing.percentile(50):.3f}ms)"
    )


@app.command("window")
def window(
    store: Annotated[str, typer.Option(help="Online store: redis or memory.")] = "redis",
    events: Annotated[int, typer.Option(help="Events to push through.")] = 2000,
) -> None:
    """Measure what per-event online state costs."""
    from bellwether.stream.store import EventWindow, InMemoryOnlineStore, RedisOnlineStore

    config = settings()
    target: EventWindow = (
        RedisOnlineStore(config.redis_url, tenant_id="loadtest", namespace="lt")
        if store == "redis"
        else InMemoryOnlineStore()
    )

    console.print(f"[dim]{_machine()}, store={store}[/dim]")
    _show(f"online store, {store}", scenarios.window_io(target, events))


@app.command("pipeline")
def pipeline(
    count: Annotated[int, typer.Option(help="Events to push through.")] = 2000,
    store: Annotated[str, typer.Option(help="Online store: redis or memory.")] = "redis",
) -> None:
    """Produce, normalize and score against the real broker.

    Needs the stack up and the employee dimension loaded. Consumers read from
    the end of the topic, so this measures these events rather than whatever
    history happens to be sitting in the log.
    """
    config = settings()
    console.print(f"[dim]{_machine()}[/dim]")
    results = scenarios.pipeline(
        config.kafka_bootstrap, config.redis_url, config.postgres_dsn, count, store
    )
    _show(f"pipeline, one consumer per stage, {store} online store", results)

    stages = [r for r in results if r.name in {"normalize", "score"}]
    if stages:
        slowest = min(stages, key=lambda r: r.rate)
        console.print(
            f"the ceiling is [bold]{slowest.name}[/bold] at {slowest.rate:,.0f} msg/s "
            f"per consumer instance"
        )


@app.command("api")
def api(
    url: Annotated[str, typer.Option(help="Base URL of a running API.")] = "http://localhost:8800",
    key: Annotated[str, typer.Option(help="API key.")] = "localdev",
    requests: Annotated[int, typer.Option(help="Requests per endpoint.")] = 500,
    concurrency: Annotated[int, typer.Option(help="Concurrent clients.")] = 16,
) -> None:
    """Hammer the read path and report percentiles."""
    console.print(f"[dim]{_machine()}, {concurrency} concurrent[/dim]")
    results = scenarios.api(url, key, requests, concurrency)
    _show(f"read path, {concurrency} concurrent clients", results)

    if any(r.errors for r in results):
        console.print("[red]some requests failed[/red]; percentiles above exclude nothing")


@app.command("all")
def run_all(
    url: Annotated[str, typer.Option(help="Base URL of a running API.")] = "http://localhost:8800",
    key: Annotated[str, typer.Option(help="API key.")] = "localdev",
) -> None:
    """Every scenario, in the order the results should be read."""
    console.print(f"[bold]{_machine()}[/bold]\n")

    _show("1. scoring cost by window size (no infrastructure)", scenarios.scoring())

    from bellwether.stream.store import InMemoryOnlineStore, RedisOnlineStore

    config = settings()
    _show("2a. online store, in memory", scenarios.window_io(InMemoryOnlineStore(), 2000))
    try:
        redis_store = RedisOnlineStore(config.redis_url, tenant_id="loadtest", namespace="lt")
        _show("2b. online store, redis", scenarios.window_io(redis_store, 2000))
    except Exception as err:  # pragma: no cover - redis is optional for this phase
        console.print(f"[yellow]redis unavailable:[/yellow] {str(err)[:100]}")

    try:
        _show("3. read path, 16 concurrent", scenarios.api(url, key, 500, 16))
    except Exception as err:  # pragma: no cover - the API may not be running
        console.print(f"[yellow]API unavailable at {url}:[/yellow] {str(err)[:100]}")
