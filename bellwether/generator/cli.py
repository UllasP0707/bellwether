"""Generator CLI.

    python -m bellwether.generator.cli population --size 500
    python -m bellwether.generator.cli backfill --days 30
    python -m bellwether.generator.cli live
    python -m bellwether.generator.cli incident --employee E0042 --scenario phish_credential_chain
    python -m bellwether.generator.cli score --employee E0042

Population size and seed are options on every command rather than persisted
state, because the population is a pure function of them — two commands with the
same seed see the same people without needing to agree through a file.
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from bellwether.config import Topics, settings
from bellwether.events.schema import SignalType
from bellwether.generator.population import build_population
from bellwether.generator.simulate import SCENARIOS, Simulator
from bellwether.generator.sinks import (
    FanoutSink,
    JsonlSink,
    KafkaSink,
    Sink,
    dump_population,
    load_events,
)
from bellwether.scoring import score_events

app = typer.Typer(add_completion=False, help="Bellwether synthetic behavior generator.")
console = Console()

SizeOpt = Annotated[int, typer.Option(help="Population size.")]
SeedOpt = Annotated[int, typer.Option(help="Population seed; fixes who is who.")]
LakeOpt = Annotated[Path, typer.Option(help="Local lake root.")]


def _build_sink(target: str, lake: Path) -> Sink:
    """Resolve a `--to` value to a sink."""
    bootstrap = settings().kafka_bootstrap
    match target:
        case "lake":
            return JsonlSink(lake)
        case "kafka":
            return KafkaSink(Topics.RAW, bootstrap)
        case "both":
            return FanoutSink(JsonlSink(lake), KafkaSink(Topics.RAW, bootstrap))
        case _:
            raise typer.BadParameter(f"unknown target {target!r}; use lake, kafka, or both")


@app.command()
def population(
    size: SizeOpt = 500,
    seed: SeedOpt = 1337,
    out: Annotated[Path, typer.Option(help="Where to write the dimension.")] = Path(
        "data/employees.json"
    ),
) -> None:
    """Build the employee population and write the dimension."""
    people = build_population(size=size, tenant_id=settings().tenant_id, seed=seed)
    dump_population(people, out)

    table = Table(title=f"population: {size} employees", header_style="bold")
    table.add_column("persona")
    table.add_column("count", justify="right")
    table.add_column("share", justify="right")
    for name, count in Counter(m.persona.name for m in people).most_common():
        table.add_row(name, str(count), f"{count / size:.1%}")
    console.print(table)

    hvt = sum(1 for m in people if m.employee.is_high_value_target)
    console.print(f"high-value targets: {hvt} ({hvt / size:.1%})")
    console.print(f"wrote {out}")


@app.command()
def backfill(
    days: Annotated[int, typer.Option(help="Days of history to generate.")] = 30,
    to: Annotated[str, typer.Option(help="Sink: lake, kafka, or both.")] = "lake",
    size: SizeOpt = 500,
    seed: SeedOpt = 1337,
    lake: LakeOpt = Path("data/events"),
) -> None:
    """Generate historical behavior as fast as possible."""
    people = build_population(size=size, tenant_id=settings().tenant_id, seed=seed)
    sim = Simulator(people, tenant_id=settings().tenant_id, seed=seed + 1)
    sink = _build_sink(to, lake)

    counts: Counter[str] = Counter()
    total = 0
    started = datetime.now(UTC)

    with console.status(f"generating {days}d for {size} employees...") as status:
        for event in sim.backfill(days=days):
            sink.write(event)
            counts[event.signal.value] += 1
            total += 1
            if total % 5000 == 0:
                status.update(f"{total:,} events...")
    sink.close()

    seconds = (datetime.now(UTC) - started).total_seconds()
    console.print(f"[green]{total:,}[/green] events in {seconds:.1f}s -> {to}")

    table = Table(title="top signals", header_style="bold")
    table.add_column("signal")
    table.add_column("count", justify="right")
    table.add_column("per employee/day", justify="right")
    for signal, count in counts.most_common(10):
        table.add_row(signal, f"{count:,}", f"{count / size / days:.3f}")
    console.print(table)


@app.command()
def live(
    to: Annotated[str, typer.Option(help="Sink: lake, kafka, or both.")] = "kafka",
    speed: Annotated[float, typer.Option(help="Simulated seconds per wall second.")] = 60.0,
    size: SizeOpt = 500,
    seed: SeedOpt = 1337,
    lake: LakeOpt = Path("data/events"),
) -> None:
    """Stream events in real time until interrupted."""
    people = build_population(size=size, tenant_id=settings().tenant_id, seed=seed)
    sim = Simulator(people, tenant_id=settings().tenant_id, seed=seed + 2)
    sink = _build_sink(to, lake)

    console.print(f"streaming to {to} at {speed:.0f}x. ctrl-c to stop.")
    total = 0
    try:
        for event in sim.live(rate_multiplier=speed):
            sink.write(event)
            total += 1
            if total % 25 == 0:
                console.print(f"  {total:,} events", end="\r")
    except KeyboardInterrupt:
        console.print(f"\nstopped after {total:,} events")
    finally:
        sink.close()


@app.command()
def incident(
    employee: Annotated[str, typer.Option(help="Employee id, e.g. E0042.")] = "E0042",
    scenario: Annotated[str, typer.Option(help=f"One of: {', '.join(SCENARIOS)}")] = (
        "phish_credential_chain"
    ),
    to: Annotated[str, typer.Option(help="Sink: lake, kafka, or both.")] = "kafka",
    size: SizeOpt = 500,
    seed: SeedOpt = 1337,
    lake: LakeOpt = Path("data/events"),
) -> None:
    """Inject a scripted incident for one employee."""
    people = build_population(size=size, tenant_id=settings().tenant_id, seed=seed)
    sim = Simulator(people, tenant_id=settings().tenant_id, seed=seed + 3)

    try:
        events = sim.incident(employee, scenario)
    except KeyError as err:
        raise typer.BadParameter(str(err)) from err

    sink = _build_sink(to, lake)
    for event in events:
        sink.write(event)
    sink.close()

    console.print(f"[yellow]{scenario}[/yellow] for {employee}:")
    for event in events:
        console.print(f"  {event.occurred_at:%H:%M:%S}  {event.signal.value}")


@app.command()
def score(
    employee: Annotated[str, typer.Option(help="Employee id to score.")] = "E0042",
    size: SizeOpt = 500,
    seed: SeedOpt = 1337,
    lake: LakeOpt = Path("data/events"),
) -> None:
    """Score one employee from the local lake.

    A shortcut past the streaming path: it proves the catalog and the scoring
    function produce a sensible, explainable number before any consumer exists,
    and it is the reference the stream/batch parity test compares against.
    """
    people = build_population(size=size, tenant_id=settings().tenant_id, seed=seed)
    member = next((m for m in people if m.employee.employee_id == employee), None)
    if member is None:
        raise typer.BadParameter(f"no such employee: {employee}")

    events = [e for e in load_events(lake) if e.employee_id == employee]
    if not events:
        console.print(f"no events for {employee}. run: backfill --to lake")
        raise typer.Exit(1)

    result = score_events(
        member.employee,
        events,
        as_of=datetime.now(UTC),
        lookback_days=settings().score_lookback_days,
    )

    console.print(
        f"\n[bold]{employee}[/bold]  {member.employee.department}/"
        f"{member.employee.seniority}"
        + ("  [red](high-value target)[/red]" if member.employee.is_high_value_target else "")
    )
    console.print(
        f"score [bold]{result.score}[/bold]  band [bold]{result.band.value}[/bold]  "
        f"from {result.events_considered} events in window"
    )
    if result.dominant_category:
        console.print(f"driven by: {result.dominant_category.value}")

    table = Table(title="contributions", header_style="bold")
    table.add_column("signal")
    table.add_column("category")
    table.add_column("n", justify="right")
    table.add_column("contribution", justify="right")
    for factor in sorted(result.factors, key=lambda f: f.contribution, reverse=True):
        colour = "red" if factor.contribution > 0 else "green"
        table.add_row(
            factor.signal,
            factor.category.value,
            str(factor.occurrences),
            f"[{colour}]{factor.contribution:+.2f}[/{colour}]",
        )
    console.print(table)


@app.command()
def catalog() -> None:
    """Print the signal catalog."""
    from bellwether.scoring.catalog import CATALOG

    table = Table(title="signal catalog", header_style="bold")
    table.add_column("signal")
    table.add_column("category")
    table.add_column("weight", justify="right")
    table.add_column("half-life (d)", justify="right")
    for signal in SignalType:
        spec = CATALOG[signal]
        colour = "green" if spec.is_mitigating else "red" if spec.weight else "dim"
        table.add_row(
            signal.value,
            spec.category.value,
            f"[{colour}]{spec.weight:+.1f}[/{colour}]",
            f"{spec.half_life_days:g}",
        )
    console.print(table)


if __name__ == "__main__":
    app()
