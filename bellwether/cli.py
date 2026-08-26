"""Bellwether's entry point.

python -m bellwether.cli vendor                 # serve the mock upstream
python -m bellwether.cli ingest                 # connectors -> events.raw
python -m bellwether.cli normalize              # raw -> normalized
python -m bellwether.cli generate --help        # the synthetic data tools
"""

from __future__ import annotations

import time
from collections import Counter
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from bellwether.batch.cli import app as batch_app
from bellwether.config import Topics, settings
from bellwether.generator.cli import app as generator_app

app = typer.Typer(add_completion=False, help="Bellwether: a human-risk platform.")
app.add_typer(generator_app, name="generate", help="Synthetic population and behaviour.")
# Importable without PySpark: nothing under `batch` imports it at module scope,
# so the CLI still starts on a machine with no JVM and only fails if a batch
# command is actually run.
app.add_typer(batch_app, name="batch", help="The Spark batch path.")
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


@app.command("load-dimension")
def load_dimension(
    size: Annotated[int, typer.Option(help="Population size.")] = 500,
    seed: Annotated[int, typer.Option(help="Population seed.")] = 1337,
) -> None:
    """Load the employee dimension into Postgres.

    Everything downstream resolves identity and employee attributes through
    this, so it is the first thing to run against a fresh database.
    """
    from bellwether.dimension import PostgresEmployeeRepository
    from bellwether.generator.population import build_population

    config = settings()
    people = [
        m.employee for m in build_population(size=size, tenant_id=config.tenant_id, seed=seed)
    ]

    repo = PostgresEmployeeRepository(config.postgres_dsn, load=False)
    count = repo.upsert_many(people)
    repo.close()

    hvt = sum(1 for e in people if e.is_high_value_target)
    console.print(f"loaded [green]{count:,}[/green] employees ({hvt:,} high-value targets)")


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

    # Identity resolution reads the same dimension scoring does. Rebuilding the
    # population here instead would give the two halves of the pipeline
    # independent opinions about who exists.
    if cursors == "postgres":
        from bellwether.dimension import PostgresEmployeeRepository

        repo = PostgresEmployeeRepository(config.postgres_dsn, tenant_id=config.tenant_id)
        if not repo.all():
            raise typer.BadParameter("employee dimension is empty; run load-dimension first")
        directory = EmployeeDirectory(repo.all())
    else:
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


@app.command("score-stream")
def score_stream(
    max_messages: Annotated[int | None, typer.Option(help="Stop after N messages.")] = None,
    group: Annotated[str, typer.Option(help="Consumer group id.")] = "bellwether-scorer",
    state: Annotated[str, typer.Option(help="Online state: redis or memory.")] = "redis",
    idle_timeout: Annotated[float, typer.Option(help="Stop after N idle seconds.")] = 5.0,
    from_beginning: Annotated[bool, typer.Option(help="Read from the earliest offset.")] = True,
    lookback: Annotated[int, typer.Option(help="Scoring window in days.")] = 30,
) -> None:
    """Score events.normalized onto the compacted risk.scores topic."""
    from bellwether.dimension import PostgresEmployeeRepository
    from bellwether.stream.runner import RunnerOptions, run_scorer
    from bellwether.stream.scorer import Scorer
    from bellwether.stream.store import (
        EventWindow,
        InMemoryOnlineStore,
        RedisOnlineStore,
        ScoreState,
    )

    config = settings()
    employees = PostgresEmployeeRepository(config.postgres_dsn, tenant_id=config.tenant_id)
    if not employees.all():
        raise typer.BadParameter("employee dimension is empty; run load-dimension first")

    store: EventWindow | ScoreState
    if state == "redis":
        store = RedisOnlineStore(config.redis_url, tenant_id=config.tenant_id)
    else:
        store = InMemoryOnlineStore()

    scorer = Scorer(
        employees=employees,
        window=store,
        state=store,
        lookback_days=lookback,
    )

    console.print(
        f"scoring {Topics.NORMALIZED} -> {Topics.SCORES} "
        f"({len(employees.all()):,} employees, {lookback}d window, group {group})"
    )
    stats = run_scorer(
        scorer=scorer,
        bootstrap=config.kafka_bootstrap,
        options=RunnerOptions(
            group_id=group,
            max_messages=max_messages,
            idle_timeout=idle_timeout,
            from_beginning=from_beginning,
        ),
    )

    table = Table(title="score", header_style="bold")
    table.add_column("metric")
    table.add_column("value", justify="right")
    table.add_row("scored", f"[green]{stats.scored:,}[/green]")
    table.add_row("band changes", f"{stats.band_changes:,}")
    table.add_row(
        "unknown employee",
        f"{stats.unknown_employee:,}"
        if not stats.unknown_employee
        else f"[yellow]{stats.unknown_employee:,}[/yellow]",
    )
    table.add_row(
        "malformed",
        f"{stats.malformed:,}" if not stats.malformed else f"[red]{stats.malformed:,}[/red]",
    )
    console.print(table)

    if stats.scored:

        def human(ms: float) -> str:
            seconds = ms / 1000
            if seconds < 90:
                return f"{seconds:.2f}s"
            if seconds < 86400:
                return f"{seconds / 3600:.1f}h"
            return f"{seconds / 86400:.1f}d"

        console.print(
            f"ingest->score   p50 {human(stats.percentile(50, pipeline=True))}  "
            f"p95 {human(stats.percentile(95, pipeline=True))}  "
            f"p99 {human(stats.percentile(99, pipeline=True))}   [dim](the SLO)[/dim]"
        )
        console.print(
            f"behaviour->score p50 {human(stats.percentile(50))}  "
            f"p95 {human(stats.percentile(95))}  "
            f"p99 {human(stats.percentile(99))}   "
            f"[dim](on a backfill this is the age of the history, not a delay)[/dim]"
        )


@app.command()
def serve(
    port: Annotated[int, typer.Option(help="Port to serve on.")] = 8800,
    host: Annotated[str, typer.Option(help="Bind address.")] = "127.0.0.1",
    reload: Annotated[bool, typer.Option(help="Reload on code changes.")] = False,
) -> None:
    """Serve the read API and the dashboard.

    Reads the online projection in Redis, the dimension and ledger in Postgres.
    Nothing here touches Kafka: serving a request by scanning a topic is how a
    dashboard ends up slower than the pipeline behind it.
    """
    import uvicorn

    from bellwether.api import PostgresAudit, TenantContext, create_app, parse_keys
    from bellwether.dimension import PostgresEmployeeRepository
    from bellwether.interventions import PostgresLedger
    from bellwether.stream.store import RedisOnlineStore

    config = settings()
    principals = parse_keys(config.api_keys)
    if not principals:
        raise typer.BadParameter("no API keys configured; set BELLWETHER_API_KEYS")

    employees = PostgresEmployeeRepository(config.postgres_dsn, tenant_id=config.tenant_id)
    if not employees.all():
        raise typer.BadParameter("employee dimension is empty; run load-dimension first")

    # One Postgres connection per store, not a pool. psycopg serialises
    # concurrent use behind a lock, so this is correct but not concurrent — the
    # read path is bounded by how fast a security team clicks, and a pool is the
    # first thing to add if that ever stops being true.
    store = RedisOnlineStore(config.redis_url, tenant_id=config.tenant_id)
    api = create_app(
        tenants={
            config.tenant_id: TenantContext(
                scores=store,
                employees=employees,
                interventions=PostgresLedger(config.postgres_dsn),
                window=store,
            )
        },
        principals=principals,
        audit=PostgresAudit(config.postgres_dsn),
        lookback_days=config.score_lookback_days,
    )

    scored = store.scored_count()
    console.print(
        f"serving [bold]{len(employees.all()):,}[/bold] employees, [bold]{scored:,}[/bold] scored"
    )
    if not scored:
        console.print("[yellow]no scores projected yet[/yellow]; run score-stream first")
    for key, principal in principals.items():
        console.print(f"  key [dim]{key}[/dim] -> {principal.actor} @ {principal.tenant_id}")
    console.print(f"dashboard http://{host}:{port}/?key={next(iter(principals))}")
    console.print(f"docs      http://{host}:{port}/docs")

    uvicorn.run(api, host=host, port=port, reload=reload, log_level="warning")


@app.command()
def intervene(
    max_messages: Annotated[int | None, typer.Option(help="Stop after N messages.")] = None,
    group: Annotated[str, typer.Option(help="Consumer group id.")] = "bellwether-interventions",
    ledger: Annotated[str, typer.Option(help="Ledger: postgres or memory.")] = "postgres",
    copy: Annotated[str, typer.Option(help="Copy source: auto, template, or model.")] = "auto",
    max_trigger_age_hours: Annotated[
        int, typer.Option(help="How old the triggering behaviour may be.")
    ] = 48,
    cooldown_hours: Annotated[int, typer.Option(help="Per-type cooldown.")] = 72,
    min_spacing_hours: Annotated[
        int, typer.Option(help="Minimum gap between any two messages to one person.")
    ] = 24,
    weekly_cap: Annotated[int, typer.Option(help="Max interventions per employee per week.")] = 3,
    allow_manager: Annotated[
        bool, typer.Option(help="Permit the manager-notification rung.")
    ] = False,
    idle_timeout: Annotated[float, typer.Option(help="Stop after N idle seconds.")] = 5.0,
    from_beginning: Annotated[bool, typer.Option(help="Read from the earliest offset.")] = True,
) -> None:
    """Decide interventions from risk.scores and publish them to the outbox.

    Nothing here delivers a message. The interventions topic is an outbox; a
    delivery worker consuming it is what would actually reach a person, and it
    is deliberately not part of this repo.
    """
    import os

    from bellwether.dimension import PostgresEmployeeRepository
    from bellwether.interventions import (
        ClaudeCopywriter,
        Copydesk,
        InMemoryLedger,
        InterventionLedger,
        Policy,
        PostgresLedger,
    )
    from bellwether.interventions.copy import Copywriter
    from bellwether.interventions.handler import InterventionStage
    from bellwether.stream.runner import RunnerOptions, run_interventions

    config = settings()
    employees = PostgresEmployeeRepository(config.postgres_dsn, tenant_id=config.tenant_id)
    if not employees.all():
        raise typer.BadParameter("employee dimension is empty; run load-dimension first")

    store: InterventionLedger = (
        PostgresLedger(config.postgres_dsn) if ledger == "postgres" else InMemoryLedger()
    )

    # `auto` uses the model when a key is present and the templates otherwise.
    # Unset credentials are a configuration state, not an error: the static path
    # is a supported way to run this, not a degraded one.
    writer: Copywriter | None = None
    if copy in {"auto", "model"}:
        if os.environ.get("ANTHROPIC_API_KEY"):
            writer = ClaudeCopywriter()
        elif copy == "model":
            raise typer.BadParameter("--copy model needs ANTHROPIC_API_KEY set")

    policy = Policy(
        max_trigger_age_hours=max_trigger_age_hours,
        cooldown_hours=cooldown_hours,
        min_spacing_hours=min_spacing_hours,
        weekly_cap=weekly_cap,
        allow_manager_notification=allow_manager,
    )
    desk = Copydesk(model=writer)
    stage = InterventionStage(employees=employees, ledger=store, copydesk=desk, policy=policy)

    console.print(
        f"deciding {Topics.SCORES} -> {Topics.INTERVENTIONS} "
        f"(copy: {'claude + guardrails' if writer else 'templates'}, "
        f"trigger age {max_trigger_age_hours}h, cooldown {cooldown_hours}h, "
        f"spacing {min_spacing_hours}h, cap {weekly_cap}/week, "
        f"manager rung {'on' if allow_manager else 'off'})"
    )
    stats = run_interventions(
        stage=stage,
        bootstrap=config.kafka_bootstrap,
        options=RunnerOptions(
            group_id=group,
            max_messages=max_messages,
            idle_timeout=idle_timeout,
            from_beginning=from_beginning,
            commit_every=1,
        ),
    )

    table = Table(title="interventions", header_style="bold")
    table.add_column("outcome")
    table.add_column("count", justify="right")
    table.add_row("sent", f"[green]{stats.sent:,}[/green]")
    for name, count in sorted(stats.by_type.items(), key=lambda kv: -kv[1]):
        table.add_row(f"  {name}", f"{count:,}")
    table.add_row("suppressed", f"{stats.suppressed:,}")
    for reason, count in sorted(stats.by_reason.items(), key=lambda kv: -kv[1]):
        table.add_row(f"  {reason}", f"[dim]{count:,}[/dim]")
    if stats.unknown_employee:
        table.add_row("unknown employee", f"[yellow]{stats.unknown_employee:,}[/yellow]")
    if stats.malformed:
        table.add_row("malformed", f"[red]{stats.malformed:,}[/red]")
    console.print(table)

    copy_stats = desk.stats
    console.print(
        f"copy: {copy_stats.model_drafts:,} from the model, "
        f"{copy_stats.template_drafts:,} from templates, "
        f"{copy_stats.guardrail_rejections:,} rejected by guardrails, "
        f"{copy_stats.model_errors:,} generation failures"
    )
    if copy_stats.rejected_rules:
        broken = ", ".join(f"{r} x{n}" for r, n in sorted(copy_stats.rejected_rules.items()))
        console.print(f"[yellow]guardrails caught:[/yellow] {broken}")
    if copy_stats.last_resort:
        console.print(
            f"[red]{copy_stats.last_resort:,} templates failed their own guardrails[/red]"
        )

    # Suppression is the expected outcome for most scores, so a run where
    # nothing was suppressed usually means the policy is not being reached.
    if stats.sent and not stats.suppressed:
        console.print("[yellow]nothing was suppressed; check the policy is being applied[/yellow]")


@app.command()
def interventions(
    employee: Annotated[str | None, typer.Option(help="Show one employee's history.")] = None,
    limit: Annotated[int, typer.Option(help="How many to show.")] = 10,
) -> None:
    """Read the intervention ledger: what was sent, to whom, and why."""
    from bellwether.interventions import PostgresLedger

    config = settings()
    store = PostgresLedger(config.postgres_dsn)

    if employee is None:
        totals = store.totals(config.tenant_id)
        store.close()
        if not totals:
            console.print("[yellow]nothing sent yet[/yellow]; run intervene first")
            raise typer.Exit(1)
        table = Table(title="interventions sent", header_style="bold")
        table.add_column("type")
        table.add_column("count", justify="right")
        for name, count in sorted(totals.items(), key=lambda kv: -kv[1]):
            table.add_row(name, f"{count:,}")
        console.print(table)
        return

    history = store.history(config.tenant_id, employee, limit=limit)
    store.close()
    if not history:
        console.print(f"[yellow]nothing sent to {employee}[/yellow]")
        raise typer.Exit(1)

    for record in history:
        console.print(
            f"\n[bold]{record.created_at:%Y-%m-%d %H:%M}[/bold]  "
            f"[cyan]{record.type.value}[/cyan] via {record.channel.value}  "
            f"score {record.score:.1f} ({record.band.value})  "
            f"[dim]copy: {record.copy_source.value}[/dim]"
        )
        if record.trigger_signal:
            console.print(f"  triggered by [yellow]{record.trigger_signal.value}[/yellow]")
        console.print(f"  [bold]{record.subject}[/bold]")
        console.print(f"  {record.body}")


@app.command()
def scores(
    employee: Annotated[str | None, typer.Option(help="Show one employee.")] = None,
    top: Annotated[int, typer.Option(help="How many to rank.")] = 15,
    timeout: Annotated[float, typer.Option(help="Seconds to wait for messages.")] = 15.0,
) -> None:
    """Read the compacted risk.scores topic and rank the population."""
    import uuid as _uuid

    from confluent_kafka import Consumer, KafkaError

    from bellwether.events.scores import RiskScoreEvent

    config = settings()
    consumer = Consumer(
        {
            "bootstrap.servers": config.kafka_bootstrap,
            "group.id": f"bellwether-scores-{_uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([Topics.SCORES])

    latest: dict[str, RiskScoreEvent] = {}
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is None:
                continue
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                break
            payload = message.value()
            if payload is None:
                continue
            record = RiskScoreEvent.model_validate_json(payload)
            # Compaction keeps the newest per key, but a live topic still holds
            # older versions until it runs, so take the latest by as_of.
            existing = latest.get(record.employee_id)
            if existing is None or record.as_of >= existing.as_of:
                latest[record.employee_id] = record
    finally:
        consumer.close()

    if not latest:
        console.print(f"[yellow]no scores on {Topics.SCORES}[/yellow]; run score-stream first")
        raise typer.Exit(1)

    if employee:
        one = latest.get(employee)
        if one is None:
            raise typer.BadParameter(f"no score for {employee}")
        record = one
        console.print(
            f"\n[bold]{record.employee_id}[/bold]  score [bold]{record.score}[/bold]  "
            f"band [bold]{record.band.value}[/bold]  from {record.events_considered} events"
        )
        if record.dominant_category:
            console.print(f"driven by: {record.dominant_category.value}")
        factors = Table(title="top factors", header_style="bold")
        factors.add_column("signal")
        factors.add_column("n", justify="right")
        factors.add_column("contribution", justify="right")
        for factor in record.top_factors:
            factors.add_row(factor.signal, str(factor.occurrences), f"{factor.contribution:+.2f}")
        console.print(factors)
        return

    ranked = sorted(latest.values(), key=lambda r: r.score, reverse=True)
    table = Table(title=f"riskiest of {len(latest):,} scored employees", header_style="bold")
    table.add_column("employee")
    table.add_column("score", justify="right")
    table.add_column("band")
    table.add_column("driven by")
    table.add_column("events", justify="right")
    for record in ranked[:top]:
        colour = {"critical": "red", "high": "red", "elevated": "yellow"}.get(
            record.band.value, "dim"
        )
        table.add_row(
            record.employee_id,
            f"[{colour}]{record.score:.1f}[/{colour}]",
            record.band.value,
            record.dominant_category.value if record.dominant_category else "-",
            str(record.events_considered),
        )
    console.print(table)

    bands = Counter(r.band.value for r in latest.values())
    console.print(
        "  ".join(
            f"{band}: {bands.get(band, 0)}"
            for band in ("critical", "high", "elevated", "moderate", "low")
        )
    )


if __name__ == "__main__":
    app()
