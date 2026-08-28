"""Erasure and tokenization from the command line.

python -m bellwether.cli privacy erase   --employee E0042 --dry-run
python -m bellwether.cli privacy erase   --employee E0042 --yes
python -m bellwether.cli privacy verify  --employee E0042
python -m bellwether.cli privacy tokenize --value dana.moreau@acme.example
"""

from __future__ import annotations

from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from bellwether.config import settings
from bellwether.privacy import erasure
from bellwether.privacy.tokens import Tokenizer

app = typer.Typer(add_completion=False, help="Erasure and field-level tokenization.")
console = Console()


@app.command("erase")
def erase(
    employee: Annotated[str, typer.Option(help="Employee id to erase.")],
    dry_run: Annotated[bool, typer.Option(help="Count what would go, change nothing.")] = True,
    yes: Annotated[bool, typer.Option("--yes", help="Actually do it.")] = False,
    purge_audit: Annotated[
        bool, typer.Option(help="Also delete rows recording who looked at them.")
    ] = False,
    purge_interventions: Annotated[
        bool, typer.Option(help="Also delete what was sent to them.")
    ] = True,
) -> None:
    """Erase one person from every store that can name them.

    Dry run by default, and `--yes` is required to make it real. This is the
    only destructive command in the CLI and the only one that cannot be undone
    by rerunning a pipeline, so the default has to be the safe one.
    """
    config = settings()
    live = yes and not dry_run

    if yes and dry_run:
        console.print("[yellow]--yes given with --dry-run[/yellow]; pass --no-dry-run to proceed")

    result = erasure.erase(
        dsn=config.postgres_dsn,
        redis_url=config.redis_url,
        tenant_id=config.tenant_id,
        employee_id=employee,
        purge_audit=purge_audit,
        purge_interventions=purge_interventions,
        dry_run=not live,
    )

    table = Table(title=f"{'erased' if live else 'would erase'} {employee}", header_style="bold")
    table.add_column("store")
    table.add_column("rows", justify="right")
    table.add_row("employee dimension", f"{result.dimension_rows:,}")
    table.add_row("redis keys", f"{result.redis_keys:,}")
    table.add_row("ranking membership", f"{result.ranking_members:,}")
    for name, count in result.warehouse_rows.items():
        table.add_row(name, f"{count:,}")
    table.add_row("intervention ledger", f"{result.intervention_rows:,}")
    table.add_row("read audit log", f"{result.audit_rows:,}")
    console.print(table)

    # Printed every time, including on a dry run. A deletion report that lists
    # only what it removed reads as if it removed everything.
    console.print("[bold]kept on purpose:[/bold]")
    for note in result.retained:
        console.print(f"  [dim]-[/dim] {note}")

    if not live:
        console.print("\n[dim]dry run; nothing changed. add --no-dry-run --yes[/dim]")
        return

    check = erasure.verify(config.postgres_dsn, config.redis_url, config.tenant_id, employee)
    if check.clean:
        console.print("\n[green]verified: nothing resolves this employee[/green]")
    else:
        console.print("\n[red]erasure incomplete[/red]")
        for finding in check.findings:
            console.print(f"  {finding}")
        raise typer.Exit(1)


@app.command("verify")
def verify(
    employee: Annotated[str, typer.Option(help="Employee id to check.")],
) -> None:
    """Check that nothing still resolves a person.

    Re-queries every store rather than trusting what `erase` reported, because
    a deletion that reports its own success is checking that it ran and not
    that it worked.
    """
    config = settings()
    check = erasure.verify(config.postgres_dsn, config.redis_url, config.tenant_id, employee)

    if check.clean:
        console.print(f"[green]{employee} does not resolve anywhere[/green]")
        return
    console.print(f"[red]{employee} is still present:[/red]")
    for finding in check.findings:
        console.print(f"  {finding}")
    raise typer.Exit(1)


@app.command("tokenize")
def tokenize(
    value: Annotated[str, typer.Option(help="Value to tokenize.")],
    kind: Annotated[str, typer.Option(help="Field kind, mixed into the MAC.")] = "email",
) -> None:
    """Show the token a value produces under this tenant's key."""
    config = settings()
    try:
        tokenizer = Tokenizer.from_secret(config.tokenization_secret, config.tenant_id)
    except ValueError as err:
        raise typer.BadParameter(f"{err}; set BELLWETHER_TOKENIZATION_SECRET") from err

    console.print(f"{kind:12s} {value}")
    console.print(f"{'token':12s} [bold]tok_{tokenizer.token(value, kind)}[/bold]")
    console.print(
        "\n[dim]deterministic, keyed, and irreversible without the key. destroying "
        f"the key unlinks every token for tenant {config.tenant_id} at once.[/dim]"
    )
