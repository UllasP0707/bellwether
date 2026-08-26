"""Warehouse loader, catalog seed and retention.

The load tests are the ones that matter: the daily DAG's whole backfill story is
that running a day twice produces what running it once produced, and that is a
property of this loader rather than of Airflow.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from bellwether.scoring.catalog import CATALOG
from bellwether.warehouse import seeds
from bellwether.warehouse.load import COLUMNS, counts, load
from bellwether.warehouse.retention import prune_lake

NOW = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)


# --- the catalog seed ---------------------------------------------------------


def test_the_committed_seed_matches_the_catalog() -> None:
    """The marts price signals from this file. A stale copy is a wrong mart.

    Regenerating and comparing, rather than trusting somebody to remember, is
    the same discipline as the stream/batch parity test one layer out: the
    scoring model exists once, and everything else is derived from it.
    """
    assert seeds.is_current(), (
        "transform/seeds/signal_catalog.csv has drifted from the catalog; "
        "run: python -m bellwether.cli warehouse seed"
    )


def test_the_seed_covers_every_signal() -> None:
    rendered = seeds.render().splitlines()
    assert len(rendered) == len(CATALOG) + 1  # header


def test_the_seed_carries_no_weighting_argument() -> None:
    """Only the first sentence of a description. The rest argues about weights.

    Those paragraphs are written for somebody reading the source, and a column
    in a warehouse is one `select *` away from a dashboard.
    """
    for line in seeds.render().splitlines()[1:]:
        description = line.rsplit(",", 1)[-1]
        assert "." not in description, description


def test_the_seed_is_deterministic() -> None:
    """Sorted, so a regeneration is an empty diff rather than a reshuffle."""
    assert seeds.render() == seeds.render()
    signals = [line.split(",")[0] for line in seeds.render().splitlines()[1:]]
    assert signals == sorted(signals)


# --- retention -----------------------------------------------------------------


def make_partition(root: Path, day: date, files: int = 2) -> Path:
    partition = root / f"dt={day.isoformat()}"
    partition.mkdir(parents=True)
    for index in range(files):
        (partition / f"part-{index}.jsonl").write_text("{}\n")
    return partition


def test_retention_removes_only_what_is_past_the_horizon(tmp_path: Path) -> None:
    for age in (1, 10, 29, 31, 400):
        make_partition(tmp_path, (NOW - timedelta(days=age)).date())

    pruned = prune_lake(tmp_path, keep_days=30, now=NOW)

    assert pruned.lake_partitions == 2
    assert pruned.lake_files == 4
    remaining = sorted(p.name for p in tmp_path.iterdir())
    assert len(remaining) == 3


def test_retention_leaves_alone_what_it_does_not_understand(tmp_path: Path) -> None:
    """The failure mode has to be keeping too much, never deleting blindly."""
    make_partition(tmp_path, (NOW - timedelta(days=400)).date())
    (tmp_path / "checkpoints").mkdir()
    (tmp_path / "dt=not-a-date").mkdir()

    pruned = prune_lake(tmp_path, keep_days=30, now=NOW)

    assert pruned.lake_partitions == 1
    assert sorted(pruned.kept) == ["checkpoints", "dt=not-a-date"]
    assert (tmp_path / "checkpoints").exists()
    assert (tmp_path / "dt=not-a-date").exists()


def test_retention_on_a_missing_lake_is_not_an_error(tmp_path: Path) -> None:
    assert prune_lake(tmp_path / "nothing-here", keep_days=30, now=NOW).total == 0


def test_retention_is_measured_from_a_given_instant(tmp_path: Path) -> None:
    """`now` is a parameter so this is testable before it is trusted with data."""
    make_partition(tmp_path, date(2026, 6, 1))

    assert prune_lake(tmp_path, keep_days=30, now=datetime(2026, 6, 15, tzinfo=UTC)).total == 0
    assert prune_lake(tmp_path, keep_days=30, now=datetime(2026, 8, 15, tzinfo=UTC)).total == 1


# --- the loader ----------------------------------------------------------------

pytestmark_note = """The loader tests need a database; they are marked below."""


def rows_for(day: date, employees: int = 3) -> list[dict[str, object]]:
    return [
        {
            "tenant_id": "load-test",
            "employee_id": f"L{index:04d}",
            "dt": day,
            "signal": "phish_sim_clicked",
            "events": index + 1,
            "first_at": NOW,
            "last_at": NOW,
        }
        for index in range(employees)
    ]


@pytest.fixture
def dsn() -> str:
    import psycopg

    from bellwether.config import settings

    target = settings().postgres_dsn
    try:
        psycopg.connect(target, connect_timeout=2).close()
    except Exception as err:
        pytest.skip(f"no database: {type(err).__name__}")
    return target


@pytest.fixture
def clean(dsn: str) -> object:
    import psycopg

    yield None
    with psycopg.connect(dsn, autocommit=True) as connection, connection.cursor() as cur:
        cur.execute("DELETE FROM raw_daily_employee_signal WHERE tenant_id = 'load-test'")


@pytest.mark.postgres
@pytest.mark.usefixtures("clean")
def test_loading_the_same_day_twice_changes_nothing(dsn: str) -> None:
    """The property the whole backfill story rests on.

    Not an upsert — a delete-then-insert scoped to the days in the input, so a
    reprocess after fixing a parser bug removes the bad rows instead of leaving
    them beside the good ones.
    """
    day = date(2026, 7, 1)
    first = load(dsn, "raw_daily_employee_signal", rows_for(day))
    before = counts(dsn, "raw_daily_employee_signal")

    second = load(dsn, "raw_daily_employee_signal", rows_for(day))
    after = counts(dsn, "raw_daily_employee_signal")

    assert first.rows == second.rows == 3
    assert before == after


@pytest.mark.postgres
@pytest.mark.usefixtures("clean")
def test_reloading_a_day_removes_rows_that_are_no_longer_there(dsn: str) -> None:
    """An upsert could not do this, which is why the loader does not use one."""
    day = date(2026, 7, 2)
    load(dsn, "raw_daily_employee_signal", rows_for(day, employees=5))
    load(dsn, "raw_daily_employee_signal", rows_for(day, employees=2))

    import psycopg

    with psycopg.connect(dsn) as connection, connection.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM raw_daily_employee_signal "
            "WHERE tenant_id = 'load-test' AND dt = %s",
            (day,),
        )
        row = cur.fetchone()

    assert row is not None and row[0] == 2


@pytest.mark.postgres
@pytest.mark.usefixtures("clean")
def test_reloading_one_day_leaves_the_others_alone(dsn: str) -> None:
    """Scoped to the days present in the input, so a backfill is surgical."""
    monday, tuesday = date(2026, 7, 6), date(2026, 7, 7)
    load(dsn, "raw_daily_employee_signal", rows_for(monday, 3) + rows_for(tuesday, 3))
    load(dsn, "raw_daily_employee_signal", rows_for(tuesday, 1))

    import psycopg

    with psycopg.connect(dsn) as connection, connection.cursor() as cur:
        cur.execute(
            "SELECT dt, count(*) FROM raw_daily_employee_signal "
            "WHERE tenant_id = 'load-test' GROUP BY dt ORDER BY dt"
        )
        rows = dict(cur.fetchall())

    assert rows == {monday: 3, tuesday: 1}


@pytest.mark.postgres
def test_an_empty_input_is_not_a_truncation(dsn: str) -> None:
    """A Spark job that produced nothing must not wipe the table it feeds."""
    assert load(dsn, "raw_daily_employee_signal", []).rows == 0


def test_a_missing_column_fails_loudly() -> None:
    """Nulls would look like data. The load has to stop instead."""
    with pytest.raises(ValueError, match="missing"):
        load("postgresql://unused", "raw_daily_employee_signal", [{"tenant_id": "x"}])


def test_an_unknown_table_is_refused() -> None:
    with pytest.raises(ValueError, match="unknown table"):
        load("postgresql://unused", "raw_something_else", [{"dt": date(2026, 7, 1)}])


def test_every_loadable_table_has_a_column_order() -> None:
    """Explicit, so a Spark job reordering its output cannot silently misload."""
    from bellwether.warehouse.load import DDL

    for table in COLUMNS:
        assert f"CREATE TABLE IF NOT EXISTS {table}" in DDL
