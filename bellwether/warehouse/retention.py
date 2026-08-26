"""Retention.

Behavioural data about identifiable people should not be kept because deleting
it was nobody's job. Every store here has a stated horizon and a function that
enforces it, so retention is a scheduled task with a row count rather than a
paragraph in a policy document.

Horizons differ by what the data is *for*, not by how big it is:

- **Raw lake partitions** are a replay buffer for connector bugs. Thirty days is
  long enough to notice a parser is wrong and reprocess; past that they are
  duplicate copies of events that already exist downstream.
- **The read audit log** outlives the data it describes. Somebody asking "who
  looked at me last quarter" needs an answer after the score they looked at is
  gone, so this is kept the longest of anything here.
- **Batch score snapshots** are recomputable from the lake, so they are the one
  thing that can be dropped freely.

Deliberately *not* here: the employee dimension, which is deletion rather than
retention and belongs with the per-person erasure path (day 10), and Kafka
topics, which enforce their own retention and should not have a second system
racing them to it.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

# The dt=YYYY-MM-DD convention both the lake writer and Spark's partitioning use.
_PREFIX = "dt="


@dataclass
class Pruned:
    """What retention removed. Counted, because a silent delete is indefensible."""

    lake_partitions: int = 0
    lake_files: int = 0
    audit_rows: int = 0
    score_rows: int = 0
    kept: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.lake_partitions + self.audit_rows + self.score_rows


def _partition_date(path: Path) -> date | None:
    if not path.name.startswith(_PREFIX):
        return None
    try:
        return date.fromisoformat(path.name[len(_PREFIX) :])
    except ValueError:
        return None


def prune_lake(root: str | Path, keep_days: int = 30, now: datetime | None = None) -> Pruned:
    """Delete lake partitions older than `keep_days`.

    `now` is a parameter for the same reason it is everywhere else in this
    codebase: a retention job that can only ever mean "today" cannot be tested,
    and the first time anyone verifies it is against real data they cannot get
    back.

    A directory whose name is not a parsable `dt=` partition is left alone. The
    job's failure mode has to be keeping too much, never deleting something it
    did not understand.
    """
    result = Pruned()
    base = Path(root)
    if not base.exists():
        return result

    cutoff = ((now or datetime.now(UTC)) - timedelta(days=keep_days)).date()
    for child in sorted(base.iterdir()):
        if not child.is_dir():
            continue
        partition = _partition_date(child)
        if partition is None:
            result.kept.append(child.name)
            continue
        if partition >= cutoff:
            continue
        result.lake_files += sum(1 for _ in child.rglob("*") if _.is_file())
        shutil.rmtree(child)
        result.lake_partitions += 1
    return result


def prune_audit(dsn: str, keep_days: int = 400, now: datetime | None = None) -> int:
    """Delete read-audit rows older than `keep_days`. Returns rows removed."""
    import psycopg

    cutoff = (now or datetime.now(UTC)) - timedelta(days=keep_days)
    with psycopg.connect(dsn, autocommit=True) as connection, connection.cursor() as cur:
        cur.execute("SELECT to_regclass('score_read_audit')")
        row = cur.fetchone()
        if row is None or row[0] is None:
            return 0
        cur.execute("DELETE FROM score_read_audit WHERE read_at < %s", (cutoff,))
        return cur.rowcount


def prune_scores(dsn: str, keep_days: int = 90, now: datetime | None = None) -> int:
    """Delete batch score snapshots older than `keep_days`.

    The most freely droppable thing in the system: every row is recomputable
    from the lake by rerunning the batch scorer at the same instant.
    """
    import psycopg

    cutoff = ((now or datetime.now(UTC)) - timedelta(days=keep_days)).date()
    with psycopg.connect(dsn, autocommit=True) as connection, connection.cursor() as cur:
        cur.execute("SELECT to_regclass('raw_employee_score')")
        row = cur.fetchone()
        if row is None or row[0] is None:
            return 0
        cur.execute("DELETE FROM raw_employee_score WHERE dt < %s", (cutoff,))
        return cur.rowcount
