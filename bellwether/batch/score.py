"""The batch scorer.

This module is the reason the project is shaped the way it is, and it is
deliberately boring: it reads rows, projects them onto the two protocols
`score_events` declares, and calls it. There is no scoring arithmetic here. If
there ever is, the parity test in `tests/test_score_parity.py` is what will
notice.

**The projection is five lines and it is not free.** `ScorableEvent` is a
structural type, so anything with `employee_id`, `signal` and `occurred_at`
satisfies it — but a Spark `Row` hands back `signal` as a plain string, and
scoring calls `.value` on it and looks it up in the catalog. So a Row does not
quite satisfy the protocol as written, and `BatchEvent` below is the adapter.
The claim worth making is the accurate one: the protocol keeps the adapter at
five lines instead of forcing a second implementation, which is what a
concrete `BehaviorEvent` parameter would have done.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from bellwether.events.schema import Employee, SignalType
from bellwether.scoring import RiskScore, score_events

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True, slots=True)
class BatchEvent:
    """A Spark row, projected onto `ScorableEvent`."""

    employee_id: str
    signal: SignalType
    occurred_at: datetime


@dataclass(frozen=True, slots=True)
class BatchSubject:
    """An employee, projected onto `ScorableSubject`.

    Three fields rather than the whole dimension row, because this is broadcast
    to every executor and the rest of it — name, email, manager — is both
    useless to scoring and the most sensitive data the system holds. Not sending
    PII to the executors is a smaller thing to reason about than securing it
    once it is there.
    """

    employee_id: str
    tenant_id: str
    is_high_value_target: bool

    @classmethod
    def of(cls, employee: Employee) -> BatchSubject:
        return cls(
            employee_id=employee.employee_id,
            tenant_id=employee.tenant_id,
            is_high_value_target=employee.is_high_value_target,
        )


def _aware(value: datetime) -> datetime:
    """Put back the timezone Spark drops.

    `TimestampType` is an instant and Spark stores it in UTC, but the Python
    object it materialises in an executor is **naive** — the wall time in the
    session timezone, with no tzinfo. Events go into the lake timezone-aware,
    because `BehaviorEvent` refuses anything else, and come back out without it.

    That is not cosmetic. `score_events` subtracts these from an aware `as_of`,
    so a naive value raises outright — which is how this was found, on the first
    run of the parity test and not before. The worse version is the one that
    does not raise: if scoring were ever made tolerant of naive timestamps, every
    decay calculation in the batch path would silently pick up whatever offset
    the session timezone happened to be, and the two paths would disagree by
    hours without either looking wrong.

    The session is pinned to UTC in `session.py`, so attaching UTC here is
    correct rather than a guess — and the pin is load-bearing because of this.
    """
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def score_rows(
    subject: BatchSubject,
    rows: list[Any],
    as_of: datetime,
    lookback_days: int = 30,
) -> RiskScore:
    """Score one employee's rows. Runs on an executor."""
    return score_events(
        subject,
        [
            BatchEvent(
                employee_id=row.employee_id,
                signal=SignalType(row.signal),
                occurred_at=_aware(row.occurred_at),
            )
            for row in rows
        ],
        as_of=as_of,
        lookback_days=lookback_days,
    )


def score_dataframe(
    session: SparkSession,
    events: DataFrame,
    employees: list[Employee],
    as_of: datetime,
    lookback_days: int = 30,
) -> DataFrame:
    """Score every employee in `employees` from the events in `events`.

    Grouped with `groupByKey`, which is usually the wrong tool: it materialises
    each group in memory and is the standard way to blow up an executor. It is
    the right tool here because the group is an employee's scoring window, which
    the online path already caps at a few thousand events, and because
    `score_events` is not decomposable into a combiner — it needs the whole set
    to attribute per-signal contributions. Trading a reduce-side aggregation for
    the guarantee that both paths run identical code is the trade this project
    exists to make.

    Employees are dropped, not scored as zero, when they have no events in the
    window. A zero would be indistinguishable from a genuinely clean record, and
    "we have no data on this person" is a different answer from "this person is
    doing fine".
    """
    from pyspark.sql import Row

    subjects = {e.employee_id: BatchSubject.of(e) for e in employees}
    broadcast = session.sparkContext.broadcast(subjects)
    cutoff = as_of - timedelta(days=lookback_days)

    def score_group(pair: tuple[str, Any]) -> Row | None:
        employee_id, rows = pair
        subject = broadcast.value.get(employee_id)
        if subject is None:
            # Events outlive their subject: somebody leaves, the dimension drops
            # them, their events stay in the lake forever. Same call the stream
            # scorer makes, for the same reason.
            return None
        result = score_rows(subject, list(rows), as_of, lookback_days)
        return Row(
            employee_id=result.employee_id,
            tenant_id=result.tenant_id,
            score=result.score,
            band=result.band.value,
            dominant_category=(
                result.dominant_category.value if result.dominant_category else None
            ),
            events_considered=result.events_considered,
            as_of=result.as_of,
        )

    scored = (
        events.where((events.occurred_at > cutoff) & (events.occurred_at <= as_of))
        .rdd.map(lambda row: (row.employee_id, row))
        .groupByKey()
        .map(score_group)
        .filter(lambda row: row is not None)
    )
    return session.createDataFrame(scored, schema=_score_schema())


def _score_schema() -> object:
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("employee_id", StringType(), nullable=False),
            StructField("tenant_id", StringType(), nullable=False),
            StructField("score", DoubleType(), nullable=False),
            StructField("band", StringType(), nullable=False),
            StructField("dominant_category", StringType(), nullable=True),
            StructField("events_considered", IntegerType(), nullable=False),
            StructField("as_of", TimestampType(), nullable=False),
        ]
    )
