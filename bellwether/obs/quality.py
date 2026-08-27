"""Data-quality contracts: the checks a schema test cannot make.

There are already 42 dbt tests over the marts, and they are the wrong tool for
what is here. A dbt test asserts an invariant about the data as it stands —
this column is not null, this key is unique, this score is between 0 and 100.
Every one of them passes on an empty table, and every one of them passes when a
connector silently stops returning half its record types.

That is the actual failure mode of an ingestion pipeline. Nothing throws. The
schema is fine, the row counts are plausible, the marts build green, and the
only symptom is that the signal mix moved and nobody looked. These checks are
*distributional*: they compare a day against the days before it, which is a
question about history that a test scoped to one table cannot ask.

Four checks, each aimed at a failure that has a plausible cause:

- **Null rate.** A vendor changes a field name and the connector maps it to
  nothing. Rows keep arriving; a column quietly goes empty.
- **Volume.** A connector's cursor sticks, or its credential expires. The
  absolute number is useless as a threshold because Bellwether's own generator
  produces a quarter as much at weekends, so this compares against a trailing
  median rather than a constant.
- **Signal mix drift.** The most valuable of the four and the least obvious. If
  `phish_sim_clicked` was 12% of yesterday's events and is 0% today, no row is
  wrong and no test fails — a single source has stopped, and scores across the
  population will drift down over the following week for a reason nobody can
  see. Measured as total variation distance, which is half the L1 distance
  between the two distributions and reads directly as "this share of the mix
  moved".
- **Late arrivals.** Events whose `occurred_at` is far behind the day being
  processed. Some lateness is normal and the event contract is built for it;
  a jump means a source is backfilling, and any window computed before it
  finishes is wrong.

Every check is a pure function of counts, so the thresholds are testable
without a database and the SQL is confined to `collect()`.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from datetime import date, timedelta

# A day whose mix moved by more than this against the trailing baseline is
# reported. Two forces set it. Too tight and it fires on ordinary daily wobble,
# and a check that goes off every Tuesday gets muted, which is worse than not
# having it. Too loose and a whole connector can stop without tripping it.
#
# 0.20 is chosen from the smaller of the four sources rather than from taste:
# Bellwether has four connectors, so the least of them going completely dark
# moves the mix by roughly its share, and the threshold has to sit below that
# with room to spare. It was briefly 0.25, which put a source contributing
# exactly a quarter of events precisely on the boundary and reported it as
# healthy.
DRIFT_THRESHOLD = 0.20
NULL_THRESHOLD = 0.01
VOLUME_THRESHOLD = 0.5
LATE_THRESHOLD = 0.10

# Enough days to have a median that means something, short enough that a real
# regime change stops being flagged within a fortnight rather than forever.
BASELINE_DAYS = 14


@dataclass(frozen=True)
class Check:
    """One measurement against one threshold."""

    name: str
    dataset: str
    value: float
    threshold: float
    detail: str = ""

    @property
    def failing(self) -> bool:
        return self.value > self.threshold

    def __str__(self) -> str:
        state = "FAIL" if self.failing else "ok"
        suffix = f" — {self.detail}" if self.detail else ""
        return (
            f"[{state}] {self.dataset}.{self.name} {self.value:.4f} (max {self.threshold}){suffix}"
        )


def null_rate(rows: int, nulls: int) -> float:
    return 0.0 if rows == 0 else nulls / rows


def total_variation(today: dict[str, int], baseline: dict[str, int]) -> float:
    """How much of the distribution moved, in [0, 1].

    Half the L1 distance between two normalised distributions, which is the
    form that reads as a share: 0.25 means a quarter of the probability mass
    changed category. Signals absent from one side count in full, which is the
    case that matters — a source going dark is a signal dropping to zero, not a
    signal shifting slightly.

    An empty baseline returns 0. The first day a system runs has nothing to
    drift from, and reporting maximum drift on day one trains people to ignore
    the check before it has ever been right.
    """
    total_today, total_base = sum(today.values()), sum(baseline.values())
    if total_today == 0 or total_base == 0:
        return 0.0
    keys = set(today) | set(baseline)
    return (
        sum(abs(today.get(k, 0) / total_today - baseline.get(k, 0) / total_base) for k in keys) / 2
    )


def volume_shift(today: int, history: list[int]) -> float:
    """Relative distance from the trailing median, in absolute terms.

    Median rather than mean because the point of the check is to catch the
    outlier, and a mean that includes yesterday's outage is a baseline that has
    already absorbed the thing being looked for.
    """
    if not history:
        return 0.0
    baseline = statistics.median(history)
    if baseline == 0:
        return 0.0 if today == 0 else 1.0
    return abs(today - baseline) / baseline


@dataclass(frozen=True)
class DailyCounts:
    """One day of the warehouse, reduced to what the checks need.

    A struct rather than four loose arguments because `collect()` runs four
    queries against Postgres and the checks run against none, and keeping the
    boundary explicit is what lets every threshold above be tested with a
    literal.
    """

    day: date
    rows: int
    employees: int
    null_employee_ids: int
    signal_events: dict[str, int]
    late_events: int
    baseline_signal_events: dict[str, int]
    baseline_rows: list[int]


def evaluate(counts: DailyCounts, dataset: str = "raw_daily_employee_signal") -> list[Check]:
    """Run every check against one day. Pure."""
    return [
        Check(
            "null_employee_id",
            dataset,
            null_rate(counts.rows, counts.null_employee_ids),
            NULL_THRESHOLD,
            f"{counts.null_employee_ids} of {counts.rows} rows",
        ),
        Check(
            "volume_shift",
            dataset,
            volume_shift(counts.rows, counts.baseline_rows),
            VOLUME_THRESHOLD,
            f"{counts.rows} rows vs a {len(counts.baseline_rows)}-day median of "
            f"{statistics.median(counts.baseline_rows) if counts.baseline_rows else 0:.0f}",
        ),
        Check(
            "signal_mix_drift",
            dataset,
            total_variation(counts.signal_events, counts.baseline_signal_events),
            DRIFT_THRESHOLD,
            _mix_detail(counts.signal_events, counts.baseline_signal_events),
        ),
        Check(
            "late_arrival_rate",
            dataset,
            null_rate(counts.rows, counts.late_events),
            LATE_THRESHOLD,
            f"{counts.late_events} rows carrying events older than the day itself",
        ),
    ]


def _mix_detail(today: dict[str, int], baseline: dict[str, int], top: int = 3) -> str:
    """Name the signals that moved most, because a distance alone is not actionable."""
    total_today, total_base = sum(today.values()) or 1, sum(baseline.values()) or 1
    moves = sorted(
        (
            (today.get(k, 0) / total_today - baseline.get(k, 0) / total_base, k)
            for k in set(today) | set(baseline)
        ),
        key=lambda pair: abs(pair[0]),
        reverse=True,
    )
    named = ", ".join(
        f"{signal} {delta:+.1%}" for delta, signal in moves[:top] if abs(delta) > 0.01
    )
    return named or "no signal moved by more than a point"


# --- the database boundary ----------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS data_quality_check (
    checked_at   timestamptz NOT NULL DEFAULT now(),
    dt           date        NOT NULL,
    dataset      text        NOT NULL,
    check_name   text        NOT NULL,
    value        double precision NOT NULL,
    threshold    double precision NOT NULL,
    failing      boolean     NOT NULL,
    detail       text        NOT NULL DEFAULT '',
    PRIMARY KEY (dt, dataset, check_name)
);
"""


def collect(dsn: str, day: date, tenant_id: str, baseline_days: int = BASELINE_DAYS) -> DailyCounts:
    """Read one day and its trailing baseline out of the warehouse."""
    import psycopg

    start = day - timedelta(days=baseline_days)
    with psycopg.connect(dsn, autocommit=True) as connection, connection.cursor() as cur:
        cur.execute(
            """
            SELECT count(*),
                   count(DISTINCT employee_id),
                   count(*) FILTER (WHERE employee_id IS NULL OR employee_id = ''),
                   coalesce(sum(events) FILTER (WHERE first_at::date < dt), 0)
            FROM raw_daily_employee_signal
            WHERE tenant_id = %s AND dt = %s
            """,
            (tenant_id, day),
        )
        rows, employees, nulls, late = cur.fetchone() or (0, 0, 0, 0)

        cur.execute(
            """
            SELECT signal, sum(events)::bigint
            FROM raw_daily_employee_signal
            WHERE tenant_id = %s AND dt = %s
            GROUP BY signal
            """,
            (tenant_id, day),
        )
        today = {signal: int(count) for signal, count in cur.fetchall()}

        cur.execute(
            """
            SELECT signal, sum(events)::bigint
            FROM raw_daily_employee_signal
            WHERE tenant_id = %s AND dt >= %s AND dt < %s
            GROUP BY signal
            """,
            (tenant_id, start, day),
        )
        baseline = {signal: int(count) for signal, count in cur.fetchall()}

        cur.execute(
            """
            SELECT count(*) FROM raw_daily_employee_signal
            WHERE tenant_id = %s AND dt >= %s AND dt < %s GROUP BY dt
            """,
            (tenant_id, start, day),
        )
        history = [int(row[0]) for row in cur.fetchall()]

    return DailyCounts(
        day=day,
        rows=int(rows),
        employees=int(employees),
        null_employee_ids=int(nulls),
        signal_events=today,
        late_events=int(late),
        baseline_signal_events=baseline,
        baseline_rows=history,
    )


def record(dsn: str, day: date, checks: list[Check]) -> int:
    """Persist the results. Replaces the day, so a rerun is idempotent."""
    import psycopg

    from bellwether.obs import metrics

    with psycopg.connect(dsn, autocommit=True) as connection, connection.cursor() as cur:
        cur.execute(_DDL)
        for check in checks:
            cur.execute(
                """
                INSERT INTO data_quality_check
                    (dt, dataset, check_name, value, threshold, failing, detail)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dt, dataset, check_name) DO UPDATE SET
                    checked_at = now(), value = EXCLUDED.value,
                    threshold = EXCLUDED.threshold, failing = EXCLUDED.failing,
                    detail = EXCLUDED.detail
                """,
                (
                    day,
                    check.dataset,
                    check.name,
                    check.value,
                    check.threshold,
                    check.failing,
                    check.detail,
                ),
            )
            metrics.quality_check.labels(check=check.name, dataset=check.dataset).set(check.value)
            metrics.quality_failures.labels(check=check.name, dataset=check.dataset).set(
                1 if check.failing else 0
            )
    return len(checks)
