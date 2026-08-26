"""Stream and batch must agree.

This is the test the project is organised around. Everything else — the single
signal catalog, the pure `score_events`, the structural `ScorableEvent` — is
machinery in service of being able to write it, and without it the central
claim in DESIGN.md is an argument for a design rather than a description of a
guarantee.

It comes in two layers.

The **Spark layer** replays one fixed event log through the real stream `Scorer`
and the real Spark job and asserts they agree employee by employee. It needs a
JVM, so it is marked and skipped where there is none, and CI pins a JDK so it is
never skipped there.

The **portable layer** below it needs no Spark and runs everywhere. It proves
the two properties that make agreement possible in the first place — that
scoring is order-independent and inert to duplicates, and that the projection
the batch path uses is faithful. Those are where a divergence would actually
come from; the Spark test is what proves it has not.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime
from pathlib import Path

import pytest

from bellwether.batch.score import BatchSubject, score_rows
from bellwether.dimension import InMemoryEmployeeRepository
from bellwether.events.schema import BehaviorEvent, Employee
from bellwether.events.scores import RiskScoreEvent
from bellwether.generator.population import build_population
from bellwether.generator.simulate import Simulator
from bellwether.scoring import score_events
from bellwether.stream.scorer import Scorer
from bellwether.stream.store import InMemoryOnlineStore

# Fixed, because the whole point is that two engines get the same answer from
# the same input. A drifting fixture would turn a real divergence into a flake
# and a flake into something somebody eventually deletes.
SEED = 20260701
POPULATION = 120
DAYS = 30
AS_OF = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)
LOOKBACK = 30


@pytest.fixture(scope="session")
def population() -> list[Employee]:
    return [m.employee for m in build_population(size=POPULATION, seed=SEED)]


@pytest.fixture(scope="session")
def event_log() -> list[BehaviorEvent]:
    """A month of behaviour for the whole population, generated once."""
    members = build_population(size=POPULATION, seed=SEED)
    simulator = Simulator(members, seed=SEED)
    return list(simulator.backfill(days=DAYS, end=AS_OF))


@pytest.fixture(scope="session")
def log_file(event_log: list[BehaviorEvent], tmp_path_factory: pytest.TempPathFactory) -> Path:
    path = tmp_path_factory.mktemp("lake") / "events.jsonl"
    path.write_text("\n".join(e.model_dump_json() for e in event_log))
    return path


def stream_scores(
    events: list[BehaviorEvent], people: list[Employee], as_of: datetime = AS_OF
) -> dict[str, RiskScoreEvent]:
    """Drive the real stream scorer and keep each employee's final score.

    Every message is scored at the same `as_of`. That is not a contrivance to
    make the test pass — it is what the parameter exists for. The stream scores
    at wall-clock now because that is when the event arrived; the batch job
    scores a stated instant because it is recomputing history. Comparing them
    means asking both for the same instant.
    """
    store = InMemoryOnlineStore()
    scorer = Scorer(InMemoryEmployeeRepository(list(people)), store, store, lookback_days=LOOKBACK)
    final: dict[str, RiskScoreEvent] = {}
    for event in events:
        decision = scorer.handle(event.model_dump_json().encode(), now=as_of)
        if decision.value is not None:
            message = RiskScoreEvent.model_validate_json(decision.value)
            final[message.employee_id] = message
    return final


# --- portable: the properties agreement depends on ----------------------------


def test_the_fixture_is_worth_comparing(event_log: list[BehaviorEvent]) -> None:
    """A parity test over a trivial log proves nothing."""
    assert len(event_log) > 1_000
    assert len({e.employee_id for e in event_log}) > POPULATION * 0.8
    assert len({e.signal for e in event_log}) >= 10


def test_the_stream_reaches_the_same_answer_in_any_order(
    event_log: list[BehaviorEvent], population: list[Employee]
) -> None:
    """Batch has no arrival order. If stream scoring depends on one, parity is lost.

    Runs the real consumer three times over the same events — as generated,
    shuffled, and reversed — and requires the final score for every employee to
    be identical.
    """
    forward = stream_scores(event_log, population)

    shuffled = list(event_log)
    random.Random(SEED).shuffle(shuffled)
    scrambled = stream_scores(shuffled, population)
    backwards = stream_scores(list(reversed(event_log)), population)

    assert forward.keys() == scrambled.keys() == backwards.keys()
    for employee_id, expected in forward.items():
        assert scrambled[employee_id].score == expected.score, employee_id
        assert backwards[employee_id].score == expected.score, employee_id
        assert backwards[employee_id].band is expected.band, employee_id


def test_duplicates_change_nothing(
    event_log: list[BehaviorEvent], population: list[Employee]
) -> None:
    """At-least-once means the batch path can see a record twice too.

    The stream absorbs it in the window; the lake reader absorbs it with a
    `dropDuplicates` on event id. Both have to, or the same log produces
    different answers depending on which engine read it.
    """
    clean = stream_scores(event_log, population)
    doubled = stream_scores(event_log + event_log[::3], population)

    for employee_id, expected in clean.items():
        assert doubled[employee_id].score == expected.score, employee_id


def test_the_batch_projection_is_faithful(
    event_log: list[BehaviorEvent], population: list[Employee]
) -> None:
    """`BatchEvent`/`BatchSubject` must not change the answer.

    The adapter exists because a Spark `Row` gives `signal` as a plain string
    while scoring wants the enum. It is five lines, and five lines is exactly
    enough room to lose a field — so this scores the same employee both ways,
    once through the models the stream holds and once through the projection the
    executors hold, and requires them to agree. No JVM involved.
    """
    by_employee: dict[str, list[BehaviorEvent]] = {}
    for event in event_log:
        by_employee.setdefault(event.employee_id, []).append(event)

    checked = 0
    for employee in population:
        events = by_employee.get(employee.employee_id)
        if not events:
            continue

        direct = score_events(employee, events, as_of=AS_OF, lookback_days=LOOKBACK)
        projected = score_rows(
            BatchSubject.of(employee),
            [_row(e) for e in events],
            as_of=AS_OF,
            lookback_days=LOOKBACK,
        )

        assert projected.score == direct.score, employee.employee_id
        assert projected.band is direct.band, employee.employee_id
        assert projected.dominant_category == direct.dominant_category
        assert projected.events_considered == direct.events_considered
        checked += 1

    assert checked > POPULATION * 0.8


def test_the_projection_carries_no_pii(population: list[Employee]) -> None:
    """Executors get three fields, and none of them identify anyone by name."""
    with_pii = next(e for e in population if e.email)
    subject = BatchSubject.of(with_pii)

    serialised = repr(subject)
    assert with_pii.email is not None
    assert with_pii.email not in serialised
    assert "@" not in serialised
    assert not hasattr(subject, "display_name")


class _Row:
    """Stands in for a `pyspark.sql.Row`: attribute access, string signal."""

    def __init__(self, employee_id: str, signal: str, occurred_at: datetime) -> None:
        self.employee_id = employee_id
        self.signal = signal
        self.occurred_at = occurred_at


def _row(event: BehaviorEvent) -> _Row:
    return _Row(event.employee_id, event.signal.value, event.occurred_at)


# --- the Spark comparison ------------------------------------------------------


@pytest.fixture(scope="session")
def spark():  # type: ignore[no-untyped-def]
    """A local Spark session, or a skip.

    Skipped in the fixture rather than at module scope so a machine without a
    JVM still runs the portable layer above. Skipping the whole file would mean
    that on most developer laptops — this one included, where the only JDK is
    23 and PySpark supports 17 and 21 — nothing in this file ran at all, and
    nobody would notice.
    """
    pytest.importorskip("pyspark", reason="the batch path needs PySpark")
    from bellwether.batch.session import spark_session

    try:
        session = spark_session(app="bellwether-parity", master="local[2]")
    except Exception as err:
        pytest.skip(f"no usable JVM: {type(err).__name__}: {str(err)[:120]}")
    yield session
    session.stop()


@pytest.mark.spark
def test_stream_and_batch_agree(
    spark,  # type: ignore[no-untyped-def]
    log_file: Path,
    event_log: list[BehaviorEvent],
    population: list[Employee],
) -> None:
    """One log, two engines, the same answer for every employee.

    The reason this is worth running rather than reasoning about: the two paths
    share `score_events` but nothing else. Different readers, different
    representations of an event, different notions of grouping, different
    process. Any of those could bend an answer without touching the scoring
    code, and only running both finds out.
    """
    from bellwether.batch.lake import read_events
    from bellwether.batch.score import score_dataframe

    expected = stream_scores(event_log, population)

    events = read_events(spark, str(log_file))
    scored = score_dataframe(spark, events, population, as_of=AS_OF, lookback_days=LOOKBACK)
    actual = {row["employee_id"]: row for row in (r.asDict() for r in scored.collect())}

    assert actual.keys() == expected.keys(), "the two paths scored different people"

    divergent = [
        (employee_id, expected[employee_id].score, actual[employee_id]["score"])
        for employee_id in expected
        if round(expected[employee_id].score, 6) != round(actual[employee_id]["score"], 6)
    ]
    assert not divergent, f"{len(divergent)} employees disagree: {divergent[:5]}"

    for employee_id, message in expected.items():
        row = actual[employee_id]
        assert row["band"] == message.band.value, employee_id
        assert row["events_considered"] == message.events_considered, employee_id
        assert row["dominant_category"] == (
            message.dominant_category.value if message.dominant_category else None
        ), employee_id


@pytest.mark.spark
def test_the_batch_path_ignores_events_outside_the_window(
    spark,  # type: ignore[no-untyped-def]
    log_file: Path,
    population: list[Employee],
) -> None:
    """Both paths bound the window; a batch job that did not would score higher."""
    from bellwether.batch.lake import read_events
    from bellwether.batch.score import score_dataframe

    events = read_events(spark, str(log_file))
    wide = score_dataframe(spark, events, population, as_of=AS_OF, lookback_days=LOOKBACK)
    narrow = score_dataframe(spark, events, population, as_of=AS_OF, lookback_days=3)

    wide_total = sum(r["score"] for r in wide.collect())
    narrow_total = sum(r["score"] for r in narrow.collect())
    assert narrow_total < wide_total


@pytest.mark.spark
def test_a_duplicated_lake_scores_the_same(
    spark,  # type: ignore[no-untyped-def]
    event_log: list[BehaviorEvent],
    population: list[Employee],
    tmp_path: Path,
) -> None:
    """The lake really can hold the same record twice, so prove the reader absorbs it."""
    from bellwether.batch.lake import read_events
    from bellwether.batch.score import score_dataframe

    lines = [e.model_dump_json() for e in event_log]
    once = tmp_path / "once.jsonl"
    twice = tmp_path / "twice.jsonl"
    once.write_text("\n".join(lines))
    twice.write_text("\n".join(lines + lines))

    def totals(path: Path) -> dict[str, float]:
        frame = score_dataframe(
            spark, read_events(spark, str(path)), population, as_of=AS_OF, lookback_days=LOOKBACK
        )
        return {r["employee_id"]: r["score"] for r in frame.collect()}

    assert totals(once) == totals(twice)
