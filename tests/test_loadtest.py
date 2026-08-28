"""Tests for the load-test harness.

A load test whose own arithmetic is wrong is worse than no load test: it
produces a document full of confident numbers that nobody can check. These
cover the two places this harness got it wrong before it got it right.
"""

from __future__ import annotations

import time

import pytest

from bellwether.loadtest.harness import Result, Span, Timing
from bellwether.loadtest.scenarios import WINDOW_SIZES, subject, window_of
from bellwether.scoring import score_events


def test_percentiles_come_from_the_whole_sample() -> None:
    timing = Timing()
    for value in range(1, 101):
        timing.add(float(value))

    assert timing.percentile(50) == 51.0
    assert timing.percentile(99) == 100.0
    assert timing.mean == pytest.approx(50.5)


def test_asking_for_p100_does_not_walk_off_the_end() -> None:
    timing = Timing()
    timing.add(1.0)
    assert timing.percentile(100) == 1.0


def test_an_empty_timing_reports_zero_rather_than_raising() -> None:
    """A phase that ran nothing has no percentile, and must not crash the run."""
    assert Timing().percentile(99) == 0.0
    assert Result("nothing", 0, 0.0).rate == 0.0


def test_a_measured_block_is_recorded_even_when_it_raises() -> None:
    timing = Timing()
    with pytest.raises(ValueError), timing.measure():
        raise ValueError("boom")

    assert len(timing) == 1


def test_a_span_measures_work_and_not_startup() -> None:
    """The bug this class exists for.

    Timing a consumer from process start to exit includes the consumer-group
    join and the idle timeout it waits out at the end. Neither is work, and
    both vary, which made five identical runs report throughputs 3x apart.
    """
    span = Span()
    handler = span.wrap(lambda payload: payload)

    time.sleep(0.05)  # stands in for the group join
    for index in range(5):
        handler(index)
    time.sleep(0.05)  # stands in for the idle timeout

    assert span.handled == 5
    assert span.seconds < 0.04, "startup and shutdown must not be counted as work"


def test_a_span_passes_the_handler_result_through() -> None:
    """It wraps a stage's handler, so it must be invisible to the stage."""
    span = Span()
    handler = span.wrap(lambda a, b=2: a * b)

    assert handler(3) == 6
    assert handler(3, b=4) == 12
    assert span.handled == 2


def test_a_span_that_saw_nothing_reports_zero() -> None:
    assert Span().seconds == 0.0


def test_a_span_of_one_message_does_not_divide_by_zero() -> None:
    span = Span()
    span.wrap(lambda x: x)(1)
    assert span.seconds > 0
    assert Result("one", 1, span.seconds).rate > 0


def test_the_window_fixture_is_deterministic_and_mixed() -> None:
    """The curve is only comparable between runs if the input is the same."""
    first, second = window_of(200), window_of(200)

    assert [e.event_id for e in first] == [e.event_id for e in second]
    assert [e.signal for e in first] == [e.signal for e in second]
    assert len({e.signal for e in first}) > 10, "a single-signal window measures the wrong thing"


def test_scoring_cost_is_linear_in_window_size() -> None:
    """The claim section 1 of docs/LOAD_TEST.md rests on.

    Asserted as a bound rather than a timing, because a test that fails when
    the machine is busy is a test that gets deleted. Ten times the events must
    not cost more than thirty times the work; anything worse than that is a
    complexity regression rather than a slow afternoon.
    """
    person = subject()
    small, large = window_of(100), window_of(1_000)

    def elapsed(events: list[object], rounds: int = 20) -> float:
        started = time.perf_counter()
        for _ in range(rounds):
            score_events(person, events, as_of=window_of(1)[0].occurred_at, lookback_days=3650)
        return time.perf_counter() - started

    assert elapsed(large) < elapsed(small) * 30  # type: ignore[arg-type]


def test_the_window_sizes_span_three_orders_of_magnitude() -> None:
    """Log-spaced, because the question is the shape and not the slope."""
    assert WINDOW_SIZES[-1] / WINDOW_SIZES[0] >= 1_000
