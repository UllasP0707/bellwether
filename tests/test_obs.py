"""Observability tests.

Two things worth reading. `test_a_traceparent_survives_a_kafka_round_trip` is
the property the whole tracing story rests on — without it there are four
unrelated traces and no way to connect a message to the log line that caused
it. The drift tests are the ones that catch the failure a schema test cannot
see, so they are checked against a source going dark rather than against
arithmetic.
"""

from __future__ import annotations

from datetime import date

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from bellwether.obs import quality, tracing
from bellwether.obs.quality import DailyCounts, evaluate, total_variation, volume_shift

# --- tracing ------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def recording() -> None:
    """A real provider, so spans have real ids.

    The default no-op provider hands out an all-zero span context that
    `inject` correctly declines to emit, which would make every assertion here
    vacuously pass.
    """
    trace.set_tracer_provider(TracerProvider())


def test_a_traceparent_survives_a_kafka_round_trip() -> None:
    """The property that makes one trace out of four processes.

    A connector, the normalizer, the scorer and the intervention stage never
    share a process or overlap in time. If the header does not survive, the
    question "why did this person get this message" has no mechanical answer.
    """
    with tracing.tracer().start_as_current_span("produce") as span:
        headers = tracing.inject()
        produced = span.get_span_context()

    assert [key for key, _ in headers] == ["traceparent"]

    context = tracing.extract([(k, v) for k, v in headers])
    assert context is not None
    recovered = trace.get_current_span(context).get_span_context()
    assert recovered.trace_id == produced.trace_id
    assert recovered.span_id == produced.span_id
    assert recovered.is_remote


def test_a_child_span_keeps_the_upstream_trace_id() -> None:
    """Continuing a trace, not merely reading its header."""
    with tracing.tracer().start_as_current_span("upstream") as parent:
        headers = tracing.inject()
        upstream_trace = parent.get_span_context().trace_id

    with tracing.message_span("downstream", headers=[(k, v) for k, v in headers]) as child:
        assert child.get_span_context().trace_id == upstream_trace


def test_injection_preserves_headers_that_are_already_there() -> None:
    with tracing.tracer().start_as_current_span("s"):
        headers = tracing.inject([("reason", b"malformed")])

    assert dict(headers)["reason"] == b"malformed"
    assert "traceparent" in dict(headers)


@pytest.mark.parametrize(
    "headers",
    [
        None,
        [],
        [("reason", b"malformed")],
        [("traceparent", b"garbage")],
        [("traceparent", b"00-0-0-01")],
        [("traceparent", b"99-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")],
        [("traceparent", None)],
    ],
)
def test_an_unusable_header_starts_a_new_trace_rather_than_failing(
    headers: list[tuple[str, bytes | None]] | None,
) -> None:
    """Untraced input is normal, not an error.

    A stage that refused messages without a valid `traceparent` would stop
    working the moment somebody replayed a topic with a tool that does not
    write headers — which is every tool.
    """
    assert tracing.extract(headers) is None

    with tracing.message_span("stage handle", headers=headers) as span:
        assert span.get_span_context().is_valid


# --- data quality -------------------------------------------------------------


def test_an_identical_distribution_has_not_drifted() -> None:
    mix = {"phish_sim_clicked": 100, "training_overdue": 50}
    assert total_variation(mix, mix) == 0.0


def test_a_source_going_dark_is_caught() -> None:
    """The failure this check exists for.

    One connector stops. Every row still present is correct, the schema is
    unchanged, every dbt test passes, and a quarter of the population's scores
    will drift down over the next week with nothing to point at.
    """
    baseline = {"phish_sim_clicked": 250, "training_overdue": 500, "usb_mass_storage": 250}
    today = {"training_overdue": 500, "usb_mass_storage": 250}

    drift = total_variation(today, baseline)
    assert drift == pytest.approx(0.25)
    assert drift > quality.DRIFT_THRESHOLD, "a quarter of the mix vanishing must be reported"


def test_a_completely_different_mix_reads_as_one() -> None:
    assert total_variation({"a": 10}, {"b": 10}) == pytest.approx(1.0)


def test_the_first_day_does_not_report_maximum_drift() -> None:
    """Nothing to compare against is not the same as everything having changed.

    Reporting drift on day one teaches people to ignore the check before it
    has ever been right about anything.
    """
    assert total_variation({"a": 10}, {}) == 0.0
    assert total_variation({}, {"a": 10}) == 0.0


def test_volume_is_measured_against_a_median_not_a_mean() -> None:
    """The baseline must not absorb the outage it is meant to detect."""
    steady = [1000, 1000, 1000, 1000, 20]

    assert volume_shift(1000, steady) == pytest.approx(0.0)
    assert volume_shift(400, steady) == pytest.approx(0.6)


def counts(**overrides: object) -> DailyCounts:
    base: dict[str, object] = dict(
        day=date(2026, 8, 25),
        rows=1000,
        employees=400,
        null_employee_ids=0,
        signal_events={"phish_sim_clicked": 250, "training_overdue": 750},
        late_events=10,
        baseline_signal_events={"phish_sim_clicked": 2500, "training_overdue": 7500},
        baseline_rows=[1000] * 14,
    )
    base.update(overrides)
    return DailyCounts(**base)  # type: ignore[arg-type]


def test_a_healthy_day_fails_nothing() -> None:
    """The check that keeps the others honest.

    A contract suite that fires on ordinary data gets muted, and a muted
    check is worse than an absent one because it looks like coverage.
    """
    assert [c for c in evaluate(counts()) if c.failing] == []


@pytest.mark.parametrize(
    ("override", "expected"),
    [
        ({"null_employee_ids": 200}, "null_employee_id"),
        ({"rows": 100}, "volume_shift"),
        ({"signal_events": {"training_overdue": 1000}}, "signal_mix_drift"),
        ({"late_events": 400}, "late_arrival_rate"),
    ],
)
def test_each_check_catches_the_failure_it_exists_for(
    override: dict[str, object], expected: str
) -> None:
    failing = [c.name for c in evaluate(counts(**override)) if c.failing]
    assert expected in failing


def test_drift_names_the_signal_that_moved() -> None:
    """A distance alone is not actionable; a name is where to look."""
    checks = {c.name: c for c in evaluate(counts(signal_events={"training_overdue": 1000}))}

    assert "phish_sim_clicked" in checks["signal_mix_drift"].detail
    assert "FAIL" in str(checks["signal_mix_drift"])
