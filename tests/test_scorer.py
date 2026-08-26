"""Stream scorer tests.

The handler takes bytes and returns a decision, so none of this needs a broker.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from bellwether.dimension import InMemoryEmployeeRepository
from bellwether.events.schema import BehaviorEvent, Employee, SignalType, Source
from bellwether.events.scores import RiskScoreEvent
from bellwether.scoring import RiskBand, score_events
from bellwether.stream.scorer import ScoreOutcome, Scorer
from bellwether.stream.store import MAX_WINDOW_EVENTS, InMemoryOnlineStore, WindowedEvent

NOW = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)


def employee(employee_id: str = "E0042", **overrides: object) -> Employee:
    base = dict(
        employee_id=employee_id,
        tenant_id="acme",
        department="engineering",
        seniority="mid",
        tenure_days=500,
        location="Remote US",
        email=f"{employee_id.lower()}@acme.example",
    )
    base.update(overrides)
    return Employee(**base)  # type: ignore[arg-type]


def raw_event(
    signal: SignalType = SignalType.PHISH_SIM_CLICKED,
    employee_id: str = "E0042",
    occurred_at: datetime | None = None,
    event_id: str | None = None,
) -> bytes:
    event = BehaviorEvent(
        tenant_id="acme",
        employee_id=employee_id,
        signal=signal,
        source=Source.EMAIL_GATEWAY,
        occurred_at=occurred_at or NOW,
        **({"event_id": event_id} if event_id else {}),  # type: ignore[arg-type]
    )
    return event.model_dump_json().encode()


def build_scorer(*employees: Employee) -> tuple[Scorer, InMemoryOnlineStore]:
    window = InMemoryOnlineStore()
    people = list(employees) or [employee()]
    return Scorer(InMemoryEmployeeRepository(people), window, window), window


# --- the happy path ---------------------------------------------------------


def test_an_event_produces_a_score_keyed_by_employee() -> None:
    scorer, _ = build_scorer()
    decision = scorer.handle(raw_event(), now=NOW)

    assert decision.outcome is ScoreOutcome.SCORED
    assert decision.key == b"E0042"
    assert decision.publishes

    message = RiskScoreEvent.model_validate_json(decision.value or b"")
    assert message.employee_id == "E0042"
    assert message.score > 0
    assert message.events_considered == 1


def test_score_carries_attribution_not_just_a_number() -> None:
    """A security team will not act on a score it cannot interrogate."""
    scorer, _ = build_scorer()
    scorer.handle(raw_event(SignalType.PHISH_CREDENTIALS_SUBMITTED), now=NOW)
    decision = scorer.handle(raw_event(SignalType.FILE_SHARED_PUBLIC_LINK), now=NOW)

    message = RiskScoreEvent.model_validate_json(decision.value or b"")
    assert message.dominant_category is not None
    assert message.top_factors
    assert {f.signal for f in message.top_factors} == {
        "phish_credentials_submitted",
        "file_shared_public_link",
    }


def test_the_window_accumulates_across_events() -> None:
    scorer, _ = build_scorer()
    scores = []
    for day in (3, 2, 1, 0):
        decision = scorer.handle(
            raw_event(occurred_at=NOW - timedelta(days=day), event_id=f"e{day}"), now=NOW
        )
        scores.append(RiskScoreEvent.model_validate_json(decision.value or b"").score)

    assert scores == sorted(scores), "each additional event should raise the score"
    assert (
        RiskScoreEvent.model_validate_json(
            scorer.handle(raw_event(event_id="last"), now=NOW).value or b""
        ).events_considered
        == 5
    )


def test_scorer_matches_the_shared_scoring_function() -> None:
    """The scorer must contribute no scoring logic of its own.

    If this drifts, the stream path has grown its own opinion and the day-6
    parity test against Spark is already lost.
    """
    person = employee()
    scorer, window = build_scorer(person)
    for day in (5, 3, 1):
        scorer.handle(raw_event(occurred_at=NOW - timedelta(days=day), event_id=f"e{day}"), now=NOW)

    decision = scorer.handle(raw_event(event_id="final"), now=NOW)
    published = RiskScoreEvent.model_validate_json(decision.value or b"")
    directly = score_events(person, window.events("E0042"), as_of=NOW)

    assert published.score == directly.score
    assert published.band is directly.band


# --- idempotency ------------------------------------------------------------


def test_replaying_an_event_does_not_double_count_it() -> None:
    """At-least-once delivery reaches the scorer too.

    The window is keyed by event_id, so a redelivered event overwrites rather
    than accumulates.
    """
    scorer, window = build_scorer()
    payload = raw_event(event_id="fixed-id")

    first = RiskScoreEvent.model_validate_json(scorer.handle(payload, now=NOW).value or b"")
    for _ in range(5):
        again = RiskScoreEvent.model_validate_json(scorer.handle(payload, now=NOW).value or b"")

    assert again.score == first.score
    assert again.events_considered == 1
    assert len(window.events("E0042")) == 1


# --- band transitions -------------------------------------------------------


def test_first_score_reports_no_previous_band() -> None:
    scorer, _ = build_scorer()
    message = RiskScoreEvent.model_validate_json(scorer.handle(raw_event(), now=NOW).value or b"")
    assert message.previous_band is None
    assert message.band_changed is False, "an employee's first score is not a crossing"


def test_a_crossing_is_flagged() -> None:
    scorer, _ = build_scorer()
    scorer.handle(raw_event(SignalType.MFA_PUSH_DENIED, event_id="a"), now=NOW)
    decision = scorer.handle(
        raw_event(SignalType.PHISH_CREDENTIALS_SUBMITTED, event_id="b"), now=NOW
    )

    message = RiskScoreEvent.model_validate_json(decision.value or b"")
    assert message.band_changed is True
    assert message.previous_band is not message.band
    assert scorer.stats.band_changes == 1


def test_movement_inside_a_band_is_not_a_crossing() -> None:
    """Day 4 fires on transitions. Twitching within a band must not count."""
    scorer, _ = build_scorer()
    scorer.handle(raw_event(SignalType.MFA_PUSH_DENIED, event_id="a"), now=NOW)
    decision = scorer.handle(raw_event(SignalType.MFA_PUSH_DENIED, event_id="b"), now=NOW)

    message = RiskScoreEvent.model_validate_json(decision.value or b"")
    assert message.band is RiskBand.LOW
    assert message.band_changed is False


# --- degradation ------------------------------------------------------------


def test_an_event_older_than_the_window_publishes_nothing() -> None:
    """A zero score would claim the employee is clean. We have no data on them.

    Found by comparing the two paths over the whole live population rather than
    over a fixture: one employee, whose single event was 33 days old, was scored
    by the stream and dropped by the batch job. The batch job was right.
    """
    scorer, _ = build_scorer()
    decision = scorer.handle(raw_event(occurred_at=NOW - timedelta(days=40)), now=NOW)

    assert decision.outcome is ScoreOutcome.EMPTY_WINDOW
    assert not decision.publishes
    assert scorer.stats.empty_window == 1
    assert scorer.stats.scored == 0


def test_a_clean_employee_with_events_is_still_scored() -> None:
    """Only *absence* of data is silent. Mitigating behaviour is a real answer."""
    scorer, _ = build_scorer()
    decision = scorer.handle(raw_event(SignalType.REAL_PHISH_REPORTED), now=NOW)

    message = RiskScoreEvent.model_validate_json(decision.value or b"")
    assert decision.outcome is ScoreOutcome.SCORED
    assert message.score == 0.0
    assert message.events_considered == 1


def test_unknown_employee_is_skipped_not_guessed() -> None:
    """Events outlive their subject: people leave, their events keep arriving."""
    scorer, _ = build_scorer()
    decision = scorer.handle(raw_event(employee_id="E9999"), now=NOW)

    assert decision.outcome is ScoreOutcome.UNKNOWN_EMPLOYEE
    assert not decision.publishes
    assert scorer.stats.unknown_employee == 1


def test_unparseable_message_is_skipped_not_raised() -> None:
    scorer, _ = build_scorer()
    decision = scorer.handle(b"{not json", now=NOW)

    assert decision.outcome is ScoreOutcome.MALFORMED
    assert not decision.publishes
    assert scorer.stats.malformed == 1


def test_stats_account_for_every_message() -> None:
    scorer, _ = build_scorer()
    scorer.handle(raw_event(event_id="a"), now=NOW)
    scorer.handle(raw_event(employee_id="E9999"), now=NOW)
    scorer.handle(b"garbage", now=NOW)

    assert scorer.stats.total == 3
    assert (scorer.stats.scored, scorer.stats.unknown_employee, scorer.stats.malformed) == (1, 1, 1)


# --- latency ----------------------------------------------------------------


def test_latency_is_measured_from_event_time() -> None:
    """Two clocks: behaviour->score is the product claim, ingest->score the SLO."""
    scorer, _ = build_scorer()
    occurred = NOW - timedelta(seconds=3)
    decision = scorer.handle(raw_event(occurred_at=occurred), now=NOW)

    message = RiskScoreEvent.model_validate_json(decision.value or b"")
    assert message.event_latency_ms == pytest.approx(3000, rel=0.01)
    assert scorer.stats.percentile(50) == pytest.approx(3000, rel=0.01)


def test_percentile_of_nothing_is_zero_not_an_error() -> None:
    scorer, _ = build_scorer()
    assert scorer.stats.percentile(99) == 0.0


# --- the window itself ------------------------------------------------------


def test_window_drops_events_outside_the_lookback() -> None:
    window = InMemoryOnlineStore()
    fresh = datetime.now(UTC)
    window.add(WindowedEvent("E1", SignalType.PHISH_SIM_CLICKED, fresh, "new"), lookback_days=30)
    window.add(
        WindowedEvent("E1", SignalType.PHISH_SIM_CLICKED, fresh - timedelta(days=60), "old"),
        lookback_days=30,
    )

    assert [e.event_id for e in window.events("E1")] == ["new"]


def test_window_is_capped_per_employee() -> None:
    """One noisy admin account must not be able to exhaust memory."""
    window = InMemoryOnlineStore()
    fresh = datetime.now(UTC)
    for i in range(MAX_WINDOW_EVENTS + 50):
        window.add(
            WindowedEvent("E1", SignalType.MFA_PUSH_DENIED, fresh - timedelta(seconds=i), f"e{i}"),
            lookback_days=30,
        )

    assert len(window.events("E1")) == MAX_WINDOW_EVENTS
    assert window.truncated == 50, "dropped events should be counted, not silent"


def test_windowed_event_survives_the_redis_encoding() -> None:
    original = WindowedEvent("E0042", SignalType.SENSITIVE_DATA_TO_GENAI, NOW, "evt-1")
    restored = WindowedEvent.from_member("E0042", original.member(), NOW.timestamp())

    assert restored == original


def test_windows_are_isolated_per_employee() -> None:
    scorer, window = build_scorer(employee("E0001"), employee("E0002"))
    scorer.handle(raw_event(employee_id="E0001", event_id="a"), now=NOW)
    scorer.handle(raw_event(employee_id="E0002", event_id="b"), now=NOW)

    assert len(window.events("E0001")) == 1
    assert len(window.events("E0002")) == 1


# --- the published contract -------------------------------------------------


def test_published_score_carries_no_pii() -> None:
    scorer, _ = build_scorer()
    decision = scorer.handle(raw_event(), now=NOW)
    assert b"@" not in (decision.value or b"")


def test_published_score_is_valid_json_with_a_schema_version() -> None:
    scorer, _ = build_scorer()
    payload = json.loads(scorer.handle(raw_event(), now=NOW).value or b"")
    assert payload["schema_version"] >= 1
    assert payload["employee_id"] == "E0042"


def test_high_value_target_scores_higher_for_the_same_behaviour() -> None:
    ordinary, _ = build_scorer(employee("E0001"))
    exec_scorer, _ = build_scorer(employee("E0002", is_executive=True))

    a = RiskScoreEvent.model_validate_json(
        ordinary.handle(raw_event(employee_id="E0001"), now=NOW).value or b""
    )
    b = RiskScoreEvent.model_validate_json(
        exec_scorer.handle(raw_event(employee_id="E0002"), now=NOW).value or b""
    )
    assert b.score > a.score
