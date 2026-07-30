from __future__ import annotations

from datetime import timedelta

from bellwether.events.schema import Employee, RiskCategory, SignalType
from bellwether.scoring import RiskBand, score_events
from bellwether.scoring.catalog import spec_for
from tests.conftest import make_event


def test_no_events_scores_zero(employee: Employee, now) -> None:
    result = score_events(employee, [], as_of=now)
    assert result.score == 0.0
    assert result.band is RiskBand.LOW
    assert result.dominant_category is None


def test_score_is_bounded(employee: Employee, now) -> None:
    """Saturating normalization must hold under absurd input.

    A cap that only holds for realistic volumes is not a cap; a compromised
    account can produce thousands of signals in an hour.
    """
    events = [
        make_event(employee.employee_id, SignalType.PHISH_CREDENTIALS_SUBMITTED, now)
        for _ in range(2000)
    ]
    result = score_events(employee, events, as_of=now)
    assert 0.0 <= result.score <= 100.0
    assert result.band is RiskBand.CRITICAL


def test_decay_reduces_contribution(employee: Employee, now) -> None:
    """The same behavior scores lower the longer ago it happened."""
    fresh = score_events(
        employee,
        [make_event(employee.employee_id, SignalType.PHISH_SIM_CLICKED, now)],
        as_of=now,
    )
    stale = score_events(
        employee,
        [
            make_event(
                employee.employee_id,
                SignalType.PHISH_SIM_CLICKED,
                now - timedelta(days=20),
            )
        ],
        as_of=now,
    )
    assert 0 < stale.score < fresh.score


def test_one_half_life_halves_the_contribution(employee: Employee, now) -> None:
    spec = spec_for(SignalType.PHISH_SIM_CLICKED)
    aged = now - timedelta(days=spec.half_life_days)
    result = score_events(
        employee, [make_event(employee.employee_id, SignalType.PHISH_SIM_CLICKED, aged)], as_of=now
    )
    assert result.factors[0].contribution == round(spec.weight / 2, 3)


def test_events_outside_the_lookback_are_ignored(employee: Employee, now) -> None:
    result = score_events(
        employee,
        [
            make_event(
                employee.employee_id,
                SignalType.PHISH_CREDENTIALS_SUBMITTED,
                now - timedelta(days=31),
            )
        ],
        as_of=now,
        lookback_days=30,
    )
    assert result.score == 0.0
    assert result.events_considered == 0


def test_future_events_do_not_exceed_full_weight(employee: Employee, now) -> None:
    """Source clock skew is real; it must not amplify a signal past its weight."""
    skewed = score_events(
        employee,
        [
            make_event(
                employee.employee_id,
                SignalType.PHISH_SIM_CLICKED,
                now + timedelta(hours=6),
            )
        ],
        as_of=now,
    )
    current = score_events(
        employee, [make_event(employee.employee_id, SignalType.PHISH_SIM_CLICKED, now)], as_of=now
    )
    assert skewed.score == current.score


def test_other_employees_events_are_ignored(employee: Employee, now) -> None:
    """A mis-partitioned stream should degrade, not contaminate."""
    result = score_events(
        employee,
        [make_event("E9999", SignalType.PHISH_CREDENTIALS_SUBMITTED, now)],
        as_of=now,
    )
    assert result.score == 0.0
    assert result.events_considered == 0


def test_mitigating_signals_reduce_the_score(employee: Employee, now) -> None:
    clicked = [make_event(employee.employee_id, SignalType.PHISH_SIM_CLICKED, now)]
    with_training = [
        *clicked,
        make_event(employee.employee_id, SignalType.TRAINING_COMPLETED, now),
    ]
    assert (
        score_events(employee, with_training, as_of=now).score
        < score_events(employee, clicked, as_of=now).score
    )


def test_score_never_goes_negative(employee: Employee, now) -> None:
    events = [
        make_event(employee.employee_id, SignalType.REAL_PHISH_REPORTED, now) for _ in range(20)
    ]
    assert score_events(employee, events, as_of=now).score == 0.0


def test_high_value_target_amplifies_the_same_behavior(
    employee: Employee, executive: Employee, now
) -> None:
    """Identical behavior, higher blast radius, higher score."""
    ordinary = score_events(
        employee,
        [make_event(employee.employee_id, SignalType.PHISH_SIM_CLICKED, now)],
        as_of=now,
    )
    exec_score = score_events(
        executive,
        [make_event(executive.employee_id, SignalType.PHISH_SIM_CLICKED, now)],
        as_of=now,
    )
    assert exec_score.score > ordinary.score


def test_amplification_does_not_apply_to_mitigating_signals(executive: Employee, now) -> None:
    """Otherwise one training completion could erase a real exposure for an exec."""
    result = score_events(
        executive,
        [make_event(executive.employee_id, SignalType.TRAINING_COMPLETED, now)],
        as_of=now,
    )
    training = spec_for(SignalType.TRAINING_COMPLETED)
    assert result.factors[0].contribution == round(training.weight, 3)


def test_attribution_groups_by_signal(employee: Employee, now) -> None:
    events = [
        make_event(employee.employee_id, SignalType.PHISH_SIM_CLICKED, now - timedelta(days=d))
        for d in (0, 1, 2)
    ] + [make_event(employee.employee_id, SignalType.FILE_SHARED_PUBLIC_LINK, now)]

    result = score_events(employee, events, as_of=now)
    by_signal = {f.signal: f for f in result.factors}

    assert by_signal[SignalType.PHISH_SIM_CLICKED.value].occurrences == 3
    assert by_signal[SignalType.PHISH_SIM_CLICKED.value].most_recent == now
    assert result.events_considered == 4


def test_category_breakdown_and_dominant_category(employee: Employee, now) -> None:
    events = [
        make_event(employee.employee_id, SignalType.PHISH_CREDENTIALS_SUBMITTED, now),
        make_event(employee.employee_id, SignalType.FILE_SHARED_EXTERNALLY, now),
    ]
    result = score_events(employee, events, as_of=now)
    assert result.dominant_category is RiskCategory.PHISHING_SUSCEPTIBILITY
    assert set(result.by_category) == {
        RiskCategory.PHISHING_SUSCEPTIBILITY,
        RiskCategory.DATA_HANDLING,
    }


def test_top_factors_excludes_mitigating_and_ranks_by_size(employee: Employee, now) -> None:
    events = [
        make_event(employee.employee_id, SignalType.PHISH_CREDENTIALS_SUBMITTED, now),
        make_event(employee.employee_id, SignalType.FILE_SHARED_EXTERNALLY, now),
        make_event(employee.employee_id, SignalType.TRAINING_COMPLETED, now),
    ]
    top = score_events(employee, events, as_of=now).top_factors(3)
    assert [f.signal for f in top] == [
        SignalType.PHISH_CREDENTIALS_SUBMITTED.value,
        SignalType.FILE_SHARED_EXTERNALLY.value,
    ]


def test_scoring_is_order_independent(employee: Employee, now) -> None:
    """Out-of-order delivery is normal; it must not change the result."""
    events = [
        make_event(employee.employee_id, SignalType.PHISH_SIM_CLICKED, now - timedelta(days=1)),
        make_event(employee.employee_id, SignalType.MFA_PUSH_FLOOD, now - timedelta(days=3)),
        make_event(employee.employee_id, SignalType.TRAINING_COMPLETED, now - timedelta(days=2)),
    ]
    assert (
        score_events(employee, events, as_of=now).score
        == score_events(employee, list(reversed(events)), as_of=now).score
    )


def test_bands_partition_the_range() -> None:
    assert RiskBand.of(0) is RiskBand.LOW
    assert RiskBand.of(19.99) is RiskBand.LOW
    assert RiskBand.of(20) is RiskBand.MODERATE
    assert RiskBand.of(40) is RiskBand.ELEVATED
    assert RiskBand.of(60) is RiskBand.HIGH
    assert RiskBand.of(80) is RiskBand.CRITICAL
    assert RiskBand.of(100) is RiskBand.CRITICAL


def test_exposure_signal_alone_scores_zero(employee: Employee, now) -> None:
    """Receiving ten phishing simulations is not a behavior worth scoring."""
    events = [
        make_event(employee.employee_id, SignalType.PHISH_SIM_DELIVERED, now) for _ in range(10)
    ]
    assert score_events(employee, events, as_of=now).score == 0.0
