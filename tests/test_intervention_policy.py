"""Policy and ledger tests.

The gates on their own, with no decider, no stage and no copy. Everything here
is a pure function of a `Policy` and a list of past interventions, which is the
property that makes the restraint in this system auditable rather than emergent.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from bellwether.events.schema import SignalType
from bellwether.interventions.policy import InMemoryLedger, Policy, band_rose, cooldown_active
from bellwether.interventions.types import Channel, CopySource, InterventionEvent, InterventionType
from bellwether.scoring import RiskBand

NOW = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)


def sent(
    ledger: InMemoryLedger,
    type: InterventionType = InterventionType.NUDGE,
    at: datetime = NOW,
    trigger_event_id: str = "seed",
    signal: SignalType | None = None,
) -> InterventionEvent:
    """Put a real intervention in the ledger so the gates have history to read.

    `signal` matters to one gate only: the urgency override reads the previous
    message's trigger to decide whether it may cut ahead of spacing. Defaults
    to a routine signal, because that is the case the override exists for.
    """
    event = InterventionEvent(
        tenant_id="acme",
        employee_id="E0042",
        type=type,
        channel=Channel.CHAT,
        trigger_signal=signal or SignalType.PHISH_SIM_CLICKED,
        trigger_event_id=trigger_event_id,
        band=RiskBand.HIGH,
        score=70.0,
        subject="s",
        body="b",
        copy_source=CopySource.TEMPLATE,
        created_at=at,
    )
    ledger.record(event)
    return event


# --- band movement ------------------------------------------------------------


@pytest.mark.parametrize(
    ("previous", "current", "rose"),
    [
        (RiskBand.ELEVATED, RiskBand.HIGH, True),
        (RiskBand.LOW, RiskBand.CRITICAL, True),
        (RiskBand.HIGH, RiskBand.HIGH, False),
        (RiskBand.HIGH, RiskBand.ELEVATED, False),
        (None, RiskBand.CRITICAL, False),
    ],
)
def test_band_movement(previous: RiskBand | None, current: RiskBand, rose: bool) -> None:
    assert band_rose(previous, current) is rose


def test_the_threshold_is_inclusive_at_its_own_band() -> None:
    policy = Policy(min_band=RiskBand.ELEVATED)
    assert policy.meets_threshold(RiskBand.ELEVATED)
    assert policy.meets_threshold(RiskBand.CRITICAL)
    assert not policy.meets_threshold(RiskBand.MODERATE)


# --- spacing --------------------------------------------------------------------


def test_no_history_means_no_cooldown() -> None:
    assert cooldown_active(None, NOW, hours=72) is False


@pytest.mark.parametrize(
    ("hours_ago", "active"), [(1, True), (71, True), (72, False), (100, False)]
)
def test_cooldown_boundary(hours_ago: int, active: bool) -> None:
    assert cooldown_active(NOW - timedelta(hours=hours_ago), NOW, hours=72) is active


# --- the ladder -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("prior", "disengaged", "expected"),
    [
        (0, False, InterventionType.NUDGE),
        (1, False, InterventionType.TRAINING),
        (0, True, InterventionType.TRAINING),
        (2, False, InterventionType.MANAGER_NOTIFICATION),
        (9, False, InterventionType.MANAGER_NOTIFICATION),
    ],
)
def test_the_ladder_climbs_one_rung_at_a_time(
    prior: int, disengaged: bool, expected: InterventionType
) -> None:
    """Never skips a rung: nobody goes from silence to their manager in one step."""
    policy = Policy(allow_manager_notification=True)
    assert policy.rung(prior, disengaged, has_manager=True) is expected


@pytest.mark.parametrize(("allowed", "has_manager"), [(False, True), (True, False), (False, False)])
def test_the_top_rung_clamps_rather_than_disappearing(allowed: bool, has_manager: bool) -> None:
    """Turning the manager rung off must not make the system quieter than intended.

    Suppressing instead of clamping would mean a deployment that disables the
    strongest action also loses the second-strongest, so the safer configuration
    would protect people less.
    """
    policy = Policy(allow_manager_notification=allowed)
    assert policy.rung(9, False, has_manager=has_manager) is InterventionType.TRAINING


# --- the ledger -----------------------------------------------------------------


def test_the_ledger_rejects_a_duplicate_regardless_of_type() -> None:
    """One behaviour, one message.

    The uniqueness key deliberately excludes the intervention type. With it
    included, a redelivered score climbed a rung and inserted cleanly as a
    different type — the same click producing a nudge and then a training
    assignment.
    """
    ledger = InMemoryLedger()
    first = sent(ledger, type=InterventionType.NUDGE, trigger_event_id="evt-1")
    second = InterventionEvent(**{**first.model_dump(), "type": InterventionType.TRAINING})

    assert ledger.record(second) is False
    assert len(ledger) == 1


def test_the_ledger_accepts_a_different_trigger() -> None:
    ledger = InMemoryLedger()
    sent(ledger, trigger_event_id="evt-1")
    sent(ledger, trigger_event_id="evt-2")

    assert len(ledger) == 2


def test_the_ledger_reads_back_newest_first() -> None:
    ledger = InMemoryLedger()
    sent(ledger, at=NOW - timedelta(days=2), trigger_event_id="a")
    sent(ledger, at=NOW, trigger_event_id="b")

    assert [e.trigger_event_id for e in ledger.history("acme", "E0042")] == ["b", "a"]


def test_last_sent_at_can_ask_about_one_type_or_any() -> None:
    ledger = InMemoryLedger()
    sent(ledger, type=InterventionType.NUDGE, at=NOW - timedelta(days=5), trigger_event_id="a")
    sent(ledger, type=InterventionType.TRAINING, at=NOW, trigger_event_id="b")

    assert ledger.last_sent_at("acme", "E0042") == NOW
    assert ledger.last_sent_at("acme", "E0042", InterventionType.NUDGE) == NOW - timedelta(days=5)
    assert ledger.last_sent_at("acme", "E0042", InterventionType.MANAGER_NOTIFICATION) is None


def test_the_ledger_is_scoped_by_tenant() -> None:
    ledger = InMemoryLedger()
    sent(ledger)

    assert ledger.count_since("other", "E0042", NOW - timedelta(days=1)) == 0
    assert ledger.history("other", "E0042") == []


def test_counting_respects_the_window() -> None:
    ledger = InMemoryLedger()
    sent(ledger, at=NOW - timedelta(days=10), trigger_event_id="old")
    sent(ledger, at=NOW - timedelta(days=1), trigger_event_id="new")

    assert ledger.count_since("acme", "E0042", NOW - timedelta(days=7)) == 1
    assert ledger.count_since("acme", "E0042", NOW - timedelta(days=30)) == 2


def test_a_recorded_intervention_reads_back_intact() -> None:
    """The ledger is the audit trail, so nothing may be lost on the way in."""
    ledger = InMemoryLedger()
    event = InterventionEvent(
        tenant_id="acme",
        employee_id="E0042",
        type=InterventionType.TRAINING,
        channel=Channel.EMAIL,
        trigger_event_id="evt-1",
        band=RiskBand.CRITICAL,
        previous_band=RiskBand.HIGH,
        score=88.5,
        subject="Please reset your password now",
        body="Hi Dana, please reset your password.",
        copy_source=CopySource.MODEL,
        guardrail_rejections=2,
        created_at=NOW,
    )
    ledger.record(event)

    assert ledger.history("acme", "E0042")[0] == event
