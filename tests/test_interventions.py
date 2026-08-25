"""Intervention tests.

Split the way the code is: the policy is a pure function of a score and a
ledger, so most of this needs no broker, no database and no model.

The tests worth reading first are `test_replaying_a_score_sends_nothing_new`
and `test_the_prompt_carries_no_pii`. The first is the property that makes
at-least-once delivery safe for a stage whose side effect reaches a person; the
second is the one that would be embarrassing to get wrong.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from bellwether.dimension import InMemoryEmployeeRepository
from bellwether.events.schema import Employee, RiskCategory, SignalType
from bellwether.events.scores import FactorPayload, RiskScoreEvent
from bellwether.interventions import (
    ClaudeCopywriter,
    CopyBrief,
    Copydesk,
    CopySource,
    CopyUnavailableError,
    Decider,
    Draft,
    Guardrails,
    InMemoryLedger,
    InterventionEvent,
    InterventionStage,
    InterventionType,
    Policy,
    TemplateCopywriter,
    Trigger,
)
from bellwether.interventions.handler import InterventionOutcome
from bellwether.scoring import RiskBand
from tests.test_intervention_policy import sent

NOW = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)


def employee(employee_id: str = "E0042", **overrides: object) -> Employee:
    base = dict(
        employee_id=employee_id,
        tenant_id="acme",
        department="engineering",
        seniority="mid",
        tenure_days=500,
        location="Remote US",
        email="dana.moreau@acme.example",
        display_name="Dana Moreau",
    )
    base.update(overrides)
    return Employee(**base)  # type: ignore[arg-type]


def score(
    band: RiskBand = RiskBand.HIGH,
    previous_band: RiskBand | None = RiskBand.ELEVATED,
    triggered_by: SignalType | None = SignalType.PHISH_SIM_CLICKED,
    trigger_event_id: str | None = "evt-1",
    triggered_at: datetime | None = NOW,
    employee_id: str = "E0042",
    by_category: dict[RiskCategory, float] | None = None,
    factors: list[FactorPayload] | None = None,
) -> RiskScoreEvent:
    return RiskScoreEvent(
        tenant_id="acme",
        employee_id=employee_id,
        score={
            RiskBand.LOW: 10.0,
            RiskBand.MODERATE: 30.0,
            RiskBand.ELEVATED: 50.0,
            RiskBand.HIGH: 70.0,
            RiskBand.CRITICAL: 90.0,
        }[band],
        band=band,
        as_of=NOW,
        previous_band=previous_band,
        band_changed=previous_band is not None and previous_band is not band,
        triggered_by=triggered_by,
        trigger_event_id=trigger_event_id,
        triggered_at=triggered_at,
        dominant_category=RiskCategory.PHISHING_SUSCEPTIBILITY,
        by_category=by_category or {RiskCategory.PHISHING_SUSCEPTIBILITY: 12.0},
        top_factors=factors
        or [
            FactorPayload(
                signal="phish_sim_clicked",
                category=RiskCategory.PHISHING_SUSCEPTIBILITY,
                occurrences=1,
                contribution=8.0,
            )
        ],
    )


def build_stage(
    *people: Employee, policy: Policy | None = None, desk: Copydesk | None = None
) -> tuple[InterventionStage, InMemoryLedger]:
    ledger = InMemoryLedger()
    repo = InMemoryEmployeeRepository(list(people) or [employee()])
    return (
        InterventionStage(repo, ledger, copydesk=desk, policy=policy or Policy()),
        ledger,
    )


# --- triggers ---------------------------------------------------------------


def test_a_rise_into_a_band_above_the_threshold_triggers() -> None:
    decider = Decider(Policy(), InMemoryLedger())
    verdict = decider.evaluate(score(RiskBand.HIGH, RiskBand.ELEVATED), NOW)

    assert verdict.send
    assert verdict.trigger is Trigger.BAND_ROSE


def test_a_first_ever_score_never_triggers() -> None:
    """Onboarding a tenant must not message everyone already above the line."""
    decider = Decider(Policy(), InMemoryLedger())
    verdict = decider.evaluate(score(RiskBand.CRITICAL, previous_band=None), NOW)

    assert not verdict.send
    assert verdict.suppressed == "no_trigger"


def test_a_falling_score_does_not_trigger() -> None:
    decider = Decider(Policy(), InMemoryLedger())
    verdict = decider.evaluate(score(RiskBand.MODERATE, RiskBand.HIGH), NOW)

    assert verdict.suppressed == "band_fell"


def test_movement_inside_a_band_does_not_trigger() -> None:
    decider = Decider(Policy(), InMemoryLedger())
    verdict = decider.evaluate(score(RiskBand.HIGH, RiskBand.HIGH), NOW)

    assert verdict.suppressed == "no_trigger"


def test_a_rise_below_the_threshold_does_not_trigger() -> None:
    """Drifting from low into moderate is not worth anyone's attention."""
    decider = Decider(Policy(), InMemoryLedger())
    verdict = decider.evaluate(score(RiskBand.MODERATE, RiskBand.LOW), NOW)

    assert verdict.suppressed == "below_threshold"


@pytest.mark.parametrize(
    "signal",
    [
        SignalType.PHISH_CREDENTIALS_SUBMITTED,
        SignalType.CREDENTIAL_IN_BREACH_DUMP,
        SignalType.EMAIL_FORWARDING_RULE_CREATED,
        SignalType.MFA_PUSH_FLOOD,
    ],
)
def test_a_critical_signal_triggers_even_below_the_threshold(signal: SignalType) -> None:
    """The person whose only event this year is handing over credentials.

    They have no accumulated history to push them over the band threshold, and
    they are exactly who this system exists to reach.
    """
    decider = Decider(Policy(), InMemoryLedger())
    verdict = decider.evaluate(
        score(RiskBand.LOW, previous_band=RiskBand.LOW, triggered_by=signal), NOW
    )

    assert verdict.send
    assert verdict.trigger is Trigger.CRITICAL_SIGNAL


def test_a_stale_trigger_does_not_fire() -> None:
    """A real bug, found in the live data rather than here.

    A credential submission 32 days old — already outside the scoring lookback,
    contributing exactly zero to the score it was attached to — still produced a
    message telling the employee to reset their password "now". The system was
    simultaneously saying the event was too old to matter and acting on it.
    """
    decider = Decider(Policy(max_trigger_age_hours=48), InMemoryLedger())
    verdict = decider.evaluate(
        score(
            triggered_by=SignalType.PHISH_CREDENTIALS_SUBMITTED,
            triggered_at=NOW - timedelta(days=32),
        ),
        NOW,
    )

    assert verdict.suppressed == "trigger_too_old"


def test_replaying_history_does_not_message_the_population() -> None:
    """Backfill safety falls out of the recency gate rather than needing a mode.

    Reprocessing rescores thirty days of behaviour with `as_of` set to now, so
    the score climbs to its true present value and every band crossing on the
    way up is an artefact of ingestion order. Without this, replaying the log
    means messaging everyone about last month.
    """
    decider = Decider(Policy(), InMemoryLedger())
    verdict = decider.evaluate(
        score(RiskBand.HIGH, RiskBand.ELEVATED, triggered_at=NOW - timedelta(days=12)), NOW
    )

    assert verdict.suppressed == "trigger_too_old"


def test_a_trigger_with_no_time_is_treated_as_stale() -> None:
    """If recency cannot be established, the honest answer is not to act."""
    decider = Decider(Policy(), InMemoryLedger())
    assert decider.evaluate(score(triggered_at=None), NOW).suppressed == "trigger_too_old"


def test_an_ordinary_signal_does_not_trigger_on_its_own() -> None:
    decider = Decider(Policy(), InMemoryLedger())
    verdict = decider.evaluate(
        score(RiskBand.LOW, RiskBand.LOW, triggered_by=SignalType.MFA_PUSH_DENIED), NOW
    )

    assert not verdict.send


# --- rate limiting ----------------------------------------------------------


def test_a_second_message_within_the_minimum_spacing_is_suppressed() -> None:
    """The gate that stops the ladder making each escalation free.

    This was missing at first. With only a per-type cooldown, an employee nudged
    this morning escalates to training on their next trigger, training's own
    cooldown has never been touched, and the second message lands hours later.
    """
    ledger = InMemoryLedger()
    sent(ledger, at=NOW - timedelta(hours=10))
    decider = Decider(Policy(min_spacing_hours=24), ledger)

    assert decider.evaluate(score(), NOW).suppressed == "too_soon"


def test_escalation_is_possible_once_the_spacing_has_passed() -> None:
    """A genuinely new critical signal tomorrow should still escalate."""
    ledger = InMemoryLedger()
    sent(ledger, at=NOW - timedelta(hours=30))
    decider = Decider(Policy(min_spacing_hours=24), ledger)
    verdict = decider.evaluate(score(), NOW)

    assert verdict.send
    assert verdict.type is InterventionType.TRAINING


def test_cooldown_suppresses_a_repeat_of_the_same_type() -> None:
    """Longer than the spacing gate, and scoped to one kind of message."""
    ledger = InMemoryLedger()
    sent(ledger, at=NOW - timedelta(hours=30))
    # A short ladder window keeps the prior from counting, so the rung stays a
    # nudge and the per-type cooldown is the gate under test.
    decider = Decider(Policy(cooldown_hours=72, ladder_window_days=1), ledger)

    assert decider.evaluate(score(), NOW).suppressed == "cooldown"


def test_cooldown_expires() -> None:
    ledger = InMemoryLedger()
    sent(ledger, at=NOW - timedelta(hours=80))
    decider = Decider(Policy(cooldown_hours=72, ladder_window_days=1), ledger)

    assert decider.evaluate(score(), NOW).send


def test_the_weekly_cap_holds_even_when_every_cooldown_has_expired() -> None:
    """Three types with independent cooldowns can still produce three messages.

    Cooldown alone does not bound the total, which is the whole reason there is
    a second gate.
    """
    ledger = InMemoryLedger()
    for i, type in enumerate(InterventionType):
        sent(ledger, type=type, at=NOW - timedelta(days=6 - i), trigger_event_id=f"seed-{i}")
    decider = Decider(Policy(weekly_cap=3, allow_manager_notification=True), ledger)

    assert decider.evaluate(score(), NOW, has_manager=True).suppressed == "weekly_cap"


def test_the_weekly_cap_is_a_rolling_window() -> None:
    ledger = InMemoryLedger()
    for i, type in enumerate(InterventionType):
        sent(ledger, type=type, at=NOW - timedelta(days=9), trigger_event_id=f"seed-{i}")
    decider = Decider(Policy(weekly_cap=3, ladder_window_days=5), ledger)

    assert decider.evaluate(score(), NOW).send


# --- the ladder -------------------------------------------------------------


def test_the_first_intervention_is_the_gentlest_one() -> None:
    decider = Decider(Policy(), InMemoryLedger())
    assert decider.evaluate(score(), NOW).type is InterventionType.NUDGE


def test_repeat_behaviour_climbs_a_rung() -> None:
    ledger = InMemoryLedger()
    sent(ledger, at=NOW - timedelta(days=10))
    decider = Decider(Policy(), ledger)

    assert decider.evaluate(score(), NOW).type is InterventionType.TRAINING


def test_disengagement_climbs_a_rung_early() -> None:
    """Repeating a nudge somebody already ignored is the least useful option."""
    decider = Decider(Policy(), InMemoryLedger())
    verdict = decider.evaluate(score(by_category={RiskCategory.SECURITY_ENGAGEMENT: 5.0}), NOW)

    assert verdict.disengaged
    assert verdict.type is InterventionType.TRAINING


def test_the_manager_rung_is_off_by_default() -> None:
    """Escalating to somebody's boss is the one action here that is irreversible."""
    ledger = InMemoryLedger()
    for i in range(4):
        sent(ledger, at=NOW - timedelta(days=20 + i), trigger_event_id=f"seed-{i}")
    decider = Decider(Policy(weekly_cap=99), ledger)

    assert decider.evaluate(score(), NOW, has_manager=True).type is InterventionType.TRAINING


def test_the_manager_rung_needs_the_flag_and_a_manager() -> None:
    ledger = InMemoryLedger()
    for i in range(4):
        sent(ledger, at=NOW - timedelta(days=20 + i), trigger_event_id=f"seed-{i}")
    decider = Decider(Policy(weekly_cap=99, allow_manager_notification=True), ledger)

    assert (
        decider.evaluate(score(), NOW, has_manager=True).type
        is InterventionType.MANAGER_NOTIFICATION
    )
    assert decider.evaluate(score(), NOW, has_manager=False).type is InterventionType.TRAINING


def test_the_ladder_clamps_rather_than_going_silent() -> None:
    """Disabling the top rung must not be worse than leaving it on."""
    ledger = InMemoryLedger()
    for i in range(9):
        sent(ledger, at=NOW - timedelta(days=20), trigger_event_id=f"seed-{i}")
    decider = Decider(Policy(weekly_cap=99), ledger)
    verdict = decider.evaluate(score(), NOW, has_manager=True)

    assert verdict.send
    assert verdict.type is InterventionType.TRAINING


# --- the stage ---------------------------------------------------------------


def test_a_triggering_score_produces_a_message() -> None:
    stage, ledger = build_stage()
    decision = stage.handle(score().model_dump_json().encode(), now=NOW)

    assert decision.outcome is InterventionOutcome.SENT
    assert decision.key == b"E0042"
    published = InterventionEvent.model_validate_json(decision.value or b"")
    assert published.employee_id == "E0042"
    assert published.type is InterventionType.NUDGE
    assert len(ledger) == 1


def test_replaying_a_score_sends_nothing_new() -> None:
    """At-least-once delivery reaches this stage too, and here it reaches a person.

    This caught a real bug. The ledger's uniqueness key originally included the
    intervention *type*, so a redelivered score found one more prior in the
    ledger, climbed a rung, and inserted cleanly as a different type — the same
    click producing first a nudge and then a training assignment. Keying on the
    triggering event alone is what makes the replay genuinely inert.
    """
    # Every rate gate is switched off, so the uniqueness index is the only thing
    # left that can stop a second send. With the defaults in place the spacing
    # gate catches a replay first, which is correct behaviour and would leave
    # the fence underneath it untested.
    stage, ledger = build_stage(
        policy=Policy(min_spacing_hours=0, cooldown_hours=0, weekly_cap=10_000)
    )
    payload = score().model_dump_json().encode()

    first = stage.handle(payload, now=NOW)
    assert first.outcome is InterventionOutcome.SENT

    for _ in range(5):
        again = stage.handle(payload, now=NOW)

    assert again.outcome is InterventionOutcome.SUPPRESSED
    assert again.reason == "already_sent"
    assert not again.publishes
    assert len(ledger) == 1


def test_a_score_with_no_trigger_id_is_refused() -> None:
    """Being able to act exactly once is a precondition for acting at all."""
    stage, ledger = build_stage()
    decision = stage.handle(score(trigger_event_id=None).model_dump_json().encode(), now=NOW)

    assert decision.reason == "no_trigger_id"
    assert len(ledger) == 0


def test_an_unknown_employee_is_not_contacted() -> None:
    """People leave. Their events keep arriving; they should not keep hearing from us."""
    stage, _ = build_stage()
    decision = stage.handle(score(employee_id="E9999").model_dump_json().encode(), now=NOW)

    assert decision.outcome is InterventionOutcome.UNKNOWN_EMPLOYEE
    assert not decision.publishes


def test_an_unparseable_message_is_skipped_not_raised() -> None:
    stage, _ = build_stage()
    decision = stage.handle(b"{not json", now=NOW)

    assert decision.outcome is InterventionOutcome.MALFORMED
    assert stage.stats.malformed == 1


def test_stats_account_for_every_message() -> None:
    stage, _ = build_stage()
    stage.handle(score().model_dump_json().encode(), now=NOW)
    stage.handle(score(RiskBand.HIGH, RiskBand.HIGH).model_dump_json().encode(), now=NOW)
    stage.handle(score(employee_id="E9999").model_dump_json().encode(), now=NOW)
    stage.handle(b"garbage", now=NOW)

    assert stage.stats.total == 4
    assert (stage.stats.sent, stage.stats.suppressed) == (1, 1)
    assert stage.stats.by_reason == {"no_trigger": 1}
    assert stage.stats.by_type == {"nudge": 1}


def test_the_ledger_row_exists_before_the_message_is_published() -> None:
    """The ordering that decides which failure an employee experiences.

    A crash between the two loses a message nobody received. The reverse
    ordering sends a second message to someone who already got one.
    """
    stage, ledger = build_stage()
    decision = stage.handle(score().model_dump_json().encode(), now=NOW)

    assert len(ledger) == 1
    published = InterventionEvent.model_validate_json(decision.value or b"")
    assert ledger.history("acme", "E0042")[0].intervention_id == published.intervention_id


# --- copy --------------------------------------------------------------------


class StubWriter:
    """A model stand-in. Records the brief so a test can inspect what it saw."""

    def __init__(self, draft: Draft | None = None, fail: bool = False) -> None:
        self.draft = draft
        self.fail = fail
        self.briefs: list[CopyBrief] = []

    def write(self, brief: CopyBrief) -> Draft:
        self.briefs.append(brief)
        if self.fail:
            raise CopyUnavailableError("stub failure")
        assert self.draft is not None
        return self.draft


GOOD_DRAFT = Draft(
    subject="Worth a two-minute check",
    body=(
        "Hi Dana, a message you opened yesterday was a phishing simulation, and "
        "nothing went wrong. Next time one asks you to sign in, please report it "
        "rather than following the link."
    ),
    source=CopySource.MODEL,
)


def test_model_copy_is_used_when_it_passes_the_guardrails() -> None:
    desk = Copydesk(model=StubWriter(GOOD_DRAFT))
    stage, _ = build_stage(desk=desk)
    decision = stage.handle(score().model_dump_json().encode(), now=NOW)

    published = InterventionEvent.model_validate_json(decision.value or b"")
    assert published.copy_source is CopySource.MODEL
    assert published.subject == GOOD_DRAFT.subject
    assert desk.stats.model_drafts == 1


def test_model_copy_that_breaks_a_guardrail_is_never_sent() -> None:
    """The headline property: model output reaches nobody unvalidated."""
    bad = Draft(
        subject="You failed a phishing test",
        body=(
            "Hi Dana, your account has been compromised because you were careless "
            "with a phishing email, and HR has been informed about this incident."
        ),
        source=CopySource.MODEL,
    )
    desk = Copydesk(model=StubWriter(bad))
    stage, _ = build_stage(desk=desk)
    decision = stage.handle(score().model_dump_json().encode(), now=NOW)

    published = InterventionEvent.model_validate_json(decision.value or b"")
    assert published.copy_source is CopySource.TEMPLATE
    assert bad.body not in published.body
    assert published.guardrail_rejections == 1
    assert set(desk.stats.rejected_rules) >= {"accusatory", "overclaiming", "threatening"}


def test_a_generation_failure_falls_back_rather_than_going_silent() -> None:
    desk = Copydesk(model=StubWriter(fail=True))
    stage, _ = build_stage(desk=desk)
    decision = stage.handle(score().model_dump_json().encode(), now=NOW)

    assert decision.outcome is InterventionOutcome.SENT
    assert desk.stats.model_errors == 1
    assert desk.stats.template_drafts == 1


def test_published_copy_carries_no_pii_beyond_the_first_name() -> None:
    stage, _ = build_stage()
    decision = stage.handle(score().model_dump_json().encode(), now=NOW)
    published = InterventionEvent.model_validate_json(decision.value or b"")

    assert "Dana" in published.body
    assert "Moreau" not in published.body
    assert "@" not in published.body
    assert "E0042" not in published.body


def test_the_prompt_carries_no_pii() -> None:
    """A writer cannot leak what it was never given.

    Cheaper as a guarantee than checking the output for every field the
    dimension holds, and it holds for a model whose behaviour we do not control.
    """
    writer = StubWriter(GOOD_DRAFT)
    stage, _ = build_stage(desk=Copydesk(model=writer))
    stage.handle(score().model_dump_json().encode(), now=NOW)

    brief = writer.briefs[0]
    rendered = ClaudeCopywriter(client=object()).prompt(brief)
    assert brief.first_name == "Dana"
    assert "Moreau" not in rendered
    assert "@" not in rendered
    assert "E0042" not in rendered
    assert "phish_sim_clicked" not in rendered


def test_the_brief_leads_with_the_behaviour_that_triggered_it() -> None:
    """A message opening on a three-week-old training lapse reads as a non-sequitur."""
    writer = StubWriter(GOOD_DRAFT)
    stage, _ = build_stage(desk=Copydesk(model=writer))
    stage.handle(
        score(
            triggered_by=SignalType.PHISH_CREDENTIALS_SUBMITTED,
            factors=[
                FactorPayload(
                    signal="training_overdue",
                    category=RiskCategory.SECURITY_ENGAGEMENT,
                    occurrences=1,
                    contribution=5.0,
                )
            ],
        )
        .model_dump_json()
        .encode(),
        now=NOW,
    )

    behaviours = writer.briefs[0].behaviours
    assert "credentials" in behaviours[0].lower()
    assert len(behaviours) == 2


def test_a_template_that_failed_its_own_guardrails_would_not_be_sent() -> None:
    """Defence in depth behind the parametrised sweep in test_guardrails.py."""

    class BadTemplates:
        def write(self, brief: CopyBrief) -> Draft:
            return Draft("You failed", "You were careless.", CopySource.TEMPLATE)

    desk = Copydesk(templates=BadTemplates())
    draft, _ = desk.compose(
        CopyBrief(first_name="Dana", type=InterventionType.NUDGE, band=RiskBand.HIGH)
    )

    assert "careless" not in draft.body
    assert desk.stats.last_resort == 1
    assert Guardrails().check(draft.subject, draft.body) == []


def test_the_copy_desk_uses_templates_when_there_is_no_model() -> None:
    """Running without an API key is a supported configuration, not a degraded one."""
    desk = Copydesk()
    draft, rejections = desk.compose(
        CopyBrief(
            first_name="Dana",
            type=InterventionType.NUDGE,
            band=RiskBand.HIGH,
            dominant_category=RiskCategory.DATA_HANDLING,
        )
    )

    assert draft.source is CopySource.TEMPLATE
    assert rejections == 0
    assert desk.stats.model_errors == 0


def test_the_manager_message_is_addressed_to_the_manager() -> None:
    draft = TemplateCopywriter().write(
        CopyBrief(
            first_name="Dana",
            type=InterventionType.MANAGER_NOTIFICATION,
            band=RiskBand.CRITICAL,
            dominant_category=RiskCategory.PHISHING_SUSCEPTIBILITY,
        )
    )

    assert "Dana" in draft.body
    assert "your team" in draft.body
    assert "phishing susceptibility" in draft.body, "the enum value would trip a guardrail"
