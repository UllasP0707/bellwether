"""Deciding whether to intervene, and at what severity.

Reads a published score and returns a verdict. Writes nothing, sends nothing,
and generates no copy — so the whole policy is testable without a database, a
broker, or a model, which is what a component this consequential needs.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from bellwether.events.schema import RiskCategory, SignalType
from bellwether.events.scores import RiskScoreEvent
from bellwether.interventions.policy import (
    InterventionLedger,
    Policy,
    band_rose,
    cooldown_active,
)
from bellwether.interventions.types import Channel, InterventionType, SuppressionReason

# Behaviours that get a response on their own merits, without waiting for a band
# crossing. Each one means the account may *already* be in someone else's hands,
# and the window in which telling the person still helps is measured in minutes.
#
# `mfa_push_flood` is here for a different reason than the rest: it is not the
# employee's mistake at all. Someone is attacking them, and the useful message
# is "do not approve the next one" — which is worth saying even to an employee
# whose score is otherwise spotless.
TRIGGER_SIGNALS: frozenset[SignalType] = frozenset(
    {
        SignalType.PHISH_CREDENTIALS_SUBMITTED,
        SignalType.CREDENTIAL_IN_BREACH_DUMP,
        SignalType.EMAIL_FORWARDING_RULE_CREATED,
        SignalType.MFA_PUSH_FLOOD,
    }
)

# Which rungs go where. Chat for a nudge, because it is low-friction and
# dismissible; email for anything that has to persist or reach a third party.
_CHANNELS: dict[InterventionType, Channel] = {
    InterventionType.NUDGE: Channel.CHAT,
    InterventionType.TRAINING: Channel.EMAIL,
    InterventionType.MANAGER_NOTIFICATION: Channel.EMAIL,
}


class Trigger(StrEnum):
    BAND_ROSE = "band_rose"
    CRITICAL_SIGNAL = "critical_signal"


@dataclass(frozen=True)
class Verdict:
    """The decision. Exactly one of `send` or `suppressed` is set."""

    trigger: Trigger | None = None
    type: InterventionType | None = None
    channel: Channel | None = None
    suppressed: SuppressionReason | None = None
    prior_in_window: int = 0
    disengaged: bool = False

    @property
    def send(self) -> bool:
        return self.suppressed is None


def _suppress(reason: SuppressionReason) -> Verdict:
    return Verdict(suppressed=reason)


class Decider:
    """Applies `Policy` to a published score."""

    def __init__(self, policy: Policy, ledger: InterventionLedger) -> None:
        self.policy = policy
        self.ledger = ledger

    def evaluate(self, score: RiskScoreEvent, now: datetime, has_manager: bool = False) -> Verdict:
        trigger = self._trigger(score)
        if isinstance(trigger, SuppressionReason):
            return _suppress(trigger)

        # Recency is checked before anything else costs a database round trip,
        # and it applies to both kinds of trigger. A signal trigger fires on the
        # event itself, so an old event means an old concern. A band rise looks
        # current but is not: replaying history rescores each employee with
        # `as_of` set to now, so the score climbs to its true present value and
        # every crossing on the way up is an artefact of ingestion order.
        if not self._is_recent(score, now):
            return _suppress(SuppressionReason.TRIGGER_TOO_OLD)

        prior = self.ledger.count_since(
            score.tenant_id,
            score.employee_id,
            now - timedelta(days=self.policy.ladder_window_days),
        )
        disengaged = score.by_category.get(RiskCategory.SECURITY_ENGAGEMENT, 0.0) > 0
        rung = self.policy.rung(prior, disengaged, has_manager)

        # Two spacing gates, coarsest first.
        #
        # `min_spacing_hours` spans every type. Without it the ladder makes each
        # escalation free: an employee who was nudged this morning escalates to
        # training on their next trigger, training's own cooldown has never been
        # touched, and the second message lands an hour after the first.
        #
        # `cooldown_hours` is then per-type and much longer, so a genuinely new
        # critical signal can still escalate tomorrow, while the same *kind* of
        # message does not repeat for three days.
        # Both are evaluated against one override rather than each having its
        # own. Fixing only the first left the inversion exactly where it was:
        # the credential submission cleared spacing and was then caught by the
        # per-type cooldown, because the routine message a minute earlier had
        # been the same rung. Two gates, one reason to bypass them.
        urgent = self._urgent_override(trigger, score)

        recent = self.ledger.last_sent_at(score.tenant_id, score.employee_id)
        if cooldown_active(recent, now, self.policy.min_spacing_hours) and not urgent:
            return _suppress(SuppressionReason.TOO_SOON)

        last = self.ledger.last_sent_at(score.tenant_id, score.employee_id, rung)
        if cooldown_active(last, now, self.policy.cooldown_hours) and not urgent:
            return _suppress(SuppressionReason.COOLDOWN)

        week = self.ledger.count_since(score.tenant_id, score.employee_id, now - timedelta(days=7))
        if week >= self.policy.weekly_cap:
            return _suppress(SuppressionReason.WEEKLY_CAP)

        return Verdict(
            trigger=trigger,
            type=rung,
            channel=_CHANNELS[rung],
            prior_in_window=prior,
            disengaged=disengaged,
        )

    def _urgent_override(self, trigger: Trigger, score: RiskScoreEvent) -> bool:
        """Whether a critical signal may cut ahead of the spacing gate.

        Found by rehearsing the demo, and it is a real inversion rather than a
        presentation problem. The scripted incident delivers a phish, records a
        click, and records a credential submission sixty-five seconds later.
        The click crossed a band and sent a message about *file sharing* --
        correct, that was the dominant category at the time -- and the
        credential submission, arriving inside the 24-hour spacing window, was
        suppressed. So the person who had just handed over their password was
        told to review their document shares.

        Spacing is right in general: two messages an hour apart is how a system
        like this gets muted. But a routine message must not be able to block
        an urgent one, and the four signals in `TRIGGER_SIGNALS` are exactly
        the ones whose useful window is minutes.

        **Once**, though. The override applies only when the previous message
        was *not* itself urgent, so the worst case is one routine message
        followed by one urgent one, rather than an unbounded run of them. The
        weekly cap and the uniqueness fence are downstream of it and still
        apply, so this widens two gates and not the policy.
        """
        if trigger is not Trigger.CRITICAL_SIGNAL or not self.policy.urgent_overrides_spacing:
            return False

        previous = self.ledger.history(score.tenant_id, score.employee_id, limit=1)
        if not previous:  # pragma: no cover - spacing only bites when one exists
            return False
        return previous[0].trigger_signal not in TRIGGER_SIGNALS

    def _is_recent(self, score: RiskScoreEvent, now: datetime) -> bool:
        """Whether the behaviour behind this score is recent enough to act on.

        An absent `triggered_at` counts as not recent. The system acts on things
        that just happened; if it cannot establish when something happened, the
        honest response is to leave it alone rather than assume the convenient
        answer.
        """
        if score.triggered_at is None:
            return False
        return now - score.triggered_at <= timedelta(hours=self.policy.max_trigger_age_hours)

    def _trigger(self, score: RiskScoreEvent) -> Trigger | SuppressionReason:
        """What, if anything, justifies contacting this person.

        Signal triggers are checked first and deliberately ignore `min_band`.
        Someone whose only security event this year is handing credentials to a
        phishing page scores below the threshold — they have no accumulated
        history to push them over it — and they are precisely the person most
        worth reaching.
        """
        if score.triggered_by in TRIGGER_SIGNALS:
            return Trigger.CRITICAL_SIGNAL

        # A first-ever score is not a crossing. Otherwise onboarding a tenant
        # fires at every employee already above the threshold on day one.
        if score.previous_band is None or score.band == score.previous_band:
            return SuppressionReason.NO_TRIGGER

        if not band_rose(score.previous_band, score.band):
            # A score drifting downward is the system working. Saying so
            # unprompted is noise.
            return SuppressionReason.BAND_FELL

        if not self.policy.meets_threshold(score.band):
            return SuppressionReason.BELOW_THRESHOLD

        return Trigger.BAND_ROSE
