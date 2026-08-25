"""The intervention stage: `risk.scores` -> `bellwether.interventions`.

Per message: decide, write copy, claim the ledger row, publish. The interesting
line is the order of the last two.

**The ledger row is written before the message is published.** If the process
dies in between, an intervention is recorded that nobody ever received, and the
employee hears nothing for the length of the cooldown. Publishing first and
recording second would instead mean a redelivered score message produces a
second message to a real person. Both are failures; only one of them is visible
to the employee, and quietly under-notifying is the one to choose.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import ValidationError

from bellwether.dimension import EmployeeRepository
from bellwether.events.schema import Employee, SignalType
from bellwether.events.scores import RiskScoreEvent
from bellwether.interventions.copy import CopyBrief, Copydesk, describe
from bellwether.interventions.decide import Decider
from bellwether.interventions.policy import InterventionLedger, Policy
from bellwether.interventions.types import InterventionEvent, SuppressionReason


class InterventionOutcome(StrEnum):
    SENT = "sent"
    SUPPRESSED = "suppressed"
    UNKNOWN_EMPLOYEE = "unknown_employee"
    MALFORMED = "malformed"


@dataclass
class InterventionStats:
    sent: int = 0
    suppressed: int = 0
    unknown_employee: int = 0
    malformed: int = 0
    by_type: dict[str, int] = field(default_factory=dict)
    by_reason: dict[str, int] = field(default_factory=dict)

    @property
    def total(self) -> int:
        return self.sent + self.suppressed + self.unknown_employee + self.malformed


@dataclass(frozen=True)
class InterventionDecision:
    outcome: InterventionOutcome
    key: bytes | None = None
    value: bytes | None = None
    reason: str | None = None
    intervention: InterventionEvent | None = None

    @property
    def publishes(self) -> bool:
        return self.value is not None


def _names(employee: Employee) -> tuple[str, tuple[str, ...]]:
    """First name for the copy, and everything that must not appear in it.

    The forbidden set is derived here rather than inside the guardrails because
    this is the boundary where PII enters the intervention path at all: the
    dimension is the only component holding an email or a surname, and one
    function assembling the deny-list keeps that true.
    """
    parts = (employee.display_name or "").split()
    first = parts[0] if parts else "there"
    forbidden = [employee.employee_id, *parts[1:]]
    if employee.email:
        forbidden.append(employee.email)
        forbidden.append(employee.email.split("@")[0])
    return first, tuple(f for f in forbidden if f)


class InterventionStage:
    """Turns a published score into at most one message."""

    def __init__(
        self,
        employees: EmployeeRepository,
        ledger: InterventionLedger,
        copydesk: Copydesk | None = None,
        policy: Policy | None = None,
    ) -> None:
        self.employees = employees
        self.ledger = ledger
        self.copydesk = copydesk or Copydesk()
        self.policy = policy or Policy()
        self.decider = Decider(self.policy, ledger)
        self.stats = InterventionStats()

    def handle(self, raw: bytes, now: datetime | None = None) -> InterventionDecision:
        decision = self._decide(raw, now or datetime.now(UTC))

        match decision.outcome:
            case InterventionOutcome.SENT:
                self.stats.sent += 1
                if decision.intervention is not None:
                    key = decision.intervention.type.value
                    self.stats.by_type[key] = self.stats.by_type.get(key, 0) + 1
            case InterventionOutcome.SUPPRESSED:
                self.stats.suppressed += 1
                reason = decision.reason or "unknown"
                self.stats.by_reason[reason] = self.stats.by_reason.get(reason, 0) + 1
            case InterventionOutcome.UNKNOWN_EMPLOYEE:
                self.stats.unknown_employee += 1
            case InterventionOutcome.MALFORMED:
                self.stats.malformed += 1

        return decision

    def _decide(self, raw: bytes, now: datetime) -> InterventionDecision:
        try:
            score = RiskScoreEvent.model_validate_json(raw)
        except (ValidationError, ValueError) as err:
            return InterventionDecision(InterventionOutcome.MALFORMED, reason=str(err)[:120])

        # The dimension is consulted before the policy runs because a message
        # needs a name and a manager, and because someone who has left the
        # company should not be contacted at all.
        employee = self.employees.get(score.employee_id)
        if employee is None:
            return InterventionDecision(
                InterventionOutcome.UNKNOWN_EMPLOYEE,
                key=score.employee_id.encode(),
                reason=SuppressionReason.UNKNOWN_EMPLOYEE.value,
            )

        # Refusing to act without a trigger id is a hard rule, not a nicety. It
        # is the ledger's uniqueness key, so a score that lacks one cannot be
        # made idempotent, and every redelivery of it would reach a real person
        # again. Being able to do something exactly once is a precondition for
        # doing it at all.
        if score.trigger_event_id is None:
            return InterventionDecision(
                InterventionOutcome.SUPPRESSED,
                key=score.employee_id.encode(),
                reason=SuppressionReason.NO_TRIGGER_ID.value,
            )

        verdict = self.decider.evaluate(score, now, has_manager=bool(employee.manager_id))
        if not verdict.send:
            assert verdict.suppressed is not None
            return InterventionDecision(
                InterventionOutcome.SUPPRESSED,
                key=score.employee_id.encode(),
                reason=verdict.suppressed.value,
            )

        assert verdict.type is not None and verdict.channel is not None
        first_name, forbidden = _names(employee)
        draft, rejections = self.copydesk.compose(
            CopyBrief(
                first_name=first_name,
                type=verdict.type,
                band=score.band,
                dominant_category=score.dominant_category,
                trigger_signal=score.triggered_by,
                behaviours=self._behaviours(score),
            ),
            forbidden=forbidden,
        )

        intervention = InterventionEvent(
            tenant_id=score.tenant_id,
            employee_id=score.employee_id,
            type=verdict.type,
            channel=verdict.channel,
            trigger_signal=score.triggered_by,
            trigger_event_id=score.trigger_event_id,
            band=score.band,
            previous_band=score.previous_band,
            score=score.score,
            dominant_category=score.dominant_category,
            subject=draft.subject,
            body=draft.body,
            copy_source=draft.source,
            guardrail_rejections=rejections,
            created_at=now,
        )

        if not self.ledger.record(intervention):
            # The uniqueness index rejected it, so this exact intervention has
            # been sent before and the score message is a redelivery. Nothing to
            # do, and nothing wrong — this is at-least-once working as intended.
            return InterventionDecision(
                InterventionOutcome.SUPPRESSED,
                key=score.employee_id.encode(),
                reason=SuppressionReason.ALREADY_SENT.value,
            )

        return InterventionDecision(
            InterventionOutcome.SENT,
            key=intervention.partition_key(),
            value=intervention.model_dump_json().encode(),
            intervention=intervention,
        )

    @staticmethod
    def _behaviours(score: RiskScoreEvent, limit: int = 3) -> tuple[str, ...]:
        """Plain-English descriptions of what drove the score.

        The triggering behaviour goes first even if it is not the largest
        contributor: it is the reason the message is being sent now, and a
        message that leads with a three-week-old training lapse reads as a
        non-sequitur to someone who just clicked something.
        """
        descriptions: list[str] = []
        if score.triggered_by is not None:
            descriptions.append(describe(score.triggered_by))
        for factor in score.top_factors:
            try:
                signal = SignalType(factor.signal)
            except ValueError:  # pragma: no cover - a signal this build predates
                continue
            text = describe(signal)
            if text not in descriptions:
                descriptions.append(text)
        return tuple(descriptions[:limit])
