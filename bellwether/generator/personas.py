"""Behavioral archetypes for the synthetic population.

Real behavior data is not uniform noise: a handful of people generate most of
the risk, most people generate almost none, and the risky ones are risky in
characteristic ways. A generator that emits uniform random signals produces a
score distribution where every employee sits near the mean — which makes the
whole platform look pointless in a demo, and hides the ranking bugs that matter.

Base rates below are events per employee per day for a baseline employee.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from bellwether.events.schema import RiskCategory, SignalType

# Baseline daily rate per signal. Rare-but-severe signals sit orders of
# magnitude below routine ones, which is what makes the score distribution
# long-tailed rather than bell-shaped.
BASE_DAILY_RATE: dict[SignalType, float] = {
    SignalType.PHISH_SIM_DELIVERED: 0.06,
    SignalType.PHISH_SIM_CLICKED: 0.0,  # only ever a follow-on to delivery
    SignalType.PHISH_SIM_REPORTED: 0.0,  # ditto
    SignalType.PHISH_CREDENTIALS_SUBMITTED: 0.0,  # follow-on to a click
    SignalType.REAL_PHISH_REPORTED: 0.004,
    SignalType.MFA_PUSH_DENIED: 0.05,
    SignalType.MFA_PUSH_FLOOD: 0.002,
    SignalType.PASSWORD_REUSE_DETECTED: 0.001,
    SignalType.CREDENTIAL_IN_BREACH_DUMP: 0.0008,
    SignalType.IMPOSSIBLE_TRAVEL_LOGIN: 0.001,
    SignalType.FILE_SHARED_EXTERNALLY: 0.4,
    SignalType.FILE_SHARED_PUBLIC_LINK: 0.01,
    SignalType.SENSITIVE_DATA_TO_GENAI: 0.02,
    SignalType.BULK_DOWNLOAD_DETECTED: 0.003,
    SignalType.USB_MASS_STORAGE_MOUNTED: 0.01,
    SignalType.OAUTH_GRANT_RISKY_SCOPE: 0.004,
    SignalType.EMAIL_FORWARDING_RULE_CREATED: 0.001,
    SignalType.ADMIN_PRIVILEGE_GRANTED: 0.0004,
    SignalType.STALE_ACCESS_UNREVIEWED: 0.002,
    SignalType.TRAINING_COMPLETED: 0.01,
    SignalType.TRAINING_OVERDUE: 0.006,
    SignalType.INTERVENTION_ACKNOWLEDGED: 0.0,  # emitted by the platform loop
    SignalType.INTERVENTION_IGNORED: 0.0,
}


@dataclass(frozen=True, slots=True)
class Persona:
    """A behavioral archetype.

    Attributes:
        share: Fraction of the population, before dimension-based reassignment.
        category_multipliers: Scales every signal in a risk category.
        signal_multipliers: Scales one signal, applied after the category
            multiplier.
        click_rate: P(clicks | simulated phish delivered).
        submit_rate: P(submits credentials | clicked). The gap between these two
            is where most of a population's real exposure lives.
        report_rate: P(reports | delivered and did not click).
    """

    name: str
    share: float
    click_rate: float
    submit_rate: float
    report_rate: float
    category_multipliers: dict[RiskCategory, float] = field(default_factory=dict)
    signal_multipliers: dict[SignalType, float] = field(default_factory=dict)

    def rate_for(self, signal: SignalType) -> float:
        """Daily arrival rate of this signal for this persona."""
        from bellwether.scoring.catalog import spec_for

        base = BASE_DAILY_RATE.get(signal, 0.0)
        if base == 0.0:
            return 0.0
        category = spec_for(signal).category
        rate = base * self.category_multipliers.get(category, 1.0)
        return rate * self.signal_multipliers.get(signal, 1.0)


PERSONAS: tuple[Persona, ...] = (
    Persona(
        name="vigilant",
        share=0.22,
        click_rate=0.01,
        submit_rate=0.02,
        report_rate=0.75,
        category_multipliers={
            RiskCategory.DATA_HANDLING: 0.4,
            RiskCategory.CREDENTIAL_HYGIENE: 0.3,
            RiskCategory.ACCESS_HYGIENE: 0.5,
        },
        signal_multipliers={
            SignalType.TRAINING_COMPLETED: 3.0,
            SignalType.REAL_PHISH_REPORTED: 4.0,
            SignalType.TRAINING_OVERDUE: 0.1,
        },
    ),
    Persona(
        name="typical",
        share=0.46,
        click_rate=0.06,
        submit_rate=0.12,
        report_rate=0.22,
    ),
    Persona(
        name="hurried",
        share=0.18,
        click_rate=0.19,
        submit_rate=0.28,
        report_rate=0.05,
        category_multipliers={
            RiskCategory.DATA_HANDLING: 2.2,
            RiskCategory.CREDENTIAL_HYGIENE: 1.8,
        },
        signal_multipliers={
            SignalType.TRAINING_OVERDUE: 4.0,
            SignalType.TRAINING_COMPLETED: 0.3,
            SignalType.FILE_SHARED_PUBLIC_LINK: 3.0,
        },
    ),
    # Not careless — targeted. Attackers pick these people deliberately, so
    # their exposure is high even when their behavior is average.
    Persona(
        name="targeted",
        share=0.06,
        click_rate=0.11,
        submit_rate=0.2,
        report_rate=0.3,
        category_multipliers={RiskCategory.CREDENTIAL_HYGIENE: 3.5},
        signal_multipliers={
            SignalType.PHISH_SIM_DELIVERED: 3.0,
            SignalType.MFA_PUSH_FLOOD: 8.0,
            SignalType.IMPOSSIBLE_TRAVEL_LOGIN: 4.0,
            SignalType.EMAIL_FORWARDING_RULE_CREATED: 3.0,
        },
    ),
    # High rates across the board, but they decay: new hires converge toward
    # typical as tenure grows (see population assignment).
    Persona(
        name="onboarding",
        share=0.05,
        click_rate=0.24,
        submit_rate=0.3,
        report_rate=0.08,
        category_multipliers={RiskCategory.ACCESS_HYGIENE: 1.6},
        signal_multipliers={
            SignalType.TRAINING_OVERDUE: 3.0,
            SignalType.OAUTH_GRANT_RISKY_SCOPE: 2.5,
        },
    ),
    Persona(
        name="shadow_it",
        share=0.03,
        click_rate=0.09,
        submit_rate=0.14,
        report_rate=0.15,
        category_multipliers={
            RiskCategory.DATA_HANDLING: 3.0,
            RiskCategory.ACCESS_HYGIENE: 3.2,
        },
        signal_multipliers={
            SignalType.SENSITIVE_DATA_TO_GENAI: 6.0,
            SignalType.OAUTH_GRANT_RISKY_SCOPE: 5.0,
            SignalType.USB_MASS_STORAGE_MOUNTED: 3.0,
        },
    ),
)

PERSONAS_BY_NAME: dict[str, Persona] = {p.name: p for p in PERSONAS}
