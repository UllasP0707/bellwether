"""The scoring function. Shared verbatim by the streaming and batch paths.

Deliberately pure: no I/O, no clock reads, no database lookups. `as_of` is a
parameter rather than `now()` so a Spark executor recomputing history and a
stream consumer scoring live traffic run identical code, and so tests are
deterministic.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Protocol

from bellwether.events.schema import RiskCategory, SignalType
from bellwether.scoring.catalog import spec_for


class ScorableEvent(Protocol):
    """The three fields scoring actually reads from an event.

    Deliberately narrower than `BehaviorEvent`. Scoring runs in three places
    that hold events in three different shapes: a stream consumer with parsed
    models, a Redis window holding a compact projection, and Spark executors
    holding `Row` objects. Requiring the full model would force the batch path
    to materialise millions of Pydantic objects — slow enough that the tempting
    fix is to reimplement the scoring logic in Spark, which is exactly the
    duplication this project exists to avoid.

    A structural type costs nothing and keeps one implementation reachable from
    all three.
    """

    @property
    def employee_id(self) -> str: ...

    @property
    def signal(self) -> SignalType: ...

    @property
    def occurred_at(self) -> datetime: ...


class ScorableSubject(Protocol):
    """What scoring needs to know about the person, beyond their events."""

    @property
    def employee_id(self) -> str: ...

    @property
    def tenant_id(self) -> str: ...

    @property
    def is_high_value_target(self) -> bool: ...


# Scale constant for normalization. Chosen so that a single credential
# submission on a phishing page (weight 25) lands around 60 — "act on this
# today" — while a population of ordinary noise stays under 25.
_SATURATION = 30.0

# How much a high-value target's aggravating signals are amplified. The same
# click is more dangerous from someone who can wire money or reset passwords.
_HVT_MULTIPLIER = 1.4


class RiskBand(StrEnum):
    """Coarse bucket for UI and intervention routing.

    Bands exist because thresholds should be named once, not re-derived at each
    call site that wants to know if a score is "bad."
    """

    LOW = "low"
    MODERATE = "moderate"
    ELEVATED = "elevated"
    HIGH = "high"
    CRITICAL = "critical"

    @classmethod
    def of(cls, score: float) -> RiskBand:
        if score >= 80:
            return cls.CRITICAL
        if score >= 60:
            return cls.HIGH
        if score >= 40:
            return cls.ELEVATED
        if score >= 20:
            return cls.MODERATE
        return cls.LOW


@dataclass(frozen=True, slots=True)
class ScoreFactor:
    """One signal's contribution, for attribution in the UI and in nudge copy."""

    signal: str
    category: RiskCategory
    occurrences: int
    contribution: float
    most_recent: datetime

    @property
    def is_mitigating(self) -> bool:
        return self.contribution < 0


@dataclass(frozen=True, slots=True)
class RiskScore:
    """A scored employee at a point in time."""

    employee_id: str
    tenant_id: str
    score: float
    band: RiskBand
    as_of: datetime
    by_category: dict[RiskCategory, float] = field(default_factory=dict)
    factors: list[ScoreFactor] = field(default_factory=list)
    events_considered: int = 0

    def top_factors(self, n: int = 3) -> list[ScoreFactor]:
        """The n largest aggravating contributions, largest first."""
        aggravating = [f for f in self.factors if not f.is_mitigating]
        return sorted(aggravating, key=lambda f: f.contribution, reverse=True)[:n]

    @property
    def dominant_category(self) -> RiskCategory | None:
        """Category driving the score, or None if nothing is contributing."""
        positive = {c: v for c, v in self.by_category.items() if v > 0}
        if not positive:
            return None
        return max(positive, key=lambda c: positive[c])


def _decay(age_days: float, half_life_days: float) -> float:
    """Exponential decay factor for a signal of the given age.

    Clamped at zero age so an event timestamped slightly in the future — clock
    skew on a source system, which does happen — cannot amplify itself above
    full weight.
    """
    if age_days <= 0:
        return 1.0
    return float(0.5 ** (age_days / half_life_days))


def _normalize(raw: float) -> float:
    """Map an unbounded weighted sum onto 0-100.

    Saturating rather than linear-with-a-cap, for two reasons: the curve is
    monotonic everywhere, so the score always moves in the direction the
    behavior did, and it compresses at the top, so the difference between
    "very risky" and "even more risky" doesn't crowd out the difference between
    "fine" and "concerning" — which is the distinction a security team acts on.
    """
    if raw <= 0:
        return 0.0
    return 100.0 * (1.0 - math.exp(-raw / _SATURATION))


def score_events(
    employee: ScorableSubject,
    events: Iterable[ScorableEvent],
    as_of: datetime,
    lookback_days: int = 30,
) -> RiskScore:
    """Score one employee from their recent behavior.

    Args:
        employee: The employee dimension row. Supplies the high-value-target
            amplification; the events alone don't know who this person is.
        events: This employee's events. Order-independent, and events outside
            the lookback window or belonging to another employee are ignored
            rather than trusted, so a mis-partitioned stream degrades to a
            wrong-ish score instead of a crash.
        as_of: Evaluation time. All decay is measured from here.
        lookback_days: Events older than this contribute nothing.

    Returns:
        The score, its per-category breakdown, and per-signal attribution.
    """
    grouped: dict[str, list[tuple[float, ScorableEvent]]] = {}
    by_category: dict[RiskCategory, float] = {c: 0.0 for c in RiskCategory}
    raw = 0.0
    considered = 0

    amplify = _HVT_MULTIPLIER if employee.is_high_value_target else 1.0

    for event in events:
        if event.employee_id != employee.employee_id:
            continue

        age_days = (as_of - event.occurred_at).total_seconds() / 86400.0
        if age_days > lookback_days:
            continue

        spec = spec_for(event.signal)
        contribution = spec.weight * _decay(age_days, spec.half_life_days)

        # Amplify aggravating signals only. Scaling up someone's mitigating
        # signals because they're an executive would let a single training
        # completion erase a real exposure.
        if contribution > 0:
            contribution *= amplify

        raw += contribution
        by_category[spec.category] += contribution
        grouped.setdefault(event.signal.value, []).append((contribution, event))
        considered += 1

    factors = [
        ScoreFactor(
            signal=signal,
            category=spec_for(SignalType(signal)).category,
            occurrences=len(entries),
            contribution=round(sum(c for c, _ in entries), 3),
            most_recent=max(e.occurred_at for _, e in entries),
        )
        for signal, entries in grouped.items()
    ]

    score = round(_normalize(raw), 2)

    return RiskScore(
        employee_id=employee.employee_id,
        tenant_id=employee.tenant_id,
        score=score,
        band=RiskBand.of(score),
        as_of=as_of,
        by_category={c: round(v, 3) for c, v in by_category.items() if v != 0.0},
        factors=factors,
        events_considered=considered,
    )
