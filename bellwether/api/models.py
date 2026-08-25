"""Response shapes.

Separate from the wire contracts on the topics, and deliberately so: a published
score is a record of what the scorer computed, while these are answers to
questions a security team asks. Serving the topic model directly would tie the
HTTP surface to a Kafka schema and leak fields — latencies, trigger ids — that
mean nothing to a caller.

**The privacy gradient is the design decision here.** Ranking the population
returns tokens and scores with no names, so browsing is pseudonymous. Looking
one person up returns their name and is written to the audit log. A tool that
sorts colleagues by how much of a liability they are should make the act of
identifying somebody deliberate.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict

from bellwether.events.schema import RiskCategory, SignalType
from bellwether.events.scores import RiskScoreEvent
from bellwether.scoring import RiskBand
from bellwether.scoring.catalog import spec_for


class FactorOut(BaseModel):
    """One signal's contribution, with the catalog's own words for it.

    The description travels with the number because a score nobody can
    interrogate does not get acted on, and `bulk_download_detected` is an
    identifier, not an explanation.
    """

    model_config = ConfigDict(frozen=True)

    signal: str
    description: str
    category: RiskCategory
    occurrences: int
    contribution: float


class RankedScore(BaseModel):
    """One row of the population ranking. No name, by design."""

    model_config = ConfigDict(frozen=True)

    employee_id: str
    score: float
    band: RiskBand
    dominant_category: RiskCategory | None = None
    department: str | None = None
    events_considered: int = 0
    as_of: datetime

    @classmethod
    def of(cls, score: RiskScoreEvent, department: str | None = None) -> RankedScore:
        return cls(
            employee_id=score.employee_id,
            score=score.score,
            band=score.band,
            dominant_category=score.dominant_category,
            department=department,
            events_considered=score.events_considered,
            as_of=score.as_of,
        )


class EmployeeScore(BaseModel):
    """One employee, named. This response is audited."""

    model_config = ConfigDict(frozen=True)

    employee_id: str
    display_name: str | None = None
    department: str
    seniority: str
    is_high_value_target: bool

    score: float
    band: RiskBand
    previous_band: RiskBand | None = None
    as_of: datetime
    dominant_category: RiskCategory | None = None
    by_category: dict[RiskCategory, float] = {}
    top_factors: list[FactorOut] = []
    events_considered: int = 0


class TimelineEntry(BaseModel):
    """One event in an employee's window, and what it is worth today.

    `contribution` is recomputed at read time rather than stored. A signal's
    weight decays, so the honest answer to "how much is this click costing them"
    changes every day, and a number frozen at ingest would drift away from the
    score it is supposed to explain.
    """

    model_config = ConfigDict(frozen=True)

    signal: SignalType
    description: str
    category: RiskCategory
    occurred_at: datetime
    age_days: float
    contribution: float


class Timeline(BaseModel):
    model_config = ConfigDict(frozen=True)

    employee_id: str
    as_of: datetime
    entries: list[TimelineEntry]


class DepartmentRisk(BaseModel):
    model_config = ConfigDict(frozen=True)

    department: str
    headcount: int
    scored: int
    mean_score: float
    p90_score: float
    critical: int
    high: int


class InterventionOut(BaseModel):
    model_config = ConfigDict(frozen=True)

    intervention_id: str
    type: str
    channel: str
    trigger_signal: SignalType | None = None
    band: RiskBand
    score: float
    subject: str
    body: str
    copy_source: str
    created_at: datetime


class CatalogEntry(BaseModel):
    """The scoring model, published.

    Exposed because a security team that cannot see how a score is built will
    not defend it to the person it is about, and "the algorithm decided" is not
    an answer anybody should have to give a colleague.
    """

    model_config = ConfigDict(frozen=True)

    signal: SignalType
    category: RiskCategory
    weight: float
    half_life_days: float
    description: str
    is_mitigating: bool


def catalog_entry(signal: SignalType) -> CatalogEntry:
    spec = spec_for(signal)
    return CatalogEntry(
        signal=spec.signal,
        category=spec.category,
        weight=spec.weight,
        half_life_days=spec.half_life_days,
        description=spec.description,
        is_mitigating=spec.is_mitigating,
    )
