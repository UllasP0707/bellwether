"""The score contract: what lands on `risk.scores`.

Separate from the in-process `RiskScore` dataclass on purpose. That one is a
computation result and can change freely; this one is published on a compacted
topic that outlives the process, so it is versioned and explicit.

Carries its own attribution rather than just a number. A security team will not
act on a score it cannot interrogate, and an intervention cannot be written
without knowing which behaviour drove the score up.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from bellwether.events.schema import RiskCategory
from bellwether.scoring import RiskBand, RiskScore

SCORE_SCHEMA_VERSION = 1


class FactorPayload(BaseModel):
    """One signal's contribution to a published score."""

    model_config = ConfigDict(frozen=True)

    signal: str
    category: RiskCategory
    occurrences: int
    contribution: float


class RiskScoreEvent(BaseModel):
    """An employee's score at a point in time."""

    model_config = ConfigDict(frozen=True)

    schema_version: int = SCORE_SCHEMA_VERSION
    tenant_id: str
    employee_id: str

    score: float
    band: RiskBand
    as_of: datetime

    # Set when this score moved the employee into a different band. Day 4 fires
    # on transitions, not levels: nudging someone every time their score twitches
    # within a band is how a system teaches people to ignore it.
    previous_band: RiskBand | None = None
    band_changed: bool = False

    dominant_category: RiskCategory | None = None
    by_category: dict[RiskCategory, float] = Field(default_factory=dict)
    top_factors: list[FactorPayload] = Field(default_factory=list)
    events_considered: int = 0

    # Two latencies, because they answer different questions.
    #
    # event_latency_ms is behaviour -> score, which is the product claim: a
    # score should move within seconds of what caused it. On a backfill it is
    # dominated by how old the history is and means little.
    #
    # pipeline_latency_ms is ingest -> score, which is what this system is
    # actually accountable for and the only one of the two that is an SLO.
    event_latency_ms: float | None = None
    pipeline_latency_ms: float | None = None

    @classmethod
    def from_risk_score(
        cls,
        result: RiskScore,
        previous_band: RiskBand | None = None,
        event_latency_ms: float | None = None,
        pipeline_latency_ms: float | None = None,
        factors: int = 5,
    ) -> RiskScoreEvent:
        return cls(
            tenant_id=result.tenant_id,
            employee_id=result.employee_id,
            score=result.score,
            band=result.band,
            as_of=result.as_of,
            previous_band=previous_band,
            band_changed=previous_band is not None and previous_band is not result.band,
            dominant_category=result.dominant_category,
            by_category=result.by_category,
            top_factors=[
                FactorPayload(
                    signal=f.signal,
                    category=f.category,
                    occurrences=f.occurrences,
                    contribution=f.contribution,
                )
                for f in result.top_factors(factors)
            ],
            events_considered=result.events_considered,
            event_latency_ms=event_latency_ms,
            pipeline_latency_ms=pipeline_latency_ms,
        )

    def partition_key(self) -> bytes:
        """Keyed by employee so compaction keeps the latest score per person."""
        return self.employee_id.encode()
