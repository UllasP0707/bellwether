"""The stream scorer: `events.normalized` -> `risk.scores`.

Per message: add the event to the employee's window, rescore the window, and
publish. The scoring itself is `score_events()` — the same function the CLI
calls and, from day 6, the same one Spark will call. This module contributes no
scoring logic of its own, which is the entire point.

**Recomputing the whole window on every event** is O(events in window) rather
than O(1), and an incremental decay update would be cheaper. It is not done
that way because incremental state is the thing a batch recomputation cannot
reproduce, and losing that would cost the stream/batch parity guarantee to buy
a constant factor on a window that is already bounded. Noted in DESIGN.md as
the first thing to revisit if the load test says it matters.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import ValidationError

from bellwether.dimension import EmployeeRepository
from bellwether.events.schema import BehaviorEvent
from bellwether.events.scores import RiskScoreEvent
from bellwether.scoring import score_events
from bellwether.stream.store import EventWindow, ScoreState, WindowedEvent


class ScoreOutcome(StrEnum):
    SCORED = "scored"
    UNKNOWN_EMPLOYEE = "unknown_employee"
    MALFORMED = "malformed"


@dataclass
class ScorerStats:
    scored: int = 0
    unknown_employee: int = 0
    malformed: int = 0
    band_changes: int = 0
    event_latencies_ms: list[float] = field(default_factory=list)
    pipeline_latencies_ms: list[float] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.scored + self.unknown_employee + self.malformed

    def percentile(self, p: float, pipeline: bool = False) -> float:
        """Latency percentile in milliseconds, 0.0 if nothing has been scored."""
        samples = self.pipeline_latencies_ms if pipeline else self.event_latencies_ms
        if not samples:
            return 0.0
        ordered = sorted(samples)
        index = min(int(p / 100 * len(ordered)), len(ordered) - 1)
        return ordered[index]


@dataclass(frozen=True)
class ScoreDecision:
    outcome: ScoreOutcome
    key: bytes | None = None
    value: bytes | None = None
    reason: str | None = None
    band_changed: bool = False

    @property
    def publishes(self) -> bool:
        return self.value is not None


class Scorer:
    """Windowed per-employee scoring."""

    def __init__(
        self,
        employees: EmployeeRepository,
        window: EventWindow,
        state: ScoreState,
        lookback_days: int = 30,
    ) -> None:
        self.employees = employees
        self.window = window
        self.state = state
        self.lookback_days = lookback_days
        self.stats = ScorerStats()

    def handle(self, raw: bytes, now: datetime | None = None) -> ScoreDecision:
        decision = self._decide(raw, now or datetime.now(UTC))
        if decision.outcome is ScoreOutcome.SCORED:
            self.stats.scored += 1
            if decision.band_changed:
                self.stats.band_changes += 1
        elif decision.outcome is ScoreOutcome.UNKNOWN_EMPLOYEE:
            self.stats.unknown_employee += 1
        else:
            self.stats.malformed += 1
        return decision

    def _decide(self, raw: bytes, now: datetime) -> ScoreDecision:
        try:
            event = BehaviorEvent.model_validate_json(raw)
        except (ValidationError, ValueError) as err:
            # The normalizer already dead-lettered anything it could not parse,
            # so reaching here means a forwarded future version. Skip it rather
            # than guess: a score computed from a half-understood event is worse
            # than no score.
            return ScoreDecision(ScoreOutcome.MALFORMED, reason=str(err)[:120])

        employee = self.employees.get(event.employee_id)
        if employee is None:
            # Events can outlive their subject: someone leaves, the dimension
            # drops them, their in-flight events keep arriving.
            return ScoreDecision(
                ScoreOutcome.UNKNOWN_EMPLOYEE,
                key=event.employee_id.encode(),
                reason=event.employee_id,
            )

        self.window.add(
            WindowedEvent(
                employee_id=event.employee_id,
                signal=event.signal,
                occurred_at=event.occurred_at,
                event_id=event.event_id,
            ),
            lookback_days=self.lookback_days,
            as_of=now,
        )

        result = score_events(
            employee,
            self.window.events(event.employee_id),
            as_of=now,
            lookback_days=self.lookback_days,
        )

        previous = self.state.band(event.employee_id)
        self.state.set_band(event.employee_id, result.band)

        event_latency_ms = (now - event.occurred_at).total_seconds() * 1000.0
        pipeline_latency_ms = (now - event.ingested_at).total_seconds() * 1000.0
        self.stats.event_latencies_ms.append(event_latency_ms)
        self.stats.pipeline_latencies_ms.append(pipeline_latency_ms)

        message = RiskScoreEvent.from_risk_score(
            result,
            previous_band=previous,
            triggered_by=event.signal,
            trigger_event_id=event.event_id,
            event_latency_ms=event_latency_ms,
            pipeline_latency_ms=pipeline_latency_ms,
        )
        return ScoreDecision(
            ScoreOutcome.SCORED,
            key=message.partition_key(),
            value=message.model_dump_json().encode(),
            band_changed=message.band_changed,
        )
