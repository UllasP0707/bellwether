from __future__ import annotations

from datetime import UTC, datetime

import pytest

from bellwether.events.schema import SIGNAL_SOURCE, BehaviorEvent, Employee, SignalType

NOW = datetime(2026, 7, 1, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def now() -> datetime:
    return NOW


@pytest.fixture
def employee() -> Employee:
    return Employee(
        employee_id="E0001",
        tenant_id="acme",
        department="marketing",
        seniority="mid",
        tenure_days=800,
        location="Remote US",
    )


@pytest.fixture
def executive() -> Employee:
    return Employee(
        employee_id="E0002",
        tenant_id="acme",
        department="executive",
        seniority="director",
        tenure_days=1500,
        location="San Francisco",
        is_executive=True,
    )


def make_event(
    employee_id: str,
    signal: SignalType,
    occurred_at: datetime,
) -> BehaviorEvent:
    """Build an event without going through the simulator."""
    return BehaviorEvent(
        tenant_id="acme",
        employee_id=employee_id,
        signal=signal,
        source=SIGNAL_SOURCE[signal],
        occurred_at=occurred_at,
        ingested_at=occurred_at,
    )
