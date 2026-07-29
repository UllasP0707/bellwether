from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from bellwether.events.schema import BehaviorEvent, Employee, SignalType, Source


def test_naive_timestamps_are_rejected() -> None:
    """A naive timestamp in the lake is wrong for anyone in another timezone,
    and nothing downstream can detect it. Reject at the boundary."""
    with pytest.raises(ValidationError):
        BehaviorEvent(
            tenant_id="acme",
            employee_id="E0001",
            signal=SignalType.MFA_PUSH_DENIED,
            source=Source.OKTA,
            occurred_at=datetime(2026, 7, 1, 12, 0, 0),  # noqa: DTZ001 — the point of the test
        )


def test_timestamps_normalize_to_utc() -> None:
    event = BehaviorEvent(
        tenant_id="acme",
        employee_id="E0001",
        signal=SignalType.MFA_PUSH_DENIED,
        source=Source.OKTA,
        occurred_at=datetime(2026, 7, 1, 12, 0, tzinfo=timezone(timedelta(hours=-7))),
    )
    assert event.occurred_at.tzinfo == UTC
    assert event.occurred_at.hour == 19


def test_lateness_is_measured_from_event_time() -> None:
    occurred = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)
    event = BehaviorEvent(
        tenant_id="acme",
        employee_id="E0001",
        signal=SignalType.MFA_PUSH_DENIED,
        source=Source.OKTA,
        occurred_at=occurred,
        ingested_at=occurred + timedelta(minutes=90),
    )
    assert event.lateness_seconds == pytest.approx(5400)


def test_partition_key_is_the_employee() -> None:
    event = BehaviorEvent(
        tenant_id="acme",
        employee_id="E0042",
        signal=SignalType.MFA_PUSH_DENIED,
        source=Source.OKTA,
        occurred_at=datetime(2026, 7, 1, tzinfo=UTC),
    )
    assert event.partition_key() == b"E0042"


def test_events_are_immutable() -> None:
    """Consumers share event objects; a mutation would be a spooky action."""
    event = BehaviorEvent(
        tenant_id="acme",
        employee_id="E0001",
        signal=SignalType.MFA_PUSH_DENIED,
        source=Source.OKTA,
        occurred_at=datetime(2026, 7, 1, tzinfo=UTC),
    )
    with pytest.raises(ValidationError):
        event.employee_id = "E9999"  # type: ignore[misc]


def test_roundtrip_through_json() -> None:
    """Events cross a Kafka topic as JSON; the trip must be lossless."""
    event = BehaviorEvent(
        tenant_id="acme",
        employee_id="E0001",
        signal=SignalType.SENSITIVE_DATA_TO_GENAI,
        source=Source.ENDPOINT_AGENT,
        occurred_at=datetime(2026, 7, 1, 12, 0, tzinfo=UTC),
        attributes={"tool": "chat.example-ai.com", "matched_classifier": "pii"},
    )
    assert BehaviorEvent.model_validate_json(event.model_dump_json()) == event


def test_events_carry_no_pii(employee: Employee) -> None:
    """The dimension holds PII; events hold a token.

    Enforced as a test because the natural, convenient mistake is to denormalize
    an email onto the event for a nicer dashboard, which quietly moves PII into
    a topic with different retention than the table it was supposed to live in.
    """
    pii = {"email", "display_name", "manager_id"}
    assert not set(BehaviorEvent.model_fields) & pii
    assert pii <= set(Employee.model_fields), "PII must live on the dimension"
    assert employee.employee_id  # events carry this and nothing else identifying


def test_high_value_target_covers_all_three_paths() -> None:
    base = dict(
        tenant_id="acme",
        department="sales",
        seniority="mid",
        tenure_days=400,
        location="London",
    )
    assert not Employee(employee_id="E1", **base).is_high_value_target
    assert Employee(employee_id="E2", is_executive=True, **base).is_high_value_target
    assert Employee(employee_id="E3", has_admin_access=True, **base).is_high_value_target
    assert Employee(employee_id="E4", handles_financial_data=True, **base).is_high_value_target
