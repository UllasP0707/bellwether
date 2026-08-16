"""Normalizer tests.

The handler takes bytes and returns a decision, so all of this runs without a
broker. The Kafka runner around it only moves messages and manages offsets.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from bellwether.events.schema import SCHEMA_VERSION, BehaviorEvent, SignalType, Source
from bellwether.stream.dedup import InMemoryDedup
from bellwether.stream.normalizer import Normalizer, Outcome


def make_raw(**overrides: object) -> bytes:
    event = BehaviorEvent(
        tenant_id="acme",
        employee_id="E0042",
        signal=SignalType.PHISH_SIM_CLICKED,
        source=Source.EMAIL_GATEWAY,
        occurred_at=datetime(2026, 7, 1, 12, 0, tzinfo=UTC),
        source_event_id="ms_1",
    )
    document = json.loads(event.model_dump_json())
    document.update(overrides)
    return json.dumps(document).encode()


# --- repartitioning ---------------------------------------------------------


def test_output_is_keyed_by_employee() -> None:
    """The reason two topics exist: raw is keyed by vendor record id, normalized
    by employee, so per-employee state needs no cross-partition coordination."""
    decision = Normalizer().handle(make_raw())

    assert decision.outcome is Outcome.EMITTED
    assert decision.key == b"E0042"
    assert decision.publishes


def test_emitted_value_is_reserialized_not_passed_through() -> None:
    """Downstream should only ever read a value this stage actually parsed."""
    decision = Normalizer().handle(make_raw(extra_field="ignored"))

    assert decision.outcome is Outcome.EMITTED
    assert decision.value is not None
    assert b"extra_field" not in decision.value
    assert BehaviorEvent.model_validate_json(decision.value).employee_id == "E0042"


def test_round_trip_preserves_the_event() -> None:
    original = BehaviorEvent(
        tenant_id="acme",
        employee_id="E0007",
        signal=SignalType.PHISH_CREDENTIALS_SUBMITTED,
        source=Source.EMAIL_GATEWAY,
        occurred_at=datetime(2026, 7, 1, 12, 0, tzinfo=UTC),
        source_event_id="ms_9",
        raw_ref="s3://bucket/raw/x.json",
        attributes={"campaign_id": "camp-101"},
    )
    decision = Normalizer().handle(original.model_dump_json().encode())

    assert decision.value is not None
    assert BehaviorEvent.model_validate_json(decision.value) == original


# --- deduplication ----------------------------------------------------------


def test_redelivery_is_suppressed() -> None:
    normalizer = Normalizer()
    raw = make_raw()

    assert normalizer.handle(raw).outcome is Outcome.EMITTED
    second = normalizer.handle(raw)

    assert second.outcome is Outcome.DUPLICATE
    assert not second.publishes, "a duplicate must not be republished"
    assert normalizer.stats.emitted == 1
    assert normalizer.stats.duplicate == 1


def test_distinct_events_are_not_confused() -> None:
    normalizer = Normalizer()
    assert normalizer.handle(make_raw(event_id="a")).outcome is Outcome.EMITTED
    assert normalizer.handle(make_raw(event_id="b")).outcome is Outcome.EMITTED
    assert normalizer.stats.emitted == 2


def test_duplicate_is_detected_before_validation() -> None:
    """A redelivered bad message should be dead-lettered once, not every time."""
    normalizer = Normalizer()
    bad = make_raw(signal="not_a_signal")

    assert normalizer.handle(bad).outcome is Outcome.DEAD_LETTERED
    assert normalizer.handle(bad).outcome is Outcome.DUPLICATE
    assert normalizer.stats.dead_lettered == 1


def test_dedup_set_is_bounded() -> None:
    """An unbounded dedup set in a long-running consumer is a slow memory leak."""
    dedup = InMemoryDedup(capacity=10)
    for i in range(100):
        dedup.seen(f"event-{i}")
    assert len(dedup) == 10


def test_dedup_evicts_least_recently_seen() -> None:
    dedup = InMemoryDedup(capacity=3)
    for key in ("a", "b", "c"):
        dedup.seen(key)
    dedup.seen("a")  # refresh a
    dedup.seen("d")  # evicts b

    assert dedup.seen("a") is True
    assert dedup.seen("b") is False, "b should have been evicted"


# --- version tolerance ------------------------------------------------------


def test_unknown_future_version_is_forwarded_not_dropped() -> None:
    """A consumer that crashes on an unfamiliar version blocks its partition."""
    raw = make_raw(schema_version=SCHEMA_VERSION + 5, signal="something_invented_later")
    decision = Normalizer().handle(raw)

    assert decision.outcome is Outcome.FORWARDED_UNKNOWN_VERSION
    assert decision.publishes, "a future version must still reach the next stage"
    assert decision.key == b"E0042", "routing fields are stable across versions"
    assert decision.value == raw, "forwarded verbatim; we did not understand it"


def test_invalid_at_a_known_version_is_dead_lettered() -> None:
    """Not the same thing as a future version: this one we should understand."""
    decision = Normalizer().handle(make_raw(signal="not_a_signal"))

    assert decision.outcome is Outcome.DEAD_LETTERED
    assert decision.publishes, "dead letters are kept, not discarded"
    assert decision.reason is not None


def test_current_version_still_validates() -> None:
    assert Normalizer().handle(make_raw(schema_version=SCHEMA_VERSION)).outcome is Outcome.EMITTED


# --- malformed input --------------------------------------------------------


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(b"not json at all", id="not-json"),
        pytest.param(b"[1, 2, 3]", id="json-but-not-an-object"),
        pytest.param(b'"a string"', id="json-scalar"),
        pytest.param(b"\xff\xfe\x00", id="undecodable-bytes"),
    ],
)
def test_garbage_is_dead_lettered_rather_than_raised(payload: bytes) -> None:
    """One poisoned message must not stall the partition behind it."""
    decision = Normalizer().handle(payload)

    assert decision.outcome is Outcome.DEAD_LETTERED
    assert decision.value == payload


@pytest.mark.parametrize("missing", ["event_id", "employee_id"])
def test_missing_routing_fields_are_dead_lettered(missing: str) -> None:
    """Without these the message cannot be placed at all."""
    document = json.loads(make_raw())
    document[missing] = ""
    decision = Normalizer().handle(json.dumps(document).encode())

    assert decision.outcome is Outcome.DEAD_LETTERED
    assert decision.reason is not None
    assert missing in decision.reason


def test_stats_account_for_every_message() -> None:
    normalizer = Normalizer()
    raw = make_raw()
    for payload in (raw, raw, b"garbage", make_raw(event_id="other")):
        normalizer.handle(payload)

    assert normalizer.stats.total == 4
    assert normalizer.stats.emitted == 2
    assert normalizer.stats.duplicate == 1
    assert normalizer.stats.dead_lettered == 1
