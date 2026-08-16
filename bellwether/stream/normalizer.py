"""The normalizer: `events.raw` -> `events.normalized`.

Connectors already emit `BehaviorEvent`, so this stage is not about parsing. It
does the three things that have to happen between ingestion and scoring:

**Repartition.** `events.raw` is keyed by the vendor's record id, which spreads
connector output evenly and lets a connector republish a record without caring
who it belongs to. Stateful per-employee scoring needs the opposite: every event
for one person on one partition. Re-keying to `employee_id` is the reason two
topics exist rather than one.

**Deduplicate.** Everything upstream is at-least-once. This is where redelivery
stops.

**Tolerate versions it doesn't recognise.** A consumer that crashes on an
unfamiliar `schema_version` takes its partition down with it and blocks every
well-formed event behind the bad one. Unknown *future* versions are forwarded
unvalidated — the routing fields are stable by contract, so this stage can still
place the event correctly and let a newer consumer interpret it. Genuinely
malformed input at a version we *do* understand is a different thing, and goes
to the dead-letter topic.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from pydantic import ValidationError

from bellwether.events.schema import SCHEMA_VERSION, BehaviorEvent
from bellwether.stream.dedup import DedupStore, InMemoryDedup


class Outcome(StrEnum):
    """What the normalizer decided about one message."""

    EMITTED = "emitted"
    DUPLICATE = "duplicate"
    FORWARDED_UNKNOWN_VERSION = "forwarded_unknown_version"
    DEAD_LETTERED = "dead_lettered"


@dataclass
class NormalizerStats:
    emitted: int = 0
    duplicate: int = 0
    forwarded_unknown_version: int = 0
    dead_lettered: int = 0

    @property
    def total(self) -> int:
        return self.emitted + self.duplicate + self.forwarded_unknown_version + self.dead_lettered

    def record(self, outcome: Outcome) -> None:
        setattr(self, outcome.value, getattr(self, outcome.value) + 1)


@dataclass(frozen=True)
class Decision:
    """The normalizer's verdict, plus what to publish and under what key."""

    outcome: Outcome
    key: bytes | None = None
    value: bytes | None = None
    reason: str | None = None

    @property
    def publishes(self) -> bool:
        return self.value is not None


# Fields every schema version is required to carry, whatever else changes.
# Routing depends only on these, which is what makes forward compatibility
# possible at all.
ROUTING_FIELDS = ("event_id", "employee_id")


class Normalizer:
    """Re-key, deduplicate, and version-tolerate."""

    def __init__(self, dedup: DedupStore | None = None) -> None:
        self.dedup = dedup if dedup is not None else InMemoryDedup()
        self.stats = NormalizerStats()

    def handle(self, raw: bytes) -> Decision:
        """Decide what to do with one raw message.

        Pure with respect to Kafka: takes bytes, returns a decision. The runner
        turns that into a produce and an offset commit.
        """
        decision = self._decide(raw)
        self.stats.record(decision.outcome)
        return decision

    def _decide(self, raw: bytes) -> Decision:
        try:
            document: Any = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as err:
            return Decision(Outcome.DEAD_LETTERED, value=raw, reason=f"undecodable: {err}")

        if not isinstance(document, dict):
            return Decision(Outcome.DEAD_LETTERED, value=raw, reason="not a JSON object")

        missing = [field for field in ROUTING_FIELDS if not document.get(field)]
        if missing:
            # Without these the message cannot even be placed, let alone
            # understood. Nothing downstream could recover it.
            return Decision(
                Outcome.DEAD_LETTERED, value=raw, reason=f"missing routing fields: {missing}"
            )

        event_id = str(document["event_id"])
        key = str(document["employee_id"]).encode()

        # Dedup before validation: a redelivered malformed message should be
        # dead-lettered once, not once per redelivery.
        if self.dedup.seen(event_id):
            return Decision(Outcome.DUPLICATE, key=key, reason=event_id)

        version = document.get("schema_version", SCHEMA_VERSION)
        try:
            event = BehaviorEvent.model_validate(document)
        except ValidationError as err:
            if isinstance(version, int) and version > SCHEMA_VERSION:
                # Written by something newer than us. Forward it rather than
                # discard it; the routing fields were enough to place it.
                return Decision(
                    Outcome.FORWARDED_UNKNOWN_VERSION,
                    key=key,
                    value=raw,
                    reason=f"schema_version {version} > {SCHEMA_VERSION}",
                )
            return Decision(
                Outcome.DEAD_LETTERED, key=key, value=raw, reason=f"invalid: {err.error_count()}"
            )

        # Re-serialise from the parsed model rather than forwarding the input
        # bytes, so anything downstream is reading a value this stage actually
        # understood.
        return Decision(Outcome.EMITTED, key=key, value=event.model_dump_json().encode())
