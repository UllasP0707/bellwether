"""The event store behind the mock vendor API."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from bellwether.events.schema import BehaviorEvent, Source
from bellwether.generator.population import build_population
from bellwether.generator.simulate import Simulator
from bellwether.vendor.payloads import to_vendor_payload


@dataclass
class VendorStore:
    """Vendor-shaped records, per source, in the order the vendor recorded them.

    Ordered by `ingested_at`, not `occurred_at`. That is what a real API does —
    you poll forward through the vendor's own arrival order — and it means a
    connector paging forward will legitimately receive events whose event times
    go backwards. Anything downstream that assumes a monotonic event-time stream
    breaks here, which is precisely the property worth exercising early.
    """

    pages: dict[Source, list[dict[str, Any]]] = field(default_factory=dict)
    emails: dict[str, str] = field(default_factory=dict)

    def records(self, source: Source) -> list[dict[str, Any]]:
        return self.pages.get(source, [])

    def slice(
        self, source: Source, start: int, limit: int
    ) -> tuple[list[dict[str, Any]], int, bool]:
        """Return `limit` records from `start`, plus the next offset and whether more remain."""
        records = self.records(source)
        window = records[start : start + limit]
        next_offset = start + len(window)
        return window, next_offset, next_offset < len(records)

    def total(self, source: Source) -> int:
        return len(self.records(source))


def build_store(
    size: int = 500,
    days: int = 30,
    seed: int = 1337,
    end: datetime | None = None,
) -> VendorStore:
    """Seed the mock vendor with a population's behaviour history.

    Uses the same seed defaults as the rest of the project, so the employee the
    demo talks about is the same employee here.

    `end` exists because a seed alone does not make this deterministic. The
    simulator generates a quarter as much activity at weekends, so the window's
    weekday composition changes what gets generated — and a window ending at
    `now` has a different composition every day. A test asserting anything about
    the *contents* of this store therefore passes or fails depending on the date
    it is run, which is how `test_signals_reaching_the_pipeline_cover_the_catalog`
    came to fail on a Tuesday having passed all week.
    """
    population = build_population(size=size, seed=seed)
    emails = {member.employee.employee_id: member.employee.email or "" for member in population}

    simulator = Simulator(population, seed=seed + 1)
    events: list[BehaviorEvent] = list(simulator.backfill(days=days, end=end))
    # Vendor arrival order.
    events.sort(key=lambda e: e.ingested_at)

    pages: dict[Source, list[dict[str, Any]]] = {}
    for event in events:
        payload = to_vendor_payload(event, emails.get(event.employee_id, "unknown@acme.example"))
        if payload is None:
            continue
        pages.setdefault(event.source, []).append(payload)

    return VendorStore(pages=pages, emails=emails)
