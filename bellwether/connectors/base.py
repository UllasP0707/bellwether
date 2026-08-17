"""The connector framework.

A connector is the only place in Bellwether that knows what a vendor payload
looks like. Its job, per record:

1. Fetch a page from the vendor, surviving rate limits and transient failures.
2. Archive the raw payload and keep the reference.
3. Parse the vendor's vocabulary into a `SignalType`.
4. Resolve the vendor's notion of identity (an email) into our employee token.
5. Emit a `BehaviorEvent`, then advance the cursor.

Subclasses implement `fetch` and `parse`. Everything else — archival, identity
resolution, deterministic ids, cursor management, counters — is here, so a new
source is roughly forty lines.
"""

from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, ClassVar

from bellwether.connectors.archive import RawArchive
from bellwether.connectors.cursors import CursorStore
from bellwether.connectors.http import VendorClient
from bellwether.events.schema import BehaviorEvent, Employee, SignalType, Source
from bellwether.generator.sinks import Sink

# Fixed namespace so `event_id` is a pure function of (source, source_event_id).
# Reprocessing the same vendor record must produce the same event id, or
# at-least-once delivery stops being harmless: downstream deduplication and the
# "replaying a duplicate is a no-op" property in DESIGN.md both depend on it.
EVENT_NAMESPACE = uuid.UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff")


def deterministic_event_id(source: Source, source_event_id: str) -> str:
    return str(uuid.uuid5(EVENT_NAMESPACE, f"{source.value}:{source_event_id}"))


@dataclass(frozen=True)
class Page:
    """One page of vendor records, plus where to resume.

    `cursor` is a position, not a "there is more" flag. Polling log APIs hand
    back a cursor that stays valid past the end of the stream precisely so a
    poller can hold it and come back for whatever arrives later; exhaustion is
    signalled by an empty page.

    Conflating the two is how a connector ends up storing "no next page" as its
    resume point and re-ingesting the vendor's entire history on every cycle.
    """

    records: list[dict[str, Any]]
    cursor: str | None


@dataclass(frozen=True)
class ParsedRecord:
    """A vendor record, understood.

    `subject_email` rather than an employee id: vendors do not know our tokens,
    and resolving that is the framework's job, not the parser's.
    """

    signal: SignalType
    occurred_at: datetime
    subject_email: str
    source_event_id: str
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class PollResult:
    """What one run of a connector did. Every skip is counted, never silent."""

    connector: str
    pages: int = 0
    fetched: int = 0
    emitted: int = 0
    unknown_event_type: int = 0
    unresolved_identity: int = 0
    malformed: int = 0
    cursor: str | None = None
    drained: bool = False

    @property
    def skipped(self) -> int:
        return self.unknown_event_type + self.unresolved_identity + self.malformed


class EmployeeDirectory:
    """Resolves a vendor's email address to an employee token.

    Unresolvable addresses are normal, not exceptional: service accounts,
    contractors, shared mailboxes and people who left last month all appear in
    vendor logs. They are counted and dropped rather than guessed at, because
    attributing a stranger's behaviour to a real employee is worse than
    ignoring it.
    """

    def __init__(self, employees: list[Employee]) -> None:
        self._by_email: dict[str, str] = {}
        self.ambiguous: set[str] = set()
        for employee in employees:
            if not employee.email:
                continue
            key = employee.email.strip().lower()
            existing = self._by_email.get(key)
            if existing is not None and existing != employee.employee_id:
                # Two people, one address. Whatever the cause - a bad import, a
                # shared mailbox, a directory bug - the honest answer is that
                # this address does not identify anyone.
                self.ambiguous.add(key)
            self._by_email[key] = employee.employee_id

    def resolve(self, email: str) -> str | None:
        """Resolve an address, or None if it does not identify exactly one person.

        Refusing to guess matters more here than almost anywhere else in the
        system: picking a winner would attribute one employee's phishing click
        to a colleague, and the resulting score would look entirely plausible.
        """
        key = email.strip().lower()
        if key in self.ambiguous:
            return None
        return self._by_email.get(key)

    def __len__(self) -> int:
        return len(self._by_email)


class Connector(ABC):
    """Base class for all source integrations."""

    source: ClassVar[Source]
    name: ClassVar[str]
    stream: ClassVar[str] = "default"

    def __init__(
        self,
        client: VendorClient,
        directory: EmployeeDirectory,
        archive: RawArchive,
        sink: Sink,
        cursors: CursorStore,
        tenant_id: str = "acme",
    ) -> None:
        self.client = client
        self.directory = directory
        self.archive = archive
        self.sink = sink
        self.cursors = cursors
        self.tenant_id = tenant_id

    # --- subclass responsibilities ---------------------------------------

    @abstractmethod
    def fetch(self, cursor: str | None, limit: int) -> Page:
        """Fetch one page starting at `cursor`."""

    @abstractmethod
    def parse(self, record: dict[str, Any]) -> ParsedRecord | None:
        """Interpret one vendor record.

        Return None for records this connector does not care about. Vendor
        endpoints carry far more event types than are risk-relevant, and
        ignoring the rest is normal operation, not an error.
        """

    # --- the run loop -----------------------------------------------------

    def run(self, max_pages: int = 100, limit: int = 100) -> PollResult:
        """Poll until the vendor stops returning records or `max_pages` is hit.

        Two orderings matter here.

        The cursor is written **after** the page's events are emitted. That is
        what makes the pipeline at-least-once: a crash in between redelivers the
        page on restart, and because `event_id` is derived from the vendor's own
        record id, the duplicates are identical and harmless. The other ordering
        would silently lose data, which is much worse.

        The loop terminates on an **empty page**, not on a missing cursor, and it
        persists the position it reached even when the source is exhausted. A
        drained connector that stored "no next page" as its resume point would
        restart from the beginning of the vendor's history on the next cycle —
        harmless downstream, since dedup absorbs it, but it would re-poll
        everything forever and burn the rate limit doing it.
        """
        result = PollResult(connector=self.name)
        cursor = self.cursors.get(self.name, self.stream)
        result.cursor = cursor

        for _ in range(max_pages):
            page = self.fetch(cursor, limit)
            result.pages += 1
            result.fetched += len(page.records)

            for record in page.records:
                event = self._to_event(record, result)
                if event is not None:
                    self.sink.write(event)
                    result.emitted += 1

            cursor = page.cursor
            self.cursors.set(self.name, self.stream, cursor)
            result.cursor = cursor

            if not page.records:
                result.drained = True
                break
            if cursor is None:
                # A vendor that offers no way to resume; nothing more to do.
                result.drained = True
                break

        return result

    def _to_event(self, record: dict[str, Any], result: PollResult) -> BehaviorEvent | None:
        """Archive, parse, resolve, and build. Returns None if the record is dropped."""
        try:
            parsed = self.parse(record)
        except (KeyError, ValueError, TypeError):
            # A malformed record must not take down the poll: one bad row from a
            # vendor would otherwise block every record behind it indefinitely.
            result.malformed += 1
            return None

        if parsed is None:
            result.unknown_event_type += 1
            return None

        employee_id = self.directory.resolve(parsed.subject_email)
        if employee_id is None:
            result.unresolved_identity += 1
            return None

        # Archived before parsing is trusted, so a parser bug is recoverable.
        raw_ref = self.archive.put(self.source, parsed.source_event_id, parsed.occurred_at, record)

        return BehaviorEvent(
            event_id=deterministic_event_id(self.source, parsed.source_event_id),
            tenant_id=self.tenant_id,
            employee_id=employee_id,
            signal=parsed.signal,
            source=self.source,
            occurred_at=parsed.occurred_at,
            ingested_at=datetime.now(UTC),
            source_event_id=parsed.source_event_id,
            raw_ref=raw_ref,
            attributes=parsed.attributes,
        )
