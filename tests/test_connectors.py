"""Connector tests.

Driven against the mock vendor over an in-process ASGI transport, so the
connectors run exactly the code they would against a real API — pagination,
retries, header parsing and all — without a socket.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from bellwether.connectors import CONNECTORS, Connector, EmployeeDirectory, VendorClient
from bellwether.connectors.archive import FileArchive, NullArchive
from bellwether.connectors.base import deterministic_event_id
from bellwether.connectors.cursors import InMemoryCursorStore
from bellwether.events.schema import BehaviorEvent, Employee, SignalType, Source
from bellwether.generator.population import build_population
from bellwether.generator.sinks import MemorySink
from bellwether.vendor.app import VendorConfig, create_app
from bellwether.vendor.payloads import VENDOR_EVENT_TYPES, to_vendor_payload
from bellwether.vendor.store import build_store

POPULATION_SIZE = 60
DAYS = 7
SEED = 1337


@pytest.fixture(scope="module")
def population() -> list[Employee]:
    return [m.employee for m in build_population(size=POPULATION_SIZE, seed=SEED)]


@pytest.fixture(scope="module")
def directory(population: list[Employee]) -> EmployeeDirectory:
    return EmployeeDirectory(population)


@pytest.fixture
def vendor() -> TestClient:
    store = build_store(size=POPULATION_SIZE, days=DAYS, seed=SEED)
    # Rate limiting off by default: each test that cares turns on the specific
    # failure it wants, deterministically, rather than racing a token bucket.
    return TestClient(create_app(store=store, config=VendorConfig(rate_limit_per_second=10**6)))


def build_connector(
    name: str,
    vendor: TestClient,
    directory: EmployeeDirectory,
    *,
    sink: MemorySink | None = None,
    cursors: InMemoryCursorStore | None = None,
    archive: object | None = None,
    max_attempts: int = 5,
) -> tuple[Connector, MemorySink]:
    sink = sink or MemorySink()
    connector = CONNECTORS[name](
        client=VendorClient.wrapping(vendor, max_attempts=max_attempts, base_delay=0.001),
        directory=directory,
        archive=archive or NullArchive(),  # type: ignore[arg-type]
        sink=sink,
        cursors=cursors or InMemoryCursorStore(),
    )
    return connector, sink


ALL = sorted(CONNECTORS)


# --- the parser-drift guard -------------------------------------------------


@pytest.mark.parametrize("source", sorted(VENDOR_EVENT_TYPES, key=lambda s: s.value))
def test_every_vendor_event_type_round_trips(source: Source, directory: EmployeeDirectory) -> None:
    """Render every signal as its vendor would, parse it back, expect the same signal.

    The vendor module and the connectors hold separate copies of each vendor's
    vocabulary, exactly as a real integration holds a parser separate from the
    vendor's docs. This is what notices when one drifts from the other.
    """
    connector_cls = next(c for c in CONNECTORS.values() if c.source is source)
    connector = connector_cls(
        client=None,  # type: ignore[arg-type] — parse() does no I/O
        directory=directory,
        archive=NullArchive(),
        sink=MemorySink(),
        cursors=InMemoryCursorStore(),
    )

    for signal in VENDOR_EVENT_TYPES[source]:
        event = BehaviorEvent(
            tenant_id="acme",
            employee_id="E0001",
            signal=signal,
            source=source,
            occurred_at=datetime(2026, 7, 1, 12, 0, tzinfo=UTC),
            attributes={"campaign_id": "camp-101", "lure": "test"},
        )
        payload = to_vendor_payload(event, "dana.okafor@acme.example")
        assert payload is not None, f"{source}/{signal} has no vendor rendering"

        parsed = connector.parse(payload)
        assert parsed is not None, f"{connector_cls.name} did not parse {signal}"
        assert parsed.signal is signal, f"{signal} parsed back as {parsed.signal}"
        assert parsed.subject_email == "dana.okafor@acme.example"
        assert parsed.occurred_at == event.occurred_at


def test_connector_vocabularies_cover_the_vendor(directory: EmployeeDirectory) -> None:
    """Neither side may know a signal the other doesn't."""
    from bellwether.connectors import email_gateway, endpoint_agent, google_workspace, okta

    tables = {
        Source.OKTA: set(okta.SIGNAL_BY_EVENT_TYPE.values()),
        Source.GOOGLE_WORKSPACE: set(google_workspace.SIGNAL_BY_ACTIVITY_NAME.values()),
        Source.EMAIL_GATEWAY: set(email_gateway.SIGNAL_BY_ACTION.values()),
        Source.ENDPOINT_AGENT: set(endpoint_agent.SIGNAL_BY_TELEMETRY_TYPE.values()),
    }
    for source, connector_signals in tables.items():
        assert connector_signals == set(VENDOR_EVENT_TYPES[source]), source


# --- pagination and cursors -------------------------------------------------


@pytest.mark.parametrize("name", ALL)
def test_connector_drains_every_page(
    name: str, vendor: TestClient, directory: EmployeeDirectory
) -> None:
    connector, sink = build_connector(name, vendor, directory)
    result = connector.run(limit=25)

    assert result.pages > 1, "test data should span multiple pages"
    assert result.emitted > 0
    assert result.drained, "the source should have been exhausted"
    assert result.cursor is not None, "a drained connector must still know where it got to"
    assert len(sink.events) == result.emitted
    assert all(e.source is CONNECTORS[name].source for e in sink.events)


@pytest.mark.parametrize("name", ALL)
def test_connector_resumes_from_its_cursor(
    name: str, vendor: TestClient, directory: EmployeeDirectory
) -> None:
    """A restart must continue, not restart."""
    cursors = InMemoryCursorStore()

    first, sink_a = build_connector(name, vendor, directory, cursors=cursors)
    partial = first.run(max_pages=1, limit=25)
    assert not partial.drained, "one page should not have drained the source"
    assert partial.cursor is not None

    second, sink_b = build_connector(name, vendor, directory, cursors=cursors)
    rest = second.run(limit=25)

    whole, sink_c = build_connector(name, vendor, directory)
    complete = whole.run(limit=25)

    assert partial.emitted + rest.emitted == complete.emitted
    resumed_ids = [e.event_id for e in sink_a.events + sink_b.events]
    assert resumed_ids == [e.event_id for e in sink_c.events]
    assert len(set(resumed_ids)) == len(resumed_ids), "resume duplicated records"


@pytest.mark.parametrize("name", ALL)
def test_event_ids_are_deterministic_across_runs(
    name: str, vendor: TestClient, directory: EmployeeDirectory
) -> None:
    """Reprocessing the same vendor record must produce the same event id.

    At-least-once delivery is only harmless if duplicates are identifiable.
    """
    first, sink_a = build_connector(name, vendor, directory)
    first.run(limit=50)
    second, sink_b = build_connector(name, vendor, directory)
    second.run(limit=50)

    assert sink_a.events
    assert [e.event_id for e in sink_a.events] == [e.event_id for e in sink_b.events]
    assert sink_a.events[0].event_id == deterministic_event_id(
        CONNECTORS[name].source, sink_a.events[0].source_event_id or ""
    )


# --- failure handling -------------------------------------------------------


def test_rate_limiting_is_survived(vendor: TestClient, directory: EmployeeDirectory) -> None:
    """Every third call is a 429; the connector must still drain the source.

    A small page size on purpose — the fault has to actually fire, and with a
    large limit the source drains in two requests and never trips it.
    """
    vendor.post("/_control/config", json={"force_429_every": 3})
    clean, _ = build_connector("email_gateway", vendor, directory)
    expected = clean.run(limit=5).emitted

    vendor.post("/_control/config", json={"force_429_every": 3})
    connector, _ = build_connector("email_gateway", vendor, directory)
    result = connector.run(limit=5)

    assert result.drained
    assert result.emitted == expected, "retrying lost or duplicated records"
    assert connector.client.stats.rate_limited > 0, "the 429 path was never exercised"
    assert connector.client.stats.retries > 0


def test_transient_server_errors_are_survived(
    vendor: TestClient, directory: EmployeeDirectory
) -> None:
    vendor.post("/_control/config", json={"force_503_every": 3})
    connector, _ = build_connector("okta", vendor, directory)

    result = connector.run(limit=5)

    assert result.emitted > 0
    assert result.drained
    assert connector.client.stats.server_errors > 0, "the 503 path was never exercised"


def test_persistent_failure_raises_rather_than_returning_short(
    vendor: TestClient, directory: EmployeeDirectory
) -> None:
    """Silently returning a partial page would look like the source ran dry."""
    from bellwether.connectors import ConnectorError

    vendor.post("/_control/config", json={"failure_rate": 1.0})
    connector, _ = build_connector("okta", vendor, directory, max_attempts=3)

    with pytest.raises(ConnectorError):
        connector.run(limit=25)


def test_retry_honours_retry_after(vendor: TestClient, directory: EmployeeDirectory) -> None:
    """The vendor's own Retry-After wins; otherwise jittered exponential, capped."""
    client = build_connector("okta", vendor, directory)[0].client

    assert client._backoff(0, "1") == 1.0, "an explicit Retry-After must be obeyed exactly"
    # Jitter spans 0.5x-1.5x of the exponential term, so an unparseable header
    # falls back to backoff rather than to zero or to the raw header.
    assert 0 < client._backoff(0, "nonsense") <= client.base_delay * 1.5
    assert client._backoff(99, None) <= client.max_delay
    assert client._backoff(0, str(client.max_delay * 10)) == client.max_delay


# --- identity and record-level filtering ------------------------------------


def test_unresolvable_identity_is_counted_not_guessed(vendor: TestClient) -> None:
    """Attributing a stranger's behaviour to a real employee is worse than dropping it."""
    connector, sink = build_connector("okta", vendor, EmployeeDirectory([]))
    result = connector.run(limit=50)

    assert result.emitted == 0
    assert sink.events == []
    assert result.unresolved_identity > 0
    assert result.fetched > 0


def test_malformed_record_does_not_stop_the_poll(
    vendor: TestClient, directory: EmployeeDirectory
) -> None:
    """One unparseable row must not block every row behind it."""
    from bellwether.connectors.base import PollResult

    connector, sink = build_connector("okta", vendor, directory)
    result = PollResult(connector="okta")

    # Right event type, but missing uuid, published and actor.
    assert connector._to_event({"eventType": "user.password.breach_detected"}, result) is None
    assert result.malformed == 1
    assert sink.events == []


def test_okta_ignores_a_successful_mfa_prompt(
    vendor: TestClient, directory: EmployeeDirectory
) -> None:
    """Same eventType as a denial; only the outcome separates them."""
    connector, _ = build_connector("okta", vendor, directory)
    approved = {
        "uuid": "x",
        "published": "2026-07-01T12:00:00.000Z",
        "eventType": "user.authentication.auth_via_mfa",
        "outcome": {"result": "SUCCESS", "reason": None},
        "actor": {"alternateId": "a@b.c"},
    }
    assert connector.parse(approved) is None


def test_unmanaged_device_telemetry_is_ignored(
    vendor: TestClient, directory: EmployeeDirectory
) -> None:
    connector, _ = build_connector("endpoint_agent", vendor, directory)
    record = {
        "record_id": "r1",
        "observed_at": "2026-07-01T12:00:00+00:00",
        "device": {"id": "d", "managed": False},
        "user_principal": "a@b.c",
        "telemetry_type": "device.removable_mount",
        "details": {},
    }
    assert connector.parse(record) is None


def test_real_phish_report_is_not_counted_as_a_simulation(
    vendor: TestClient, directory: EmployeeDirectory
) -> None:
    connector, _ = build_connector("email_gateway", vendor, directory)
    record = {
        "event_id": "ms_1",
        "timestamp": 1782000000,
        "recipient": "a@b.c",
        "action": "link_clicked",
        "campaign": {"id": None, "subject": None, "simulated": False},
    }
    assert connector.parse(record) is None


# --- archival and PII -------------------------------------------------------


def test_raw_payload_is_archived_and_referenced(
    tmp_path, vendor: TestClient, directory: EmployeeDirectory
) -> None:
    archive = FileArchive(tmp_path)
    connector, sink = build_connector("okta", vendor, directory, archive=archive)
    result = connector.run(limit=25)

    assert archive.written == result.emitted
    assert all(e.raw_ref is not None for e in sink.events)
    assert all(str(e.raw_ref).startswith("file://") for e in sink.events)
    assert list(tmp_path.glob("raw/source=okta/dt=*/*.json"))


@pytest.mark.parametrize("name", ALL)
def test_emitted_events_carry_no_vendor_email(
    name: str, vendor: TestClient, directory: EmployeeDirectory
) -> None:
    """Email enters at the connector boundary and must stop there.

    The vendor identifies people by address; everything downstream sees only the
    token. A connector leaking the address into attributes would put PII on a
    topic with different retention than the table it belongs in.
    """
    connector, sink = build_connector(name, vendor, directory)
    connector.run(limit=50)

    assert sink.events
    for event in sink.events:
        assert "@" not in event.model_dump_json(exclude={"raw_ref"})


@pytest.mark.parametrize("name", ALL)
def test_ingest_time_is_stamped_at_ingestion(
    name: str, vendor: TestClient, directory: EmployeeDirectory
) -> None:
    """Event time comes from the vendor, ingest time from us."""
    before = datetime.now(UTC)
    connector, sink = build_connector(name, vendor, directory)
    connector.run(limit=50)

    for event in sink.events:
        assert event.ingested_at >= before
        assert event.occurred_at < event.ingested_at


@pytest.mark.parametrize("name", ALL)
def test_all_emitted_signals_are_priced(
    name: str, vendor: TestClient, directory: EmployeeDirectory
) -> None:
    from bellwether.scoring.catalog import CATALOG

    connector, sink = build_connector(name, vendor, directory)
    connector.run(limit=50)
    assert {e.signal for e in sink.events} <= set(CATALOG)


def test_signals_reaching_the_pipeline_cover_the_catalog(
    vendor: TestClient, directory: EmployeeDirectory
) -> None:
    """Which signals the four connectors actually deliver, and which they don't.

    Training, breach-intel and Slack have no connector yet. Asserting the gap
    keeps it visible instead of leaving someone to wonder why nobody's training
    score ever moves.
    """
    from bellwether.events.schema import SIGNAL_SOURCE
    from bellwether.vendor.payloads import UNCONNECTED_SOURCES

    delivered: set[SignalType] = set()
    for name in ALL:
        connector, sink = build_connector(name, vendor, directory)
        connector.run(limit=100)
        delivered |= {e.signal for e in sink.events}

    unreachable = {s for s, src in SIGNAL_SOURCE.items() if src in UNCONNECTED_SOURCES}
    assert delivered & unreachable == set()
    assert len(delivered) >= 15, f"only {len(delivered)} signals reach the pipeline"


@pytest.mark.parametrize("name", ALL)
def test_a_drained_connector_does_not_re_ingest_on_the_next_run(
    name: str, vendor: TestClient, directory: EmployeeDirectory
) -> None:
    """The bug this guards against: a drained connector that persists "no next
    page" as its resume point restarts from the beginning of the vendor's
    history every cycle. Downstream survives it, because dedup absorbs the
    replay, so the only symptom is the connector quietly re-polling everything
    forever and burning the rate limit doing it.
    """
    cursors = InMemoryCursorStore()

    first, _ = build_connector(name, vendor, directory, cursors=cursors)
    initial = first.run(limit=50)
    assert initial.emitted > 0
    assert initial.drained

    second, sink = build_connector(name, vendor, directory, cursors=cursors)
    again = second.run(limit=50)

    assert again.fetched == 0, f"re-fetched {again.fetched} records it already had"
    assert again.emitted == 0
    assert sink.events == []
    assert again.drained


@pytest.mark.parametrize("name", ALL)
def test_resume_picks_up_records_that_arrive_later(
    name: str, vendor: TestClient, directory: EmployeeDirectory
) -> None:
    """Draining must not mean the cursor stops being usable."""
    cursors = InMemoryCursorStore()
    first, _ = build_connector(name, vendor, directory, cursors=cursors)
    first.run(limit=50)

    stored = cursors.get(name, CONNECTORS[name].stream)
    assert stored is not None, "position was forgotten once the source ran dry"


def test_an_ambiguous_address_resolves_to_nobody() -> None:
    """Two people, one address: the directory must refuse to pick a winner.

    Guessing would attribute one employee's phishing click to a colleague, and
    the resulting score would look completely plausible - which is what makes
    it the worst available failure for this product.
    """
    shared = "sam.chen@acme.example"
    directory = EmployeeDirectory(
        [
            Employee(
                employee_id="E0001",
                tenant_id="acme",
                department="sales",
                seniority="mid",
                tenure_days=100,
                location="London",
                email=shared,
            ),
            Employee(
                employee_id="E0002",
                tenant_id="acme",
                department="legal",
                seniority="senior",
                tenure_days=900,
                location="London",
                email=shared,
            ),
        ]
    )

    assert directory.resolve(shared) is None
    assert shared in directory.ambiguous


def test_ambiguous_identities_are_counted_as_unresolved(
    vendor: TestClient, directory: EmployeeDirectory
) -> None:
    """A refused resolution shows up in the drop counters, not silently."""
    from bellwether.generator.population import build_population as _pop

    people = [m.employee for m in _pop(size=POPULATION_SIZE, seed=SEED)]
    collided = [e.model_copy(update={"email": "shared@acme.example"}) for e in people]

    connector, sink = build_connector("okta", vendor, EmployeeDirectory(collided))
    result = connector.run(limit=50)

    assert result.emitted == 0
    assert result.unresolved_identity > 0
    assert sink.events == []
