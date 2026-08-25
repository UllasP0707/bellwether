"""Round-trip tests for the Postgres-backed stores.

Every other test file here is pure, which is the right default and left a gap:
three stores had write paths exercised constantly and read paths exercised
never. Two bugs came out of that in one afternoon — a `uuid` column arriving
back as a `UUID` object where the contract says `str`, and a set of topic
configs the broker accepted and discarded. Both were invisible until something
read the value back.

Skipped when no database is reachable, so `pytest` stays fast and offline; CI
runs a Postgres service so they are not optional there.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest

from bellwether.config import settings
from bellwether.connectors.cursors import PostgresCursorStore
from bellwether.dimension import PostgresEmployeeRepository
from bellwether.events.schema import Employee, SignalType
from bellwether.interventions.policy import PostgresLedger
from bellwether.interventions.types import (
    Channel,
    CopySource,
    InterventionEvent,
    InterventionType,
)
from bellwether.scoring import RiskBand

pytestmark = pytest.mark.postgres

NOW = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)


@pytest.fixture(scope="session")
def dsn() -> str:
    """The configured database, or skip the module."""
    import psycopg

    target = settings().postgres_dsn
    try:
        psycopg.connect(target, connect_timeout=2).close()
    except Exception as err:
        pytest.skip(f"no database at {target.rsplit('@', 1)[-1]}: {type(err).__name__}")
    return target


@pytest.fixture
def tenant() -> str:
    """A fresh tenant per test, so these never collide with real local data."""
    return f"test-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def ledger(dsn: str, tenant: str) -> Iterator[PostgresLedger]:
    store = PostgresLedger(dsn)
    yield store
    with store._connection.cursor() as cur:
        cur.execute("DELETE FROM intervention WHERE tenant_id = %s", (tenant,))
    store.close()


def intervention(tenant: str, **overrides: object) -> InterventionEvent:
    base: dict[str, object] = dict(
        tenant_id=tenant,
        employee_id="E0042",
        type=InterventionType.NUDGE,
        channel=Channel.CHAT,
        trigger_signal=SignalType.PHISH_CREDENTIALS_SUBMITTED,
        trigger_event_id="evt-1",
        band=RiskBand.CRITICAL,
        previous_band=RiskBand.HIGH,
        score=88.5,
        subject="Please reset your password now",
        body="Hi Dana, please reset your password now.",
        copy_source=CopySource.MODEL,
        guardrail_rejections=1,
        created_at=NOW,
    )
    base.update(overrides)
    return InterventionEvent(**base)  # type: ignore[arg-type]


# --- the intervention ledger --------------------------------------------------


def test_an_intervention_reads_back_as_what_was_written(
    ledger: PostgresLedger, tenant: str
) -> None:
    """A real bug: the uuid column came back as a UUID, not the str the contract says.

    The write path adapts fine, so nothing failed until something read a
    person's intervention history — which, until the CLI existed, nothing did.
    """
    event = intervention(tenant)
    assert ledger.record(event) is True

    (stored,) = ledger.history(tenant, "E0042")
    assert stored.intervention_id == event.intervention_id
    assert isinstance(stored.intervention_id, str)
    assert stored.subject == event.subject
    assert stored.body == event.body
    assert stored.trigger_signal is SignalType.PHISH_CREDENTIALS_SUBMITTED
    assert stored.band is RiskBand.CRITICAL
    assert stored.previous_band is RiskBand.HIGH
    assert stored.copy_source is CopySource.MODEL
    assert stored.score == pytest.approx(88.5)


def test_the_unique_index_fences_a_replay(ledger: PostgresLedger, tenant: str) -> None:
    """One behaviour, one message — enforced by the database, not by a read."""
    first = intervention(tenant)
    assert ledger.record(first) is True

    # Same trigger, different rung and a fresh id: what a redelivered score
    # produces once the ladder has one more prior to count.
    climbed = intervention(
        tenant, type=InterventionType.TRAINING, intervention_id=str(uuid.uuid4())
    )
    assert ledger.record(climbed) is False
    assert len(ledger.history(tenant, "E0042")) == 1


def test_a_different_trigger_is_a_different_intervention(
    ledger: PostgresLedger, tenant: str
) -> None:
    ledger.record(intervention(tenant, trigger_event_id="evt-1"))
    ledger.record(intervention(tenant, trigger_event_id="evt-2", intervention_id=str(uuid.uuid4())))

    assert len(ledger.history(tenant, "E0042")) == 2


def test_last_sent_at_narrows_by_type(ledger: PostgresLedger, tenant: str) -> None:
    ledger.record(intervention(tenant, trigger_event_id="a", created_at=NOW - timedelta(days=5)))
    ledger.record(
        intervention(
            tenant,
            trigger_event_id="b",
            type=InterventionType.TRAINING,
            intervention_id=str(uuid.uuid4()),
            created_at=NOW,
        )
    )

    assert ledger.last_sent_at(tenant, "E0042") == NOW
    assert ledger.last_sent_at(tenant, "E0042", InterventionType.NUDGE) == NOW - timedelta(days=5)
    assert ledger.last_sent_at(tenant, "E0042", InterventionType.MANAGER_NOTIFICATION) is None


def test_counting_and_totals(ledger: PostgresLedger, tenant: str) -> None:
    ledger.record(intervention(tenant, trigger_event_id="old", created_at=NOW - timedelta(days=10)))
    ledger.record(
        intervention(
            tenant,
            trigger_event_id="new",
            type=InterventionType.TRAINING,
            intervention_id=str(uuid.uuid4()),
            created_at=NOW - timedelta(days=1),
        )
    )

    assert ledger.count_since(tenant, "E0042", NOW - timedelta(days=7)) == 1
    assert ledger.count_since(tenant, "E0042", NOW - timedelta(days=30)) == 2
    assert ledger.totals(tenant) == {"nudge": 1, "training": 1}


def test_the_ledger_is_scoped_by_tenant(ledger: PostgresLedger, tenant: str) -> None:
    ledger.record(intervention(tenant))

    assert ledger.history("someone-else", "E0042") == []
    assert ledger.count_since("someone-else", "E0042", NOW - timedelta(days=30)) == 0
    assert ledger.totals("someone-else") == {}


# --- the employee dimension ----------------------------------------------------


@pytest.fixture
def dimension(dsn: str, tenant: str) -> Iterator[PostgresEmployeeRepository]:
    repo = PostgresEmployeeRepository(dsn, tenant_id=tenant, load=False)
    yield repo
    with repo._connection.cursor() as cur:
        cur.execute("DELETE FROM employee WHERE tenant_id = %s", (tenant,))
    repo.close()


def person(tenant: str, employee_id: str, **overrides: object) -> Employee:
    base: dict[str, object] = dict(
        employee_id=employee_id,
        tenant_id=tenant,
        department="engineering",
        seniority="mid",
        tenure_days=500,
        location="Remote US",
        email=f"{employee_id.lower()}@acme.example",
        display_name="Dana Moreau",
        is_executive=True,
    )
    base.update(overrides)
    return Employee(**base)  # type: ignore[arg-type]


def test_an_employee_reads_back_intact(dimension: PostgresEmployeeRepository, tenant: str) -> None:
    original = person(tenant, "T0001")
    dimension.upsert_many([original])

    assert dimension.get("T0001") == original
    assert dimension.resolve_email("T0001@ACME.EXAMPLE") == "T0001"


def test_upsert_updates_rather_than_duplicating(
    dimension: PostgresEmployeeRepository, tenant: str
) -> None:
    dimension.upsert_many([person(tenant, "T0001", department="engineering")])
    dimension.upsert_many([person(tenant, "T0001", department="finance")])

    assert len(dimension.all()) == 1
    stored = dimension.get("T0001")
    assert stored is not None and stored.department == "finance"


def test_an_ambiguous_address_resolves_to_nobody(
    dimension: PostgresEmployeeRepository, tenant: str
) -> None:
    """Survives the round trip, not just the in-memory construction."""
    shared = "shared@acme.example"
    dimension.upsert_many(
        [person(tenant, "T0001", email=shared), person(tenant, "T0002", email=shared)]
    )
    dimension.refresh()

    assert dimension.resolve_email(shared) is None


def test_the_dimension_is_scoped_by_tenant(dsn: str, tenant: str) -> None:
    writer = PostgresEmployeeRepository(dsn, tenant_id=tenant, load=False)
    writer.upsert_many([person(tenant, "T0001")])
    try:
        other = PostgresEmployeeRepository(dsn, tenant_id=f"{tenant}-other")
        assert other.get("T0001") is None
        other.close()
    finally:
        with writer._connection.cursor() as cur:
            cur.execute("DELETE FROM employee WHERE tenant_id = %s", (tenant,))
        writer.close()


# --- connector cursors ----------------------------------------------------------


def test_a_cursor_survives_a_new_connection(dsn: str, tenant: str) -> None:
    """The whole point of the store: a restart must not re-ingest history."""
    store = PostgresCursorStore(dsn)
    store.set(tenant, "default", "page-7")
    store.close()

    reopened = PostgresCursorStore(dsn)
    try:
        assert reopened.get(tenant, "default") == "page-7"
        assert reopened.get(tenant, "other-stream") is None
    finally:
        with reopened._connection.cursor() as cur:
            cur.execute("DELETE FROM connector_cursor WHERE connector = %s", (tenant,))
        reopened.close()


def test_a_null_cursor_round_trips_as_null(dsn: str, tenant: str) -> None:
    """Distinguishing "no cursor" from "cursor is the empty string" matters here."""
    store = PostgresCursorStore(dsn)
    try:
        store.set(tenant, "default", "page-1")
        store.set(tenant, "default", None)
        assert store.get(tenant, "default") is None
    finally:
        with store._connection.cursor() as cur:
            cur.execute("DELETE FROM connector_cursor WHERE connector = %s", (tenant,))
        store.close()
