"""API tests.

Two tenants throughout, even where one would do. Isolation bugs do not show up
in a single-tenant fixture — the query returns the right thing because there is
nothing else it could return — so the second tenant exists to make the wrong
answer possible.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from bellwether.api import InMemoryAudit, Principal, TenantContext, create_app, parse_keys
from bellwether.dimension import InMemoryEmployeeRepository
from bellwether.events.schema import Employee, RiskCategory, SignalType
from bellwether.events.scores import FactorPayload, RiskScoreEvent
from bellwether.interventions import (
    Channel,
    CopySource,
    InMemoryLedger,
    InterventionEvent,
    InterventionType,
)
from bellwether.scoring import RiskBand
from bellwether.stream.store import InMemoryOnlineStore, WindowedEvent

NOW = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)

ACME = "acme"
RIVAL = "rival"
KEYS = {
    "acme-key": Principal(actor="analyst@acme", tenant_id=ACME),
    "rival-key": Principal(actor="analyst@rival", tenant_id=RIVAL),
    "stranger-key": Principal(actor="nobody", tenant_id="unserved"),
}


def employee(
    employee_id: str, tenant: str = ACME, department: str = "engineering", **extra: object
) -> Employee:
    base: dict[str, object] = dict(
        employee_id=employee_id,
        tenant_id=tenant,
        department=department,
        seniority="mid",
        tenure_days=500,
        location="Remote US",
        email=f"{employee_id.lower()}@{tenant}.example",
        display_name="Dana Moreau",
    )
    base.update(extra)
    return Employee(**base)  # type: ignore[arg-type]


def score(
    employee_id: str, value: float, tenant: str = ACME, band: RiskBand | None = None
) -> RiskScoreEvent:
    return RiskScoreEvent(
        tenant_id=tenant,
        employee_id=employee_id,
        score=value,
        band=band or RiskBand.of(value),
        as_of=NOW,
        previous_band=RiskBand.ELEVATED,
        dominant_category=RiskCategory.PHISHING_SUSCEPTIBILITY,
        by_category={RiskCategory.PHISHING_SUSCEPTIBILITY: 20.0},
        top_factors=[
            FactorPayload(
                signal="phish_credentials_submitted",
                category=RiskCategory.PHISHING_SUSCEPTIBILITY,
                occurrences=1,
                contribution=24.0,
            )
        ],
        events_considered=7,
    )


def build(tenant: str, people: list[Employee], scores: list[RiskScoreEvent]) -> TenantContext:
    store = InMemoryOnlineStore()
    for s in scores:
        store.record(s)
    recently = datetime.now(UTC) - timedelta(days=3)
    for s in scores:
        store.add(
            WindowedEvent(
                s.employee_id, SignalType.PHISH_SIM_CLICKED, recently, f"e-{s.employee_id}"
            ),
            lookback_days=30,
            as_of=datetime.now(UTC),
        )
    ledger = InMemoryLedger()
    return TenantContext(
        scores=store,
        employees=InMemoryEmployeeRepository(people),
        interventions=ledger,
        window=store,
    )


@pytest.fixture
def audit() -> InMemoryAudit:
    return InMemoryAudit()


@pytest.fixture
def contexts() -> dict[str, TenantContext]:
    acme = build(
        ACME,
        [
            employee("E0001", department="finance"),
            employee("E0002", department="engineering"),
            employee("E0003", department="engineering"),
            employee("E0004", department="support"),
        ],
        [
            score("E0001", 91.0),
            score("E0002", 64.0),
            score("E0003", 12.0),
        ],
    )
    rival = build(RIVAL, [employee("R0001", tenant=RIVAL)], [score("R0001", 88.0, tenant=RIVAL)])
    return {ACME: acme, RIVAL: rival}


@pytest.fixture
def client(contexts: dict[str, TenantContext], audit: InMemoryAudit) -> TestClient:
    return TestClient(create_app(tenants=contexts, principals=KEYS, audit=audit))


def get(client: TestClient, path: str, key: str = "acme-key"):  # type: ignore[no-untyped-def]
    return client.get(path, headers={"X-API-Key": key})


# --- authentication -----------------------------------------------------------


def test_health_needs_no_credential(client: TestClient) -> None:
    assert client.get("/healthz").json() == {"status": "ok"}


@pytest.mark.parametrize(
    "path",
    [
        "/v1/catalog",
        "/v1/population/ranking",
        "/v1/population/departments",
        "/v1/employees/E0001/score",
    ],
)
def test_everything_else_needs_one(client: TestClient, path: str) -> None:
    assert client.get(path).status_code == 401
    assert client.get(path, headers={"X-API-Key": "made-up"}).status_code == 401


def test_a_key_for_a_tenant_we_do_not_serve_is_refused(client: TestClient) -> None:
    assert get(client, "/v1/catalog", key="stranger-key").status_code == 403


# --- tenant isolation -----------------------------------------------------------


def test_a_tenant_cannot_read_another_tenants_employee(client: TestClient) -> None:
    """The id is valid and the employee exists — just not for this caller."""
    assert get(client, "/v1/employees/R0001/score").status_code == 404
    assert get(client, "/v1/employees/E0001/score", key="rival-key").status_code == 404


def test_a_foreign_employee_looks_exactly_like_a_missing_one(client: TestClient) -> None:
    """404 rather than 403, in both the status and the body.

    A 403 would confirm the person exists, which is precisely what tenancy is
    supposed to hide. The two responses have to be indistinguishable.
    """
    missing = get(client, "/v1/employees/E9999/score")
    foreign = get(client, "/v1/employees/R0001/score")

    assert missing.status_code == foreign.status_code == 404
    assert missing.json() == foreign.json()


def test_rankings_never_cross_a_tenant(client: TestClient) -> None:
    ours = {r["employee_id"] for r in get(client, "/v1/population/ranking").json()}
    theirs = {
        r["employee_id"] for r in get(client, "/v1/population/ranking", key="rival-key").json()
    }

    assert ours == {"E0001", "E0002", "E0003"}
    assert theirs == {"R0001"}


def test_there_is_no_tenant_parameter_to_set(client: TestClient) -> None:
    """Tenancy is a property of the credential, so it cannot be overridden."""
    response = get(client, "/v1/population/ranking?tenant_id=rival&tenant=rival")
    assert {r["employee_id"] for r in response.json()} == {"E0001", "E0002", "E0003"}


# --- the privacy gradient -------------------------------------------------------


def test_the_ranking_is_pseudonymous(client: TestClient) -> None:
    """Browsing colleagues by risk should not hand over a list of names."""
    body = get(client, "/v1/population/ranking").text

    assert "Dana" not in body
    assert "Moreau" not in body
    assert "@" not in body
    assert "E0001" in body


def test_looking_one_person_up_names_them(client: TestClient) -> None:
    body = get(client, "/v1/employees/E0001/score").json()

    assert body["display_name"] == "Dana Moreau"
    assert body["department"] == "finance"


def test_no_endpoint_leaks_an_email_address(client: TestClient) -> None:
    for path in (
        "/v1/employees/E0001/score",
        "/v1/employees/E0001/timeline",
        "/v1/employees/E0001/interventions",
        "/v1/population/ranking",
        "/v1/population/departments",
    ):
        assert "@" not in get(client, path).text, path


# --- the audit log ---------------------------------------------------------------


def test_looking_at_a_person_is_recorded(client: TestClient, audit: InMemoryAudit) -> None:
    get(client, "/v1/employees/E0001/score")

    (record,) = audit.records
    assert (record.actor, record.tenant_id, record.employee_id, record.endpoint) == (
        "analyst@acme",
        ACME,
        "E0001",
        "score",
    )


def test_every_per_employee_endpoint_is_audited(client: TestClient, audit: InMemoryAudit) -> None:
    get(client, "/v1/employees/E0001/score")
    get(client, "/v1/employees/E0001/timeline")
    get(client, "/v1/employees/E0001/interventions")

    assert {r.endpoint for r in audit.records} == {"score", "timeline", "interventions"}


def test_browsing_the_population_is_not_audited(client: TestClient, audit: InMemoryAudit) -> None:
    """Otherwise every dashboard refresh buries the reads that matter."""
    get(client, "/v1/population/ranking")
    get(client, "/v1/population/departments")
    get(client, "/v1/catalog")

    assert audit.records == []


def test_a_read_is_audited_even_when_the_person_has_no_score(
    client: TestClient, audit: InMemoryAudit
) -> None:
    """The look happened. Whether it found anything is a separate question."""
    assert get(client, "/v1/employees/E0004/score").status_code == 404
    assert [r.employee_id for r in audit.records] == ["E0004"]


def test_a_refused_read_is_not_audited(client: TestClient, audit: InMemoryAudit) -> None:
    get(client, "/v1/employees/R0001/score")
    get(client, "/v1/employees/E0001/score", key="rival-key")

    assert audit.records == []


def test_the_audit_log_is_readable_and_tenant_scoped(client: TestClient) -> None:
    get(client, "/v1/employees/E0001/score")
    get(client, "/v1/employees/R0001/score", key="rival-key")

    ours = get(client, "/v1/audit").json()
    theirs = get(client, "/v1/audit", key="rival-key").json()

    # Each tenant sees its own reads and none of the other's, even though both
    # went through the same audit log.
    assert [r["employee_id"] for r in ours] == ["E0001"]
    assert [r["employee_id"] for r in theirs] == ["R0001"]


# --- content --------------------------------------------------------------------


def test_a_score_carries_the_reason_in_words(client: TestClient) -> None:
    """A number nobody can interrogate does not get acted on."""
    body = get(client, "/v1/employees/E0001/score").json()

    assert body["score"] == 91.0
    assert body["band"] == "critical"
    assert body["dominant_category"] == "phishing_susceptibility"
    (factor,) = body["top_factors"]
    assert factor["signal"] == "phish_credentials_submitted"
    assert "credentials" in factor["description"].lower()
    assert factor["signal"] not in factor["description"], "the description is prose, not the id"


def test_the_timeline_prices_each_event_as_of_today(client: TestClient) -> None:
    body = get(client, "/v1/employees/E0001/timeline").json()

    (entry,) = body["entries"]
    assert entry["signal"] == "phish_sim_clicked"
    assert 2.5 < entry["age_days"] < 3.5
    # Weight 8.0 on a 21-day half-life, three days old: decayed but not gone.
    assert 7.0 < entry["contribution"] < 8.0


def test_the_timeline_drops_what_the_score_already_ignores(client: TestClient) -> None:
    """The explanation has to cover the same events as the number it explains."""
    store = InMemoryOnlineStore()
    store.record(score("E0001", 40.0))
    for age, event_id in ((2, "fresh"), (400, "ancient")):
        store.add(
            WindowedEvent(
                "E0001",
                SignalType.PHISH_SIM_CLICKED,
                datetime.now(UTC) - timedelta(days=age),
                event_id,
            ),
            lookback_days=3650,
            as_of=datetime.now(UTC),
        )
    context = TenantContext(
        scores=store,
        employees=InMemoryEmployeeRepository([employee("E0001")]),
        interventions=InMemoryLedger(),
        window=store,
    )
    local = TestClient(create_app(tenants={ACME: context}, principals=KEYS))

    entries = get(local, "/v1/employees/E0001/timeline").json()["entries"]
    assert len(entries) == 1


def test_the_ranking_is_ordered_by_score(client: TestClient) -> None:
    scores = [r["score"] for r in get(client, "/v1/population/ranking").json()]
    assert scores == sorted(scores, reverse=True)


def test_the_ranking_can_be_filtered_to_a_band(client: TestClient) -> None:
    rows = get(client, "/v1/population/ranking?band=critical").json()
    assert [r["employee_id"] for r in rows] == ["E0001"]


def test_the_ranking_is_paged_and_bounded(client: TestClient) -> None:
    assert len(get(client, "/v1/population/ranking?limit=2").json()) == 2
    assert [r["employee_id"] for r in get(client, "/v1/population/ranking?offset=1").json()] == [
        "E0002",
        "E0003",
    ]
    assert get(client, "/v1/population/ranking?limit=5000").status_code == 422


def test_departments_count_the_unscored_in_headcount(client: TestClient) -> None:
    """Otherwise a department nobody has data on looks like a safe one."""
    rows = {r["department"]: r for r in get(client, "/v1/population/departments").json()}

    assert rows["engineering"]["headcount"] == 2
    assert rows["engineering"]["scored"] == 2
    assert rows["finance"]["mean_score"] == 91.0
    assert rows["finance"]["critical"] == 1
    assert "support" not in rows, "nobody in support has been scored"


def test_departments_are_ranked_by_mean_risk(client: TestClient) -> None:
    rows = get(client, "/v1/population/departments").json()
    assert [r["department"] for r in rows] == ["finance", "engineering"]


def test_the_catalog_is_published(client: TestClient) -> None:
    """The scoring model itself, so a team can defend a score to the person."""
    rows = get(client, "/v1/catalog").json()

    assert len(rows) == 23
    submitted = next(r for r in rows if r["signal"] == "phish_credentials_submitted")
    assert submitted["weight"] == 25.0
    assert submitted["is_mitigating"] is False
    assert any(r["is_mitigating"] for r in rows), "mitigating signals should be visible too"


def test_interventions_read_back_for_one_person(
    client: TestClient, contexts: dict[str, TenantContext]
) -> None:
    contexts[ACME].interventions.record(
        InterventionEvent(
            tenant_id=ACME,
            employee_id="E0001",
            type=InterventionType.NUDGE,
            channel=Channel.CHAT,
            trigger_signal=SignalType.PHISH_CREDENTIALS_SUBMITTED,
            trigger_event_id="evt-1",
            band=RiskBand.CRITICAL,
            score=91.0,
            subject="Please reset your password now",
            body="Hi Dana, please reset your password now.",
            copy_source=CopySource.TEMPLATE,
            created_at=NOW,
        )
    )

    (sent,) = get(client, "/v1/employees/E0001/interventions").json()
    assert sent["subject"] == "Please reset your password now"
    assert sent["trigger_signal"] == "phish_credentials_submitted"
    assert sent["copy_source"] == "template"


# --- input ------------------------------------------------------------------------


@pytest.mark.parametrize("bad", ["../../etc/passwd", "E0001;DROP", "a b", "x" * 80])
def test_a_malformed_employee_id_is_rejected_at_the_edge(client: TestClient, bad: str) -> None:
    assert get(client, f"/v1/employees/{bad}/score").status_code in (404, 422)


def test_the_dashboard_is_served(client: TestClient) -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert "Bellwether" in response.text


# --- key parsing --------------------------------------------------------------------


def test_keys_parse_into_principals() -> None:
    parsed = parse_keys("k1:acme:analyst, k2:rival:auditor")
    assert parsed["k1"] == Principal(actor="analyst", tenant_id="acme")
    assert parsed["k2"].tenant_id == "rival"


@pytest.mark.parametrize("spec", ["nope", "k:tenant", "k:tenant:actor:extra", "k::actor"])
def test_a_malformed_key_spec_fails_loudly(spec: str) -> None:
    """Silently ignoring a bad entry would start the API with no way in."""
    with pytest.raises(ValueError):
        parse_keys(spec)
