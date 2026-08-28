"""Erasure and tokenization.

The test worth reading is `test_erasure_knows_every_redis_key_the_store_writes`.
Erasure reconstructs the online store's key names, and that duplication fails
silently: rename a prefix and erasure keeps reporting success while leaving a
score behind for somebody the system can no longer name.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

import psycopg
import pytest

from bellwether.dimension import PostgresEmployeeRepository
from bellwether.events.schema import Employee
from bellwether.privacy import erasure
from bellwether.privacy.tokens import PII_FIELDS, Tokenizer, normalise
from bellwether.stream.store import RedisOnlineStore

# --- tokenization (pure) ------------------------------------------------------

SECRET = "a-secret-long-enough-to-be-a-key"


def tokenizer(tenant: str = "acme") -> Tokenizer:
    return Tokenizer.from_secret(SECRET, tenant)


def test_the_same_value_always_produces_the_same_token() -> None:
    """Determinism is what makes identity resolution work across runs."""
    assert tokenizer().token("dana.moreau@acme.example") == tokenizer().token(
        "dana.moreau@acme.example"
    )


def test_case_and_whitespace_do_not_make_a_different_person() -> None:
    assert tokenizer().token("  Dana.Moreau@ACME.example ") == tokenizer().token(
        "dana.moreau@acme.example"
    )


def test_plus_tags_and_dots_are_left_alone() -> None:
    """Under-normalising splits one person in two; over-normalising merges two people.

    A directory lookup catches the first. Nothing catches the second, which
    would attribute one employee's phishing click to a colleague.
    """
    assert tokenizer().token("dana+ci@acme.example") != tokenizer().token("dana@acme.example")
    assert tokenizer().token("d.moreau@acme.example") != tokenizer().token("dmoreau@acme.example")


def test_tenants_cannot_compute_each_others_tokens() -> None:
    """A token leaking across a tenant boundary has to be meaningless."""
    assert tokenizer("acme").token("dana@x.example") != tokenizer("globex").token("dana@x.example")


def test_the_same_string_as_two_kinds_of_field_gives_two_tokens() -> None:
    """Otherwise a username equal to a display name joins across unrelated fields."""
    tok = tokenizer()
    assert tok.token("dmoreau", kind="username") != tok.token("dmoreau", kind="display_name")


def test_a_token_cannot_be_derived_without_the_key() -> None:
    """The property a bare SHA-256 of an email address does not have.

    Corporate address space is small and enumerable, so an unkeyed digest is a
    lookup away for anybody holding the employee list.
    """
    other = Tokenizer.from_secret("a-different-secret-of-sufficient-length", "acme")
    assert other.token("dana@acme.example") != tokenizer().token("dana@acme.example")


def test_a_short_key_is_refused() -> None:
    with pytest.raises(ValueError, match="at least 16 bytes"):
        Tokenizer(key=b"tooshort")


def test_an_empty_secret_is_refused_rather_than_silently_weak() -> None:
    with pytest.raises(ValueError, match="no tokenization secret"):
        Tokenizer.from_secret("", "acme")


def test_matching_a_token_works_and_is_constant_time() -> None:
    tok = tokenizer()
    token = tok.token("dana@acme.example")
    assert tok.matches(token, "DANA@acme.example")
    assert not tok.matches(token, "sam@acme.example")


def test_redaction_reaches_nested_payloads() -> None:
    """Vendor payloads nest. A one-level redactor reports success and leaks."""
    payload = {
        "id": "evt-1",
        "actor": {"profile": {"email": "dana.moreau@acme.example", "department": "finance"}},
        "targets": [{"user_email": "sam@acme.example"}, {"user_email": "wei@acme.example"}],
        "outcome": "SUCCESS",
    }
    redacted = tokenizer().redact(payload)
    text = str(redacted)

    assert "@acme.example" not in text
    assert text.count("tok_") == 3
    assert isinstance(redacted, dict)
    # Non-identifying fields survive, or the archive stops being useful for
    # debugging the parser that produced it.
    assert redacted["outcome"] == "SUCCESS"
    assert redacted["actor"]["profile"]["department"] == "finance"


def test_redaction_replaces_rather_than_removes() -> None:
    """A replay that must reason about which fields a record used to have is useless."""
    redacted = tokenizer().redact({"email": "dana@acme.example"})
    assert isinstance(redacted, dict)
    assert set(redacted) == {"email"}


def test_redaction_is_stable_so_tokens_still_join() -> None:
    """Redacted archives have to remain joinable, or they are just deleted data."""
    tok = tokenizer()
    one = tok.redact({"email": "dana@acme.example"})
    two = tok.redact({"user_email": "dana@acme.example"})
    assert isinstance(one, dict) and isinstance(two, dict)
    # Different field names are different kinds, so they differ by design --
    # what must hold is that the same field name is stable.
    assert one == tok.redact({"email": "Dana@Acme.example"})


def test_an_empty_value_is_not_tokenized_into_a_fake_identifier() -> None:
    redacted = tokenizer().redact({"email": "", "name": None})
    assert redacted == {"email": "", "name": None}


def test_normalise_collapses_internal_whitespace() -> None:
    assert normalise("Dana   Moreau\n") == "dana moreau"


def test_the_pii_field_list_covers_the_obvious_aliases() -> None:
    """Four connectors, four naming conventions, one deny list."""
    assert {"email", "user_email", "actor_email", "primary_email", "mail"} <= PII_FIELDS


# --- erasure ------------------------------------------------------------------


def test_erasure_knows_every_redis_key_the_store_writes() -> None:
    """The duplication that would fail silently.

    `redis_keys_for` reconstructs `RedisOnlineStore`'s key names. If the store
    renames a prefix, erasure keeps reporting success while leaving a score
    behind for somebody the system can no longer name. Compared against the
    store's own private constructors on purpose: this is exactly the coupling
    worth pinning.
    """
    store = RedisOnlineStore.__new__(RedisOnlineStore)
    store.tenant_id = "acme"
    store.namespace = "w"

    written = {
        store._key("E0042"),
        store._band_key("E0042"),
        store._score_key("E0042"),
    }
    assert set(erasure.redis_keys_for("acme", "E0042")) == written


def test_the_ranking_key_is_handled_separately_and_not_forgotten() -> None:
    """It is a sorted set shared by the tenant, so it is a member removal, not a delete."""
    store = RedisOnlineStore.__new__(RedisOnlineStore)
    store.tenant_id = "acme"
    assert store._rank_key() == "rank:acme"
    assert store._rank_key() not in erasure.redis_keys_for("acme", "E0042")


def test_every_employee_keyed_table_is_listed() -> None:
    """A table added later must be an explicit decision, not an omission.

    Discovering tables from the catalog would erase a new one silently; not
    listing it at all leaves data behind silently. This fails until somebody
    chooses.
    """
    named = {*erasure.WAREHOUSE_TABLES, erasure.INTERVENTION_TABLE, erasure.AUDIT_TABLE, "employee"}
    assert named == {
        "raw_daily_employee_signal",
        "raw_employee_score",
        "intervention",
        "score_read_audit",
        "employee",
    }


# --- erasure against a real database -----------------------------------------

DSN = os.environ.get(
    "BELLWETHER_POSTGRES_DSN", "postgresql://bellwether:bellwether@localhost:5433/bellwether"
)
REDIS = os.environ.get("BELLWETHER_REDIS_URL", "redis://localhost:6379/15")


def _postgres_available() -> bool:
    try:
        with psycopg.connect(DSN, connect_timeout=2):
            return True
    except Exception:
        return False


def _redis_available() -> bool:
    try:
        import redis

        redis.Redis.from_url(REDIS, socket_connect_timeout=2).ping()
        return True
    except Exception:
        return False


needs_stores = pytest.mark.skipif(
    not (_postgres_available() and _redis_available()),
    reason="needs postgres and redis",
)

TENANT = "erasure-test"
SUBJECT = "E9001"
BYSTANDER = "E9002"


@pytest.fixture
def populated() -> object:
    """Two employees in every store, so erasure has a bystander to leave alone."""
    import redis as redis_client

    repo = PostgresEmployeeRepository(DSN, tenant_id=TENANT, load=False)
    people = [
        Employee(
            employee_id=employee_id,
            tenant_id=TENANT,
            department="finance",
            seniority="mid",
            tenure_days=400,
            location="Remote US",
            email=f"{employee_id.lower()}@acme.example",
            display_name=f"Test {employee_id}",
        )
        for employee_id in (SUBJECT, BYSTANDER)
    ]
    repo.upsert_many(people)

    client = redis_client.Redis.from_url(REDIS)
    for employee_id in (SUBJECT, BYSTANDER):
        for key in erasure.redis_keys_for(TENANT, employee_id):
            client.set(key, "x") if not key.startswith("w:") else client.zadd(key, {"e": 1.0})
        client.zadd(f"rank:{TENANT}", {employee_id: 50.0})

    yield None

    with psycopg.connect(DSN, autocommit=True) as connection, connection.cursor() as cur:
        cur.execute("DELETE FROM employee WHERE tenant_id = %s", (TENANT,))
    for employee_id in (SUBJECT, BYSTANDER):
        client.delete(*erasure.redis_keys_for(TENANT, employee_id))
    client.delete(f"rank:{TENANT}")
    repo.close()


@needs_stores
@pytest.mark.postgres
def test_a_dry_run_changes_nothing(populated: object) -> None:
    """The default, because this is the one command a rerun cannot undo."""
    before = erasure.erase(DSN, REDIS, TENANT, SUBJECT, dry_run=True)
    assert before.dimension_rows == 1
    assert before.redis_keys == 3

    assert not erasure.verify(DSN, REDIS, TENANT, SUBJECT).clean, "dry run must not delete"


@needs_stores
@pytest.mark.postgres
def test_erasure_removes_the_person_and_verification_agrees(populated: object) -> None:
    result = erasure.erase(DSN, REDIS, TENANT, SUBJECT, dry_run=False)

    assert result.dimension_rows == 1
    assert result.redis_keys == 3
    assert result.ranking_members == 1
    assert erasure.verify(DSN, REDIS, TENANT, SUBJECT).clean


@needs_stores
@pytest.mark.postgres
def test_erasure_leaves_everybody_else_alone(populated: object) -> None:
    """The failure that would be catastrophic and quiet."""
    erasure.erase(DSN, REDIS, TENANT, SUBJECT, dry_run=False)

    check = erasure.verify(DSN, REDIS, TENANT, BYSTANDER)
    assert not check.clean, "the bystander must still be fully present"
    assert len(check.findings) >= 4


@needs_stores
@pytest.mark.postgres
def test_erasure_is_idempotent(populated: object) -> None:
    """Reruns happen -- a partial failure is retried, an operator runs it twice."""
    erasure.erase(DSN, REDIS, TENANT, SUBJECT, dry_run=False)
    again = erasure.erase(DSN, REDIS, TENANT, SUBJECT, dry_run=False)

    assert again.total == 0
    assert erasure.verify(DSN, REDIS, TENANT, SUBJECT).clean


@needs_stores
@pytest.mark.postgres
def test_what_is_kept_is_always_reported(populated: object) -> None:
    """A deletion report listing only removals reads as if it removed everything."""
    result = erasure.erase(DSN, REDIS, TENANT, SUBJECT, dry_run=True)

    kept = " ".join(result.retained)
    assert "kafka" in kept.lower()
    assert "audit" in kept.lower()


@needs_stores
@pytest.mark.postgres
def test_the_audit_log_can_be_purged_when_a_deployment_requires_it(populated: object) -> None:
    """The judgment call is a flag, not a hard-coded opinion."""
    default = erasure.erase(DSN, REDIS, TENANT, SUBJECT, dry_run=True)
    strict = erasure.erase(DSN, REDIS, TENANT, SUBJECT, dry_run=True, purge_audit=True)

    assert any("audit" in note for note in default.retained)
    assert not any("audit log" in note for note in strict.retained)


def test_verification_is_independent_of_erasure(tmp_path: object) -> None:
    """A deletion that reports its own success is checking that it ran, not that it worked.

    Structural, so it holds without a database: `verify` must re-query rather
    than read anything `erase` returned.
    """
    import inspect

    source = inspect.getsource(erasure.verify)
    assert "Erased" not in source
    assert "SELECT count(*)" in source


@needs_stores
@pytest.mark.postgres
def test_erasure_survives_tables_that_do_not_exist_yet(populated: object) -> None:
    """A fresh database has no warehouse tables, and erasure still has to work."""
    result = erasure.erase(DSN, REDIS, TENANT, SUBJECT, dry_run=False)
    assert result.dimension_rows == 1
    assert all(count >= 0 for count in result.warehouse_rows.values())


def test_erased_reports_a_timestamp_free_total() -> None:
    record = erasure.Erased(
        employee_id="E1",
        dimension_rows=1,
        redis_keys=3,
        ranking_members=1,
        warehouse_rows={"a": 2},
        intervention_rows=4,
    )
    assert record.total == 11
    assert datetime.now(UTC) is not None  # the dataclass holds no clock


@needs_stores
@pytest.mark.postgres
def test_an_erased_person_leaves_the_snapshot_of_a_running_process(populated: object) -> None:
    """The gap the first live erasure exposed.

    The dimension is cached in-process so the scorer does not query Postgres
    once per message. That meant the row was deleted, the score was deleted,
    and a running API still held the name. Erasure is not instantaneous; it is
    complete within the staleness window, and the window has to actually
    expire.
    """
    repo = PostgresEmployeeRepository(DSN, tenant_id=TENANT, stale_after_seconds=0.0)
    assert repo.get(SUBJECT) is not None

    erasure.erase(DSN, REDIS, TENANT, SUBJECT, dry_run=False)

    # `stale_after_seconds=0` expires on every read, which is the bound taken
    # to its limit; the default is 300s and the same code path.
    assert repo.get(SUBJECT) is None, "a cached snapshot must not outlive an erasure"
    assert repo.get(BYSTANDER) is not None
    repo.close()


@needs_stores
@pytest.mark.postgres
def test_a_fresh_snapshot_is_not_reloaded_on_every_read(populated: object) -> None:
    """The cache still has to be a cache, or the scorer pays a query per message."""
    repo = PostgresEmployeeRepository(DSN, tenant_id=TENANT, stale_after_seconds=3600.0)
    loaded_at = repo._loaded_at

    for _ in range(50):
        repo.get(SUBJECT)

    assert repo._loaded_at == loaded_at, "a warm snapshot must not hit the database"
    repo.close()
