"""Erasing one person.

Deletion, not retention: retention is a horizon that applies to everyone and
runs on a schedule (see [`warehouse.retention`](../warehouse/retention.py)).
This is a named individual, on request, now.

The whole operation is tractable because of a decision made on day 1: events
carry `employee_id`, a tenant-scoped token, and PII lives only on the employee
dimension. So erasure is one `DELETE` plus a projection drop, and everything
else — thirty days of Kafka segments, Parquet in the lake, a Spark shuffle
spill, last March's backup — holds a token that now resolves to nobody. Had an
email address been denormalised onto the event for a nicer dashboard, erasure
would mean rewriting a data lake, and nobody rewrites a data lake.

## What is deleted

- **The dimension row.** The only place a name, an address or a manager lives.
- **The Redis projection.** Window, band, score snapshot, and the ranking
  member — otherwise the dashboard keeps showing a score for somebody the
  system can no longer name, which is the worst of both.
- **Warehouse rows keyed to them.** Recomputable from the lake, so keeping
  them would mean the next batch run silently restores what was just erased.

## What is deliberately kept, and why

**The read audit log.** Rows saying *who looked at this person* are an
accountability record about the **actor**, not about the subject. Deleting
them would mean anyone who wanted to erase the evidence that they browsed a
colleague's risk score need only get that colleague erased. Once the dimension
row is gone the rows hold a token that resolves to nobody, so what remains is
already pseudonymous.

This is a judgment call and it should be recorded as one. A strict reading of
a right-to-erasure request covers any data relating to the person, and these
rows relate to them. The counter-argument is that the security interest in an
immutable access record, and the privacy interest of every *other* employee in
it, both survive one person's departure. If a deployment disagreed, the change
is one flag and one query — `purge_audit=True` below.

**Kafka topics.** A log is append-only; there is no delete for one key, and
compaction tombstones do not apply to topics retained by time. They age out on
their own horizon, holding tokens. A second system racing the broker to delete
data is how two components end up disagreeing about whether it exists.

**The raw archive.** Vendor payloads *do* contain addresses, and they are the
one place the token/PII split does not save us. Two options and neither is
free: tokenize the payload at archival time (see
[`tokens.redact`](tokens.py)), which costs the ability to debug a parser
against exactly what the vendor sent, or keep it and accept a shorter horizon.
This build keeps them and prunes at 30 days; `erase()` reports the count rather
than pretending it handled them.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Erased:
    """What erasure removed, and what it knowingly did not.

    Counted rather than asserted. "Deleted" with no number is the kind of
    claim that stays true in a runbook long after the code stopped doing it.
    """

    employee_id: str
    dimension_rows: int = 0
    redis_keys: int = 0
    ranking_members: int = 0
    warehouse_rows: dict[str, int] = field(default_factory=dict)
    intervention_rows: int = 0
    audit_rows: int = 0
    retained: list[str] = field(default_factory=list)
    """Stores that hold data about this person on purpose. Never silently empty."""

    @property
    def total(self) -> int:
        return (
            self.dimension_rows
            + self.redis_keys
            + self.ranking_members
            + sum(self.warehouse_rows.values())
            + self.intervention_rows
            + self.audit_rows
        )


# Tables keyed by employee, and whether erasure touches them.
#
# Listed explicitly rather than discovered from the catalog, because a table
# added later must not be erased by accident *or* skipped by accident: a new
# name here is a decision somebody made, and its absence fails the test below.
WAREHOUSE_TABLES = ("raw_daily_employee_signal", "raw_employee_score")
INTERVENTION_TABLE = "intervention"
AUDIT_TABLE = "score_read_audit"


def redis_keys_for(tenant_id: str, employee_id: str, namespace: str = "w") -> tuple[str, ...]:
    """Every Redis key that mentions one person.

    Mirrors `RedisOnlineStore`'s key construction. That duplication is the
    weak point of this module and a test asserts the two agree, because the
    failure mode is silent: rename a key prefix and erasure keeps reporting
    success while leaving the score behind.
    """
    return (
        f"{namespace}:{tenant_id}:{employee_id}",
        f"band:{tenant_id}:{employee_id}",
        f"score:{tenant_id}:{employee_id}",
    )


def erase(
    dsn: str,
    redis_url: str,
    tenant_id: str,
    employee_id: str,
    purge_audit: bool = False,
    purge_interventions: bool = True,
    dry_run: bool = False,
) -> Erased:
    """Erase one person. Returns what was removed.

    Ordered so a crash part-way leaves the system in the safer state. The
    dimension row goes **last**: it is the only thing that can resolve this
    token back to a person, so while it exists the erasure can be re-run and
    finish the job. Deleting it first would leave orphaned scores that nothing
    could identify as needing removal — data that is still there and no longer
    findable, which is the worst of both.
    """
    import psycopg
    import redis as redis_client

    result = Erased(employee_id=employee_id)
    result.retained.append("kafka topics (append-only; age out on their own horizon)")
    result.retained.append("raw vendor payloads (pruned at 30 days by retention)")

    client = redis_client.Redis.from_url(redis_url)
    keys = redis_keys_for(tenant_id, employee_id)
    if dry_run:
        result.redis_keys = sum(1 for key in keys if client.exists(key))
        result.ranking_members = 1 if client.zscore(f"rank:{tenant_id}", employee_id) else 0
    else:
        # `int(...)` on the return: redis-py types these as possibly-awaitable
        # because the same class backs the async client.
        result.redis_keys = int(client.delete(*keys))  # type: ignore[arg-type]
        result.ranking_members = int(client.zrem(f"rank:{tenant_id}", employee_id))  # type: ignore[arg-type]

    with psycopg.connect(dsn, autocommit=False) as connection, connection.cursor() as cur:
        for table in WAREHOUSE_TABLES:
            result.warehouse_rows[table] = _delete(
                cur, table, "tenant_id = %s AND employee_id = %s", (tenant_id, employee_id), dry_run
            )

        if purge_interventions:
            result.intervention_rows = _delete(
                cur,
                INTERVENTION_TABLE,
                "tenant_id = %s AND employee_id = %s",
                (tenant_id, employee_id),
                dry_run,
            )
        else:
            result.retained.append("intervention ledger (what was sent, kept for audit)")

        if purge_audit:
            result.audit_rows = _delete(
                cur,
                AUDIT_TABLE,
                "tenant_id = %s AND employee_id = %s",
                (tenant_id, employee_id),
                dry_run,
            )
        else:
            result.retained.append(
                "read audit log (a record about the actor, not the subject; now pseudonymous)"
            )

        # Last, deliberately. See the docstring.
        result.dimension_rows = _delete(
            cur,
            "employee",
            "tenant_id = %s AND employee_id = %s",
            (tenant_id, employee_id),
            dry_run,
        )

        if dry_run:
            connection.rollback()
        else:
            connection.commit()

    return result


def _delete(cursor: object, table: str, where: str, params: tuple[str, ...], dry_run: bool) -> int:
    """Delete, or count what a delete would remove.

    The table name is interpolated and the parameters are bound. That split is
    the rule rather than an inconsistency: an identifier cannot be a bind
    parameter in SQL, and every table name reaching here comes from the module
    constants above, never from a request.
    """
    cur = cursor  # typed loosely; psycopg's cursor is not exported for annotation
    if not _exists(cur, table):
        return 0
    if dry_run:
        cur.execute(f"SELECT count(*) FROM {table} WHERE {where}", params)  # type: ignore[attr-defined]
        row = cur.fetchone()  # type: ignore[attr-defined]
        return int(row[0]) if row else 0
    cur.execute(f"DELETE FROM {table} WHERE {where}", params)  # type: ignore[attr-defined]
    return int(cur.rowcount)  # type: ignore[attr-defined]


def _exists(cursor: object, table: str) -> bool:
    cursor.execute("SELECT to_regclass(%s)", (table,))  # type: ignore[attr-defined]
    row = cursor.fetchone()  # type: ignore[attr-defined]
    return bool(row and row[0])


@dataclass
class Verification:
    """Whether anything still resolves the erased person."""

    employee_id: str
    findings: list[str] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return not self.findings


def verify(dsn: str, redis_url: str, tenant_id: str, employee_id: str) -> Verification:
    """Check that nothing resolves this person any more.

    Separate from `erase` on purpose. A deletion that reports its own success
    is checking that it ran, not that it worked, and the two differ every time
    a store is added and nobody updates the eraser. This re-queries every
    store from scratch.
    """
    import psycopg
    import redis as redis_client

    result = Verification(employee_id=employee_id)

    client = redis_client.Redis.from_url(redis_url)
    for key in redis_keys_for(tenant_id, employee_id):
        if client.exists(key):
            result.findings.append(f"redis key {key} still present")
    if client.zscore(f"rank:{tenant_id}", employee_id) is not None:
        result.findings.append(f"still ranked in rank:{tenant_id}")

    with psycopg.connect(dsn) as connection, connection.cursor() as cur:
        for table in ("employee", *WAREHOUSE_TABLES, INTERVENTION_TABLE):
            if not _exists(cur, table):
                continue
            cur.execute(
                f"SELECT count(*) FROM {table} WHERE tenant_id = %s AND employee_id = %s",
                (tenant_id, employee_id),
            )
            row = cur.fetchone()
            count = int(row[0]) if row else 0
            if count:
                result.findings.append(f"{table} still holds {count} row(s)")

    return result
