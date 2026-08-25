"""The read audit log.

Who looked at whose risk score is itself sensitive. A tool that ranks colleagues
by how much of a liability they are will be opened for reasons that have nothing
to do with security, and the only thing that makes that answerable after the
fact is a record of every look.

Only per-employee reads are audited. Ranking the population is pseudonymous —
the list carries tokens and scores, no names — so it is browsing, not looking
somebody up. Auditing it too would bury the reads that matter under a row for
every dashboard refresh.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

DDL = """
CREATE TABLE IF NOT EXISTS score_read_audit (
    id          bigserial   PRIMARY KEY,
    read_at     timestamptz NOT NULL DEFAULT now(),
    actor       text        NOT NULL,
    tenant_id   text        NOT NULL,
    employee_id text        NOT NULL,
    endpoint    text        NOT NULL
);

-- Two access patterns, both of them questions somebody actually asks: "who has
-- been looking at this person" and "what has this account been looking at".
CREATE INDEX IF NOT EXISTS score_read_subject_idx
    ON score_read_audit (tenant_id, employee_id, read_at DESC);
CREATE INDEX IF NOT EXISTS score_read_actor_idx
    ON score_read_audit (tenant_id, actor, read_at DESC);
"""


@dataclass(frozen=True)
class ReadRecord:
    actor: str
    tenant_id: str
    employee_id: str
    endpoint: str
    read_at: datetime


class ReadAudit(Protocol):
    def record(self, actor: str, tenant_id: str, employee_id: str, endpoint: str) -> None: ...

    def recent(self, tenant_id: str, limit: int = 50) -> list[ReadRecord]: ...


class InMemoryAudit:
    def __init__(self) -> None:
        self.records: list[ReadRecord] = []

    def record(self, actor: str, tenant_id: str, employee_id: str, endpoint: str) -> None:
        self.records.append(ReadRecord(actor, tenant_id, employee_id, endpoint, datetime.now(UTC)))

    def recent(self, tenant_id: str, limit: int = 50) -> list[ReadRecord]:
        matching = [r for r in self.records if r.tenant_id == tenant_id]
        return sorted(matching, key=lambda r: r.read_at, reverse=True)[:limit]


class PostgresAudit:
    """Durable audit.

    Written synchronously, in the request path, before the response is built. An
    audit log that can be outrun by the thing it audits is not an audit log, and
    the cost — one insert against an append-only table — is not worth optimising
    away for a read volume bounded by how fast a security team can click.
    """

    def __init__(self, dsn: str) -> None:
        import psycopg

        self._connection = psycopg.connect(dsn, autocommit=True)
        with self._connection.cursor() as cur:
            cur.execute(DDL)

    def record(self, actor: str, tenant_id: str, employee_id: str, endpoint: str) -> None:
        with self._connection.cursor() as cur:
            cur.execute(
                "INSERT INTO score_read_audit (actor, tenant_id, employee_id, endpoint) "
                "VALUES (%s, %s, %s, %s)",
                (actor, tenant_id, employee_id, endpoint),
            )

    def recent(self, tenant_id: str, limit: int = 50) -> list[ReadRecord]:
        with self._connection.cursor() as cur:
            cur.execute(
                "SELECT actor, tenant_id, employee_id, endpoint, read_at "
                "FROM score_read_audit WHERE tenant_id = %s ORDER BY read_at DESC LIMIT %s",
                (tenant_id, limit),
            )
            return [ReadRecord(*row) for row in cur.fetchall()]

    def close(self) -> None:
        self._connection.close()
