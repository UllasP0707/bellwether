"""The employee dimension.

The only place PII lives, and the only source of truth for who exists. Two
callers need it and they used to disagree: connectors resolved vendor emails
against a freshly generated population, while scoring needed employee
attributes that nothing had persisted. Both now read this.

Lookups are served from an in-process snapshot rather than a query per event. A
scorer consulting Postgres once per message would put a network round trip in
the hot path to answer a question whose answer changes about as often as
someone changes jobs.

The cost is staleness, and it is bounded on purpose. That bound is a privacy
property rather than a performance one: a person erased from the database is
still resident in every process holding a snapshot, so the snapshot expires --
see `STALE_AFTER_SECONDS`.
"""

from __future__ import annotations

import time
from typing import Protocol

from bellwether.events.schema import Employee

DDL = """
CREATE TABLE IF NOT EXISTS employee (
    employee_id            text PRIMARY KEY,
    tenant_id              text        NOT NULL,
    department             text        NOT NULL,
    seniority              text        NOT NULL,
    tenure_days            integer     NOT NULL,
    location               text        NOT NULL,
    has_admin_access       boolean     NOT NULL DEFAULT false,
    handles_financial_data boolean     NOT NULL DEFAULT false,
    is_executive           boolean     NOT NULL DEFAULT false,
    email                  text,
    display_name           text,
    manager_id             text,
    updated_at             timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS employee_email_idx  ON employee (lower(email));
CREATE INDEX IF NOT EXISTS employee_tenant_idx ON employee (tenant_id);
"""

_COLUMNS = (
    "employee_id",
    "tenant_id",
    "department",
    "seniority",
    "tenure_days",
    "location",
    "has_admin_access",
    "handles_financial_data",
    "is_executive",
    "email",
    "display_name",
    "manager_id",
)


class EmployeeRepository(Protocol):
    """Who exists, and what scoring needs to know about them."""

    def get(self, employee_id: str) -> Employee | None: ...

    def resolve_email(self, email: str) -> str | None: ...

    def all(self) -> list[Employee]: ...


class InMemoryEmployeeRepository:
    """For tests and for running without Postgres."""

    def __init__(self, employees: list[Employee] | None = None) -> None:
        self._by_id: dict[str, Employee] = {}
        self._by_email: dict[str, str] = {}
        self.ambiguous: set[str] = set()
        for employee in employees or []:
            self.add(employee)

    def add(self, employee: Employee) -> None:
        self._by_id[employee.employee_id] = employee
        if not employee.email:
            return
        key = employee.email.strip().lower()
        existing = self._by_email.get(key)
        if existing is not None and existing != employee.employee_id:
            self.ambiguous.add(key)
        self._by_email[key] = employee.employee_id

    def get(self, employee_id: str) -> Employee | None:
        return self._by_id.get(employee_id)

    def resolve_email(self, email: str) -> str | None:
        """None if the address does not identify exactly one person."""
        key = email.strip().lower()
        if key in self.ambiguous:
            return None
        return self._by_email.get(key)

    def all(self) -> list[Employee]:
        return list(self._by_id.values())

    def __len__(self) -> int:
        return len(self._by_id)


# How long the in-process snapshot may be stale.
#
# This number is a privacy bound, not a performance tuning knob. The dimension
# is cached because the scorer reads it once per message and a query per
# message would be most of the latency budget — but caching it means a person
# erased from the database is still resident in every running process, and the
# first live erasure proved it: the row was gone, the score was gone, and the
# API still held the name.
#
# So the snapshot expires. Erasure is not instantaneous, it is complete within
# this window, and the honest thing is to state the bound rather than to claim
# a delete that some processes have not seen.
STALE_AFTER_SECONDS = 300.0


class PostgresEmployeeRepository:
    """Postgres-backed, read through an in-process snapshot that expires."""

    def __init__(
        self,
        dsn: str,
        tenant_id: str | None = None,
        load: bool = True,
        stale_after_seconds: float | None = STALE_AFTER_SECONDS,
    ) -> None:
        import psycopg

        self._connection = psycopg.connect(dsn, autocommit=True)
        self.tenant_id = tenant_id
        self.stale_after_seconds = stale_after_seconds
        self._loaded_at = 0.0
        with self._connection.cursor() as cur:
            cur.execute(DDL)
        self._snapshot = InMemoryEmployeeRepository()
        if load:
            self.refresh()

    def refresh(self) -> int:
        """Reload the snapshot. Returns how many employees are now known."""
        query = f"SELECT {', '.join(_COLUMNS)} FROM employee"
        params: tuple[str, ...] = ()
        if self.tenant_id:
            query += " WHERE tenant_id = %s"
            params = (self.tenant_id,)

        with self._connection.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

        snapshot = InMemoryEmployeeRepository()
        for row in rows:
            snapshot.add(Employee(**dict(zip(_COLUMNS, row, strict=True))))
        self._snapshot = snapshot
        self._loaded_at = time.monotonic()
        return len(snapshot)

    def _fresh(self) -> InMemoryEmployeeRepository:
        """The snapshot, reloaded if it has expired.

        A monotonic comparison per read, which is tens of nanoseconds against
        a per-message budget measured in milliseconds. `monotonic` rather than
        wall time so an NTP correction cannot make the cache look fresh for an
        hour.

        `None` disables expiry; `0` refreshes on every read. Those were both
        spelled `0` for about ten minutes, and the falsy check meant the test
        asking for "always fresh" silently got "never refresh" — the exact
        failure this whole mechanism exists to prevent, in the mechanism
        itself.
        """
        if self.stale_after_seconds is None:
            return self._snapshot
        if time.monotonic() - self._loaded_at >= self.stale_after_seconds:
            self.refresh()
        return self._snapshot

    def upsert_many(self, employees: list[Employee]) -> int:
        """Insert or update, then refresh the snapshot.

        Upsert rather than truncate-and-load: employee ids are referenced by
        events already in the lake, so replacing the table wholesale would
        briefly orphan every one of them.
        """
        columns = ", ".join(_COLUMNS)
        placeholders = ", ".join(["%s"] * len(_COLUMNS))
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in _COLUMNS if c != "employee_id")

        with self._connection.cursor() as cur:
            cur.executemany(
                f"INSERT INTO employee ({columns}) VALUES ({placeholders}) "
                f"ON CONFLICT (employee_id) DO UPDATE SET {updates}, updated_at = now()",
                [tuple(getattr(e, c) for c in _COLUMNS) for e in employees],
            )
        return self.refresh()

    def get(self, employee_id: str) -> Employee | None:
        return self._fresh().get(employee_id)

    def resolve_email(self, email: str) -> str | None:
        return self._fresh().resolve_email(email)

    def all(self) -> list[Employee]:
        return self._fresh().all()

    def __len__(self) -> int:
        return len(self._fresh())

    def close(self) -> None:
        self._connection.close()
