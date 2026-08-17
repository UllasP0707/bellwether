"""The employee dimension.

The only place PII lives, and the only source of truth for who exists. Two
callers need it and they used to disagree: connectors resolved vendor emails
against a freshly generated population, while scoring needed employee
attributes that nothing had persisted. Both now read this.

Lookups are served from an in-process snapshot rather than a query per event. A
scorer consulting Postgres once per message would put a network round trip in
the hot path to answer a question whose answer changes about as often as
someone changes jobs. The cost is staleness bounded by `refresh()`.
"""

from __future__ import annotations

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


class PostgresEmployeeRepository:
    """Postgres-backed, read through an in-process snapshot."""

    def __init__(self, dsn: str, tenant_id: str | None = None, load: bool = True) -> None:
        import psycopg

        self._connection = psycopg.connect(dsn, autocommit=True)
        self.tenant_id = tenant_id
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
        return len(snapshot)

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
        return self._snapshot.get(employee_id)

    def resolve_email(self, email: str) -> str | None:
        return self._snapshot.resolve_email(email)

    def all(self) -> list[Employee]:
        return self._snapshot.all()

    def __len__(self) -> int:
        return len(self._snapshot)

    def close(self) -> None:
        self._connection.close()
