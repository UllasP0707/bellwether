"""Cursor persistence.

A connector's cursor is the only thing standing between a restart and
re-ingesting the vendor's entire history, so it outlives the process.

Keyed by `(connector, stream)` rather than by connector alone because several
real APIs are a family of substreams behind one integration — Google Workspace
reports are per-application, and each advances independently.
"""

from __future__ import annotations

from typing import Protocol


class CursorStore(Protocol):
    """Where a connector remembers how far it got."""

    def get(self, connector: str, stream: str) -> str | None: ...

    def set(self, connector: str, stream: str, cursor: str | None) -> None: ...


class InMemoryCursorStore:
    """Non-durable store for tests and dry runs."""

    def __init__(self) -> None:
        self._cursors: dict[tuple[str, str], str | None] = {}

    def get(self, connector: str, stream: str) -> str | None:
        return self._cursors.get((connector, stream))

    def set(self, connector: str, stream: str, cursor: str | None) -> None:
        self._cursors[(connector, stream)] = cursor


DDL = """
CREATE TABLE IF NOT EXISTS connector_cursor (
    connector   text        NOT NULL,
    stream      text        NOT NULL,
    cursor      text,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (connector, stream)
)
"""


class PostgresCursorStore:
    """Durable cursor storage.

    Writes are upserts on the composite key, so a connector restarting mid-run
    resumes from its last committed page rather than the beginning.
    """

    def __init__(self, dsn: str) -> None:
        import psycopg

        self._connection = psycopg.connect(dsn, autocommit=True)
        with self._connection.cursor() as cur:
            cur.execute(DDL)

    def get(self, connector: str, stream: str) -> str | None:
        with self._connection.cursor() as cur:
            cur.execute(
                "SELECT cursor FROM connector_cursor WHERE connector = %s AND stream = %s",
                (connector, stream),
            )
            row = cur.fetchone()
        return None if row is None else row[0]

    def set(self, connector: str, stream: str, cursor: str | None) -> None:
        with self._connection.cursor() as cur:
            cur.execute(
                """
                INSERT INTO connector_cursor (connector, stream, cursor, updated_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT (connector, stream)
                DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = now()
                """,
                (connector, stream, cursor),
            )

    def close(self) -> None:
        self._connection.close()
