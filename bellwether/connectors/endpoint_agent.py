"""Sentry Agent endpoint telemetry connector.

Pagination: numeric `offset` against a reported `total`.
Identity: `user_principal`.
Time: RFC3339 with an explicit offset.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar

from bellwether.connectors.base import Connector, Page, ParsedRecord
from bellwether.events.schema import SignalType, Source

SIGNAL_BY_TELEMETRY_TYPE: dict[str, SignalType] = {
    "dlp.genai_paste": SignalType.SENSITIVE_DATA_TO_GENAI,
    "device.removable_mount": SignalType.USB_MASS_STORAGE_MOUNTED,
}


class EndpointAgentConnector(Connector):
    source: ClassVar[Source] = Source.ENDPOINT_AGENT
    name: ClassVar[str] = "endpoint_agent"

    def fetch(self, cursor: str | None, limit: int) -> Page:
        offset = int(cursor) if cursor else 0
        body = self.client.get("/api/telemetry", {"offset": offset, "limit": limit}).json()

        # The server echoes the offset it reached, which is exactly the resume
        # position. `total` is deliberately unused: it moves under a live feed,
        # so comparing against it would either loop or stop early.
        return Page(records=body.get("records", []), cursor=str(int(body.get("offset", offset))))

    def parse(self, record: dict[str, Any]) -> ParsedRecord | None:
        signal = SIGNAL_BY_TELEMETRY_TYPE.get(record.get("telemetry_type", ""))
        if signal is None:
            return None

        # Telemetry from an unmanaged device is not something the company can
        # act on, and scoring an employee for it would be unfair.
        if not (record.get("device") or {}).get("managed", False):
            return None

        return ParsedRecord(
            signal=signal,
            occurred_at=datetime.fromisoformat(record["observed_at"]),
            subject_email=record["user_principal"],
            source_event_id=record["record_id"],
            attributes=dict(record.get("details", {})),
        )
