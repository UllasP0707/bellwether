"""MailShield email security gateway connector.

Pagination: `cursor` in, `next_cursor` / `has_more` out.
Identity: `recipient`.
Time: epoch **seconds**, as an integer.

This is the source the demo runs on: the phishing chain — delivered, clicked,
credentials submitted — arrives here.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, ClassVar

from bellwether.connectors.base import Connector, Page, ParsedRecord
from bellwether.events.schema import SignalType, Source

SIGNAL_BY_ACTION: dict[str, SignalType] = {
    "delivered": SignalType.PHISH_SIM_DELIVERED,
    "link_clicked": SignalType.PHISH_SIM_CLICKED,
    "reported_by_user": SignalType.PHISH_SIM_REPORTED,
    "credentials_submitted": SignalType.PHISH_CREDENTIALS_SUBMITTED,
    "reported_malicious": SignalType.REAL_PHISH_REPORTED,
}

# Actions that only make sense against a simulated campaign. A real reported
# phish is not a simulation result and must not be counted as one.
_SIMULATION_ONLY = {
    SignalType.PHISH_SIM_DELIVERED,
    SignalType.PHISH_SIM_CLICKED,
    SignalType.PHISH_SIM_REPORTED,
    SignalType.PHISH_CREDENTIALS_SUBMITTED,
}


class EmailGatewayConnector(Connector):
    source: ClassVar[Source] = Source.EMAIL_GATEWAY
    name: ClassVar[str] = "email_gateway"

    def fetch(self, cursor: str | None, limit: int) -> Page:
        params: dict[str, Any] = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        body = self.client.get("/v2/events", params).json()
        # has_more is deliberately ignored for paging. next_cursor is where to
        # resume, and it stays meaningful once has_more goes false; the empty
        # page that follows is what ends the run.
        return Page(records=body.get("data", []), cursor=body.get("next_cursor"))

    def parse(self, record: dict[str, Any]) -> ParsedRecord | None:
        signal = SIGNAL_BY_ACTION.get(record.get("action", ""))
        if signal is None:
            return None

        campaign = record.get("campaign") or {}
        if signal in _SIMULATION_ONLY and not campaign.get("simulated", False):
            return None

        return ParsedRecord(
            signal=signal,
            # Epoch seconds, not ISO. The one vendor here that does this, which
            # is why the framework takes a datetime and not a string.
            occurred_at=datetime.fromtimestamp(int(record["timestamp"]), tz=UTC),
            subject_email=record["recipient"],
            source_event_id=record["event_id"],
            attributes={
                "campaign_id": campaign.get("id"),
                "lure": campaign.get("subject"),
            },
        )
