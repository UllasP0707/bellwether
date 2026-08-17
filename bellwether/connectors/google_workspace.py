"""Google Workspace Admin Reports connector.

Pagination: `pageToken` in, `nextPageToken` in the body.
Identity: `actor.email`.
Time: RFC3339, on `id.time`.

Google reports are really a family of substreams — drive, gmail, token, login —
each with its own cursor. This polls the aggregate stream for simplicity, but
`stream` on the connector is what would carry the application name if it
polled them separately, and the cursor store is keyed to allow it.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar

from bellwether.connectors.base import Connector, Page, ParsedRecord
from bellwether.events.schema import SignalType, Source

SIGNAL_BY_ACTIVITY_NAME: dict[str, SignalType] = {
    "change_user_access": SignalType.FILE_SHARED_EXTERNALLY,
    "change_document_visibility": SignalType.FILE_SHARED_PUBLIC_LINK,
    "download": SignalType.BULK_DOWNLOAD_DETECTED,
    "authorize": SignalType.OAUTH_GRANT_RISKY_SCOPE,
    "create_forwarding_rule": SignalType.EMAIL_FORWARDING_RULE_CREATED,
}


class GoogleWorkspaceConnector(Connector):
    source: ClassVar[Source] = Source.GOOGLE_WORKSPACE
    name: ClassVar[str] = "google_workspace"
    stream: ClassVar[str] = "all"

    def fetch(self, cursor: str | None, limit: int) -> Page:
        params: dict[str, Any] = {"maxResults": limit}
        if cursor:
            params["pageToken"] = cursor
        body = self.client.get(
            f"/admin/reports/v1/activity/users/all/applications/{self.stream}", params
        ).json()
        return Page(records=body.get("items", []), cursor=body.get("nextPageToken"))

    def parse(self, record: dict[str, Any]) -> ParsedRecord | None:
        events = record.get("events") or []
        if not events:
            return None

        activity = events[0]
        signal = SIGNAL_BY_ACTIVITY_NAME.get(activity.get("name", ""))
        if signal is None:
            return None

        # Google sends parameters as a list of {name, value} objects rather than
        # an object, so every consumer would otherwise reimplement this flatten.
        attributes = {
            param["name"]: param.get("value")
            for param in activity.get("parameters", [])
            if "name" in param
        }

        return ParsedRecord(
            signal=signal,
            occurred_at=datetime.fromisoformat(record["id"]["time"]),
            subject_email=record["actor"]["email"],
            source_event_id=record["id"]["uniqueQualifier"],
            attributes=attributes,
        )
