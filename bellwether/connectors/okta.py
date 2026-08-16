"""Okta System Log connector.

Pagination: opaque `after` cursor, next page advertised in a `Link` header.
Identity: `actor.alternateId` (an email).
Time: RFC3339 with milliseconds and a `Z` suffix.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, ClassVar

from bellwether.connectors.base import Connector, Page, ParsedRecord
from bellwether.events.schema import SignalType, Source

# Okta's vocabulary, as this connector understands it. Held independently of the
# mock vendor's table so the two can disagree and a test can notice.
SIGNAL_BY_EVENT_TYPE: dict[str, SignalType] = {
    "user.authentication.auth_via_mfa": SignalType.MFA_PUSH_DENIED,
    "system.push.send_factor_verify_push": SignalType.MFA_PUSH_FLOOD,
    "user.password.breach_detected": SignalType.PASSWORD_REUSE_DETECTED,
    "policy.evaluate_sign_on": SignalType.IMPOSSIBLE_TRAVEL_LOGIN,
    "group.user_membership.add": SignalType.ADMIN_PRIVILEGE_GRANTED,
    "application.user_membership.stale": SignalType.STALE_ACCESS_UNREVIEWED,
}

_NEXT_AFTER = re.compile(r'[?&]after=([^&>]+)[^>]*>;\s*rel="next"')


class OktaConnector(Connector):
    source: ClassVar[Source] = Source.OKTA
    name: ClassVar[str] = "okta"

    def fetch(self, cursor: str | None, limit: int) -> Page:
        params: dict[str, Any] = {"limit": limit}
        if cursor:
            params["after"] = cursor
        response = self.client.get("/api/v1/logs", params)

        # Okta signals "more available" only by sending a Link header. Its
        # absence is the terminator; an empty body is not, since a page can be
        # empty after server-side filtering while more pages remain.
        match = _NEXT_AFTER.search(response.headers.get("Link", ""))
        return Page(records=response.json(), next_cursor=match.group(1) if match else None)

    def parse(self, record: dict[str, Any]) -> ParsedRecord | None:
        signal = SIGNAL_BY_EVENT_TYPE.get(record["eventType"])
        if signal is None:
            return None

        # A denied MFA push and an approved one share an eventType; only the
        # outcome separates them. Treating every auth_via_mfa as a denial would
        # make almost everyone look risky.
        if signal is SignalType.MFA_PUSH_DENIED and record["outcome"]["result"] != "FAILURE":
            return None

        return ParsedRecord(
            signal=signal,
            occurred_at=datetime.fromisoformat(record["published"]),
            subject_email=record["actor"]["alternateId"],
            source_event_id=record["uuid"],
            attributes=dict(record.get("debugContext", {}).get("debugData", {})),
        )
