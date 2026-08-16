"""Renders canonical events back into the shapes real vendors emit.

Four vendors, four ways of saying the same thing. The differences here are the
point — they are what the connectors have to absorb so that nothing downstream
of `events.raw` ever sees them:

| Vendor            | Identity        | Timestamp        | Event vocabulary        |
| ----------------- | --------------- | ---------------- | ----------------------- |
| Okta              | `alternateId`   | RFC3339 millis Z | dotted `user.session.x` |
| Google Workspace  | `actor.email`   | RFC3339          | `applicationName`+`name`|
| MailShield        | `recipient`     | epoch seconds    | flat `action`           |
| Sentry Agent      | `user_principal`| RFC3339 offset   | `telemetry_type`        |

Note that every vendor identifies people by **email**, not by our employee
token. Resolving that is the connector's job, and it is the reason PII enters
the system at the connector boundary and stops there.
"""

from __future__ import annotations

import hashlib
from datetime import UTC
from typing import Any

from bellwether.events.schema import BehaviorEvent, SignalType, Source

# The vendor's own vocabulary for each signal it reports. Connectors hold the
# inverse mapping independently; `tests/test_vendor_roundtrip.py` asserts the
# two agree, which is what catches a parser drifting from the payloads it reads.
VENDOR_EVENT_TYPES: dict[Source, dict[SignalType, str]] = {
    Source.OKTA: {
        SignalType.MFA_PUSH_DENIED: "user.authentication.auth_via_mfa",
        SignalType.MFA_PUSH_FLOOD: "system.push.send_factor_verify_push",
        SignalType.PASSWORD_REUSE_DETECTED: "user.password.breach_detected",
        SignalType.IMPOSSIBLE_TRAVEL_LOGIN: "policy.evaluate_sign_on",
        SignalType.ADMIN_PRIVILEGE_GRANTED: "group.user_membership.add",
        SignalType.STALE_ACCESS_UNREVIEWED: "application.user_membership.stale",
    },
    Source.GOOGLE_WORKSPACE: {
        SignalType.FILE_SHARED_EXTERNALLY: "change_user_access",
        SignalType.FILE_SHARED_PUBLIC_LINK: "change_document_visibility",
        SignalType.BULK_DOWNLOAD_DETECTED: "download",
        SignalType.OAUTH_GRANT_RISKY_SCOPE: "authorize",
        SignalType.EMAIL_FORWARDING_RULE_CREATED: "create_forwarding_rule",
    },
    Source.EMAIL_GATEWAY: {
        SignalType.PHISH_SIM_DELIVERED: "delivered",
        SignalType.PHISH_SIM_CLICKED: "link_clicked",
        SignalType.PHISH_SIM_REPORTED: "reported_by_user",
        SignalType.PHISH_CREDENTIALS_SUBMITTED: "credentials_submitted",
        SignalType.REAL_PHISH_REPORTED: "reported_malicious",
    },
    Source.ENDPOINT_AGENT: {
        SignalType.SENSITIVE_DATA_TO_GENAI: "dlp.genai_paste",
        SignalType.USB_MASS_STORAGE_MOUNTED: "device.removable_mount",
    },
}

# Sources with no connector yet. Kept explicit so the gap is visible rather than
# discovered later by someone wondering why nobody's training score moves.
UNCONNECTED_SOURCES: frozenset[Source] = frozenset(
    {Source.TRAINING_PLATFORM, Source.BREACH_INTEL, Source.SLACK}
)


def _stable_id(event: BehaviorEvent, prefix: str) -> str:
    """A vendor-side identifier that is stable for a given event.

    Derived from `event_id` rather than random so that re-serving the same page
    yields the same ids. Connectors deduplicate on these, and a mock that
    invented a fresh id per request would make deduplication untestable.
    """
    digest = hashlib.sha256(event.event_id.encode()).hexdigest()
    return f"{prefix}{digest[:24]}"


def _okta(event: BehaviorEvent, email: str) -> dict[str, Any]:
    attrs = event.attributes
    denied = event.signal is SignalType.MFA_PUSH_DENIED
    return {
        "uuid": _stable_id(event, ""),
        "published": event.occurred_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "eventType": VENDOR_EVENT_TYPES[Source.OKTA][event.signal],
        "severity": "WARN" if denied else "INFO",
        "displayMessage": event.signal.value.replace("_", " ").title(),
        "actor": {
            "id": _stable_id(event, "00u"),
            "type": "User",
            "alternateId": email,
        },
        "outcome": {
            "result": "FAILURE" if denied else "SUCCESS",
            "reason": "MFA_DENIED" if denied else None,
        },
        "client": {
            "ipAddress": "203.0.113.7",
            "geographicalContext": {"country": attrs.get("to_country", "US")},
        },
        "debugContext": {"debugData": {str(k): str(v) for k, v in attrs.items()}},
    }


def _google(event: BehaviorEvent, email: str) -> dict[str, Any]:
    application = {
        SignalType.OAUTH_GRANT_RISKY_SCOPE: "token",
        SignalType.EMAIL_FORWARDING_RULE_CREATED: "gmail",
    }.get(event.signal, "drive")
    return {
        "kind": "admin#reports#activity",
        "id": {
            "time": event.occurred_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "uniqueQualifier": _stable_id(event, ""),
            "applicationName": application,
            "customerId": "C01acme",
        },
        "actor": {"email": email, "profileId": _stable_id(event, "1")},
        "events": [
            {
                "type": "access",
                "name": VENDOR_EVENT_TYPES[Source.GOOGLE_WORKSPACE][event.signal],
                # Google's parameter lists are name/value pairs, not an object.
                # Flattening them is a small, real piece of connector work.
                "parameters": [
                    {"name": str(k), "value": str(v)} for k, v in event.attributes.items()
                ],
            }
        ],
    }


def _mailshield(event: BehaviorEvent, email: str) -> dict[str, Any]:
    return {
        "event_id": _stable_id(event, "ms_"),
        # Epoch seconds, not a string. A connector that assumes ISO everywhere
        # breaks here, which is the point of the variation.
        "timestamp": int(event.occurred_at.timestamp()),
        "recipient": email,
        "action": VENDOR_EVENT_TYPES[Source.EMAIL_GATEWAY][event.signal],
        "campaign": {
            "id": event.attributes.get("campaign_id"),
            "subject": event.attributes.get("lure"),
            "simulated": event.signal is not SignalType.REAL_PHISH_REPORTED,
        },
    }


def _sentry_agent(event: BehaviorEvent, email: str) -> dict[str, Any]:
    return {
        "record_id": _stable_id(event, "rec-"),
        "observed_at": event.occurred_at.astimezone(UTC).isoformat(),
        "device": {"id": _stable_id(event, "dev-")[:16], "managed": True},
        "user_principal": email,
        "telemetry_type": VENDOR_EVENT_TYPES[Source.ENDPOINT_AGENT][event.signal],
        "details": dict(event.attributes),
    }


_RENDERERS = {
    Source.OKTA: _okta,
    Source.GOOGLE_WORKSPACE: _google,
    Source.EMAIL_GATEWAY: _mailshield,
    Source.ENDPOINT_AGENT: _sentry_agent,
}


def to_vendor_payload(event: BehaviorEvent, email: str) -> dict[str, Any] | None:
    """Render `event` as its source system would have emitted it.

    Returns None for sources with no connector yet, so the mock serves only what
    something is actually reading.
    """
    renderer = _RENDERERS.get(event.source)
    if renderer is None:
        return None
    if event.signal not in VENDOR_EVENT_TYPES.get(event.source, {}):
        return None
    return renderer(event, email)
