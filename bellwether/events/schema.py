"""Canonical event contracts.

Connectors are the only components allowed to know what a source system's
payload looks like. Everything downstream of `bellwether.events.raw` speaks
`BehaviorEvent` and nothing else.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

SCHEMA_VERSION = 1


class Source(StrEnum):
    """Upstream system a signal came from."""

    OKTA = "okta"
    GOOGLE_WORKSPACE = "google_workspace"
    SLACK = "slack"
    EMAIL_GATEWAY = "email_gateway"
    ENDPOINT_AGENT = "endpoint_agent"
    TRAINING_PLATFORM = "training_platform"
    BREACH_INTEL = "breach_intel"


class RiskCategory(StrEnum):
    """Risk domain a signal contributes to.

    Scores are attributed per category as well as in aggregate, because
    "Dana is a 78" is not actionable but "Dana is a 78, almost entirely
    phishing susceptibility" tells you which intervention to send.
    """

    PHISHING_SUSCEPTIBILITY = "phishing_susceptibility"
    CREDENTIAL_HYGIENE = "credential_hygiene"
    DATA_HANDLING = "data_handling"
    ACCESS_HYGIENE = "access_hygiene"
    SECURITY_ENGAGEMENT = "security_engagement"


class SignalType(StrEnum):
    """Every behavior Bellwether understands.

    Adding a member here is incomplete until it has a `SignalSpec` in
    `bellwether.scoring.catalog`; a test enforces that.
    """

    # Phishing susceptibility
    PHISH_SIM_DELIVERED = "phish_sim_delivered"
    PHISH_SIM_CLICKED = "phish_sim_clicked"
    PHISH_SIM_REPORTED = "phish_sim_reported"
    PHISH_CREDENTIALS_SUBMITTED = "phish_credentials_submitted"
    REAL_PHISH_REPORTED = "real_phish_reported"

    # Credential hygiene
    MFA_PUSH_DENIED = "mfa_push_denied"
    MFA_PUSH_FLOOD = "mfa_push_flood"
    PASSWORD_REUSE_DETECTED = "password_reuse_detected"
    CREDENTIAL_IN_BREACH_DUMP = "credential_in_breach_dump"
    IMPOSSIBLE_TRAVEL_LOGIN = "impossible_travel_login"

    # Data handling
    FILE_SHARED_EXTERNALLY = "file_shared_externally"
    FILE_SHARED_PUBLIC_LINK = "file_shared_public_link"
    SENSITIVE_DATA_TO_GENAI = "sensitive_data_to_genai"
    BULK_DOWNLOAD_DETECTED = "bulk_download_detected"
    USB_MASS_STORAGE_MOUNTED = "usb_mass_storage_mounted"

    # Access hygiene
    OAUTH_GRANT_RISKY_SCOPE = "oauth_grant_risky_scope"
    EMAIL_FORWARDING_RULE_CREATED = "email_forwarding_rule_created"
    ADMIN_PRIVILEGE_GRANTED = "admin_privilege_granted"
    STALE_ACCESS_UNREVIEWED = "stale_access_unreviewed"

    # Security engagement (mostly mitigating)
    TRAINING_COMPLETED = "training_completed"
    TRAINING_OVERDUE = "training_overdue"
    INTERVENTION_ACKNOWLEDGED = "intervention_acknowledged"
    INTERVENTION_IGNORED = "intervention_ignored"


SIGNAL_SOURCE: dict[SignalType, Source] = {
    SignalType.PHISH_SIM_DELIVERED: Source.EMAIL_GATEWAY,
    SignalType.PHISH_SIM_CLICKED: Source.EMAIL_GATEWAY,
    SignalType.PHISH_SIM_REPORTED: Source.EMAIL_GATEWAY,
    SignalType.PHISH_CREDENTIALS_SUBMITTED: Source.EMAIL_GATEWAY,
    SignalType.REAL_PHISH_REPORTED: Source.EMAIL_GATEWAY,
    SignalType.MFA_PUSH_DENIED: Source.OKTA,
    SignalType.MFA_PUSH_FLOOD: Source.OKTA,
    SignalType.PASSWORD_REUSE_DETECTED: Source.OKTA,
    SignalType.CREDENTIAL_IN_BREACH_DUMP: Source.BREACH_INTEL,
    SignalType.IMPOSSIBLE_TRAVEL_LOGIN: Source.OKTA,
    SignalType.FILE_SHARED_EXTERNALLY: Source.GOOGLE_WORKSPACE,
    SignalType.FILE_SHARED_PUBLIC_LINK: Source.GOOGLE_WORKSPACE,
    SignalType.SENSITIVE_DATA_TO_GENAI: Source.ENDPOINT_AGENT,
    SignalType.BULK_DOWNLOAD_DETECTED: Source.GOOGLE_WORKSPACE,
    SignalType.USB_MASS_STORAGE_MOUNTED: Source.ENDPOINT_AGENT,
    SignalType.OAUTH_GRANT_RISKY_SCOPE: Source.GOOGLE_WORKSPACE,
    SignalType.EMAIL_FORWARDING_RULE_CREATED: Source.GOOGLE_WORKSPACE,
    SignalType.ADMIN_PRIVILEGE_GRANTED: Source.OKTA,
    SignalType.STALE_ACCESS_UNREVIEWED: Source.OKTA,
    SignalType.TRAINING_COMPLETED: Source.TRAINING_PLATFORM,
    SignalType.TRAINING_OVERDUE: Source.TRAINING_PLATFORM,
    SignalType.INTERVENTION_ACKNOWLEDGED: Source.SLACK,
    SignalType.INTERVENTION_IGNORED: Source.SLACK,
}
"""Which upstream system reports each signal.

Lives with the contracts rather than in a connector because it is the rule a
connector is checked against: an Okta connector emitting a
`file_shared_externally` event is a bug, and this is what makes that detectable.
"""


class Employee(BaseModel):
    """The employee dimension.

    The only place PII lives. Events carry `employee_id` alone, so retention
    and deletion have exactly one enforcement point.
    """

    model_config = ConfigDict(frozen=True)

    employee_id: str
    tenant_id: str
    department: str
    seniority: str
    tenure_days: int
    location: str
    has_admin_access: bool = False
    handles_financial_data: bool = False
    is_executive: bool = False

    # PII. Never copied onto an event.
    email: str | None = None
    display_name: str | None = None
    manager_id: str | None = None

    @property
    def is_high_value_target(self) -> bool:
        """Whether an attacker would specifically choose this person.

        Multiplies score impact: the same click is more dangerous from someone
        who can wire money or reset passwords.
        """
        return self.is_executive or self.handles_financial_data or self.has_admin_access


class BehaviorEvent(BaseModel):
    """A single normalized employee behavior signal."""

    model_config = ConfigDict(frozen=True)

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    schema_version: int = SCHEMA_VERSION

    tenant_id: str
    employee_id: str
    signal: SignalType
    source: Source

    # Kept distinct on purpose. Sources deliver late and out of order, so a
    # batch job windowing on ingest time and a stream windowing on event time
    # would otherwise disagree without either being wrong.
    occurred_at: datetime
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    source_event_id: str | None = None
    raw_ref: str | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)

    @field_validator("occurred_at", "ingested_at")
    @classmethod
    def _require_tz(cls, v: datetime) -> datetime:
        """Reject naive datetimes.

        A naive timestamp that reaches the lake is silently wrong for anyone in
        a different timezone, and nothing downstream can detect it.
        """
        if v.tzinfo is None:
            raise ValueError("timestamps must be timezone-aware")
        return v.astimezone(UTC)

    @property
    def lateness_seconds(self) -> float:
        """How far behind event time this event was ingested."""
        return (self.ingested_at - self.occurred_at).total_seconds()

    def partition_key(self) -> bytes:
        """Kafka key.

        Keying by employee puts all of one person's behavior on one partition,
        so the per-employee scorer needs no cross-partition coordination. The
        cost is that a noisy account can hot-spot a partition.
        """
        return self.employee_id.encode()
