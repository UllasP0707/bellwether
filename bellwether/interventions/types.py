"""The intervention contract.

An intervention is a message to a person, so the record of one has to answer
questions after the fact: who was contacted, why, what were they told, and who
wrote it. All four are on the wire, because "the system nudged Dana last
Tuesday" is not a defensible answer to "why did Dana get this?".
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from bellwether.events.schema import RiskCategory, SignalType
from bellwether.scoring import RiskBand

INTERVENTION_SCHEMA_VERSION = 1


class InterventionType(StrEnum):
    """Rungs on the escalation ladder, in ascending severity.

    The order is load-bearing: `Policy` climbs this list and never skips a rung,
    so someone cannot go from having heard nothing to having their manager
    emailed on the strength of one bad afternoon.
    """

    NUDGE = "nudge"
    TRAINING = "training"
    MANAGER_NOTIFICATION = "manager_notification"


LADDER: tuple[InterventionType, ...] = (
    InterventionType.NUDGE,
    InterventionType.TRAINING,
    InterventionType.MANAGER_NOTIFICATION,
)


class Channel(StrEnum):
    CHAT = "chat"
    EMAIL = "email"


class CopySource(StrEnum):
    """Who wrote the words.

    Recorded on every intervention because it is the first thing anyone will
    want to know when a message reads badly, and because it is the only way to
    measure how often the model is actually being used versus quietly falling
    back.
    """

    MODEL = "model"
    TEMPLATE = "template"


class SuppressionReason(StrEnum):
    """Why an intervention that was triggered did not send.

    Enumerated rather than free text because "nothing happened" is a result the
    system has to be able to explain, and counting the reasons is how you find
    out that a policy is throttling everything.
    """

    NO_TRIGGER = "no_trigger"
    BAND_FELL = "band_fell"
    BELOW_THRESHOLD = "below_threshold"
    COOLDOWN = "cooldown"
    TOO_SOON = "too_soon"
    WEEKLY_CAP = "weekly_cap"
    ALREADY_SENT = "already_sent"
    NO_TRIGGER_ID = "no_trigger_id"
    UNKNOWN_EMPLOYEE = "unknown_employee"
    MALFORMED = "malformed"


class InterventionEvent(BaseModel):
    """One intervention, as published to `bellwether.interventions`.

    That topic is an **outbox**, not a delivery. Nothing in this repo talks to
    Slack or an SMTP server; a delivery worker would consume this and would be
    the thing that can actually fail to reach someone. Keeping the two separate
    means the decision to contact a person is recorded even when delivery
    breaks, which is the order you want for anything auditable.
    """

    model_config = ConfigDict(frozen=True)

    schema_version: int = INTERVENTION_SCHEMA_VERSION
    intervention_id: str = Field(default_factory=lambda: str(uuid.uuid4()))

    tenant_id: str
    employee_id: str
    type: InterventionType
    channel: Channel

    # Why this fired. `trigger_event_id` is what makes a redelivered score
    # message inert: it is the uniqueness key in the ledger, so the second
    # attempt to act on the same behaviour cannot produce a second message.
    trigger_signal: SignalType | None = None
    trigger_event_id: str | None = None
    band: RiskBand
    previous_band: RiskBand | None = None
    score: float
    dominant_category: RiskCategory | None = None

    subject: str
    body: str
    copy_source: CopySource
    guardrail_rejections: int = 0

    created_at: datetime

    def partition_key(self) -> bytes:
        """Keyed by employee so one person's history stays ordered."""
        return self.employee_id.encode()
