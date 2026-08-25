"""Interventions: the part of Bellwether that acts on a person.

Everything upstream of here computes. This is the only place Bellwether does
something a human notices, which is why most of the code in this package is
concerned with *not* acting.
"""

from bellwether.interventions.copy import (
    ClaudeCopywriter,
    CopyBrief,
    Copydesk,
    CopyUnavailableError,
    Draft,
    TemplateCopywriter,
)
from bellwether.interventions.decide import TRIGGER_SIGNALS, Decider, Trigger, Verdict
from bellwether.interventions.guardrails import Guardrails, Violation
from bellwether.interventions.handler import (
    InterventionOutcome,
    InterventionStage,
    InterventionStats,
)
from bellwether.interventions.policy import (
    InMemoryLedger,
    InterventionLedger,
    Policy,
    PostgresLedger,
    band_rose,
    cooldown_active,
)
from bellwether.interventions.types import (
    Channel,
    CopySource,
    InterventionEvent,
    InterventionType,
    SuppressionReason,
)

__all__ = [
    "TRIGGER_SIGNALS",
    "Channel",
    "ClaudeCopywriter",
    "CopyBrief",
    "CopySource",
    "CopyUnavailableError",
    "Copydesk",
    "Decider",
    "Draft",
    "Guardrails",
    "InMemoryLedger",
    "InterventionEvent",
    "InterventionLedger",
    "InterventionOutcome",
    "InterventionStage",
    "InterventionStats",
    "InterventionType",
    "Policy",
    "PostgresLedger",
    "SuppressionReason",
    "TemplateCopywriter",
    "Trigger",
    "Verdict",
    "Violation",
    "band_rose",
    "cooldown_active",
]
