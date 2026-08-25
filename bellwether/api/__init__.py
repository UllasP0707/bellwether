"""The read path: HTTP over the online projection.

Live per-employee reads and the population ranking are served from Redis, where
the scorer projects each score as it computes it. Anything historical — trend,
cohort, month-over-month — is not here and is not meant to be: an online store
that gets scanned stops being fast for the queries it exists for.
"""

from bellwether.api.app import TenantContext, create_app
from bellwether.api.audit import InMemoryAudit, PostgresAudit, ReadAudit, ReadRecord
from bellwether.api.security import Principal, parse_keys

__all__ = [
    "InMemoryAudit",
    "PostgresAudit",
    "Principal",
    "ReadAudit",
    "ReadRecord",
    "TenantContext",
    "create_app",
    "parse_keys",
]
