"""Who is asking, and what they are allowed to see.

The tenant comes from the credential and nowhere else. There is deliberately no
`tenant_id` parameter on any endpoint: the moment tenancy is something a caller
can state, isolation depends on every handler remembering to check it, and one
handler eventually will not.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Principal:
    """An authenticated caller."""

    actor: str
    tenant_id: str


def parse_keys(spec: str) -> dict[str, Principal]:
    """Parse `key:tenant:actor` triples.

    A configuration format rather than a user store, which is the honest shape
    for a demo. Real deployment swaps this for the identity provider the company
    already runs; what matters is that whatever replaces it still returns a
    tenant the request cannot influence.
    """
    principals: dict[str, Principal] = {}
    for entry in spec.split(","):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(":")
        if len(parts) != 3:
            raise ValueError(f"expected key:tenant:actor, got {entry!r}")
        key, tenant, actor = (p.strip() for p in parts)
        if not (key and tenant and actor):
            raise ValueError(f"empty field in {entry!r}")
        principals[key] = Principal(actor=actor, tenant_id=tenant)
    return principals
