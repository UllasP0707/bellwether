"""A mock upstream: the vendor APIs Bellwether ingests from.

This exists so the connectors are real integrations rather than a loop over an
in-process list. Each endpoint imitates its real counterpart's payload shape,
pagination idiom, timestamp format, and failure behaviour, because those are
exactly the things that make connector code non-trivial.

Nothing in `bellwether.connectors` is allowed to import from here. The two sides
are kept separate on purpose: a connector should be written against the vendor's
observable behaviour, the same way it would be in production. A round-trip test
holds them to the same vocabulary.
"""

from bellwether.vendor.payloads import VENDOR_EVENT_TYPES, to_vendor_payload

__all__ = ["VENDOR_EVENT_TYPES", "to_vendor_payload"]
