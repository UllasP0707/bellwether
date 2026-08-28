"""Data protection: erasing one person, and tokenizing the fields that name them.

Two mechanisms with different reach, which is the point of having both.

[`erasure`](erasure.py) deletes a named individual on request. It works
because events carry a token and PII lives only on the employee dimension, so
one `DELETE` plus a projection drop leaves every downstream copy holding an
identifier that resolves to nobody.

[`tokens`](tokens.py) is the answer where `DELETE` cannot reach -- Kafka
segments, Parquet in the lake, a backup from last March. Destroying a tenant's
key unlinks every token derived from it, everywhere, at once. That is erasure
at a granularity a database cannot offer and a bluntness a person cannot use:
it is tenant offboarding, not a right-to-erasure request.
"""

from bellwether.privacy.erasure import Erased, Verification, erase, verify
from bellwether.privacy.tokens import PII_FIELDS, Tokenizer, normalise

__all__ = [
    "PII_FIELDS",
    "Erased",
    "Tokenizer",
    "Verification",
    "erase",
    "normalise",
    "verify",
]
