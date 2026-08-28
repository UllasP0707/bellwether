"""Field-level tokenization, and the erasure guarantee it buys.

A token here is `HMAC-SHA256(tenant key, normalised value)`, truncated and
hex-encoded. Three properties, and the third is the one that matters:

**Deterministic.** The same address always produces the same token, so a
connector can resolve identity across runs, and two vendors naming the same
person agree without either of them holding a shared secret.

**Irreversible without the key.** Not a hash of the value — a *keyed* MAC. A
plain SHA-256 of an email address is reversible in practice: the space of
corporate addresses is small and enumerable, so `sha256("dana.moreau@acme.com")`
is a lookup away for anyone with the employee list. That mistake is common
enough to be worth naming, because it produces something that looks tokenized
and is not.

**Shreddable.** Destroy a tenant's key and every token derived from it becomes
permanently unlinkable, everywhere, at once — in Parquet files, in Kafka
segments past their compaction window, in a Spark shuffle spill, in a backup
taken last March. That is the only erasure guarantee that actually holds across
a data lake, because `DELETE` reaches rows in a database and nothing else.

The cost is stated plainly: a tenant whose key is destroyed loses the ability
to resolve *any* of their historical data, not just one person's. Shredding is
tenant offboarding, not per-person erasure — for one person, see
[`erasure`](erasure.py), which deletes the dimension row and relies on the
event contract carrying tokens rather than names.
"""

from __future__ import annotations

import hmac
import re
from dataclasses import dataclass
from hashlib import sha256

# 128 bits, hex-encoded. Full SHA-256 is 64 characters of which nothing reads
# the back half; 32 is short enough to eyeball in a log line and long enough
# that a collision across a population of employees is not a consideration.
TOKEN_CHARS = 32

# Fields a vendor payload may carry that identify a person. Archived payloads
# are kept for replay after a connector bug, and a replay buffer should not be
# a second copy of the directory.
PII_FIELDS = frozenset(
    {
        "email",
        "email_address",
        "user_email",
        "actor_email",
        "primary_email",
        "mail",
        "username",
        "user_name",
        "login",
        "display_name",
        "full_name",
        "name",
        "first_name",
        "last_name",
        "given_name",
        "family_name",
        "phone",
        "phone_number",
        "mobile",
        "ip",
        "ip_address",
        "client_ip",
    }
)

_WHITESPACE = re.compile(r"\s+")


def normalise(value: str) -> str:
    """Fold the variations that mean the same person.

    Case and surrounding whitespace only. Deliberately *not* the clever
    normalisations — stripping dots from Gmail local parts, dropping `+tag`
    suffixes — because those are provider-specific rules, and applying one to
    a provider that does not follow it silently merges two different people
    into one token. Under-normalising splits one person into two tokens, which
    a directory lookup catches; over-normalising attributes one employee's
    phishing click to a colleague, which nothing catches.
    """
    return _WHITESPACE.sub(" ", value).strip().lower()


@dataclass(frozen=True)
class Tokenizer:
    """Keyed tokenization for one tenant.

    The key is per tenant so that shredding one tenant cannot affect another,
    and so a token is meaningless if it leaks across a tenant boundary.
    """

    key: bytes
    length: int = TOKEN_CHARS

    def __post_init__(self) -> None:
        if len(self.key) < 16:
            # A short key makes the MAC brute-forceable and turns every
            # property above into a claim rather than a fact.
            raise ValueError("tokenization key must be at least 16 bytes")

    @classmethod
    def from_secret(cls, secret: str, tenant_id: str) -> Tokenizer:
        """Derive a tenant key from one configured secret.

        One secret in the environment, one key per tenant, so adding a tenant
        needs no new configuration and no tenant's tokens can be computed from
        another's.
        """
        if not secret:
            raise ValueError("no tokenization secret configured")
        return cls(key=sha256(f"{tenant_id}\x00{secret}".encode()).digest())

    def token(self, value: str, kind: str = "email") -> str:
        """Tokenize one value.

        `kind` is mixed into the MAC so the same string used as two different
        kinds of identifier produces two different tokens. Without it, a
        display name that happens to equal a username would tokenize
        identically and join across fields that have nothing to do with each
        other.
        """
        message = f"{kind}\x00{normalise(value)}".encode()
        return hmac.new(self.key, message, sha256).hexdigest()[: self.length]

    def matches(self, token: str, value: str, kind: str = "email") -> bool:
        """Constant-time comparison, so a lookup cannot be timed."""
        return hmac.compare_digest(token, self.token(value, kind))

    def redact(self, payload: object, fields: frozenset[str] = PII_FIELDS) -> object:
        """Replace identifying fields in a nested payload with tokens.

        Recursive over dicts and lists, because vendor payloads nest and the
        address is as often at `actor.profile.email` as at the top level. A
        redactor that only looked one level down would report success while
        leaving the archive full of addresses.

        Values are replaced rather than removed. A missing key changes the
        shape of the payload, and a replay that has to reason about which
        fields a record *used* to have is a replay nobody trusts.
        """
        if isinstance(payload, dict):
            return {
                key: (
                    f"tok_{self.token(value, kind=key)}"
                    if key.lower() in fields and isinstance(value, str) and value
                    else self.redact(value, fields)
                )
                for key, value in payload.items()
            }
        if isinstance(payload, list):
            return [self.redact(item, fields) for item in payload]
        return payload
