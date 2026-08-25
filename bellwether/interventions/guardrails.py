"""What generated copy has to survive before a person is allowed to read it.

Pure functions over strings. No model client, no employee record, no I/O — the
caller assembles the forbidden terms, which keeps the rules testable in
isolation and means the same validator runs over template output as over model
output.

That last part is the point. A fallback that could not pass its own guardrails
is not a fallback, it is a second unvalidated path with a reassuring name, and
`tests/test_guardrails.py` asserts every static template clears every rule.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, field

# --- the rules --------------------------------------------------------------
#
# Grouped by the kind of harm each prevents, because they are not
# interchangeable: a length violation is a quality problem, and an overclaim is
# a false statement to a worried person.

# Blame. The security literature is consistent that shaming people trains them
# to hide incidents, which costs far more than the original mistake.
_ACCUSATORY = [
    r"\byou (failed|fell for|screwed up|should have known)\b",
    r"\byou (are|were)\s+(being\s+)?(negligent|careless|reckless|irresponsible)\b",
    r"\byour (mistake|error|fault|failure)\b",
    r"\bnegligen(t|ce)\b",
    r"\bcareless(ly|ness)?\b",
    r"\bviolat(ion|ions|ed|ing)\b",
]

# Threats. Bellwether has no authority to promise any of these, and a nudge that
# implies consequences is a nudge people route around rather than act on.
_THREATENING = [
    r"\bdisciplinar(y|ies)\b",
    r"\btermina(te|ted|tion)\b",
    r"\bsuspend(ed|ing)?\b",
    r"\breported to (hr|your manager|management|security)\b",
    r"\b(hr|human resources) (has|have) been\b",
    r"\bfurther (action|consequences)\b",
]

# Overclaiming. This is the rule most specific to this product. Bellwether
# observes *behaviour* — a click, a submission on a simulated page, a credential
# appearing in a third-party dump. None of that establishes that an account is
# in someone else's hands. Telling an employee they have been breached when they
# have not is a false statement that causes real alarm, and it is exactly the
# leap a fluent language model makes when asked to convey urgency.
_OVERCLAIMING = [
    r"\byour account (has been|was|is) (compromised|breached|hacked|stolen)\b",
    r"\b(we (have )?)?detected a breach\b",
    r"\bhackers? (have|has|now|are)\b",
    r"\byou (have )?(leaked|exposed) (customer|company|client) data\b",
    r"\byour (password|credentials) (has|have) been stolen\b",
    r"\bthis is (an )?(active|confirmed) (attack|compromise|incident)\b",
]

# Internal identifiers. A signal name is a database key, not a sentence, and
# `phish_credentials_submitted` appearing in a message to a human means the copy
# path leaked its own plumbing.
_INTERNAL_TOKEN = r"\b[a-z]+(?:_[a-z]+){2,}\b"

# At least one of these has to appear, so the message says what to *do*. A nudge
# that conveys concern and no action is the most common way security comms waste
# everyone's attention.
#
# Deliberately a short, conservative list rather than an attempt at every
# imperative in English. The costs are wildly asymmetric: rejecting a perfectly
# good draft sends a static template instead, which nobody notices, while
# accepting a vague one puts a message in front of a worried person that does
# not tell them what to do.
_ACTIONS = (
    "reset",
    "rotate",
    "change",
    "update",
    "review",
    "revoke",
    "remove",
    "delete",
    "disable",
    "turn off",
    "enable",
    "turn on",
    "complete",
    "confirm",
    "report",
    "contact",
    "check",
    "approve",
)

# Matched at a word boundary and against the *body* only. Two mistakes are easy
# here and both were made first: scanning subject and body together lets a
# subject like "A quick security check-in" discharge the body's obligation to
# say what to do, and an unanchored substring lets "unchanged" count as
# "change".
_ACTION_RE = re.compile(r"\b(" + "|".join(_ACTIONS) + r")", re.IGNORECASE)

_URL = re.compile(r"https?://([^/\s)\]]+)", re.IGNORECASE)
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")


@dataclass(frozen=True, slots=True)
class Violation:
    """One broken rule. `rule` groups them for counting; `detail` is for logs."""

    rule: str
    detail: str

    def __str__(self) -> str:
        return f"{self.rule}: {self.detail}"


@dataclass(frozen=True)
class Guardrails:
    """The validator.

    Bounds are deliberately tight. A nudge people actually read is two or three
    sentences; anything longer is a document, and a document is something an
    employee saves for later and never opens.
    """

    max_subject_chars: int = 80
    min_body_words: int = 12
    max_body_words: int = 70
    allowed_link_host: str = "security.acme.internal"
    require_action: bool = True

    _groups: tuple[tuple[str, list[str]], ...] = field(
        default=(
            ("accusatory", _ACCUSATORY),
            ("threatening", _THREATENING),
            ("overclaiming", _OVERCLAIMING),
        ),
        repr=False,
    )

    def check(
        self,
        subject: str,
        body: str,
        forbidden: Iterable[str] = (),
        first_name: str | None = None,
    ) -> list[Violation]:
        """Every rule this copy breaks. Empty means it may be sent.

        Args:
            subject: The message subject.
            body: The message body.
            forbidden: Terms that must not appear — surname, email address,
                employee token. Assembled by the caller so this module never
                touches an employee record.
            first_name: Permitted in the copy; excluded from the PII scan so a
                surname check does not trip on a first name that happens to be
                a substring.

        Returns:
            Violations, in rule order. A non-empty list means fall back.
        """
        violations: list[Violation] = []
        combined = f"{subject}\n{body}"
        lowered = combined.lower()

        if not subject.strip():
            violations.append(Violation("length", "empty subject"))
        elif len(subject) > self.max_subject_chars:
            violations.append(
                Violation("length", f"subject {len(subject)} > {self.max_subject_chars} chars")
            )

        words = len(body.split())
        if words < self.min_body_words:
            violations.append(Violation("length", f"body {words} words < {self.min_body_words}"))
        elif words > self.max_body_words:
            violations.append(Violation("length", f"body {words} words > {self.max_body_words}"))

        for rule, patterns in self._groups:
            for pattern in patterns:
                match = re.search(pattern, lowered)
                if match:
                    violations.append(Violation(rule, f"{match.group(0)!r}"))

        for address in _EMAIL.findall(combined):
            violations.append(Violation("pii", f"email address {address!r}"))

        # Word-anchored, not substring. A bare `in` check means any employee
        # whose surname happens to be a fragment of an English word can never
        # receive normal copy: "Lin" is inside "public link", so two employees
        # named Lin had every template rejected and fell through to the
        # last-resort text. It fails safe, which is why it went unnoticed until
        # the fallback counter was there to say how often it was firing.
        for term in forbidden:
            term = term.strip()
            if not term or (first_name and term.lower() == first_name.lower()):
                continue
            if re.search(rf"\b{re.escape(term.lower())}\b", lowered):
                violations.append(Violation("pii", f"forbidden term {term!r}"))

        for host in _URL.findall(combined):
            if host.lower() != self.allowed_link_host.lower():
                violations.append(Violation("link", f"host {host!r} is not the security portal"))

        token = re.search(_INTERNAL_TOKEN, combined)
        if token:
            violations.append(Violation("internal", f"identifier {token.group(0)!r}"))

        if self.require_action and not _ACTION_RE.search(body):
            violations.append(Violation("no_action", "body names no concrete action"))

        return violations
