"""Writing the message.

Two writers behind one protocol, and a desk that prefers the model but only
ships what passes [`guardrails`](guardrails.py). Generation failure, a timeout,
a missing API key and a badly-worded draft all land in the same place: the
static template. The system degrades to boring, never to silent, and never to
unvalidated.

The brief a writer receives is deliberately impoverished — a first name, a band,
and the catalog's own plain-English descriptions of what happened. It carries no
email, no surname, no employee token, and no signal identifiers, so the prompt
cannot leak what the copy is forbidden to contain.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Protocol

from bellwether.events.schema import RiskCategory, SignalType
from bellwether.interventions.guardrails import Guardrails, Violation
from bellwether.interventions.types import CopySource, InterventionType
from bellwether.scoring import RiskBand
from bellwether.scoring.catalog import spec_for

MODEL = "claude-sonnet-5"

# Human labels. The enum values (`phishing_susceptibility`) are database keys,
# and a guardrail rejects them on sight — putting one in front of an employee
# means the copy path leaked its own plumbing.
CATEGORY_LABEL: dict[RiskCategory, str] = {
    RiskCategory.PHISHING_SUSCEPTIBILITY: "phishing susceptibility",
    RiskCategory.CREDENTIAL_HYGIENE: "credential hygiene",
    RiskCategory.DATA_HANDLING: "data handling",
    RiskCategory.ACCESS_HYGIENE: "access hygiene",
    RiskCategory.SECURITY_ENGAGEMENT: "security engagement",
}


class CopyUnavailableError(RuntimeError):
    """The writer could not produce a draft. Always recoverable by falling back."""


@dataclass(frozen=True)
class CopyBrief:
    """Everything a writer is allowed to know about the person.

    Note what is absent. A writer never sees the email address, the surname, the
    employee token, the raw signal names or the numeric score. It cannot leak
    what it was never given, which is a cheaper guarantee than checking the
    output for every field we hold.
    """

    first_name: str
    type: InterventionType
    band: RiskBand
    dominant_category: RiskCategory | None = None
    trigger_signal: SignalType | None = None
    behaviours: tuple[str, ...] = ()
    """Plain-English descriptions, taken from the signal catalog."""

    @property
    def category_label(self) -> str:
        if self.dominant_category is None:
            return "account security"
        return CATEGORY_LABEL.get(self.dominant_category, "account security")


@dataclass(frozen=True)
class Draft:
    subject: str
    body: str
    source: CopySource


def describe(signal: SignalType) -> str:
    """The catalog's first sentence about a signal.

    Only the first: the rest of a `SignalSpec` description is written for
    whoever is reading the catalog and explains weighting decisions, which is
    not something to feed a copywriter, human or otherwise.
    """
    return spec_for(signal).description.split(".")[0].strip()


class Copywriter(Protocol):
    def write(self, brief: CopyBrief) -> Draft: ...


# --- the static path --------------------------------------------------------

_BY_CATEGORY: dict[RiskCategory, tuple[str, str]] = {
    RiskCategory.PHISHING_SUSCEPTIBILITY: (
        "A quick check on a recent email",
        "Hi {name}, one of the messages you interacted with recently was a phishing "
        "simulation, and nothing has gone wrong. When a message asks you to sign in, "
        "open the app yourself instead of following the link, and report anything that "
        "feels off using the Report Phishing button. It takes ten seconds.",
    ),
    RiskCategory.CREDENTIAL_HYGIENE: (
        "Worth changing your password this week",
        "Hi {name}, some recent sign-in activity on your account is worth a second "
        "look. Please reset your password, then confirm the devices listed in your "
        "account settings are ones you recognise. If something there is unfamiliar, "
        "contact the security team and we will take it from there.",
    ),
    RiskCategory.DATA_HANDLING: (
        "A note on how some files were shared",
        "Hi {name}, a few documents you shared recently went out more broadly than "
        "they probably needed to. Please review your recent shares and remove any "
        "public links you no longer need. When a file has to go outside the company, "
        "sharing it with a named person keeps it traceable.",
    ),
    RiskCategory.ACCESS_HYGIENE: (
        "Some access worth reviewing",
        "Hi {name}, a few apps and permissions connected to your account look broader "
        "or older than they need to be. Please review the third-party apps in your "
        "account settings and revoke anything you no longer use. Trimming unused "
        "access is the quickest way to shrink what an attacker could reach.",
    ),
    RiskCategory.SECURITY_ENGAGEMENT: (
        "Your security training is still open",
        "Hi {name}, there is a short security module assigned to you that has not been "
        "opened yet. Please complete it when you have ten minutes free. It really is "
        "brief, and finishing it clears the reminder so you stop hearing from us "
        "about it.",
    ),
}

_DEFAULT = (
    "A quick security check-in",
    "Hi {name}, your recent account activity is worth a short look. Please review your "
    "account settings, confirm the devices and apps listed are ones you recognise, and "
    "reset your password if anything looks unfamiliar. Contact the security team if you "
    "would like a hand with any of it.",
)

# Overrides for the behaviours that trigger on their own merits. These are more
# direct than the category copy because the useful window is short.
_BY_TRIGGER: dict[SignalType, tuple[str, str]] = {
    SignalType.PHISH_CREDENTIALS_SUBMITTED: (
        "Please reset your password now",
        "Hi {name}, credentials were entered on a page that was part of a phishing "
        "simulation, so nothing has been lost. It is still a good moment to act: "
        "please reset your password now, and if you use that password anywhere else, "
        "change it there too. Real attackers run the same page.",
    ),
    SignalType.CREDENTIAL_IN_BREACH_DUMP: (
        "A password of yours turned up in a public breach",
        "Hi {name}, a password linked to your work address has appeared in a "
        "third-party data breach somewhere else on the internet. Please reset your "
        "work password, and change it on any personal site where you reused it. Doing "
        "it now closes the window before anyone tries the old one.",
    ),
    SignalType.EMAIL_FORWARDING_RULE_CREATED: (
        "Please confirm a mail forwarding rule",
        "Hi {name}, a rule was added to your mailbox that forwards your mail to an "
        "outside address. If you set it up, there is nothing to do. If you did not, "
        "please remove it and contact the security team, because a quiet forwarding "
        "rule is a common way access is kept after a password is guessed.",
    ),
    SignalType.MFA_PUSH_FLOOD: (
        "Do not approve unexpected sign-in prompts",
        "Hi {name}, your account received a burst of sign-in approval prompts in a few "
        "minutes. That pattern usually means somebody else has a password of yours and "
        "is hoping you tap approve out of habit. Please decline anything you did not "
        "start, reset your password, and contact the security team.",
    ),
}

# The escalated rungs. Training adds an assignment; manager notification is
# addressed to a different person entirely, so it gets its own copy rather than
# a suffix on the employee's.
_TRAINING_SUFFIX = " A short module on this is now in your training portal."

_MANAGER = (
    "Security follow-up for {name}",
    "{name} on your team has had repeated security reminders from us without the "
    "underlying activity changing, most recently around {category}. Nothing here "
    "suggests bad intent. A short conversation usually lands better than another "
    "automated message, so please check in with them, and contact the security team "
    "if you would like the detail.",
)


class TemplateCopywriter:
    """The floor. Always available, never clever, always passes the guardrails.

    Keyed on the trigger signal first and the dominant category second: when a
    specific behaviour prompted the message, saying which one is more useful
    than naming the category it rolls up into.
    """

    def write(self, brief: CopyBrief) -> Draft:
        if brief.type is InterventionType.MANAGER_NOTIFICATION:
            subject, body = _MANAGER
            return Draft(
                subject=subject.format(name=brief.first_name),
                body=body.format(name=brief.first_name, category=brief.category_label),
                source=CopySource.TEMPLATE,
            )

        template: tuple[str, str] | None = None
        if brief.trigger_signal is not None:
            template = _BY_TRIGGER.get(brief.trigger_signal)
        if template is None and brief.dominant_category is not None:
            template = _BY_CATEGORY.get(brief.dominant_category)

        subject, body = template or _DEFAULT
        text = body.format(name=brief.first_name)
        if brief.type is InterventionType.TRAINING:
            text += _TRAINING_SUFFIX
        return Draft(subject=subject, body=text, source=CopySource.TEMPLATE)


# --- the model path ---------------------------------------------------------

_SYSTEM = """\
You write short internal security messages for a company's employees.

Rules, all of them hard:
- Two or three sentences. Between 20 and 60 words in the body.
- Never blame, shame, or imply consequences. No mention of HR, discipline, or
  reporting the person to anyone.
- Never assert that an account has been compromised, breached, or hacked. You
  are told what was *observed*, which is not the same thing. Do not upgrade it.
- Name exactly one concrete thing the person should do.
- Use the first name given and no other personal detail.
- No links, no internal identifiers, no scores or numbers.
- Warm and matter-of-fact. Assume a competent adult having a busy week.

Reply with only a JSON object: {"subject": "...", "body": "..."}\
"""


class ClaudeCopywriter:
    """Generated copy, with a hard timeout and no retries.

    No retries on purpose. This sits in a consumer's message loop, and the
    fallback is a template that is already known to be acceptable — spending
    three attempts and thirty seconds to maybe get slightly better wording is a
    bad trade against holding up the partition behind it.
    """

    def __init__(
        self,
        client: Any | None = None,
        model: str = MODEL,
        max_tokens: int = 400,
        timeout: float = 8.0,
    ) -> None:
        if client is None:
            import anthropic

            client = anthropic.Anthropic(timeout=timeout, max_retries=0)
        self._client = client
        self.model = model
        self.max_tokens = max_tokens

    def prompt(self, brief: CopyBrief) -> str:
        """The user turn. Exposed so a test can assert what leaves the process."""
        lines = [
            f"First name: {brief.first_name}",
            f"Message type: {brief.type.value.replace('_', ' ')}",
            f"Overall concern: {brief.category_label}",
        ]
        if brief.behaviours:
            lines.append("What was observed:")
            lines.extend(f"- {b}" for b in brief.behaviours)
        if brief.type is InterventionType.MANAGER_NOTIFICATION:
            lines.append("Write to this person's manager, not to them. Be brief and non-punitive.")
        return "\n".join(lines)

    def write(self, brief: CopyBrief) -> Draft:
        try:
            response = self._client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=_SYSTEM,
                messages=[{"role": "user", "content": self.prompt(brief)}],
            )
            text = "".join(
                getattr(block, "text", "")
                for block in response.content
                if getattr(block, "type", "") == "text"
            )
        except Exception as err:
            raise CopyUnavailableError(str(err)[:160]) from err

        return Draft(**_parse(text), source=CopySource.MODEL)


def _parse(text: str) -> dict[str, str]:
    """Pull the JSON object out of a model response.

    Tolerant of a fenced block or a sentence of preamble, because a stricter
    parser would send a perfectly good draft to the fallback over a stray
    backtick.
    """
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end <= start:
        raise CopyUnavailableError("response contained no JSON object")
    try:
        payload = json.loads(text[start : end + 1])
        return {"subject": str(payload["subject"]), "body": str(payload["body"])}
    except (json.JSONDecodeError, KeyError, TypeError) as err:
        raise CopyUnavailableError(f"unparseable response: {err}") from err


# --- the desk ---------------------------------------------------------------


@dataclass
class CopyStats:
    model_drafts: int = 0
    template_drafts: int = 0
    model_errors: int = 0
    guardrail_rejections: int = 0
    last_resort: int = 0
    rejected_rules: dict[str, int] = field(default_factory=dict)


_LAST_RESORT = Draft(
    subject="A quick security check-in",
    body=(
        "Please review your account settings, confirm the devices and apps listed are "
        "ones you recognise, and contact the security team if anything looks "
        "unfamiliar to you."
    ),
    source=CopySource.TEMPLATE,
)


class Copydesk:
    """Chooses the words that actually get sent.

    Order: model, validate, fall back. The interesting property is that the
    guardrails run over the template output too. A fallback that could not pass
    its own validator would be a second unchecked path wearing a safe name, so
    if it ever fails, `_LAST_RESORT` — which has no substitutions and cannot
    vary — goes out instead, and the event is counted rather than swallowed.
    """

    def __init__(
        self,
        templates: Copywriter | None = None,
        model: Copywriter | None = None,
        guardrails: Guardrails | None = None,
    ) -> None:
        self.templates = templates or TemplateCopywriter()
        self.model = model
        self.guardrails = guardrails or Guardrails()
        self.stats = CopyStats()

    def compose(self, brief: CopyBrief, forbidden: tuple[str, ...] = ()) -> tuple[Draft, int]:
        """Returns the draft to send and how many drafts the guardrails rejected."""
        rejections = 0

        if self.model is not None:
            try:
                draft = self.model.write(brief)
            except CopyUnavailableError:
                self.stats.model_errors += 1
            else:
                violations = self._check(draft, brief, forbidden)
                if not violations:
                    self.stats.model_drafts += 1
                    return draft, rejections
                rejections += 1
                self.stats.guardrail_rejections += 1
                self._count(violations)

        draft = self.templates.write(brief)
        if self._check(draft, brief, forbidden):
            self.stats.last_resort += 1
            return _LAST_RESORT, rejections

        self.stats.template_drafts += 1
        return draft, rejections

    def _check(self, draft: Draft, brief: CopyBrief, forbidden: tuple[str, ...]) -> list[Violation]:
        return self.guardrails.check(
            draft.subject, draft.body, forbidden=forbidden, first_name=brief.first_name
        )

    def _count(self, violations: list[Violation]) -> None:
        for violation in violations:
            self.stats.rejected_rules[violation.rule] = (
                self.stats.rejected_rules.get(violation.rule, 0) + 1
            )
