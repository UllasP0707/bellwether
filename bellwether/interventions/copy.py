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

from bellwether.config import settings
from bellwether.events.schema import RiskCategory, SignalType
from bellwether.interventions.guardrails import Guardrails, Violation
from bellwether.interventions.types import CopySource, InterventionType
from bellwether.obs import metrics
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
    """The writer could not produce a draft. Always recoverable by falling back.

    `kind` exists because the first live run made the case for it. 163 of 209
    drafts fell back to templates and the summary called all of them
    "generation failures", which is true and useless: the provider had hit a
    rate limit, and a reader of that line could not distinguish an exhausted
    quota from a model writing unacceptable copy. One is fixed by a billing
    page and the other by a prompt, so collapsing them hides the only thing an
    operator needs to know.
    """

    def __init__(self, detail: str, kind: str = "error") -> None:
        super().__init__(detail)
        self.kind = kind


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

    @property
    def shape(self) -> tuple[object, ...]:
        """Everything a model writer is told, with the person removed.

        Two briefs sharing a shape produce the same prompt, so a draft written
        for one is a correct draft for the other. That is what makes generated
        copy cacheable — and the reason it is true is that the brief was made
        deliberately impoverished for privacy reasons, long before anybody
        cared how slow generation is.
        """
        return (self.type, self.dominant_category, self.trigger_signal, self.behaviours)


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
- Address the reader as {name} — write that literally, braces included. It is a
  placeholder substituted later. Never invent or guess a name.
- Never blame, shame, or imply consequences. No mention of HR, discipline, or
  reporting the person to anyone.
- Never assert that an account has been compromised, breached, or hacked. You
  are told what was *observed*, which is not the same thing. Do not upgrade it.
- Name exactly one concrete thing the person should do.
- No links, no internal identifiers, no scores or numbers.
- Warm and matter-of-fact. Assume a competent adult having a busy week.

Reply with only a JSON object: {"subject": "...", "body": "..."}\
"""

NAME_PLACEHOLDER = "{name}"


def _prompt(brief: CopyBrief) -> str:
    """The user turn, shared by every model writer.

    One function rather than one per provider, so a test asserting what leaves
    the process covers all of them.

    **This carries no personal data at all** — not even the first name, which
    an earlier version sent. The model writes to `{name}` and the desk
    substitutes afterwards, which is a stronger claim than "only a first name"
    and costs nothing: a first name was never what made the copy useful.

    It also makes the prompt *low-cardinality*, which is what
    [`CachedCopywriter`](#) exploits. The privacy property and the performance
    property are the same property.
    """
    lines = [
        f"Message type: {brief.type.value.replace('_', ' ')}",
        f"Overall concern: {brief.category_label}",
    ]
    if brief.behaviours:
        lines.append("What was observed:")
        lines.extend(f"- {b}" for b in brief.behaviours)
    if brief.type is InterventionType.MANAGER_NOTIFICATION:
        lines.append("Write to this person's manager, not to them. Be brief and non-punitive.")
    return "\n".join(lines)


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
        return _prompt(brief)

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
            raise CopyUnavailableError(str(err)[:160], kind=_classify(err)) from err

        return Draft(**_parse(text), source=CopySource.MODEL)


class ChatCompletionsCopywriter:
    """Any OpenAI-compatible `/chat/completions` endpoint.

    Named for the protocol rather than a vendor because that is what it is
    coupled to: OpenRouter, vLLM, Ollama and an in-house gateway all speak it,
    and which model writes the copy is a deployment decision. Written against
    `httpx` — already a dependency for the connectors — rather than pulling in
    a second SDK to send one POST.

    **Reasoning models need headroom.** The token budget covers the reasoning
    the model does before it emits anything, so a ceiling sized for a
    three-sentence email produces a truncated response and no draft at all. The
    budget here is deliberately several times the length of the answer, and a
    response that still comes back empty is treated as a failure rather than as
    an empty email.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        model: str,
        client: Any | None = None,
        max_tokens: int = 1400,
        timeout: float = 25.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.max_tokens = max_tokens
        if client is None:
            import httpx

            client = httpx.Client(
                timeout=timeout,
                headers={"Authorization": f"Bearer {api_key}"},
            )
        self._client = client

    def prompt(self, brief: CopyBrief) -> str:
        """The user turn. Identical to the Anthropic path's, deliberately.

        Both writers see the same impoverished brief, so a provider swap cannot
        change what leaves the process — only who reads it.
        """
        return _prompt(brief)

    def write(self, brief: CopyBrief) -> Draft:
        try:
            response = self._client.post(
                f"{self.base_url}/chat/completions",
                json={
                    "model": self.model,
                    "max_tokens": self.max_tokens,
                    "messages": [
                        {"role": "system", "content": _SYSTEM},
                        {"role": "user", "content": self.prompt(brief)},
                    ],
                },
            )
            status = getattr(response, "status_code", 200)
            response.raise_for_status()
            payload = response.json()
            choice = (payload.get("choices") or [{}])[0]
            text = (choice.get("message") or {}).get("content") or ""
        except CopyUnavailableError:
            raise
        except Exception as err:
            raise CopyUnavailableError(
                f"{type(err).__name__}: {str(err)[:140]}", kind=_classify(err)
            ) from err

        if not text.strip():
            # A reasoning model that spent the whole budget thinking, or a
            # provider returning a refusal with no body. Either way there is
            # nothing to validate, and an empty subject line is worse than a
            # template.
            raise CopyUnavailableError(
                f"empty content (finish: {choice.get('finish_reason')}, http {status})",
                kind="empty",
            )

        return Draft(**_parse(text), source=CopySource.MODEL)


def _classify(err: Exception) -> str:
    """Group a failure by what an operator would do about it.

    Rate limiting is separated from every other transport failure because it
    is the one with a different remedy and, on a free tier, by far the most
    common: a whole run can fall back to templates without a single thing being
    wrong with the model, the prompt or the guardrails.
    """
    text = str(err).lower()
    if isinstance(err, TimeoutError) or "timeout" in text or "timed out" in text:
        return "timeout"
    if "429" in text or "rate limit" in text or "too many requests" in text:
        return "rate_limited"
    if "connect" in text or "resolve" in text:
        return "unreachable"
    return "http"


class CachedCopywriter:
    """Bounds model calls by brief shape rather than by message volume.

    Generation against a hosted reasoning model measured 8 to 40 seconds a call.
    That is not a tuning problem, it is an architectural one: the intervention
    stage is a serial Kafka consumer, so a synchronous 27-second median puts
    the whole partition behind one email. Raising the timeout makes it worse
    and lowering it makes the model path decorative, because every draft
    becomes a template.

    The way out is that the brief has *low cardinality*. It holds a rung, a
    category, a trigger and up to three catalog descriptions — no name, no
    score, no identifier — so across a whole population there are a few dozen
    distinct shapes, not one per employee. Generate a handful of variants for
    each and reuse them, and total model calls become a function of the signal
    catalog rather than of traffic.

    Variants rather than a single cached draft, so a department does not all
    receive a byte-identical email; rotation rather than random choice, so a
    test can predict what comes out.

    **Drafts are validated before they are cached.** A rejected draft that got
    stored would be re-rejected on every future use of that shape, permanently
    converting one bad generation into a dead cache slot — the guardrails run
    again at send time regardless, but keeping known-bad copy is pointless.
    """

    def __init__(
        self,
        writer: Copywriter,
        variants: int = 3,
        guardrails: Guardrails | None = None,
    ) -> None:
        self.writer = writer
        self.variants = max(1, variants)
        self.guardrails = guardrails or Guardrails()
        self._cache: dict[tuple[object, ...], list[Draft]] = {}
        self._turn: dict[tuple[object, ...], int] = {}
        self.generated = 0
        self.reused = 0

    def write(self, brief: CopyBrief) -> Draft:
        key = brief.shape
        held = self._cache.setdefault(key, [])

        if len(held) < self.variants:
            metrics.copy_cache.labels(result="miss").inc()
            with metrics.timed(metrics.copy_generation_seconds):
                draft = self.writer.write(brief)
            # Validated with no forbidden terms: the prompt carries no personal
            # data, so a surname can only appear by coincidence, and the
            # per-employee check at send time is what catches that.
            if self.guardrails.check(draft.subject, draft.body):
                raise CopyUnavailableError("draft failed validation; not cached", kind="rejected")
            held.append(draft)
            self.generated += 1
            return draft

        index = self._turn.get(key, 0)
        self._turn[key] = index + 1
        self.reused += 1
        metrics.copy_cache.labels(result="hit").inc()
        return held[index % len(held)]

    @property
    def shapes(self) -> int:
        return len(self._cache)


def render(draft: Draft, first_name: str) -> Draft:
    """Substitute the person back in.

    A draft is a template until this runs. Applied to template and model output
    alike — the static templates have already interpolated their own name, so
    this is a no-op for them, and having one render step means there is one
    answer to "what exactly did this person receive".
    """
    if NAME_PLACEHOLDER not in draft.subject and NAME_PLACEHOLDER not in draft.body:
        return draft
    return Draft(
        subject=draft.subject.replace(NAME_PLACEHOLDER, first_name),
        body=draft.body.replace(NAME_PLACEHOLDER, first_name),
        source=draft.source,
    )


def _parse(text: str) -> dict[str, str]:
    """Pull the JSON object out of a model response.

    Tolerant of a fenced block or a sentence of preamble, because a stricter
    parser would send a perfectly good draft to the fallback over a stray
    backtick.
    """
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end <= start:
        raise CopyUnavailableError("response contained no JSON object", kind="unparseable")
    try:
        payload = json.loads(text[start : end + 1])
        return {"subject": str(payload["subject"]), "body": str(payload["body"])}
    except (json.JSONDecodeError, KeyError, TypeError) as err:
        raise CopyUnavailableError(f"unparseable response: {err}", kind="unparseable") from err


# --- the desk ---------------------------------------------------------------


@dataclass
class CopyStats:
    model_drafts: int = 0
    template_drafts: int = 0
    model_errors: int = 0
    guardrail_rejections: int = 0
    last_resort: int = 0
    rejected_rules: dict[str, int] = field(default_factory=dict)
    error_kinds: dict[str, int] = field(default_factory=dict)
    """Why the model path failed, grouped by what an operator would do about it."""


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
                # Rendered before validation, never after. The guardrails have
                # to see the exact bytes the employee will, or the name is an
                # unchecked substitution into validated copy.
                draft = render(self.model.write(brief), brief.first_name)
            except CopyUnavailableError as err:
                self.stats.model_errors += 1
                self.stats.error_kinds[err.kind] = self.stats.error_kinds.get(err.kind, 0) + 1
                metrics.copy_failures.labels(kind=err.kind).inc()
            else:
                violations = self._check(draft, brief, forbidden)
                if not violations:
                    self.stats.model_drafts += 1
                    metrics.copy_drafts.labels(source=draft.source.value).inc()
                    return draft, rejections
                rejections += 1
                self.stats.guardrail_rejections += 1
                self._count(violations)

        draft = render(self.templates.write(brief), brief.first_name)
        if self._check(draft, brief, forbidden):
            self.stats.last_resort += 1
            metrics.copy_drafts.labels(source="last_resort").inc()
            return _LAST_RESORT, rejections

        self.stats.template_drafts += 1
        metrics.copy_drafts.labels(source=draft.source.value).inc()
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
            metrics.guardrail_rejections.labels(rule=violation.rule).inc()


def copywriter(provider: str | None = None, variants: int = 3) -> tuple[Copywriter | None, str]:
    """Resolve the configured model writer, or `None` for templates only.

    Returns the writer and a one-line description of what was chosen, because
    silently running on templates when somebody expected generated copy — or
    the reverse — is the kind of thing that should be printed, not inferred
    from the output.

    `auto` means "use whichever credential is present". Having no credential is
    a configuration, not a failure: the template path is validated by the same
    guardrails and is a supported way to run this.
    """
    import os

    config = settings()
    choice = provider or config.copy_provider

    if choice == "auto":
        if config.copy_api_key:
            choice = "chat"
        elif os.environ.get("ANTHROPIC_API_KEY"):
            choice = "anthropic"
        else:
            choice = "template"

    writer: Copywriter
    match choice:
        case "template":
            return None, "static templates"
        case "anthropic":
            if not os.environ.get("ANTHROPIC_API_KEY"):
                raise ValueError("copy provider 'anthropic' needs ANTHROPIC_API_KEY")
            model = config.copy_model or MODEL
            writer = ClaudeCopywriter(model=model, timeout=config.copy_timeout_seconds)
            label = model
        case "chat":
            if not config.copy_api_key:
                raise ValueError("copy provider 'chat' needs BELLWETHER_COPY_API_KEY")
            if not config.copy_model:
                raise ValueError("copy provider 'chat' needs BELLWETHER_COPY_MODEL")
            writer = ChatCompletionsCopywriter(
                base_url=config.copy_base_url,
                api_key=config.copy_api_key,
                model=config.copy_model,
                timeout=config.copy_timeout_seconds,
            )
            host = config.copy_base_url.split("//")[-1].split("/")[0]
            label = f"{config.copy_model} via {host}"
        case _:
            raise ValueError(f"unknown copy provider {choice!r}")

    return CachedCopywriter(writer, variants=variants), f"{label} ({variants} variants/shape)"
