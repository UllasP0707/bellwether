"""Guardrail tests.

The parametrised sweep at the bottom is the one that matters. Everything above
it checks that a rule catches what it is meant to catch; the sweep checks that
the fallback path can actually satisfy every rule, for every combination of
intervention type and cause the system can produce.

Without it, "generation failure falls back to a static template" would be a
claim about a path nobody had validated — a second unchecked route to a real
person, wearing a reassuring name.
"""

from __future__ import annotations

import pytest

from bellwether.events.schema import RiskCategory, SignalType
from bellwether.interventions.copy import (
    _BY_CATEGORY,
    _BY_TRIGGER,
    _LAST_RESORT,
    CopyBrief,
    TemplateCopywriter,
)
from bellwether.interventions.guardrails import Guardrails
from bellwether.interventions.types import InterventionType
from bellwether.scoring import RiskBand

GUARDRAILS = Guardrails()

# A body that breaks nothing, used as the base for single-rule tests.
CLEAN_SUBJECT = "A quick security check-in"
CLEAN_BODY = (
    "Hi Dana, some recent activity on your account is worth a short look. Please "
    "reset your password and confirm the devices listed in your settings are ones "
    "you recognise."
)

# What the dimension would hand over for one employee.
FORBIDDEN = ("E0042", "Moreau", "dana.moreau@acme.example", "dana.moreau")


def rules(subject: str = CLEAN_SUBJECT, body: str = CLEAN_BODY, **kwargs: object) -> set[str]:
    violations = GUARDRAILS.check(subject, body, **kwargs)  # type: ignore[arg-type]
    return {v.rule for v in violations}


def test_clean_copy_passes() -> None:
    assert GUARDRAILS.check(CLEAN_SUBJECT, CLEAN_BODY) == []


# --- blame ------------------------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [
        "Hi Dana, you failed a phishing test. Please reset your password today.",
        "Hi Dana, this was careless. Please reset your password today and be careful.",
        "Hi Dana, your mistake exposed the company. Please reset your password today.",
        "Hi Dana, you were being negligent here. Please reset your password today.",
        "Hi Dana, this is a policy violation. Please reset your password immediately today.",
    ],
)
def test_blame_is_rejected(body: str) -> None:
    """Shaming people trains them to hide incidents, which costs far more."""
    assert "accusatory" in rules(body=body)


# --- threats ----------------------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [
        "Hi Dana, please reset your password today or this becomes a disciplinary matter.",
        "Hi Dana, this has been reported to your manager. Please reset your password today.",
        "Hi Dana, HR has been informed of this. Please reset your password today please.",
        "Hi Dana, please reset your password today. Further action may follow after this.",
    ],
)
def test_threats_are_rejected(body: str) -> None:
    """Bellwether has no authority to promise any of these."""
    assert "threatening" in rules(body=body)


# --- overclaiming -----------------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [
        "Hi Dana, your account has been compromised. Please reset your password right now.",
        "Hi Dana, we detected a breach on your account. Please reset your password now.",
        "Hi Dana, hackers now have your login. Please reset your password immediately today.",
        "Hi Dana, your password has been stolen. Please reset it right now and tell us.",
        "Hi Dana, this is an active attack on you. Please reset your password right now.",
    ],
)
def test_overclaiming_is_rejected(body: str) -> None:
    """The rule most specific to this product.

    Bellwether observes behaviour — a click, a submission on a *simulated* page,
    an address appearing in someone else's breach dump. None of that establishes
    that an account is in another person's hands. Telling an employee they have
    been breached when they have not is a false statement that causes real alarm,
    and it is exactly the leap a fluent model makes when asked to convey urgency.
    """
    assert "overclaiming" in rules(body=body)


def test_hedged_language_about_an_attack_is_allowed() -> None:
    """The rule has to reject false certainty without banning the actual warning."""
    body = (
        "Hi Dana, your account received a burst of sign-in prompts, which usually "
        "means somebody else has a password of yours. Please decline anything you "
        "did not start and reset your password."
    )
    assert GUARDRAILS.check("Unexpected sign-in prompts", body) == []


# --- leakage ----------------------------------------------------------------


def test_an_email_address_is_rejected() -> None:
    body = CLEAN_BODY + " Reply to security@acme.example if you need a hand."
    assert "pii" in rules(body=body)


def test_a_surname_is_rejected() -> None:
    body = CLEAN_BODY.replace("Hi Dana", "Hi Dana Moreau")
    assert "pii" in rules(body=body, forbidden=FORBIDDEN, first_name="Dana")


def test_the_first_name_is_allowed_even_though_it_is_inside_a_forbidden_term() -> None:
    """`dana.moreau` is forbidden; `Dana` is the whole point of the message.

    Excluding the first name from the scan explicitly, rather than relying on
    substring luck, is what keeps a name like `Sam` from tripping on
    `sam.okafor@...`.
    """
    assert (
        GUARDRAILS.check(
            CLEAN_SUBJECT, CLEAN_BODY, forbidden=("Dana", *FORBIDDEN), first_name="Dana"
        )
        == []
    )


def test_a_surname_inside_an_ordinary_word_is_not_a_leak() -> None:
    """A real bug, found in production counters rather than in this file.

    The forbidden-term check was a bare substring match, so the surname "Lin"
    matched inside "public link" and every template was rejected for anyone
    called Lin — who then received the last-resort copy instead. It fails safe,
    which is exactly why nothing surfaced it until the fallback was counted.
    """
    body = (
        "Hi Yuki, a few documents you shared recently went out more broadly than "
        "they needed to. Please review your shares and remove any public links."
    )
    assert (
        GUARDRAILS.check(
            "A note on how some files were shared",
            body,
            forbidden=("E0486", "Lin", "yuki.lin@acme.example", "yuki.lin"),
            first_name="Yuki",
        )
        == []
    )


def test_a_surname_standing_alone_is_still_a_leak() -> None:
    body = CLEAN_BODY.replace("Hi Dana", "Hi Yuki Lin")
    assert "pii" in rules(body=body, forbidden=("Lin",), first_name="Yuki")


def test_an_internal_identifier_is_rejected() -> None:
    """A signal name is a database key, not a sentence."""
    body = CLEAN_BODY + " Reason: phish_credentials_submitted was observed."
    assert "internal" in rules(body=body)


def test_a_link_off_the_security_portal_is_rejected() -> None:
    body = CLEAN_BODY + " Details at https://not-us.example/reset now."
    assert "link" in rules(body=body)


def test_a_link_to_the_security_portal_is_allowed() -> None:
    body = CLEAN_BODY + " Details at https://security.acme.internal/reset now."
    assert GUARDRAILS.check(CLEAN_SUBJECT, body) == []


# --- shape ------------------------------------------------------------------


def test_copy_with_no_action_is_rejected() -> None:
    """A message that conveys concern and no action wastes everyone's attention."""
    body = (
        "Hi Dana, your recent account activity has been noted by the security team "
        "and is currently under consideration by us as a matter of some interest."
    )
    assert "no_action" in rules(body=body)


def test_an_overlong_body_is_rejected() -> None:
    assert "length" in rules(body="Please reset your password. " * 40)


def test_a_terse_body_is_rejected() -> None:
    assert "length" in rules(body="Reset your password.")


def test_an_empty_subject_is_rejected() -> None:
    assert "length" in rules(subject="   ")


def test_an_overlong_subject_is_rejected() -> None:
    assert "length" in rules(subject="Please reset your password " * 6)


def test_an_action_word_in_the_subject_does_not_satisfy_the_body() -> None:
    """A real bug: "A quick security check-in" contains "check".

    Scanning subject and body together let the standing subject line discharge
    every body's obligation to say what to do, which quietly disabled the rule
    for the most common template in the set.
    """
    assert "no_action" in rules(
        subject="A quick security check-in",
        body=(
            "Hi Dana, your recent account activity has been noted by the security team "
            "and is currently under consideration as a matter of some mild interest."
        ),
    )


def test_a_verb_inside_a_longer_word_does_not_count_as_an_action() -> None:
    """ "unchanged" is not an instruction to change anything."""
    assert "no_action" in rules(
        body=(
            "Hi Dana, your account settings are unchanged since last quarter and the "
            "security team has noted that fact for its own records this week."
        )
    )


def test_every_violation_is_reported_not_just_the_first() -> None:
    """The log has to say everything that was wrong, not the first thing."""
    body = "You failed. Your account has been compromised and HR has been told."
    assert {"accusatory", "overclaiming", "threatening", "no_action"} <= rules(body=body)


# --- the fallback has to pass its own validator -----------------------------

TEMPLATES = TemplateCopywriter()


@pytest.mark.parametrize("type", list(InterventionType))
@pytest.mark.parametrize("category", list(RiskCategory))
def test_every_category_template_passes_the_guardrails(
    type: InterventionType, category: RiskCategory
) -> None:
    draft = TEMPLATES.write(
        CopyBrief(first_name="Dana", type=type, band=RiskBand.HIGH, dominant_category=category)
    )
    assert (
        GUARDRAILS.check(draft.subject, draft.body, forbidden=FORBIDDEN, first_name="Dana") == []
    ), draft.body


@pytest.mark.parametrize("type", list(InterventionType))
@pytest.mark.parametrize("signal", sorted(_BY_TRIGGER, key=lambda s: s.value))
def test_every_trigger_template_passes_the_guardrails(
    type: InterventionType, signal: SignalType
) -> None:
    draft = TEMPLATES.write(
        CopyBrief(
            first_name="Dana",
            type=type,
            band=RiskBand.CRITICAL,
            dominant_category=RiskCategory.PHISHING_SUSCEPTIBILITY,
            trigger_signal=signal,
        )
    )
    assert (
        GUARDRAILS.check(draft.subject, draft.body, forbidden=FORBIDDEN, first_name="Dana") == []
    ), draft.body


def test_the_no_category_template_passes_the_guardrails() -> None:
    draft = TEMPLATES.write(
        CopyBrief(first_name="Dana", type=InterventionType.NUDGE, band=RiskBand.ELEVATED)
    )
    assert GUARDRAILS.check(draft.subject, draft.body) == []


def test_the_last_resort_copy_passes_the_guardrails() -> None:
    """It has no substitutions, so if this fails it fails identically for everyone."""
    assert GUARDRAILS.check(_LAST_RESORT.subject, _LAST_RESORT.body) == []


def test_every_category_has_a_template() -> None:
    """A missing category would silently fall through to generic copy."""
    assert set(_BY_CATEGORY) == set(RiskCategory)
