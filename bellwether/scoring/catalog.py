"""The signal catalog: the single definition of what each behavior means.

This module is the reason the project is organized the way it is. The streaming
scorer and the Spark batch scorer are two evaluation strategies over this one
table. A weight change is one edit that both paths pick up, and
`tests/test_score_parity.py` replays a fixed event log through both to prove
they still agree.

Weights are on an open-ended scale, not 0-100 — normalization happens once, in
`bellwether.scoring.score`. Keeping raw weights unbounded means adding a new signal
never requires rebalancing the existing ones.
"""

from __future__ import annotations

from dataclasses import dataclass

from bellwether.events.schema import RiskCategory, SignalType


@dataclass(frozen=True, slots=True)
class SignalSpec:
    """How one behavior contributes to risk.

    Attributes:
        weight: Positive aggravates risk, negative mitigates it.
        half_life_days: Days until this signal's contribution halves. Encodes
            how quickly the behavior stops predicting future behavior — a
            breached credential stays relevant far longer than a training
            lapse.
    """

    signal: SignalType
    category: RiskCategory
    weight: float
    half_life_days: float
    description: str

    @property
    def is_mitigating(self) -> bool:
        return self.weight < 0


def _spec(
    signal: SignalType,
    category: RiskCategory,
    weight: float,
    half_life_days: float,
    description: str,
) -> tuple[SignalType, SignalSpec]:
    return signal, SignalSpec(signal, category, weight, half_life_days, description)


CATALOG: dict[SignalType, SignalSpec] = dict(
    [
        # --- Phishing susceptibility ---------------------------------------
        # Delivery is not a behavior; it is carried at weight zero so the
        # denominator "how many simulations did this person actually receive"
        # is available to the batch path without a second source.
        _spec(
            SignalType.PHISH_SIM_DELIVERED,
            RiskCategory.PHISHING_SUSCEPTIBILITY,
            0.0,
            30.0,
            "Simulated phishing email delivered. Exposure, not behavior.",
        ),
        _spec(
            SignalType.PHISH_SIM_CLICKED,
            RiskCategory.PHISHING_SUSCEPTIBILITY,
            8.0,
            21.0,
            "Clicked a link in a simulated phishing email.",
        ),
        _spec(
            SignalType.PHISH_CREDENTIALS_SUBMITTED,
            RiskCategory.PHISHING_SUSCEPTIBILITY,
            25.0,
            45.0,
            "Entered credentials into a simulated phishing page. The click is a "
            "lapse of attention; this is the behavior that actually loses an account.",
        ),
        _spec(
            SignalType.PHISH_SIM_REPORTED,
            RiskCategory.PHISHING_SUSCEPTIBILITY,
            -6.0,
            30.0,
            "Reported a simulated phish instead of engaging with it.",
        ),
        _spec(
            SignalType.REAL_PHISH_REPORTED,
            RiskCategory.PHISHING_SUSCEPTIBILITY,
            -10.0,
            60.0,
            "Reported a genuine phishing attempt. Strongest positive signal in "
            "the catalog: this person is functioning as a detection layer.",
        ),
        # --- Credential hygiene --------------------------------------------
        _spec(
            SignalType.MFA_PUSH_DENIED,
            RiskCategory.CREDENTIAL_HYGIENE,
            1.0,
            7.0,
            "Denied an MFA push. Weakly risky alone — usually just a stale session.",
        ),
        _spec(
            SignalType.MFA_PUSH_FLOOD,
            RiskCategory.CREDENTIAL_HYGIENE,
            14.0,
            21.0,
            "Received a burst of MFA pushes, consistent with an active fatigue "
            "attack against this account.",
        ),
        _spec(
            SignalType.PASSWORD_REUSE_DETECTED,
            RiskCategory.CREDENTIAL_HYGIENE,
            10.0,
            90.0,
            "Corporate password observed reused on a non-corporate service.",
        ),
        _spec(
            SignalType.CREDENTIAL_IN_BREACH_DUMP,
            RiskCategory.CREDENTIAL_HYGIENE,
            18.0,
            180.0,
            "Credential appeared in a third-party breach. Long half-life: the "
            "exposure persists until the password is actually rotated.",
        ),
        _spec(
            SignalType.IMPOSSIBLE_TRAVEL_LOGIN,
            RiskCategory.CREDENTIAL_HYGIENE,
            9.0,
            14.0,
            "Successful logins from geographically incompatible locations.",
        ),
        # --- Data handling -------------------------------------------------
        _spec(
            SignalType.FILE_SHARED_EXTERNALLY,
            RiskCategory.DATA_HANDLING,
            2.0,
            14.0,
            "Shared a file with a named external address. Routine for most roles.",
        ),
        _spec(
            SignalType.FILE_SHARED_PUBLIC_LINK,
            RiskCategory.DATA_HANDLING,
            11.0,
            30.0,
            "Created a public, unauthenticated link to a corporate document.",
        ),
        _spec(
            SignalType.SENSITIVE_DATA_TO_GENAI,
            RiskCategory.DATA_HANDLING,
            13.0,
            21.0,
            "Pasted content matching a sensitive-data classifier into a "
            "third-party AI tool.",
        ),
        _spec(
            SignalType.BULK_DOWNLOAD_DETECTED,
            RiskCategory.DATA_HANDLING,
            15.0,
            30.0,
            "Downloaded documents far above this person's own baseline.",
        ),
        _spec(
            SignalType.USB_MASS_STORAGE_MOUNTED,
            RiskCategory.DATA_HANDLING,
            4.0,
            14.0,
            "Mounted removable storage on a managed device.",
        ),
        # --- Access hygiene ------------------------------------------------
        _spec(
            SignalType.OAUTH_GRANT_RISKY_SCOPE,
            RiskCategory.ACCESS_HYGIENE,
            12.0,
            60.0,
            "Granted a third-party app broad scopes (mail.read, drive full access).",
        ),
        _spec(
            SignalType.EMAIL_FORWARDING_RULE_CREATED,
            RiskCategory.ACCESS_HYGIENE,
            16.0,
            60.0,
            "Created an auto-forwarding rule to an external address. Classic "
            "post-compromise persistence, and occasionally just convenience.",
        ),
        _spec(
            SignalType.ADMIN_PRIVILEGE_GRANTED,
            RiskCategory.ACCESS_HYGIENE,
            5.0,
            120.0,
            "Received elevated privileges. Not misbehavior — it raises the "
            "blast radius of every other signal.",
        ),
        _spec(
            SignalType.STALE_ACCESS_UNREVIEWED,
            RiskCategory.ACCESS_HYGIENE,
            6.0,
            90.0,
            "Retains access to systems unused for 90+ days.",
        ),
        # --- Security engagement -------------------------------------------
        _spec(
            SignalType.TRAINING_COMPLETED,
            RiskCategory.SECURITY_ENGAGEMENT,
            -7.0,
            60.0,
            "Completed assigned security training.",
        ),
        _spec(
            SignalType.TRAINING_OVERDUE,
            RiskCategory.SECURITY_ENGAGEMENT,
            5.0,
            30.0,
            "Assigned training is past due.",
        ),
        _spec(
            SignalType.INTERVENTION_ACKNOWLEDGED,
            RiskCategory.SECURITY_ENGAGEMENT,
            -4.0,
            30.0,
            "Engaged with an intervention Bellwether sent.",
        ),
        _spec(
            SignalType.INTERVENTION_IGNORED,
            RiskCategory.SECURITY_ENGAGEMENT,
            3.0,
            30.0,
            "Ignored an intervention. Predicts that future nudges will also "
            "miss, which is what should drive escalation.",
        ),
    ]
)


def spec_for(signal: SignalType) -> SignalSpec:
    """Look up a signal's spec.

    Raises:
        KeyError: If the signal has no spec. Deliberately not a silent default:
            an unpriced signal quietly contributing zero is a scoring bug that
            would be invisible in production.
    """
    try:
        return CATALOG[signal]
    except KeyError:
        raise KeyError(f"{signal} has no SignalSpec in the catalog") from None
