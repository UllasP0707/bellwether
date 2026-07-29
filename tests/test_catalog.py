"""The catalog is the single source of scoring truth, so it gets its own tests."""

from __future__ import annotations

import pytest

from bellwether.events.schema import RiskCategory, SignalType
from bellwether.scoring.catalog import CATALOG, spec_for


def test_every_signal_is_priced() -> None:
    """A signal without a spec would silently contribute nothing.

    This is the test that makes the "one catalog" claim enforceable rather than
    aspirational: adding a SignalType member fails CI until it is priced.
    """
    missing = [s.value for s in SignalType if s not in CATALOG]
    assert not missing, f"signals missing a SignalSpec: {missing}"


def test_no_orphan_specs() -> None:
    assert set(CATALOG) <= set(SignalType)


def test_unpriced_signal_raises() -> None:
    class Fake(str):
        pass

    with pytest.raises(KeyError):
        spec_for(Fake("not_a_signal"))  # type: ignore[arg-type]


def test_spec_category_matches_declared_category() -> None:
    for signal, spec in CATALOG.items():
        assert spec.signal is signal, f"{signal} spec points at {spec.signal}"
        assert isinstance(spec.category, RiskCategory)


def test_half_lives_are_positive() -> None:
    """A zero or negative half-life makes the decay function nonsense."""
    for signal, spec in CATALOG.items():
        assert spec.half_life_days > 0, f"{signal} has half-life {spec.half_life_days}"


def test_credential_submission_outweighs_click() -> None:
    """Domain invariant: submitting credentials is worse than clicking.

    Encoded as a test because it is the kind of relationship a well-meaning
    weight tweak breaks without anyone noticing.
    """
    click = spec_for(SignalType.PHISH_SIM_CLICKED)
    submit = spec_for(SignalType.PHISH_CREDENTIALS_SUBMITTED)
    assert submit.weight > click.weight


def test_reporting_is_mitigating() -> None:
    for signal in (
        SignalType.PHISH_SIM_REPORTED,
        SignalType.REAL_PHISH_REPORTED,
        SignalType.TRAINING_COMPLETED,
        SignalType.INTERVENTION_ACKNOWLEDGED,
    ):
        assert spec_for(signal).is_mitigating, f"{signal} should reduce risk"


def test_exposure_only_signals_are_unweighted() -> None:
    """Delivery is exposure, not behavior; it must not raise anyone's score."""
    assert spec_for(SignalType.PHISH_SIM_DELIVERED).weight == 0.0
