"""Tests for the generator.

The generator is test infrastructure, but the demo's credibility rests on it: a
population whose scores all cluster at the mean makes the platform look
pointless, and a chain that emits credential submissions with no preceding click
would hide sequence bugs in anything downstream.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta

from bellwether.events.schema import SignalType
from bellwether.generator.population import build_population
from bellwether.generator.simulate import SCENARIOS, Simulator
from bellwether.scoring import score_events

END = datetime(2026, 7, 1, tzinfo=UTC)


def _backfill(days: int = 30, size: int = 120, seed: int = 1337):
    people = build_population(size=size, seed=seed)
    sim = Simulator(people, seed=seed + 1)
    return people, list(sim.backfill(days=days, end=END))


def test_population_is_deterministic() -> None:
    a = build_population(size=50, seed=99)
    b = build_population(size=50, seed=99)
    assert [m.employee for m in a] == [m.employee for m in b]
    assert [m.persona.name for m in a] == [m.persona.name for m in b]


def test_different_seeds_give_different_populations() -> None:
    a = build_population(size=50, seed=1)
    b = build_population(size=50, seed=2)
    assert [m.employee for m in a] != [m.employee for m in b]


def test_managers_resolve_to_real_employees() -> None:
    """A dangling manager id breaks escalation exactly when escalation matters."""
    people = build_population(size=200, seed=5)
    ids = {m.employee.employee_id for m in people}
    for member in people:
        manager = member.employee.manager_id
        assert manager is None or manager in ids


def test_backfill_is_deterministic() -> None:
    _, first = _backfill(days=7, size=40)
    _, second = _backfill(days=7, size=40)
    assert [(e.employee_id, e.signal, e.occurred_at) for e in first] == [
        (e.employee_id, e.signal, e.occurred_at) for e in second
    ]


def test_backfill_respects_the_window() -> None:
    _, events = _backfill(days=14, size=40)
    assert events
    for event in events:
        assert event.occurred_at <= END
        assert event.occurred_at >= END - timedelta(days=15)


def test_ingest_time_never_precedes_event_time() -> None:
    _, events = _backfill(days=7, size=40)
    assert all(e.ingested_at >= e.occurred_at for e in events)


def test_some_events_arrive_late() -> None:
    """Late arrival is the reason both timestamps exist. If the generator never
    produces it, nothing downstream is actually being exercised."""
    _, events = _backfill(days=14, size=80)
    late = [e for e in events if e.lateness_seconds > 300]
    assert late, "generator produced no late-arriving events"
    assert len(late) / len(events) < 0.15, "too many late events to be plausible"


def test_credential_submission_always_follows_a_click() -> None:
    """Chain invariant: nobody submits credentials to a page they never opened."""
    _, events = _backfill(days=30, size=150)
    by_employee: dict[str, list] = defaultdict(list)
    for event in events:
        by_employee[event.employee_id].append(event)

    submissions = 0
    for employee_events in by_employee.values():
        clicks = sorted(
            e.occurred_at for e in employee_events if e.signal is SignalType.PHISH_SIM_CLICKED
        )
        for event in employee_events:
            if event.signal is not SignalType.PHISH_CREDENTIALS_SUBMITTED:
                continue
            submissions += 1
            assert any(c <= event.occurred_at for c in clicks), (
                f"{event.employee_id} submitted credentials with no prior click"
            )
    assert submissions > 0, "no credential submissions generated; chain never fired"


def test_clicks_always_follow_a_delivery() -> None:
    _, events = _backfill(days=30, size=150)
    by_employee: dict[str, list] = defaultdict(list)
    for event in events:
        by_employee[event.employee_id].append(event)

    for employee_events in by_employee.values():
        deliveries = sorted(
            e.occurred_at for e in employee_events if e.signal is SignalType.PHISH_SIM_DELIVERED
        )
        for event in employee_events:
            if event.signal is SignalType.PHISH_SIM_CLICKED:
                assert any(d <= event.occurred_at for d in deliveries)


def test_nobody_both_clicks_and_reports_the_same_delivery() -> None:
    """The chain branches; it must not do both."""
    people = build_population(size=1, seed=3)
    sim = Simulator(people, seed=4)
    member = people[0]
    for _ in range(200):
        chain = sim._phish_chain(member.employee.employee_id, END, member.persona)
        signals = {e.signal for e in chain}
        assert not (
            SignalType.PHISH_SIM_CLICKED in signals and SignalType.PHISH_SIM_REPORTED in signals
        )


def test_score_distribution_is_long_tailed() -> None:
    """Most employees low, a few genuinely high.

    This is the property that makes a risk platform worth building. If the
    generator produced a bell curve around the mean, the dashboard would rank
    noise and the demo would prove nothing.
    """
    people, events = _backfill(days=30, size=200)
    by_employee: dict[str, list] = defaultdict(list)
    for event in events:
        by_employee[event.employee_id].append(event)

    scores = sorted(
        score_events(m.employee, by_employee.get(m.employee.employee_id, []), as_of=END).score
        for m in people
    )

    median = scores[len(scores) // 2]
    top = scores[-1]
    assert median < 35, f"median score {median} is too high; everyone looks risky"
    assert top > 55, f"top score {top} is too low; nobody stands out"
    assert top > median * 2


def test_all_generated_signals_are_priced() -> None:
    """Belt and braces: the generator can't emit something the scorer ignores."""
    from bellwether.scoring.catalog import CATALOG

    _, events = _backfill(days=14, size=100)
    emitted = {e.signal for e in events}
    assert emitted, "generator emitted nothing"
    assert emitted <= set(CATALOG)


def test_business_hours_skew() -> None:
    """Behavior should cluster in the working day, or the data looks synthetic."""
    _, events = _backfill(days=21, size=100)
    hours = Counter(e.occurred_at.hour for e in events)
    working = sum(count for hour, count in hours.items() if 8 <= hour <= 19)
    assert working / sum(hours.values()) > 0.6


def test_every_scenario_produces_events_for_the_right_employee() -> None:
    people = build_population(size=100, seed=1337)
    sim = Simulator(people, seed=8)
    for name in SCENARIOS:
        events = sim.incident("E0042", name, at=END)
        assert events, f"{name} produced nothing"
        assert all(e.employee_id == "E0042" for e in events)
        assert events == sorted(events, key=lambda e: e.occurred_at)


def test_phish_chain_scenario_moves_the_score_into_actionable_range() -> None:
    """The demo's spine. If this chain doesn't visibly move the score, there is
    no demo."""
    people = build_population(size=100, seed=1337)
    sim = Simulator(people, seed=8)
    member = next(m for m in people if m.employee.employee_id == "E0042")

    events = sim.incident("E0042", "phish_credential_chain", at=END)
    result = score_events(member.employee, events, as_of=END + timedelta(seconds=120))

    assert result.score > 40, f"scripted incident only reached {result.score}"
    assert result.dominant_category is not None


def test_unknown_scenario_and_employee_raise() -> None:
    import pytest

    sim = Simulator(build_population(size=10, seed=1), seed=1)
    with pytest.raises(KeyError):
        sim.incident("E0000", "not_a_scenario")
    with pytest.raises(KeyError):
        sim.incident("E9999", "phish_credential_chain")
