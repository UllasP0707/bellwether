"""Behavior simulator.

Emits `BehaviorEvent`s for a population, in three modes:

- `backfill` — N days of history as fast as the CPU allows, for seeding the lake.
- `live` — real-time trickle, for the demo and for load testing.
- `incident` — a scripted causal chain, so the demo has a story with a
  beginning and an end.

Two properties are deliberate rather than incidental:

**Events arrive as causal chains, not independent draws.** A credential
submission is preceded by a click, which is preceded by a delivery, seconds to
minutes apart. Independent sampling would produce submissions with no
corresponding click, which is both unrealistic and would hide bugs in anything
that reasons about sequence.

**Some events arrive late.** `ingested_at` usually trails `occurred_at` by
seconds, but ~4% of events are minutes-to-hours late, because that is what real
connectors do — polling APIs, retry backoff, source-side buffering. Anything
downstream that windows on ingest time will disagree with the batch path on
exactly these events, which is why both timestamps travel on the event.
"""

from __future__ import annotations

import math
import random
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

from bellwether.events.schema import SIGNAL_SOURCE, BehaviorEvent, SignalType
from bellwether.generator.personas import BASE_DAILY_RATE
from bellwether.generator.population import PopulatedEmployee

_PHISH_LURES = (
    "IT: mandatory password reset",
    "DocuSign: contract awaiting signature",
    "Payroll: direct deposit update required",
    "Shared drive: Q3 compensation review",
    "Voicemail transcription attached",
)

_RISKY_SCOPES = (
    "https://mail.google.com/",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/admin.directory.user",
)

_GENAI_TOOLS = ("chat.example-ai.com", "notebook.example-llm.io", "summarize.example.ai")


def _poisson(rng: random.Random, lam: float) -> int:
    """Draw from Poisson(lam) by Knuth's method.

    Adequate because every rate in the catalog is well under 1 event/day, so the
    loop almost always exits on the first iteration.
    """
    if lam <= 0.0:
        return 0
    threshold = math.exp(-lam)
    k = 0
    p = 1.0
    while True:
        p *= rng.random()
        if p <= threshold:
            return k
        k += 1


def _attributes(rng: random.Random, signal: SignalType) -> dict[str, Any]:
    """Plausible source-specific payload fields for a signal."""
    match signal:
        case (
            SignalType.PHISH_SIM_DELIVERED
            | SignalType.PHISH_SIM_CLICKED
            | SignalType.PHISH_SIM_REPORTED
            | SignalType.PHISH_CREDENTIALS_SUBMITTED
        ):
            return {
                "campaign_id": f"camp-{rng.randint(100, 140)}",
                "lure": rng.choice(_PHISH_LURES),
            }
        case SignalType.MFA_PUSH_FLOOD:
            return {"push_count": rng.randint(6, 40), "window_seconds": rng.choice([60, 120, 300])}
        case SignalType.IMPOSSIBLE_TRAVEL_LOGIN:
            return {"from_country": "US", "to_country": rng.choice(["NG", "RU", "BR", "VN"])}
        case SignalType.FILE_SHARED_EXTERNALLY | SignalType.FILE_SHARED_PUBLIC_LINK:
            return {
                "file_count": rng.randint(1, 6),
                "classification": rng.choice(["internal", "confidential", "public"]),
            }
        case SignalType.SENSITIVE_DATA_TO_GENAI:
            return {
                "tool": rng.choice(_GENAI_TOOLS),
                "matched_classifier": rng.choice(["pii", "source_code", "customer_records"]),
            }
        case SignalType.BULK_DOWNLOAD_DETECTED:
            return {
                "file_count": rng.randint(80, 900),
                "baseline_multiple": round(rng.uniform(4, 40), 1),
            }
        case SignalType.OAUTH_GRANT_RISKY_SCOPE:
            return {"app": f"app-{rng.randint(10, 99)}", "scope": rng.choice(_RISKY_SCOPES)}
        case SignalType.EMAIL_FORWARDING_RULE_CREATED:
            return {"destination_domain": rng.choice(["gmail.com", "proton.me", "mail.ru"])}
        case SignalType.CREDENTIAL_IN_BREACH_DUMP:
            return {"breach": rng.choice(["combolist-2024-11", "forum-dump-8821"])}
        case SignalType.TRAINING_COMPLETED | SignalType.TRAINING_OVERDUE:
            return {"module": rng.choice(["phishing-101", "data-handling", "secure-auth"])}
        case _:
            return {}


class Simulator:
    """Generates behavior events for a population."""

    def __init__(
        self,
        population: list[PopulatedEmployee],
        tenant_id: str = "acme",
        seed: int = 7,
    ) -> None:
        self.population = population
        self.tenant_id = tenant_id
        self.rng = random.Random(seed)
        self._by_id = {p.employee.employee_id: p for p in population}

    # -- event construction ------------------------------------------------

    def _event(
        self,
        employee_id: str,
        signal: SignalType,
        occurred_at: datetime,
    ) -> BehaviorEvent:
        rng = self.rng
        # Normal case: seconds of connector lag. Tail case: a genuinely late
        # arrival, which is the interesting one for event-time correctness.
        if rng.random() < 0.04:
            lag = timedelta(seconds=rng.uniform(300, 14400))
        else:
            lag = timedelta(seconds=rng.uniform(0.5, 45))

        return BehaviorEvent(
            tenant_id=self.tenant_id,
            employee_id=employee_id,
            signal=signal,
            source=SIGNAL_SOURCE[signal],
            occurred_at=occurred_at,
            ingested_at=occurred_at + lag,
            source_event_id=f"{SIGNAL_SOURCE[signal].value}-{rng.getrandbits(48):012x}",
            attributes=_attributes(rng, signal),
        )

    def _business_time(self, day_start: datetime) -> datetime:
        """A timestamp inside `day_start`, weighted toward working hours."""
        rng = self.rng
        # Mode at 11:00 rather than midday: the peak of "checking email and
        # clicking things" is late morning, not lunchtime.
        in_hours = rng.random() < 0.88
        hour = rng.triangular(8, 19, 11) if in_hours else rng.uniform(0, 24)
        return day_start + timedelta(hours=hour, minutes=rng.uniform(0, 59))

    # -- generation modes --------------------------------------------------

    def day_for(self, member: PopulatedEmployee, day_start: datetime) -> list[BehaviorEvent]:
        """All of one employee's events for one day, ordered by event time."""
        rng = self.rng
        persona = member.persona
        employee_id = member.employee.employee_id
        events: list[BehaviorEvent] = []

        # Weekends are quieter but not silent, and the people working them skew
        # risky, which is a correlation worth having in the data.
        weekend_factor = 0.25 if day_start.weekday() >= 5 else 1.0

        for signal in BASE_DAILY_RATE:
            rate = persona.rate_for(signal) * weekend_factor
            for _ in range(_poisson(rng, rate)):
                at = self._business_time(day_start)
                events.append(self._event(employee_id, signal, at))

                if signal is SignalType.PHISH_SIM_DELIVERED:
                    events.extend(self._phish_chain(employee_id, at, persona))

        events.sort(key=lambda e: e.occurred_at)
        return events

    def _phish_chain(
        self,
        employee_id: str,
        delivered_at: datetime,
        persona: Any,
    ) -> list[BehaviorEvent]:
        """Follow-on events after a simulated phish is delivered."""
        rng = self.rng
        chain: list[BehaviorEvent] = []

        if rng.random() < persona.click_rate:
            clicked_at = delivered_at + timedelta(seconds=rng.uniform(30, 5400))
            chain.append(self._event(employee_id, SignalType.PHISH_SIM_CLICKED, clicked_at))

            if rng.random() < persona.submit_rate:
                chain.append(
                    self._event(
                        employee_id,
                        SignalType.PHISH_CREDENTIALS_SUBMITTED,
                        clicked_at + timedelta(seconds=rng.uniform(8, 240)),
                    )
                )
        elif rng.random() < persona.report_rate:
            chain.append(
                self._event(
                    employee_id,
                    SignalType.PHISH_SIM_REPORTED,
                    delivered_at + timedelta(seconds=rng.uniform(60, 7200)),
                )
            )

        return chain

    def backfill(
        self,
        days: int = 30,
        end: datetime | None = None,
    ) -> Iterator[BehaviorEvent]:
        """Generate `days` of history ending at `end`, in event-time order.

        Sorted per day rather than globally: a chain started late on day N can
        spill into day N+1, so downstream consumers still see a small amount of
        out-of-order arrival. That is intentional — a pipeline that only ever
        sees perfectly ordered input is not being tested.
        """
        end = end or datetime.now(UTC)
        start = (end - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)

        for offset in range(days):
            day_start = start + timedelta(days=offset)
            day_events: list[BehaviorEvent] = []
            for member in self.population:
                day_events.extend(self.day_for(member, day_start))
            day_events.sort(key=lambda e: e.occurred_at)
            for event in day_events:
                if event.occurred_at <= end:
                    yield event

    def live(self, rate_multiplier: float = 60.0) -> Iterator[BehaviorEvent]:
        """Stream events in real time, indefinitely.

        Args:
            rate_multiplier: Compresses simulated time. At 60, one wall-clock
                minute of the demo covers an hour of simulated behavior — fast
                enough that a viewer sees scores move, slow enough to narrate.
        """
        while True:
            tick_started = time.monotonic()
            now = datetime.now(UTC)
            # Each tick covers `rate_multiplier` seconds of simulated time.
            fraction_of_day = rate_multiplier / 86400.0

            batch: list[BehaviorEvent] = []
            for member in self.population:
                for signal in BASE_DAILY_RATE:
                    rate = member.persona.rate_for(signal) * fraction_of_day
                    for _ in range(_poisson(self.rng, rate)):
                        at = now + timedelta(seconds=self.rng.uniform(0, 1))
                        batch.append(self._event(member.employee.employee_id, signal, at))
                        if signal is SignalType.PHISH_SIM_DELIVERED:
                            batch.extend(
                                self._phish_chain(member.employee.employee_id, at, member.persona)
                            )

            batch.sort(key=lambda e: e.occurred_at)
            yield from batch

            elapsed = time.monotonic() - tick_started
            time.sleep(max(0.0, 1.0 - elapsed))

    def incident(
        self,
        employee_id: str,
        scenario: str,
        at: datetime | None = None,
    ) -> list[BehaviorEvent]:
        """Emit a scripted chain for one employee.

        Raises:
            KeyError: Unknown scenario or unknown employee.
        """
        if employee_id not in self._by_id:
            raise KeyError(f"no such employee: {employee_id}")
        if scenario not in SCENARIOS:
            raise KeyError(f"unknown scenario: {scenario} (have: {', '.join(SCENARIOS)})")

        at = at or datetime.now(UTC)
        return SCENARIOS[scenario](self, employee_id, at)


def _phish_credential_chain(
    sim: Simulator, employee_id: str, at: datetime
) -> list[BehaviorEvent]:
    """The demo's spine: delivery, click, credential submission, ~2 minutes."""
    return [
        sim._event(employee_id, SignalType.PHISH_SIM_DELIVERED, at),
        sim._event(employee_id, SignalType.PHISH_SIM_CLICKED, at + timedelta(seconds=47)),
        sim._event(
            employee_id, SignalType.PHISH_CREDENTIALS_SUBMITTED, at + timedelta(seconds=112)
        ),
    ]


def _mfa_fatigue(sim: Simulator, employee_id: str, at: datetime) -> list[BehaviorEvent]:
    """An account under active MFA fatigue attack."""
    events = [
        sim._event(employee_id, SignalType.MFA_PUSH_DENIED, at + timedelta(seconds=9 * i))
        for i in range(6)
    ]
    events.append(sim._event(employee_id, SignalType.MFA_PUSH_FLOOD, at + timedelta(seconds=70)))
    return events


def _data_exfil(sim: Simulator, employee_id: str, at: datetime) -> list[BehaviorEvent]:
    """Staged exfiltration: gather, expose, carry out."""
    return [
        sim._event(employee_id, SignalType.BULK_DOWNLOAD_DETECTED, at),
        sim._event(
            employee_id, SignalType.FILE_SHARED_PUBLIC_LINK, at + timedelta(minutes=4)
        ),
        sim._event(
            employee_id, SignalType.USB_MASS_STORAGE_MOUNTED, at + timedelta(minutes=11)
        ),
    ]


def _account_takeover(sim: Simulator, employee_id: str, at: datetime) -> list[BehaviorEvent]:
    """Post-compromise persistence, the pattern security teams most want caught."""
    return [
        sim._event(employee_id, SignalType.IMPOSSIBLE_TRAVEL_LOGIN, at),
        sim._event(
            employee_id, SignalType.EMAIL_FORWARDING_RULE_CREATED, at + timedelta(minutes=2)
        ),
        sim._event(
            employee_id, SignalType.OAUTH_GRANT_RISKY_SCOPE, at + timedelta(minutes=6)
        ),
    ]


SCENARIOS: dict[str, Callable[[Simulator, str, datetime], list[BehaviorEvent]]] = {
    "phish_credential_chain": _phish_credential_chain,
    "mfa_fatigue": _mfa_fatigue,
    "data_exfil": _data_exfil,
    "account_takeover": _account_takeover,
}
