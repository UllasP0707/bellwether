"""Online state for the scorer.

Two things have to survive between messages: each employee's recent events, and
the band they were last seen in.

**The window** is a per-employee sorted set scored by event time. Adding is
idempotent because the member is derived from `event_id` — replaying an event
that is already in the window is a no-op, which extends the at-least-once story
from the normalizer into the scorer without any extra bookkeeping.

Only the three fields scoring reads are stored. Keeping whole events would make
the window several times larger to no purpose, and `ScorableEvent` exists
precisely so a projection is enough.

**The band** is remembered so a crossing can be detected. Day 4's interventions
fire on transitions rather than on levels — nudging someone every time their
score twitches inside the same band is how a system trains people to ignore it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol, cast

from bellwether.events.schema import SignalType
from bellwether.scoring import RiskBand

# A single employee should not be able to exhaust memory. An admin account
# generating audit spam is the realistic case; the window keeps the most recent
# events and counts what it dropped rather than growing without bound.
MAX_WINDOW_EVENTS = 5_000

_FIELD_SEP = "\x1f"


@dataclass(frozen=True, slots=True)
class WindowedEvent:
    """A stored event, reduced to what scoring reads.

    Structurally a `ScorableEvent`, so it goes straight into `score_events()`
    with no conversion.
    """

    employee_id: str
    signal: SignalType
    occurred_at: datetime
    event_id: str

    def member(self) -> str:
        """Sorted-set member. Unique per source event, hence idempotent."""
        return f"{self.event_id}{_FIELD_SEP}{self.signal.value}"

    @classmethod
    def from_member(cls, employee_id: str, member: str, score: float) -> WindowedEvent:
        event_id, _, signal = member.partition(_FIELD_SEP)
        return cls(
            employee_id=employee_id,
            signal=SignalType(signal),
            occurred_at=datetime.fromtimestamp(score, tz=UTC),
            event_id=event_id,
        )


class EventWindow(Protocol):
    """Each employee's recent events.

    `as_of` is a parameter rather than a `now()` call inside, for the same
    reason `score_events` takes one: the window has to trim relative to the
    time being scored, not to wall-clock time. Otherwise a batch recomputation
    of last month would silently evaluate against a window trimmed to today,
    and the two paths would disagree without either being obviously wrong.
    """

    def add(
        self, event: WindowedEvent, lookback_days: int, as_of: datetime | None = None
    ) -> None: ...

    def events(self, employee_id: str) -> list[WindowedEvent]: ...


class ScoreState(Protocol):
    """The band an employee was last seen in."""

    def band(self, employee_id: str) -> RiskBand | None: ...

    def set_band(self, employee_id: str, band: RiskBand) -> None: ...


class InMemoryWindow:
    """Window and band state in a dict. For tests and single-process runs."""

    def __init__(self) -> None:
        self._events: dict[str, dict[str, WindowedEvent]] = {}
        self._bands: dict[str, RiskBand] = {}
        self.truncated = 0

    def add(
        self, event: WindowedEvent, lookback_days: int = 30, as_of: datetime | None = None
    ) -> None:
        bucket = self._events.setdefault(event.employee_id, {})
        bucket[event.member()] = event

        cutoff = (as_of or datetime.now(UTC)) - timedelta(days=lookback_days)
        for member, stored in list(bucket.items()):
            if stored.occurred_at < cutoff:
                del bucket[member]

        if len(bucket) > MAX_WINDOW_EVENTS:
            for member, _ in sorted(bucket.items(), key=lambda kv: kv[1].occurred_at)[
                : len(bucket) - MAX_WINDOW_EVENTS
            ]:
                del bucket[member]
                self.truncated += 1

    def events(self, employee_id: str) -> list[WindowedEvent]:
        return list(self._events.get(employee_id, {}).values())

    def band(self, employee_id: str) -> RiskBand | None:
        return self._bands.get(employee_id)

    def set_band(self, employee_id: str, band: RiskBand) -> None:
        self._bands[employee_id] = band


class RedisWindow:
    """Window and band state in Redis.

    The window survives a consumer restart, which is the point: rebuilding it
    would otherwise mean replaying thirty days of the normalized topic before
    the first score could be emitted.
    """

    def __init__(self, url: str, tenant_id: str = "acme", namespace: str = "w") -> None:
        import redis

        self._redis = redis.Redis.from_url(url)
        self.tenant_id = tenant_id
        self.namespace = namespace
        self.truncated = 0

    def _key(self, employee_id: str) -> str:
        return f"{self.namespace}:{self.tenant_id}:{employee_id}"

    def _band_key(self, employee_id: str) -> str:
        return f"band:{self.tenant_id}:{employee_id}"

    def add(
        self, event: WindowedEvent, lookback_days: int = 30, as_of: datetime | None = None
    ) -> None:
        key = self._key(event.employee_id)
        cutoff = ((as_of or datetime.now(UTC)) - timedelta(days=lookback_days)).timestamp()

        # Pipelined: add, drop what aged out, cap the size, and refresh the TTL
        # in one round trip. Doing these as four calls would triple the latency
        # this sits in the middle of.
        pipe = self._redis.pipeline(transaction=False)
        pipe.zadd(key, {event.member(): event.occurred_at.timestamp()})
        pipe.zremrangebyscore(key, "-inf", cutoff)
        pipe.zremrangebyrank(key, 0, -(MAX_WINDOW_EVENTS + 1))
        # Expire idle windows so someone who leaves stops costing memory.
        pipe.expire(key, int(timedelta(days=lookback_days * 2).total_seconds()))
        results = pipe.execute()

        self.truncated += int(results[2] or 0)

    def events(self, employee_id: str) -> list[WindowedEvent]:
        raw = cast(
            "list[tuple[bytes, float]]",
            self._redis.zrange(self._key(employee_id), 0, -1, withscores=True),
        )
        return [
            WindowedEvent.from_member(employee_id, member.decode(), score) for member, score in raw
        ]

    def band(self, employee_id: str) -> RiskBand | None:
        value = cast("bytes | None", self._redis.get(self._band_key(employee_id)))
        return RiskBand(value.decode()) if value else None

    def set_band(self, employee_id: str, band: RiskBand) -> None:
        self._redis.set(self._band_key(employee_id), band.value)
