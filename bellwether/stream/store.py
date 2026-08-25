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
from bellwether.events.scores import RiskScoreEvent
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
    """What the scorer remembers about an employee between messages.

    `record` takes the whole published score rather than just the band because
    the two writes belong together: the band is what the *next* message compares
    against, and the projection is what the read path serves. Splitting them
    into two calls means two round trips and a window where the dashboard and
    the crossing detector disagree.
    """

    def band(self, employee_id: str) -> RiskBand | None: ...

    def record(self, score: RiskScoreEvent) -> None: ...


class ScoreReader(Protocol):
    """The read side of the projection, used by the API and nothing else.

    Separate from `ScoreState` because the two have opposite shapes: the scorer
    writes one employee at a time and never reads a ranking, the API reads
    rankings constantly and never writes. Keeping them apart means the API
    cannot accidentally hold something that can mutate scoring state.
    """

    def latest(self, employee_id: str) -> RiskScoreEvent | None: ...

    def ranking(self, limit: int = 50, offset: int = 0) -> list[RiskScoreEvent]: ...

    def scored_count(self) -> int: ...


class InMemoryOnlineStore:
    """Window, band and score projection in dicts. Tests and single-process runs."""

    def __init__(self) -> None:
        self._events: dict[str, dict[str, WindowedEvent]] = {}
        self._bands: dict[str, RiskBand] = {}
        self._latest: dict[str, RiskScoreEvent] = {}
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

    def record(self, score: RiskScoreEvent) -> None:
        self._bands[score.employee_id] = score.band
        self._latest[score.employee_id] = score

    def latest(self, employee_id: str) -> RiskScoreEvent | None:
        return self._latest.get(employee_id)

    def ranking(self, limit: int = 50, offset: int = 0) -> list[RiskScoreEvent]:
        ordered = sorted(self._latest.values(), key=lambda s: s.score, reverse=True)
        return ordered[offset : offset + limit]

    def scored_count(self) -> int:
        return len(self._latest)


class RedisOnlineStore:
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

    def _score_key(self, employee_id: str) -> str:
        return f"score:{self.tenant_id}:{employee_id}"

    def _rank_key(self) -> str:
        return f"rank:{self.tenant_id}"

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

    def record(self, score: RiskScoreEvent) -> None:
        """Remember the band and project the score for the read path.

        One pipeline for three writes. The sorted set is what makes "who are the
        twenty riskiest people" a range query rather than a scan of the score
        topic, which is the difference between a dashboard that loads and one
        that reads thirty days of Kafka on every refresh.

        No TTL, unlike the window. This is serving state, and an employee whose
        score expired would vanish from the ranking rather than appear as
        low-risk — a worse answer than a stale one. It is bounded by headcount
        rather than by traffic, and deletion is explicit (day 10).
        """
        pipe = self._redis.pipeline(transaction=False)
        pipe.set(self._band_key(score.employee_id), score.band.value)
        pipe.set(self._score_key(score.employee_id), score.model_dump_json())
        pipe.zadd(self._rank_key(), {score.employee_id: score.score})
        pipe.execute()

    def latest(self, employee_id: str) -> RiskScoreEvent | None:
        raw = cast("bytes | None", self._redis.get(self._score_key(employee_id)))
        return RiskScoreEvent.model_validate_json(raw) if raw else None

    def ranking(self, limit: int = 50, offset: int = 0) -> list[RiskScoreEvent]:
        """Riskiest first, read in two round trips regardless of page size."""
        members = cast(
            "list[bytes]",
            self._redis.zrevrange(self._rank_key(), offset, offset + limit - 1),
        )
        if not members:
            return []
        keys = [self._score_key(m.decode()) for m in members]
        raw = cast("list[bytes | None]", self._redis.mget(keys))
        return [RiskScoreEvent.model_validate_json(r) for r in raw if r]

    def scored_count(self) -> int:
        return int(cast("int", self._redis.zcard(self._rank_key())))
