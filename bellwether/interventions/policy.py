"""The gates an intervention has to pass, and the ledger that enforces them.

The reason this is the largest module in the package: a human-risk platform
whose failure mode is *messaging people too much* stops being used, and then it
protects nobody. Every rule here exists to make the system quieter than the raw
signal would make it.

The ledger lives in Postgres rather than in memory or Redis because it is the
one piece of state whose loss is not recoverable by replaying the log. Scores
can be recomputed from `events.normalized`; the fact that Dana was already
messaged on Tuesday cannot be, and a consumer that forgets it will message her
again.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from bellwether.interventions.types import LADDER, InterventionEvent, InterventionType
from bellwether.scoring import RiskBand

DDL = """
CREATE TABLE IF NOT EXISTS intervention (
    intervention_id   uuid PRIMARY KEY,
    tenant_id         text             NOT NULL,
    employee_id       text             NOT NULL,
    type              text             NOT NULL,
    channel           text             NOT NULL,
    trigger_signal    text,
    trigger_event_id  text,
    band              text             NOT NULL,
    previous_band     text,
    score             double precision NOT NULL,
    dominant_category text,
    subject           text             NOT NULL,
    body              text             NOT NULL,
    copy_source       text             NOT NULL,
    created_at        timestamptz      NOT NULL
);

-- The idempotency fence, and the rule it encodes: **one behaviour, one
-- message**. Delivery into this stage is at-least-once, so the same score can
-- arrive twice; cooldown makes a duplicate unlikely and this makes it
-- impossible.
--
-- `type` is deliberately not part of the key. It was, and that was wrong: a
-- redelivered score finds one more prior intervention in the ledger than it did
-- the first time, climbs a rung, and inserts cleanly as a *different* type. The
-- employee gets nudged and then, for the same click, sent to training. Keying on
-- the triggering event alone is what makes replay genuinely inert.
--
-- `coalesce` is required because NULLs are distinct under a unique index, which
-- would let a triggerless intervention past the fence entirely — hence also the
-- stage's refusal to act on a score that carries no trigger id.
CREATE UNIQUE INDEX IF NOT EXISTS intervention_trigger_idx
    ON intervention (tenant_id, employee_id, coalesce(trigger_event_id, ''));

CREATE INDEX IF NOT EXISTS intervention_recent_idx
    ON intervention (tenant_id, employee_id, created_at DESC);
"""

_COLUMNS = (
    "intervention_id",
    "tenant_id",
    "employee_id",
    "type",
    "channel",
    "trigger_signal",
    "trigger_event_id",
    "band",
    "previous_band",
    "score",
    "dominant_category",
    "subject",
    "body",
    "copy_source",
    "created_at",
)

# `intervention_id` is a uuid column, and psycopg hands back a `UUID` object,
# which is not what the wire contract says the field is. Casting at the query
# rather than converting after it keeps the row mapping a plain zip.
_SELECT = ", ".join(
    "intervention_id::text AS intervention_id" if c == "intervention_id" else c for c in _COLUMNS
)


@dataclass(frozen=True)
class Policy:
    """How restrained the system is. Every default here errs toward silence.

    Attributes:
        min_band: Nothing fires below this. Someone drifting from low to
            moderate does not need to hear about it.
        max_trigger_age_hours: How old the behaviour that caused a rescore may
            be. An intervention is a response to something recent; past a couple
            of days there is no moment left to be salient, and the message reads
            as the system having just noticed.

            This also makes replaying history safe without a separate operating
            mode. A backfill rescores thirty days of behaviour with `as_of` set
            to now, so every band crossing it produces is an artefact of
            ingestion order rather than anything that happened to anyone — and
            without this gate, reprocessing the log means messaging the entire
            population about last month.
        cooldown_hours: One intervention per employee *per type* per window.
        min_spacing_hours: Minimum gap between any two interventions to the
            same person, whatever their types. This exists because the per-type
            cooldown and the escalation ladder interact badly on their own: an
            employee who has already been nudged escalates to training on their
            next trigger, training has its own untouched cooldown, and the
            second message goes out an hour after the first. Each rung is free
            unless something spans them.
        weekly_cap: Total interventions per employee per rolling 7 days,
            across all types. The outer bound, in case a run of genuinely
            distinct triggers clears every other gate.
        ladder_window_days: How far back prior interventions count toward
            escalation. Longer than the cooldown, so repeat behaviour escalates
            while an isolated lapse does not.
        allow_manager_notification: Off by default. Telling someone's manager
            is the only action here that cannot be walked back, so it is opt-in
            at the deployment level rather than a threshold the system can
            cross on its own.
    """

    min_band: RiskBand = RiskBand.ELEVATED
    max_trigger_age_hours: int = 48
    cooldown_hours: int = 72
    min_spacing_hours: int = 24
    weekly_cap: int = 3
    ladder_window_days: int = 30
    # Whether one of the four critical signals may cut ahead of
    # `min_spacing_hours`. On by default, and bounded: it applies only when
    # the previous message was not itself urgent. See `Decider._urgent_override`
    # for the inversion that made it necessary.
    urgent_overrides_spacing: bool = True
    allow_manager_notification: bool = False

    def meets_threshold(self, band: RiskBand) -> bool:
        return _BAND_ORDER[band] >= _BAND_ORDER[self.min_band]

    def rung(self, prior: int, disengaged: bool, has_manager: bool) -> InterventionType:
        """Which rung of the ladder to use.

        Climbs one rung per prior intervention in the window, plus one if the
        employee is disengaged — their security-engagement signals are net
        aggravating, meaning overdue training or ignored prompts. A nudge that
        went unread predicts the next nudge will too, so repeating it is the
        least useful option available.

        Clamped to the highest permitted rung rather than suppressed: if
        manager notification is switched off, or the employee has no manager on
        record, the right outcome is to send the strongest thing that *is*
        allowed. Going silent because the top rung is disabled would make
        disabling it strictly worse than leaving it on.
        """
        index = prior + (1 if disengaged else 0)
        ceiling = len(LADDER) - 1 if (self.allow_manager_notification and has_manager) else 1
        return LADDER[min(index, ceiling)]


_BAND_ORDER: dict[RiskBand, int] = {
    RiskBand.LOW: 0,
    RiskBand.MODERATE: 1,
    RiskBand.ELEVATED: 2,
    RiskBand.HIGH: 3,
    RiskBand.CRITICAL: 4,
}


def band_rose(previous: RiskBand | None, current: RiskBand) -> bool:
    """Whether this score moved the employee *up* a band.

    A first-ever score is not a rise. Onboarding a tenant would otherwise fire
    an intervention at every employee already above the threshold on day one,
    which is both a spam incident and a terrible first impression.
    """
    if previous is None:
        return False
    return _BAND_ORDER[current] > _BAND_ORDER[previous]


class InterventionLedger(Protocol):
    """What has already been sent to whom."""

    def last_sent_at(
        self, tenant_id: str, employee_id: str, type: InterventionType | None = None
    ) -> datetime | None:
        """When this person was last contacted; `type=None` means any type."""
        ...

    def count_since(self, tenant_id: str, employee_id: str, since: datetime) -> int: ...

    def record(self, event: InterventionEvent) -> bool:
        """Persist an intervention. False if an identical one is already there."""
        ...

    def history(
        self, tenant_id: str, employee_id: str, limit: int = 20
    ) -> list[InterventionEvent]: ...


class InMemoryLedger:
    """For tests and for running without Postgres."""

    def __init__(self) -> None:
        self._events: list[InterventionEvent] = []
        self._keys: set[tuple[str, str, str]] = set()

    @staticmethod
    def _key(event: InterventionEvent) -> tuple[str, str, str]:
        return (event.tenant_id, event.employee_id, event.trigger_event_id or "")

    def last_sent_at(
        self, tenant_id: str, employee_id: str, type: InterventionType | None = None
    ) -> datetime | None:
        times = [
            e.created_at
            for e in self._events
            if e.tenant_id == tenant_id
            and e.employee_id == employee_id
            and (type is None or e.type is type)
        ]
        return max(times) if times else None

    def count_since(self, tenant_id: str, employee_id: str, since: datetime) -> int:
        return sum(
            1
            for e in self._events
            if e.tenant_id == tenant_id and e.employee_id == employee_id and e.created_at >= since
        )

    def record(self, event: InterventionEvent) -> bool:
        key = self._key(event)
        if key in self._keys:
            return False
        self._keys.add(key)
        self._events.append(event)
        return True

    def history(self, tenant_id: str, employee_id: str, limit: int = 20) -> list[InterventionEvent]:
        matching = [
            e for e in self._events if e.tenant_id == tenant_id and e.employee_id == employee_id
        ]
        return sorted(matching, key=lambda e: e.created_at, reverse=True)[:limit]

    def __len__(self) -> int:
        return len(self._events)


class PostgresLedger:
    """The durable ledger.

    Reads are per-employee point lookups on an indexed key rather than a cached
    snapshot, unlike the employee dimension. The dimension can be stale by a few
    minutes without consequence; this cannot, because a stale read here is
    exactly how a second message goes out.
    """

    def __init__(self, dsn: str) -> None:
        import psycopg

        self._connection = psycopg.connect(dsn, autocommit=True)
        with self._connection.cursor() as cur:
            cur.execute(DDL)

    def last_sent_at(
        self, tenant_id: str, employee_id: str, type: InterventionType | None = None
    ) -> datetime | None:
        with self._connection.cursor() as cur:
            cur.execute(
                "SELECT max(created_at) FROM intervention "
                "WHERE tenant_id = %s AND employee_id = %s "
                "AND (%s::text IS NULL OR type = %s::text)",
                (
                    tenant_id,
                    employee_id,
                    type.value if type else None,
                    type.value if type else None,
                ),
            )
            row = cur.fetchone()
        return None if row is None else row[0]

    def count_since(self, tenant_id: str, employee_id: str, since: datetime) -> int:
        with self._connection.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM intervention "
                "WHERE tenant_id = %s AND employee_id = %s AND created_at >= %s",
                (tenant_id, employee_id, since),
            )
            row = cur.fetchone()
        return 0 if row is None else int(row[0])

    def record(self, event: InterventionEvent) -> bool:
        """Insert, or report that this exact intervention already exists.

        `ON CONFLICT DO NOTHING` against the uniqueness index rather than a
        read-then-write: the check and the write have to be one statement, or a
        restart between them reintroduces the duplicate the index exists to
        prevent.
        """
        columns = ", ".join(_COLUMNS)
        placeholders = ", ".join(["%s"] * len(_COLUMNS))
        values = (
            event.intervention_id,
            event.tenant_id,
            event.employee_id,
            event.type.value,
            event.channel.value,
            event.trigger_signal.value if event.trigger_signal else None,
            event.trigger_event_id,
            event.band.value,
            event.previous_band.value if event.previous_band else None,
            event.score,
            event.dominant_category.value if event.dominant_category else None,
            event.subject,
            event.body,
            event.copy_source.value,
            event.created_at,
        )
        with self._connection.cursor() as cur:
            cur.execute(
                f"INSERT INTO intervention ({columns}) VALUES ({placeholders}) "
                "ON CONFLICT DO NOTHING",
                values,
            )
            return cur.rowcount == 1

    def history(self, tenant_id: str, employee_id: str, limit: int = 20) -> list[InterventionEvent]:
        with self._connection.cursor() as cur:
            cur.execute(
                f"SELECT {_SELECT} FROM intervention "
                "WHERE tenant_id = %s AND employee_id = %s "
                "ORDER BY created_at DESC LIMIT %s",
                (tenant_id, employee_id, limit),
            )
            rows = cur.fetchall()
        return [
            InterventionEvent.model_validate(dict(zip(_COLUMNS, row, strict=True))) for row in rows
        ]

    def totals(self, tenant_id: str) -> dict[str, int]:
        """Sent counts by type. For the CLI and the operability dashboard."""
        with self._connection.cursor() as cur:
            cur.execute(
                "SELECT type, count(*) FROM intervention WHERE tenant_id = %s GROUP BY type",
                (tenant_id,),
            )
            return {row[0]: int(row[1]) for row in cur.fetchall()}

    def close(self) -> None:
        self._connection.close()


def cooldown_active(last: datetime | None, now: datetime, hours: int) -> bool:
    if last is None:
        return False
    if last.tzinfo is None:  # pragma: no cover - defensive, psycopg returns aware
        last = last.replace(tzinfo=UTC)
    return now - last < timedelta(hours=hours)
