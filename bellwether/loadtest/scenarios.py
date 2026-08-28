"""What gets measured, and why each measurement is the one that matters.

The load test is not "how many events per second" — that number alone is a
boast, and it moves with the machine. It is *where this breaks and what breaks
first*, which needs the phases isolated so a single slow number can be
attributed to something.

Four scenarios, each isolating one suspect:

- **`scoring`** takes the broker, Redis, Kafka and the network out entirely and
  times `score_events` against windows of increasing size. This is the one that
  settles the standing question in DESIGN.md: the scorer recomputes the whole
  window on every event, which is O(window) per message, and until now that has
  been an argument rather than a curve.
- **`window`** adds the Redis round trips back and measures what per-event
  state costs on top of the arithmetic.
- **`stages`** runs the real consumers against the real broker, so the number
  includes deserialisation, the dimension lookup, publishing and committing.
- **`api`** hammers the read path concurrently, because the read path has a
  known limitation — one Postgres connection per store, no pool — and a claim
  like that should be a measurement.

Every scenario is deterministic given a seed, so a change in a number means a
change in the code rather than a change in the weather.
"""

from __future__ import annotations

import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

from bellwether.events.schema import Employee, Source
from bellwether.loadtest.harness import Result, Span, Timing
from bellwether.scoring import score_events
from bellwether.scoring.catalog import CATALOG
from bellwether.stream.store import EventWindow, WindowedEvent

AS_OF = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)

# The sizes are log-spaced rather than linear because the question is the
# *shape* of the curve. Ten evenly spaced points between 100 and 1000 would
# show a straight line whatever the complexity class is.
WINDOW_SIZES = (10, 50, 100, 500, 1_000, 5_000, 10_000)


def subject() -> Employee:
    """An ordinary employee.

    Not a high-value target: the multiplier is applied once at the end, so it
    changes the answer and not the amount of work, and using it here would
    only make the numbers harder to compare against the catalog weights.
    """
    return Employee(
        employee_id="E0001",
        tenant_id="acme",
        department="engineering",
        seniority="mid",
        tenure_days=500,
        location="Remote US",
    )


def window_of(size: int, seed: int = 7) -> list[WindowedEvent]:
    """A realistic window: mixed signals, spread across the lookback.

    Mixed rather than uniform because scoring groups by signal and then by
    category, so a window of one repeated signal would measure a
    smaller dictionary than any real employee has.
    """
    rng = random.Random(seed)
    signals = list(CATALOG)
    return [
        WindowedEvent(
            employee_id="E0001",
            signal=rng.choice(signals),
            occurred_at=AS_OF - timedelta(seconds=rng.randint(0, 30 * 86_400)),
            event_id=f"evt-{index}",
        )
        for index in range(size)
    ]


def scoring(repeats: int = 200) -> list[Result]:
    """Time the pure scoring function against growing windows.

    No broker, no Redis, no network. Whatever this shows is a property of the
    algorithm and reproduces on any machine.
    """
    results: list[Result] = []
    for size in WINDOW_SIZES:
        events = window_of(size)
        person = subject()
        timing = Timing()
        # Fewer repeats for the big windows: the point is a stable median, and
        # ten thousand events scored two hundred times is a minute of nothing.
        rounds = max(5, repeats // max(1, size // 100))
        for _ in range(rounds):
            with timing.measure():
                score_events(person, events, as_of=AS_OF, lookback_days=30)
        results.append(
            Result(
                name=f"score, {size:,}-event window",
                count=rounds,
                seconds=sum(timing.samples) / 1000.0,
                timing=timing,
                note=f"{timing.percentile(50) * 1000 / max(size, 1):.2f} us/event",
            )
        )
    return results


def window_io(store: EventWindow, events: int = 2_000) -> list[Result]:
    """What the per-event Redis state costs on top of the arithmetic.

    Two round trips per message in the current design — add the event to the
    sorted set, then read the whole window back — so this is measured as the
    pair rather than separately. Splitting them would make each look fast and
    hide that the stage does both, every time.
    """
    add_timing, read_timing = Timing(), Timing()
    population = [f"E{index:04d}" for index in range(200)]
    rng = random.Random(11)
    signals = list(CATALOG)

    for index in range(events):
        employee = rng.choice(population)
        event = WindowedEvent(
            employee_id=employee,
            signal=rng.choice(signals),
            occurred_at=AS_OF - timedelta(seconds=rng.randint(0, 30 * 86_400)),
            event_id=f"load-{index}",
        )
        with add_timing.measure():
            store.add(event, lookback_days=30, as_of=AS_OF)
        with read_timing.measure():
            store.events(employee)

    return [
        Result("window add", events, sum(add_timing.samples) / 1000.0, add_timing),
        Result("window read", events, sum(read_timing.samples) / 1000.0, read_timing),
    ]


LOAD_RAW = "bellwether.loadtest.raw"
LOAD_NORMALIZED = "bellwether.loadtest.normalized"


def reset_topics(bootstrap: str, names: tuple[str, ...], partitions: int = 6) -> None:
    """Drop and recreate the load-test topics.

    Isolated topics rather than the real ones, and recreated rather than
    reused, because the first attempt at this measured nothing at all. A fresh
    consumer group reading from the tail starts *after* the messages the test
    just produced, and reading from the beginning of the real topic would
    reprocess every event the pipeline has ever seen. Neither number is the
    throughput of this run.
    """
    from confluent_kafka.admin import AdminClient
    from confluent_kafka.cimpl import NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    doomed = [name for name in names if name in existing]
    if doomed:
        for future in admin.delete_topics(doomed, operation_timeout=30).values():
            future.result()
        # Deletion is asynchronous even once the future resolves, and creating
        # a topic the broker is still tearing down fails intermittently.
        time.sleep(2)

    created = admin.create_topics([NewTopic(n, partitions, 1) for n in names])
    for future in created.values():
        try:
            future.result()
        except Exception as err:  # pragma: no cover - already-exists is fine
            if "already exists" not in str(err).lower():
                raise


def clear_redis(url: str, pattern: str) -> int:
    """Delete the load test's own keys. Returns how many.

    `scan_iter` rather than `KEYS`, which blocks the whole server for the
    length of the scan -- a habit worth keeping even against a throwaway
    keyspace, because the version of this that reaches production is always
    the one that was already written.
    """
    import redis as redis_client

    client = redis_client.Redis.from_url(url)
    keys = list(client.scan_iter(match=pattern, count=500))
    if keys:
        client.delete(*keys)
    return len(keys)


def pipeline(
    bootstrap: str, redis_url: str, dsn: str, count: int = 2_000, store: str = "redis"
) -> list[Result]:
    """Produce, normalize and score against the real broker, and time all three.

    The events are built here rather than taken from the simulator, for one
    reason that the first attempt at this made unavoidable: `generate live`
    at an accelerated rate emits a phishing chain whose later steps are dated
    up to ninety minutes *ahead* of the wall clock, so measuring
    `now - ingested_at` against it produced a p50 latency of minus twenty-four
    seconds. That is a fine thing for a demo generator to do and a useless
    basis for an SLO, so these events carry `ingested_at = now`, which is what
    a connector would actually stamp.
    """
    from bellwether.dimension import PostgresEmployeeRepository
    from bellwether.events.schema import BehaviorEvent
    from bellwether.generator.sinks import KafkaSink
    from bellwether.stream.normalizer import Normalizer
    from bellwether.stream.runner import RunnerOptions, run_normalizer, run_scorer
    from bellwether.stream.scorer import Scorer
    from bellwether.stream.store import InMemoryOnlineStore, RedisOnlineStore

    employees = PostgresEmployeeRepository(dsn, tenant_id="acme")
    people = [e.employee_id for e in employees.all()] or ["E0001"]
    rng = random.Random(23)
    signals = list(CATALOG)
    group = f"loadtest-{int(datetime.now(UTC).timestamp())}"

    reset_topics(bootstrap, (LOAD_RAW, LOAD_NORMALIZED))
    sink = KafkaSink(LOAD_RAW, bootstrap)
    started = datetime.now(UTC)
    for index in range(count):
        stamp = datetime.now(UTC)
        sink.write(
            BehaviorEvent(
                tenant_id="acme",
                employee_id=rng.choice(people),
                signal=rng.choice(signals),
                # Recent but not identical, so the window has some spread; the
                # ingest timestamp is the honest one and the SLO reads it.
                occurred_at=stamp - timedelta(seconds=rng.uniform(0, 120)),
                ingested_at=stamp,
                source=Source.ENDPOINT_AGENT,
                source_event_id=f"{group}-{index}",
            )
        )
    sink.close()
    produce_seconds = (datetime.now(UTC) - started).total_seconds()

    # Fresh topics, so reading from the beginning reads exactly this run.
    options = RunnerOptions(group_id=f"{group}-n", idle_timeout=6.0, from_beginning=True)
    normalizer = Normalizer()
    normalize_span = Span()
    normalizer.handle = normalize_span.wrap(normalizer.handle)  # type: ignore[assignment]
    normalizer_stats = run_normalizer(
        bootstrap=bootstrap,
        normalizer=normalizer,
        options=options,
        source_topic=LOAD_RAW,
        target_topic=LOAD_NORMALIZED,
    )

    # Swappable so the scorer's ceiling can be attributed rather than guessed:
    # the same run against an in-memory window isolates exactly how much of the
    # per-message budget is Redis round trips.
    # A load-test tenant, not `acme`, for two reasons that only showed up once
    # the numbers were compared. Writing into the real keyspace meant the load
    # test was overwriting the projection the dashboard serves. And reading it
    # meant the Redis run scored windows already full of thirty days of real
    # history while the in-memory run started empty — so the first comparison
    # between them was partly measuring window size rather than round trips.
    #
    # Annotated as the concrete union rather than as the protocols: both stores
    # satisfy `EventWindow` *and* `ScoreState`, and a union of two protocols
    # satisfies neither of them.
    online: RedisOnlineStore | InMemoryOnlineStore = (
        RedisOnlineStore(redis_url, tenant_id="loadtest", namespace="lt")
        if store == "redis"
        else InMemoryOnlineStore()
    )
    # Clear the window between runs, or a run is not comparable to the one
    # before it. Leaving it warm made five identical invocations report 807,
    # 260, 783, 737 and 389 messages per second -- not noise, but the window
    # growing by five thousand events each time and the O(window) rescore
    # faithfully getting slower. A real finding, and useless as a baseline.
    if isinstance(online, RedisOnlineStore):
        clear_redis(redis_url, "lt:*")

    scorer = Scorer(employees=employees, window=online, state=online)
    score_span = Span()
    scorer.handle = score_span.wrap(scorer.handle)  # type: ignore[assignment]
    scorer_stats = run_scorer(
        scorer=scorer,
        bootstrap=bootstrap,
        options=RunnerOptions(group_id=f"{group}-s", idle_timeout=6.0, from_beginning=True),
        source_topic=LOAD_NORMALIZED,
        target_topic="bellwether.loadtest.scores",
    )

    end_to_end = Timing()
    for sample in scorer_stats.pipeline_latencies_ms:
        end_to_end.add(sample)

    return [
        Result("produce -> events.raw", count, produce_seconds),
        Result("normalize", normalizer_stats.emitted, normalize_span.seconds),
        Result("score", scorer_stats.scored, score_span.seconds),
        Result(
            "backlog drain (ingest -> scored)",
            len(end_to_end),
            score_span.seconds,
            end_to_end,
            note=f"{scorer_stats.future_dated} future-dated" if scorer_stats.future_dated else "",
        ),
    ]


def api(
    base_url: str,
    key: str,
    requests: int = 500,
    concurrency: int = 16,
    employees: tuple[str, ...] = ("E0042", "E0208", "E0069"),
) -> list[Result]:
    """Concurrent reads against a running API.

    Two endpoints with different shapes on purpose. The ranking is one Redis
    range query and should be flat; the per-employee lookup touches Redis,
    Postgres for the dimension, and Postgres again to write an audit row before
    the response is built, so it is the one where the missing connection pool
    should show up.
    """
    import httpx

    paths = {
        "GET /population/ranking": ["/v1/population/ranking?limit=25"],
        "GET /employees/{id}/score": [f"/v1/employees/{e}/score" for e in employees],
        "GET /population/departments": ["/v1/population/departments"],
    }

    results: list[Result] = []
    with httpx.Client(base_url=base_url, headers={"X-API-Key": key}, timeout=30.0) as client:
        for name, candidates in paths.items():
            timing, errors = Timing(), 0

            def once(
                index: int, candidates: list[str] = candidates, timing: Timing = timing
            ) -> int:
                path = candidates[index % len(candidates)]
                with timing.measure():
                    response = client.get(path)
                return 0 if response.status_code == 200 else 1

            started = datetime.now(UTC)
            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                errors = sum(pool.map(once, range(requests)))
            elapsed = (datetime.now(UTC) - started).total_seconds()

            results.append(
                Result(
                    name=f"{name} x{concurrency}",
                    count=requests,
                    seconds=elapsed,
                    timing=timing,
                    errors=errors,
                )
            )
    return results
