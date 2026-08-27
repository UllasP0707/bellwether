"""Kafka plumbing for the stream stages.

Deliberately thin, and shared. Every stage is a handler that takes bytes and
returns a decision; this file moves messages, routes what a decision wants
published, and manages offsets. Nothing here knows what a risk score is.

**It is also the only place any stage is instrumented.** That is a consequence
of the shape rather than a separate decision: a handler already reports what it
decided, so counting outcomes, timing the handler and continuing the trace can
all happen once here instead of three times in three stages that would drift
apart. Consumer lag has to live here regardless — it is the one number no
handler can compute, because only the broker knows where the end of the log is.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from bellwether.config import Topics, settings
from bellwether.interventions.handler import InterventionStage, InterventionStats
from bellwether.obs import metrics, tracing
from bellwether.stream.normalizer import Normalizer, NormalizerStats, Outcome
from bellwether.stream.scorer import Scorer, ScorerStats

if TYPE_CHECKING:  # pragma: no cover
    from confluent_kafka import Consumer, Message, Producer

# How often to ask the broker where the end of each partition is. Lag is a
# gauge nobody reads between scrapes, and `get_watermark_offsets` is a network
# round trip per partition, so doing it per message would make the metric more
# expensive than the work it measures.
LAG_INTERVAL_SECONDS = 5.0


class StageDecision(Protocol):
    """What a handler decided about one message."""

    @property
    def key(self) -> bytes | None: ...

    @property
    def value(self) -> bytes | None: ...

    @property
    def reason(self) -> str | None: ...

    @property
    def publishes(self) -> bool: ...


@dataclass
class RunnerOptions:
    """How long to run and how eagerly to commit.

    `commit_every` trades duplicate work against commit overhead. Larger batches
    commit less often, so a crash replays more; because every stage is
    idempotent on `event_id`, replaying is cheap and this can be tuned for
    throughput rather than for correctness.
    """

    group_id: str = "bellwether"
    commit_every: int = 500
    idle_timeout: float = 5.0
    max_messages: int | None = None
    from_beginning: bool = True


def _consumer(bootstrap: str, options: RunnerOptions) -> Consumer:
    from confluent_kafka import Consumer

    return Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": options.group_id,
            "auto.offset.reset": "earliest" if options.from_beginning else "latest",
            # Offsets are committed by hand, after the resulting messages are
            # durably produced. Auto-commit would acknowledge input the stage
            # has not finished acting on, which is how a rebalance loses data.
            "enable.auto.commit": False,
            # Long enough that a slow scoring pass over a big window does not
            # look like a dead consumer and trigger a needless rebalance.
            "max.poll.interval.ms": 300_000,
        }
    )


def _producer(bootstrap: str) -> Producer:
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "linger.ms": 20,
            "compression.type": "lz4",
            "enable.idempotence": True,
            "acks": "all",
        }
    )


def _report_lag(consumer: Consumer, stage: str) -> None:
    """Publish `high watermark - position` for every partition this member owns.

    Best-effort by design. Lag is a diagnostic, and a broker that is slow to
    answer a metadata call must not take down the consumer that was asking —
    the stage failing because its monitoring failed is strictly worse than
    having no monitoring.
    """
    try:
        assignment = consumer.assignment()
        if not assignment:
            return
        for partition in consumer.position(assignment):
            if partition.offset < 0:
                continue
            _, high = consumer.get_watermark_offsets(partition, timeout=1.0, cached=True)
            metrics.consumer_lag.labels(
                stage=stage, topic=partition.topic, partition=str(partition.partition)
            ).set(max(0, high - partition.offset))
    except Exception:  # pragma: no cover - a broker hiccup is not a stage failure
        return


def _headers(message: Message) -> list[tuple[str, bytes | None]]:
    """Kafka headers as a list of pairs, whatever shape the client hands back.

    `confluent_kafka` types `headers()` as a dict *or* a list of pairs, and
    duplicate keys are legal on the wire, so the list is the honest form.
    """
    raw = message.headers()
    if raw is None:
        return []
    pairs = raw.items() if isinstance(raw, dict) else raw
    return [
        (key, value if value is None or isinstance(value, bytes) else str(value).encode())
        for key, value in pairs
    ]


def run_stage(
    source_topic: str,
    handle: Callable[[bytes], StageDecision],
    route: Callable[[StageDecision], str],
    bootstrap: str | None = None,
    options: RunnerOptions | None = None,
    processed: Callable[[], int] | None = None,
    stage: str = "stage",
) -> None:
    """Consume, hand each message to `handle`, publish, commit.

    Stops after `max_messages`, or once the source has been quiet for
    `idle_timeout` seconds, which makes a stage usable both as a long-running
    service and as a step in a scripted demo.
    """
    from confluent_kafka import KafkaError

    bootstrap = bootstrap or settings().kafka_bootstrap
    options = options or RunnerOptions()

    consumer = _consumer(bootstrap, options)
    producer = _producer(bootstrap)
    consumer.subscribe([source_topic])

    since_commit = 0
    last_message_at = time.monotonic()
    last_lag_at = 0.0

    def commit() -> None:
        """Flush produced messages before acknowledging their inputs.

        This ordering is the at-least-once guarantee: if the process dies
        between the flush and the commit, the input is redelivered and the
        duplicates are absorbed downstream. Committing first would drop anything
        still sitting in the producer's queue.

        Committing nothing is not the same as committing zero messages:
        librdkafka raises `_NO_OFFSET` when asked to commit with no offsets
        stored. That made the unconditional commit after the loop a crash in two
        real cases — running a stage against an empty topic, and any stage whose
        message count is an exact multiple of `commit_every`, which the
        intervention stage hits on every single message because it commits
        eagerly.
        """
        nonlocal since_commit
        if since_commit == 0:
            return
        producer.flush(30)
        consumer.commit(asynchronous=False)
        metrics.stage_commits.labels(stage=stage).inc()
        since_commit = 0

    try:
        while True:
            if options.max_messages and processed and processed() >= options.max_messages:
                break

            now = time.monotonic()
            if now - last_lag_at > LAG_INTERVAL_SECONDS:
                _report_lag(consumer, stage)
                last_lag_at = now

            message = consumer.poll(0.5)
            if message is None:
                if time.monotonic() - last_message_at > options.idle_timeout:
                    break
                continue

            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"consumer error: {error}")

            last_message_at = time.monotonic()
            payload = message.value()
            if payload is None:
                continue

            # The span continues whatever trace produced this message rather
            # than starting a new one, which is what makes a connector fetch
            # and the intervention it eventually causes the same trace across
            # three topics and four processes.
            with tracing.message_span(
                f"{stage} handle", headers=_headers(message), topic=source_topic
            ) as span:
                with metrics.timed(metrics.stage_handle_seconds, stage=stage):
                    decision = handle(payload)

                outcome = str(getattr(decision, "outcome", "handled"))
                metrics.stage_messages.labels(stage=stage, outcome=outcome).inc()
                span.set_attribute("bellwether.outcome", outcome)
                if decision.reason:
                    span.set_attribute("bellwether.reason", decision.reason)

                if decision.publishes:
                    target = route(decision)
                    # The reason rides as a header rather than in the body so a
                    # dead letter stays byte-identical to what arrived — the
                    # point of keeping it is to be able to replay the original.
                    # `traceparent` rides alongside it for the same reason: the
                    # payload is a contract, and threading a trace id through
                    # it would make observability a schema change.
                    outgoing: list[tuple[str, str | bytes | None]] = list(tracing.inject())
                    if decision.reason:
                        outgoing.append(("reason", decision.reason.encode()))
                    producer.produce(
                        target, key=decision.key, value=decision.value, headers=outgoing or None
                    )
                    producer.poll(0)
                    metrics.stage_published.labels(stage=stage, topic=target).inc()

            since_commit += 1
            if since_commit >= options.commit_every:
                commit()

        commit()
    finally:
        producer.flush(30)
        consumer.close()


def run_normalizer(
    bootstrap: str | None = None,
    normalizer: Normalizer | None = None,
    options: RunnerOptions | None = None,
    source_topic: str = Topics.RAW,
    target_topic: str = Topics.NORMALIZED,
    dlq_topic: str = Topics.DLQ,
) -> NormalizerStats:
    """Re-key `events.raw` onto `events.normalized`, deduplicating."""
    normalizer = normalizer or Normalizer()

    run_stage(
        source_topic=source_topic,
        handle=normalizer.handle,
        route=lambda d: (
            dlq_topic if getattr(d, "outcome", None) is Outcome.DEAD_LETTERED else target_topic
        ),
        bootstrap=bootstrap,
        options=options or RunnerOptions(group_id="bellwether-normalizer"),
        processed=lambda: normalizer.stats.total,
        stage="normalizer",
    )
    return normalizer.stats


def run_scorer(
    scorer: Scorer,
    bootstrap: str | None = None,
    options: RunnerOptions | None = None,
    source_topic: str = Topics.NORMALIZED,
    target_topic: str = Topics.SCORES,
) -> ScorerStats:
    """Score `events.normalized` onto the compacted `risk.scores` topic."""
    run_stage(
        source_topic=source_topic,
        handle=scorer.handle,
        route=lambda _: target_topic,
        bootstrap=bootstrap,
        options=options or RunnerOptions(group_id="bellwether-scorer"),
        processed=lambda: scorer.stats.total,
        stage="scorer",
    )
    return scorer.stats


def run_interventions(
    stage: InterventionStage,
    bootstrap: str | None = None,
    options: RunnerOptions | None = None,
    source_topic: str = Topics.SCORES,
    target_topic: str = Topics.INTERVENTIONS,
) -> InterventionStats:
    """Decide interventions from `risk.scores` onto `bellwether.interventions`.

    `commit_every` is 1 by default here, unlike the other stages. Elsewhere a
    larger batch is free because reprocessing is inert; this stage's side effect
    is a row in the intervention ledger, and while the uniqueness index makes a
    duplicate *send* impossible, committing promptly keeps the amount of work
    replayed after a crash small enough to reason about.
    """
    run_stage(
        source_topic=source_topic,
        handle=stage.handle,
        route=lambda _: target_topic,
        bootstrap=bootstrap,
        options=options or RunnerOptions(group_id="bellwether-interventions", commit_every=1),
        processed=lambda: stage.stats.total,
        stage="intervention",
    )
    return stage.stats
