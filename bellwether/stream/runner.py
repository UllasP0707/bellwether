"""Kafka plumbing for the stream stages.

Deliberately thin, and shared. Every stage is a handler that takes bytes and
returns a decision; this file moves messages, routes what a decision wants
published, and manages offsets. Nothing here knows what a risk score is.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from bellwether.config import Topics, settings
from bellwether.interventions.handler import InterventionStage, InterventionStats
from bellwether.stream.normalizer import Normalizer, NormalizerStats, Outcome
from bellwether.stream.scorer import Scorer, ScorerStats

if TYPE_CHECKING:  # pragma: no cover
    from confluent_kafka import Consumer, Producer


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


def run_stage(
    source_topic: str,
    handle: Callable[[bytes], StageDecision],
    route: Callable[[StageDecision], str],
    bootstrap: str | None = None,
    options: RunnerOptions | None = None,
    processed: Callable[[], int] | None = None,
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

    def commit() -> None:
        """Flush produced messages before acknowledging their inputs.

        This ordering is the at-least-once guarantee: if the process dies
        between the flush and the commit, the input is redelivered and the
        duplicates are absorbed downstream. Committing first would drop anything
        still sitting in the producer's queue.
        """
        nonlocal since_commit
        producer.flush(30)
        consumer.commit(asynchronous=False)
        since_commit = 0

    try:
        while True:
            if options.max_messages and processed and processed() >= options.max_messages:
                break

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

            decision = handle(payload)
            if decision.publishes:
                # The reason rides as a header rather than in the body so a
                # dead letter stays byte-identical to what arrived — the point
                # of keeping it is to be able to replay the original.
                headers: list[tuple[str, str | bytes | None]] | None = (
                    [("reason", (decision.reason or "").encode())] if decision.reason else None
                )
                producer.produce(
                    route(decision), key=decision.key, value=decision.value, headers=headers
                )
                producer.poll(0)

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
    )
    return stage.stats
