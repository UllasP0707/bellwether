"""Kafka plumbing for the stream stages.

Deliberately thin. Everything that decides anything lives in a handler that
takes bytes and returns a `Decision`; this file only moves messages and manages
offsets, so the interesting behaviour stays testable without a broker.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from bellwether.config import Topics, settings
from bellwether.stream.normalizer import Decision, Normalizer, NormalizerStats, Outcome

if TYPE_CHECKING:  # pragma: no cover
    from confluent_kafka import Consumer, Producer


@dataclass
class RunnerOptions:
    """How long to run and how eagerly to commit.

    `commit_every` trades duplicate work against commit overhead. Larger batches
    commit less often, so a crash replays more; because every stage downstream
    is idempotent on `event_id`, replaying is cheap and this can be tuned for
    throughput rather than for correctness.
    """

    group_id: str = "bellwether-normalizer"
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
            # durably produced. Auto-commit would acknowledge input this stage
            # has not finished acting on, which is how a rebalance loses data.
            "enable.auto.commit": False,
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


def run_normalizer(
    bootstrap: str | None = None,
    normalizer: Normalizer | None = None,
    options: RunnerOptions | None = None,
    source_topic: str = Topics.RAW,
    target_topic: str = Topics.NORMALIZED,
    dlq_topic: str = Topics.DLQ,
) -> NormalizerStats:
    """Consume `source_topic`, normalize, produce, commit. Returns final stats.

    Stops after `max_messages`, or once the source has been quiet for
    `idle_timeout` seconds — which makes it usable both as a long-running
    service and as a batch step in a demo.
    """
    from confluent_kafka import KafkaError

    bootstrap = bootstrap or settings().kafka_bootstrap
    normalizer = normalizer or Normalizer()
    options = options or RunnerOptions()

    consumer = _consumer(bootstrap, options)
    producer = _producer(bootstrap)
    consumer.subscribe([source_topic])

    since_commit = 0
    last_message_at = time.monotonic()

    def commit() -> None:
        """Flush produced messages before acknowledging their inputs.

        This ordering is the whole at-least-once guarantee: if the process dies
        between the flush and the commit, the input is redelivered and the
        duplicates are suppressed downstream. Committing first would drop
        anything still sitting in the producer's queue.
        """
        nonlocal since_commit
        producer.flush(30)
        consumer.commit(asynchronous=False)
        since_commit = 0

    try:
        while True:
            if options.max_messages and normalizer.stats.total >= options.max_messages:
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

            decision: Decision = normalizer.handle(payload)
            if decision.publishes:
                topic = dlq_topic if decision.outcome is Outcome.DEAD_LETTERED else target_topic
                # The reason rides as a header rather than in the body so a
                # dead letter stays byte-identical to what arrived — the point
                # of keeping it is to be able to replay the original.
                headers: list[tuple[str, str | bytes | None]] | None = (
                    [("reason", (decision.reason or "").encode())] if decision.reason else None
                )
                producer.produce(
                    topic,
                    key=decision.key,
                    value=decision.value,
                    headers=headers,
                )
                producer.poll(0)

            since_commit += 1
            if since_commit >= options.commit_every:
                commit()

        commit()
    finally:
        producer.flush(30)
        consumer.close()

    return normalizer.stats
