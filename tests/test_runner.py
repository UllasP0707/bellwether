"""Runner tests.

The runner is thin, but the two things it decides — when to publish and when to
acknowledge — are the whole at-least-once story, and neither is visible from a
handler test. Fakes rather than a broker: these assert ordering and bookkeeping,
which a real Redpanda would only make slower to check, and the end-to-end
behaviour is verified against a live cluster separately.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from bellwether.stream import runner
from bellwether.stream.runner import RunnerOptions, run_stage


@dataclass(frozen=True)
class Decision:
    key: bytes | None = b"k"
    value: bytes | None = b"v"
    reason: str | None = None

    @property
    def publishes(self) -> bool:
        return self.value is not None


class FakeMessage:
    def __init__(self, value: bytes) -> None:
        self._value = value

    def error(self) -> None:
        return None

    def value(self) -> bytes:
        return self._value


@dataclass
class FakeConsumer:
    messages: list[bytes]
    commits: int = 0
    closed: bool = False
    subscribed: list[str] = field(default_factory=list)

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = topics

    def poll(self, timeout: float) -> FakeMessage | None:
        if not self.messages:
            return None
        return FakeMessage(self.messages.pop(0))

    def commit(self, asynchronous: bool = True) -> None:
        self.commits += 1

    def close(self) -> None:
        self.closed = True


@dataclass
class FakeProducer:
    produced: list[tuple[str, bytes | None]] = field(default_factory=list)
    flushes: int = 0

    def produce(self, topic: str, **kwargs: Any) -> None:
        self.produced.append((topic, kwargs.get("value")))

    def poll(self, timeout: float) -> None:
        return None

    def flush(self, timeout: float) -> int:
        self.flushes += 1
        return 0


@pytest.fixture
def broker(monkeypatch: pytest.MonkeyPatch) -> tuple[list[bytes], list[Any]]:
    """Swap the Kafka clients for fakes and hand back the instances created."""
    inbox: list[bytes] = []
    created: list[Any] = []

    def fake_consumer(bootstrap: str, options: RunnerOptions) -> FakeConsumer:
        consumer = FakeConsumer(messages=inbox)
        created.append(consumer)
        return consumer

    def fake_producer(bootstrap: str) -> FakeProducer:
        producer = FakeProducer()
        created.append(producer)
        return producer

    monkeypatch.setattr(runner, "_consumer", fake_consumer)
    monkeypatch.setattr(runner, "_producer", fake_producer)
    return inbox, created


def drive(
    inbox: list[bytes], created: list[Any], **options: Any
) -> tuple[FakeConsumer, FakeProducer]:
    run_stage(
        source_topic="in",
        handle=lambda raw: Decision(value=raw),
        route=lambda _: "out",
        bootstrap="fake:9092",
        options=RunnerOptions(idle_timeout=0.0, **options),
    )
    return created[0], created[1]


def test_an_empty_topic_is_not_a_crash(broker: tuple[list[bytes], list[Any]]) -> None:
    """A real bug. librdkafka raises _NO_OFFSET when asked to commit nothing.

    The runner committed unconditionally after its loop, so every stage crashed
    on startup against a topic with nothing in it — which is the normal state of
    a freshly created topic and of any stage that has caught up.
    """
    inbox, created = broker
    consumer, _ = drive(inbox, created)

    assert consumer.commits == 0
    assert consumer.closed


def test_committing_every_message_does_not_commit_a_trailing_nothing(
    broker: tuple[list[bytes], list[Any]],
) -> None:
    """The intervention stage commits eagerly, so it hits this on every run.

    With `commit_every=1` the last message commits inside the loop and the
    post-loop commit has nothing left to acknowledge.
    """
    inbox, created = broker
    inbox.extend([b"a", b"b", b"c"])
    consumer, _ = drive(inbox, created, commit_every=1)

    assert consumer.commits == 3


def test_a_partial_batch_is_committed_on_the_way_out(
    broker: tuple[list[bytes], list[Any]],
) -> None:
    inbox, created = broker
    inbox.extend([b"a", b"b", b"c"])
    consumer, _ = drive(inbox, created, commit_every=10)

    assert consumer.commits == 1


def test_output_is_flushed_before_input_is_acknowledged(
    broker: tuple[list[bytes], list[Any]],
) -> None:
    """The ordering that makes at-least-once safe rather than lossy."""
    inbox, created = broker
    inbox.extend([b"a"])
    consumer, producer = drive(inbox, created, commit_every=1)

    assert producer.flushes >= 1
    assert [value for _, value in producer.produced] == [b"a"]
    assert consumer.commits == 1


def test_a_decision_that_publishes_nothing_still_advances_the_offset(
    broker: tuple[list[bytes], list[Any]],
) -> None:
    """A suppressed intervention is still work done; not committing it replays it."""
    inbox, created = broker
    inbox.extend([b"a", b"b"])
    run_stage(
        source_topic="in",
        handle=lambda raw: Decision(value=None),
        route=lambda _: "out",
        bootstrap="fake:9092",
        options=RunnerOptions(idle_timeout=0.0, commit_every=10),
    )
    consumer, producer = created[0], created[1]

    assert producer.produced == []
    assert consumer.commits == 1
