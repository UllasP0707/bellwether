"""Where generated events go.

The generator doesn't know whether it's feeding Kafka, the lake, or a test, so
the same simulator drives the demo, the backfill, and the load test.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Protocol, TextIO

from bellwether.events.schema import BehaviorEvent
from bellwether.generator.population import PopulatedEmployee


class Sink(Protocol):
    """Anything that accepts events."""

    def write(self, event: BehaviorEvent) -> None: ...

    def close(self) -> None: ...


class JsonlSink:
    """Writes newline-delimited JSON partitioned by event date.

    Stands in for the raw lake before the S3 writer exists, and the layout
    (`dt=YYYY-MM-DD/`) is the one Spark will expect, so switching from local
    files to object storage is a path change and nothing else.

    JSONL rather than Parquet on purpose: the raw landing zone should be
    append-only and cheap to write from many producers. Columnar conversion is
    the first batch job's job, not the connector's.
    """

    def __init__(self, root: Path | str = "data/events") -> None:
        self.root = Path(root)
        self._handles: dict[str, TextIO] = {}
        self.counts: dict[str, int] = defaultdict(int)

    def write(self, event: BehaviorEvent) -> None:
        partition = event.occurred_at.strftime("%Y-%m-%d")
        handle = self._handles.get(partition)
        if handle is None:
            directory = self.root / f"dt={partition}"
            directory.mkdir(parents=True, exist_ok=True)
            handle = (directory / "events.jsonl").open("a", encoding="utf-8")
            self._handles[partition] = handle
        handle.write(event.model_dump_json() + "\n")
        self.counts[partition] += 1

    def close(self) -> None:
        for handle in self._handles.values():
            handle.close()
        self._handles.clear()


class KafkaSink:
    """Produces to a Kafka topic, keyed by employee.

    `linger.ms` is set well above the default: the generator produces in bursts,
    and batching is what makes throughput numbers in the load test reflect the
    consumer rather than per-message overhead in the producer.
    """

    def __init__(self, topic: str, bootstrap: str) -> None:
        from confluent_kafka import Producer  # imported lazily; file sinks need no broker

        self.topic = topic
        self.produced = 0
        self.failed = 0
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "linger.ms": 20,
                "compression.type": "lz4",
                "enable.idempotence": True,
                "acks": "all",
            }
        )

    def _on_delivery(self, err: object, _msg: object) -> None:
        if err is not None:
            self.failed += 1

    def write(self, event: BehaviorEvent) -> None:
        self._producer.produce(
            self.topic,
            key=event.partition_key(),
            value=event.model_dump_json().encode(),
            on_delivery=self._on_delivery,
        )
        self.produced += 1
        # Serve delivery callbacks without blocking; the queue is drained on close.
        self._producer.poll(0)

    def close(self) -> None:
        self._producer.flush(30)


class FanoutSink:
    """Writes every event to several sinks.

    Used by backfill, which needs history in both the lake (so Spark has
    something to read) and the topic (so the stream path can be replayed).
    """

    def __init__(self, *sinks: Sink) -> None:
        self.sinks = sinks

    def write(self, event: BehaviorEvent) -> None:
        for sink in self.sinks:
            sink.write(event)

    def close(self) -> None:
        for sink in self.sinks:
            sink.close()


def load_events(root: Path | str = "data/events") -> list[BehaviorEvent]:
    """Read every event back out of a JSONL lake.

    Fine for the current population; the Spark path replaces it once the volume
    stops fitting in memory.
    """
    events: list[BehaviorEvent] = []
    for path in sorted(Path(root).glob("dt=*/events.jsonl")):
        with path.open(encoding="utf-8") as handle:
            events.extend(
                BehaviorEvent.model_validate_json(line) for line in handle if line.strip()
            )
    return events


def dump_population(population: list[PopulatedEmployee], path: Path | str) -> None:
    """Write the employee dimension to JSON, including generator personas.

    Personas are stored alongside the employees so the simulator can resume with
    the same behavioral assignment, but they live under a separate key — nothing
    in the platform is allowed to read them, because inferring risk from
    behavior is the thing being demonstrated.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "employees": [member.employee.model_dump(mode="json") for member in population],
        "_generator_personas": {
            member.employee.employee_id: member.persona.name for member in population
        },
    }
    target.write_text(json.dumps(payload, indent=2), encoding="utf-8")
