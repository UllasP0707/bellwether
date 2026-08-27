"""Operability: metrics, traces, and contracts about the data itself.

Three things that get grouped under "observability" and answer different
questions, kept apart on purpose:

- [`metrics`](metrics.py) answers *is it healthy* — rates, latencies, lag.
  Cheap, always on, aggregate, and never labelled by a person.
- [`tracing`](tracing.py) answers *what happened to this one thing*, across
  four processes and three Kafka topics. Sampled, off unless configured.
- [`quality`](quality.py) answers *is the data still what it was*, which
  neither of the other two can see: a pipeline with perfect latency and no
  errors can be quietly ingesting half of what it did last week.

Importing this package pulls in no exporter and starts no thread.
"""

from bellwether.obs.quality import Check, DailyCounts, evaluate
from bellwether.obs.tracing import configure, inject, message_span

__all__ = ["Check", "DailyCounts", "configure", "evaluate", "inject", "message_span"]
