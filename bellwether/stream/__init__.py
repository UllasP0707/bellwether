"""Kafka consumers.

Each stage is split into a pure handler and a thin Kafka runner. The handler is
where the decisions live and it is tested directly; the runner only moves bytes
and manages offsets. That split is what keeps the interesting logic testable
without a broker.
"""

from bellwether.stream.dedup import DedupStore, InMemoryDedup, RedisDedup
from bellwether.stream.normalizer import Normalizer, NormalizerStats, Outcome

__all__ = [
    "DedupStore",
    "InMemoryDedup",
    "Normalizer",
    "NormalizerStats",
    "Outcome",
    "RedisDedup",
]
