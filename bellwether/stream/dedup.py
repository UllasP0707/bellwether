"""Duplicate suppression.

Delivery is at-least-once end to end: connectors commit their cursor after
emitting, and consumers commit offsets after producing. Both choices prefer
redelivery to loss, which is only reasonable if something downstream can tell
that a record has been seen before.

That is this. Keyed on `event_id`, which connectors derive deterministically
from the vendor's own record id, so the same source record always produces the
same key no matter how many times it is reprocessed.
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Protocol

# Long enough to cover a connector replaying its cursor or a consumer group
# rebalancing, short enough that the key space stays bounded. A deliberate
# reprocess of older history is expected to re-emit, and downstream scoring is
# idempotent over event_id anyway, so this is an optimisation rather than a
# correctness boundary.
DEFAULT_TTL_SECONDS = 7 * 24 * 3600


class DedupStore(Protocol):
    """Remembers which event ids have been seen."""

    def seen(self, event_id: str) -> bool:
        """Record `event_id` and return whether it was already present."""
        ...


class InMemoryDedup:
    """Bounded LRU set, for tests and single-process runs.

    Bounded rather than unbounded because an unbounded dedup set in a
    long-running consumer is a memory leak with a slow fuse.
    """

    def __init__(self, capacity: int = 100_000) -> None:
        self.capacity = capacity
        self._seen: OrderedDict[str, None] = OrderedDict()

    def seen(self, event_id: str) -> bool:
        if event_id in self._seen:
            self._seen.move_to_end(event_id)
            return True
        self._seen[event_id] = None
        if len(self._seen) > self.capacity:
            self._seen.popitem(last=False)
        return False

    def __len__(self) -> int:
        return len(self._seen)


class RedisDedup:
    """Shared dedup across consumer instances.

    Uses `SET NX EX`, which is atomic: two consumers processing the same
    redelivered event concurrently cannot both conclude they are first. An
    exists-then-set pair would race exactly when a rebalance makes it likely.
    """

    def __init__(self, url: str, ttl_seconds: int = DEFAULT_TTL_SECONDS, prefix: str = "dedup:"):
        import redis

        self._redis = redis.Redis.from_url(url)
        self.ttl_seconds = ttl_seconds
        self.prefix = prefix

    def seen(self, event_id: str) -> bool:
        created = self._redis.set(f"{self.prefix}{event_id}", b"1", nx=True, ex=self.ttl_seconds)
        return not created
