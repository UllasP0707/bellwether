"""HTTP client for polling vendor APIs.

Everything interesting about talking to a vendor is failure handling, so that is
what this wraps: retry on 429 and 5xx, honour `Retry-After` when the vendor
sends it, exponential backoff with jitter when it doesn't, and give up loudly
rather than silently returning a short page.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Any

import httpx


class ConnectorError(RuntimeError):
    """A vendor call failed in a way retrying will not fix."""


@dataclass
class ClientStats:
    """Counters worth surfacing: a connector that silently retries 400 times a
    minute looks healthy from the outside and is not."""

    requests: int = 0
    retries: int = 0
    rate_limited: int = 0
    server_errors: int = 0
    total_wait_seconds: float = 0.0


@dataclass
class VendorClient:
    """A polling client with retry and backoff.

    Args:
        base_url: Vendor root, e.g. `http://localhost:8900`.
        max_attempts: Total tries per request, including the first.
        base_delay: First backoff interval; doubles per attempt.
        max_delay: Ceiling on any single backoff.
    """

    base_url: str
    max_attempts: int = 5
    base_delay: float = 0.25
    max_delay: float = 8.0
    timeout: float = 10.0
    stats: ClientStats = field(default_factory=ClientStats)
    _client: httpx.Client | None = field(default=None, repr=False)
    _rng: random.Random = field(default_factory=lambda: random.Random(0), repr=False)

    def __post_init__(self) -> None:
        if self._client is None:
            self._client = httpx.Client(base_url=self.base_url, timeout=self.timeout)

    @classmethod
    def wrapping(cls, client: httpx.Client, **kwargs: Any) -> VendorClient:
        """Build a client around an existing transport.

        Lets tests drive a connector against an in-process ASGI app with no
        socket, while the connector runs exactly the code it would in
        production.
        """
        return cls(base_url=str(client.base_url), _client=client, **kwargs)

    @property
    def client(self) -> httpx.Client:
        if self._client is None:
            raise ConnectorError("client is closed")
        return self._client

    def _backoff(self, attempt: int, retry_after: str | None) -> float:
        """How long to wait before the next attempt.

        A vendor's own `Retry-After` wins when present — it knows when its
        window resets and guessing shorter just burns quota. Otherwise
        exponential with jitter, because synchronised retries across connectors
        would arrive as a thundering herd exactly when the vendor is unhealthy.
        """
        if retry_after:
            try:
                return min(float(retry_after), self.max_delay)
            except ValueError:
                pass
        exponential = min(self.base_delay * (2.0**attempt), self.max_delay)
        # Cap after jitter, not before. Jitter spans 0.5x-1.5x, so capping first
        # would let the delay reach 1.5x max_delay and quietly break the ceiling
        # this parameter exists to guarantee.
        return min(exponential * (0.5 + self._rng.random()), self.max_delay)

    def get(self, path: str, params: dict[str, Any] | None = None) -> httpx.Response:
        """GET with retry. Raises ConnectorError once attempts are exhausted."""
        last_status: int | None = None

        for attempt in range(self.max_attempts):
            self.stats.requests += 1
            response = self.client.get(path, params=params)
            last_status = response.status_code

            if response.status_code == 429:
                self.stats.rate_limited += 1
            elif 500 <= response.status_code < 600:
                self.stats.server_errors += 1
            else:
                response.raise_for_status()
                return response

            if attempt == self.max_attempts - 1:
                break

            delay = self._backoff(attempt, response.headers.get("Retry-After"))
            self.stats.retries += 1
            self.stats.total_wait_seconds += delay
            time.sleep(delay)

        raise ConnectorError(
            f"GET {path} failed after {self.max_attempts} attempts (last status {last_status})"
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
