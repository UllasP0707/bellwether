"""Measurement primitives for the load test.

Small on purpose. The interesting content of a load test is the scenarios and
the conclusion, not the timing code, and timing code that is clever is timing
code nobody trusts.

Two things here are deliberate rather than incidental.

**`perf_counter`, not `time.time`.** A wall clock can step backwards over an
NTP correction and produce a negative duration, which shows up as an
impossible percentile in a document somebody is meant to believe.

**Percentiles from the full sample, not a sketch.** A run here is at most a few
hundred thousand observations, which is a few megabytes held for a few seconds
— cheap enough that there is no reason to accept the error a t-digest would
introduce into the number the whole exercise exists to produce. At production
volume this would be exactly the wrong choice, and that is the point: the
constraint that makes the simple option correct is stated rather than assumed.
"""

from __future__ import annotations

import statistics
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field


@dataclass
class Timing:
    """Durations in milliseconds, with the percentiles that matter."""

    samples: list[float] = field(default_factory=list)

    def add(self, milliseconds: float) -> None:
        self.samples.append(milliseconds)

    @contextmanager
    def measure(self) -> Iterator[None]:
        started = time.perf_counter()
        try:
            yield
        finally:
            self.add((time.perf_counter() - started) * 1000.0)

    def percentile(self, p: float) -> float:
        if not self.samples:
            return 0.0
        ordered = sorted(self.samples)
        # `min` rather than a bare index because p=100 would otherwise walk off
        # the end, and asking for p100 is a reasonable thing to do.
        return ordered[min(int(p / 100 * len(ordered)), len(ordered) - 1)]

    @property
    def mean(self) -> float:
        return statistics.fmean(self.samples) if self.samples else 0.0

    def __len__(self) -> int:
        return len(self.samples)


@dataclass
class Result:
    """What one scenario measured.

    `errors` is separate from `count` because a load test that reports
    throughput while quietly failing half its requests is reporting the speed
    of failing.
    """

    name: str
    count: int
    seconds: float
    timing: Timing = field(default_factory=Timing)
    errors: int = 0
    note: str = ""

    @property
    def rate(self) -> float:
        """Operations per second."""
        return self.count / self.seconds if self.seconds > 0 else 0.0

    @property
    def row(self) -> tuple[str, str, str, str, str, str, str]:
        return (
            self.name,
            f"{self.count:,}",
            f"{self.seconds:.2f}s",
            f"{self.rate:,.0f}/s",
            f"{self.timing.percentile(50):.2f}",
            f"{self.timing.percentile(95):.2f}",
            f"{self.timing.percentile(99):.2f}",
        )


HEADERS = ("scenario", "n", "wall", "rate", "p50 ms", "p95 ms", "p99 ms")


def run(name: str, count: int, work: Callable[[], None], note: str = "") -> Result:
    """Time a block of work as one unit. For phases with no per-item timing."""
    started = time.perf_counter()
    work()
    return Result(name=name, count=count, seconds=time.perf_counter() - started, note=note)


@dataclass
class Span:
    """Wall time between the first and last message a stage actually handled.

    This exists because the obvious measurement is wrong. Timing a consumer
    from process start to process exit includes the consumer-group join and
    the idle timeout it waits out before deciding the topic is quiet, and
    subtracting a constant for the idle period does not fix it: group join
    against a freshly created topic takes anywhere from a fraction of a second
    to several. Measuring throughput that way produced 807, 260 and 783
    messages per second on three consecutive identical runs, and the system
    under test was not what varied.

    Wrapping the handler and recording the first and last timestamps measures
    the interval during which the stage was doing the work.
    """

    first: float | None = None
    last: float = 0.0
    handled: int = 0

    def wrap(self, handler: Callable[..., object]) -> Callable[..., object]:
        def timed(*args: object, **kwargs: object) -> object:
            now = time.perf_counter()
            if self.first is None:
                self.first = now
            result = handler(*args, **kwargs)
            self.last = time.perf_counter()
            self.handled += 1
            return result

        return timed

    @property
    def seconds(self) -> float:
        if self.first is None:
            return 0.0
        # A single message has no interval. Floor it rather than report an
        # infinite rate, and note that one message is not a throughput sample.
        return max(self.last - self.first, 1e-6)
