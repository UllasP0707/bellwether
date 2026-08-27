"""Every metric Bellwether exports, declared once.

The same argument as the signal catalog, one layer out. A metric name and its
label set are a contract with whatever is querying them — a dashboard panel, an
alert rule, a capacity model — and a name invented at the call site is a name
nobody can find. Declaring them here means a rename is one edit and a
cardinality mistake is visible next to the others.

**Cardinality is the thing to get right.** Every label value is a separate time
series held in memory by every scraping Prometheus forever, so nothing here is
labelled by `employee_id`, `event_id` or `trigger_event_id`. That is a
resourcing argument and also a privacy one: a metrics endpoint is typically the
least protected surface a service exposes, and "who is risky" is exactly what
this system is careful about elsewhere. Bands and departments are bounded and
non-identifying; people are not.

**These do not replace the per-run `Stats` dataclasses.** A run summary answers
"what did this invocation do" and is printed at the end of a CLI command; a
counter answers "what is the fleet doing" and only means anything across
processes and over time. Collapsing them would either reset the counter on
every run or leave the operator with nothing to print.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

# One registry rather than the library's global default, so a test can build a
# fresh one and assert on it without the previous test's counts leaking in.
REGISTRY = CollectorRegistry()

# Latency buckets are chosen from the SLO, not from the library's defaults. The
# claim this project makes is single-digit seconds from behaviour to score, so
# the buckets need resolution either side of that; the default set tops out at
# 10s and would put every interesting failure in `+Inf`.
_PIPELINE_BUCKETS = (0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, float("inf"))
_HANDLER_BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, float("inf"))
# Generation is a different order of magnitude from everything else here, and
# deliberately so — see `CachedCopywriter`.
_COPY_BUCKETS = (0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 45.0, 60.0, float("inf"))

# --- ingestion ---------------------------------------------------------------

events_fetched = Counter(
    "bellwether_connector_records_fetched_total",
    "Vendor records read by a connector.",
    ["source"],
    registry=REGISTRY,
)
events_emitted = Counter(
    "bellwether_connector_events_emitted_total",
    "Canonical events a connector published.",
    ["source"],
    registry=REGISTRY,
)
events_dropped = Counter(
    "bellwether_connector_records_dropped_total",
    "Vendor records a connector could not turn into an event, by reason.",
    ["source", "reason"],
    registry=REGISTRY,
)
vendor_retries = Counter(
    "bellwether_connector_retries_total",
    "Requests retried after a rate limit or a server error.",
    ["source"],
    registry=REGISTRY,
)

# --- the stream stages -------------------------------------------------------

stage_messages = Counter(
    "bellwether_stage_messages_total",
    "Messages a stage handled, by what it decided.",
    ["stage", "outcome"],
    registry=REGISTRY,
)
stage_published = Counter(
    "bellwether_stage_messages_published_total",
    "Messages a stage produced downstream.",
    ["stage", "topic"],
    registry=REGISTRY,
)
stage_handle_seconds = Histogram(
    "bellwether_stage_handle_seconds",
    "Wall time inside a stage's handler for one message.",
    ["stage"],
    buckets=_HANDLER_BUCKETS,
    registry=REGISTRY,
)
stage_commits = Counter(
    "bellwether_stage_commits_total",
    "Offset commits, each one following a producer flush.",
    ["stage"],
    registry=REGISTRY,
)

# The one metric here that is not derived from anything in-process. Lag is
# `high watermark - committed offset`, which only the broker knows, and it is
# the first thing to look at when scores stop moving: a stage can be perfectly
# healthy by every counter above while falling an hour behind.
consumer_lag = Gauge(
    "bellwether_consumer_lag_messages",
    "Messages between a consumer group's position and the log end.",
    ["stage", "topic", "partition"],
    registry=REGISTRY,
)

# --- scoring -----------------------------------------------------------------

score_latency_seconds = Histogram(
    "bellwether_score_latency_seconds",
    "Age of the event at the moment its score was published.",
    # `ingest` is the SLO — how long the pipeline took. `behaviour` includes
    # however long the vendor sat on the record, which is not ours to fix but
    # is what the employee actually experiences.
    ["kind"],
    buckets=_PIPELINE_BUCKETS,
    registry=REGISTRY,
)
band_transitions = Counter(
    "bellwether_band_transitions_total",
    "Scores where the employee's band changed.",
    ["direction"],
    registry=REGISTRY,
)
population_band = Gauge(
    "bellwether_population_band_employees",
    "Employees currently in each risk band.",
    ["band"],
    registry=REGISTRY,
)

# --- interventions -----------------------------------------------------------

interventions_sent = Counter(
    "bellwether_interventions_sent_total",
    "Interventions written to the ledger and published to the outbox.",
    ["type", "trigger"],
    registry=REGISTRY,
)
interventions_suppressed = Counter(
    "bellwether_interventions_suppressed_total",
    "Decisions that did not reach a person, by which gate stopped them.",
    ["reason"],
    registry=REGISTRY,
)
copy_drafts = Counter(
    "bellwether_copy_drafts_total",
    "Drafts that were actually sent, by where the words came from.",
    ["source"],
    registry=REGISTRY,
)
copy_generation_seconds = Histogram(
    "bellwether_copy_generation_seconds",
    "Wall time for one model call, cache misses only.",
    buckets=_COPY_BUCKETS,
    registry=REGISTRY,
)
copy_failures = Counter(
    "bellwether_copy_failures_total",
    "Model calls that produced no usable draft, by what an operator would do about it.",
    ["kind"],
    registry=REGISTRY,
)
copy_cache = Counter(
    "bellwether_copy_cache_total",
    "Draft lookups by brief shape, hit or miss.",
    ["result"],
    registry=REGISTRY,
)
guardrail_rejections = Counter(
    "bellwether_copy_guardrail_rejections_total",
    "Drafts refused before sending, by which rule refused them.",
    ["rule"],
    registry=REGISTRY,
)

# --- the read path -----------------------------------------------------------

api_requests = Counter(
    "bellwether_api_requests_total",
    "Requests served, by route template and status class.",
    # The route *template*, never the path: labelling by path would create a
    # time series per employee looked up, which is both unbounded and a list of
    # who the security team is interested in.
    ["route", "status"],
    registry=REGISTRY,
)
api_request_seconds = Histogram(
    "bellwether_api_request_seconds",
    "Time to serve one request.",
    ["route"],
    buckets=_HANDLER_BUCKETS,
    registry=REGISTRY,
)

# --- data quality ------------------------------------------------------------

quality_check = Gauge(
    "bellwether_data_quality_value",
    "Most recent value of a data-quality check.",
    ["check", "dataset"],
    registry=REGISTRY,
)
quality_failures = Gauge(
    "bellwether_data_quality_failing",
    "1 when a check is outside its threshold, 0 when it is inside.",
    ["check", "dataset"],
    registry=REGISTRY,
)


@contextmanager
def timed(histogram: Histogram, **labels: str) -> Iterator[None]:
    """Observe elapsed wall time, including when the block raises.

    `Histogram.time()` from the library does the same thing; this exists so the
    label-or-no-label call reads identically at every call site, which the
    library's decorator form does not.
    """
    started = time.perf_counter()
    try:
        yield
    finally:
        target = histogram.labels(**labels) if labels else histogram
        target.observe(time.perf_counter() - started)


def serve(port: int) -> None:
    """Expose the registry over HTTP for Prometheus to scrape.

    Pull rather than push, even for the stream stages, which are long-running
    consumers rather than jobs. A Pushgateway would be the answer for something
    that exits, and it brings a failure mode worth avoiding: pushed metrics
    outlive the process that pushed them, so a dead consumer keeps reporting
    the last numbers it had. A scrape that fails is unambiguous.
    """
    start_http_server(port, registry=REGISTRY)
