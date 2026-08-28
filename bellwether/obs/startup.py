"""Turning observability on for one process.

One function, called by every CLI command that runs long enough to be worth
watching. It exists so the decision about *when* observability is on lives in
one place rather than being re-litigated in six command bodies, and so the
answer is always the same: on if configured, silently off if not, never a
reason the process fails to start.

The port defaults come from `docker/prometheus.yml`. Fixed per component
rather than allocated, because a scrape target has to be predictable and these
are CLI invocations rather than anything a service discovery could enumerate.
"""

from __future__ import annotations

from bellwether.config import settings
from bellwether.obs import metrics, tracing

PORTS = {
    "api": 9101,
    "normalizer": 9102,
    "scorer": 9103,
    "intervention": 9104,
    "ingest": 9105,
}


def start(component: str, metrics_port: int | None = None) -> str:
    """Start the exporter and the tracer for one component. Returns what happened.

    Returned rather than logged so the caller can print it next to everything
    else it prints. A line saying tracing is off is worth as much as one saying
    it is on: "the trace is missing" and "tracing was never enabled" look
    identical afterwards, and only one of them is a bug.
    """
    config = settings()
    notes: list[str] = []

    port = metrics_port if metrics_port is not None else config.metrics_port
    if port == -1:
        port = PORTS.get(component, 0)
    if port:
        try:
            metrics.serve(port)
            notes.append(f"metrics :{port}/metrics")
        except OSError as err:
            # A port already in use means another copy of this stage is
            # running, which is a normal thing to do deliberately. Refusing to
            # start the stage over it would make the monitoring the most
            # fragile part of the system.
            notes.append(f"metrics off ({err.strerror or err})")

    if tracing.configure(f"bellwether-{component}"):
        notes.append(f"traces -> {config.otlp_endpoint}")

    return ", ".join(notes) or "metrics and traces off"
