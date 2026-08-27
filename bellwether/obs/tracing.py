"""Distributed tracing, and the part of it that is actually hard.

A trace inside one process is a solved problem. This pipeline is four processes
joined by three Kafka topics, and a connector fetching an Okta record has
finished and exited long before the intervention that record eventually causes
is decided. Without context propagation you get four unrelated traces and no
way to answer the only question worth asking — *why did this person get this
message* — because nothing links the message to the log line.

So the W3C `traceparent` rides in the Kafka message headers, the same way it
would ride in an HTTP header, and each stage continues the trace it was handed
rather than starting a new one. The result is one trace spanning connector to
intervention across three topics.

**A caveat worth stating rather than hiding.** The stages here are batch
consumers, so a span covers one message and its parent is the message that
produced it, which is a *causal* link and not a nested one: the parent span has
usually already ended. That is what `Link` is for in OpenTelemetry, and using
parenting anyway is a deliberate simplification, because a trace view that
renders as a waterfall is the thing that makes this useful to look at.

Inert unless configured. With no `BELLWETHER_OTLP_ENDPOINT` set, `configure()`
installs nothing and every function here is a few nanoseconds of no-op, so
tracing is never a reason a stage fails to start.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace import Span

TRACEPARENT = "traceparent"

_configured = False


def configure(service_name: str, endpoint: str | None = None) -> bool:
    """Install an OTLP exporter. Returns whether tracing is actually on.

    Idempotent, because every CLI command that might start a stage calls it and
    a second `TracerProvider` would silently drop the first one's spans.
    """
    global _configured
    if _configured:
        return True

    from bellwether.config import settings

    target = endpoint or settings().otlp_endpoint
    if not target:
        return False

    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider(
        resource=Resource.create({"service.name": service_name, "service.namespace": "bellwether"})
    )
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{target.rstrip('/')}/v1/traces"))
    )
    trace.set_tracer_provider(provider)
    _configured = True
    return True


def tracer(name: str = "bellwether") -> trace.Tracer:
    return trace.get_tracer(name)


def inject(headers: list[tuple[str, bytes]] | None = None) -> list[tuple[str, bytes]]:
    """Add the current span's `traceparent` to a Kafka header list.

    Written by hand rather than through `TraceContextTextMapPropagator` because
    the propagator's carrier interface wants a mutable mapping and Kafka headers
    are a list of pairs that permits duplicate keys. Fifteen lines of adapter to
    reuse the library, or the four below to emit the format the library will
    parse back.
    """
    out = list(headers or [])
    span = trace.get_current_span()
    ctx = span.get_span_context()
    if not ctx.is_valid:
        return out
    flags = "01" if ctx.trace_flags.sampled else "00"
    value = f"00-{ctx.trace_id:032x}-{ctx.span_id:016x}-{flags}"
    return [*out, (TRACEPARENT, value.encode())]


def _parse(value: str) -> trace.SpanContext | None:
    parts = value.split("-")
    if len(parts) != 4 or parts[0] != "00":
        return None
    try:
        trace_id, span_id = int(parts[1], 16), int(parts[2], 16)
        flags = int(parts[3], 16)
    except ValueError:
        return None
    if not trace_id or not span_id:
        return None
    return trace.SpanContext(
        trace_id=trace_id,
        span_id=span_id,
        is_remote=True,
        trace_flags=trace.TraceFlags(flags),
    )


def extract(headers: Sequence[tuple[str, bytes | None]] | None) -> otel_context.Context | None:
    """Recover the upstream context from Kafka headers, if there is one.

    A message with no `traceparent`, or one a broker or a bridge mangled, is not
    an error: it starts a new trace. A stage that refused to process untraced
    messages would be a stage that stops working the moment somebody replays a
    topic with a tool that does not know about headers.
    """
    if not headers:
        return None
    for key, raw in headers:
        if key.lower() != TRACEPARENT or raw is None:
            continue
        parsed = _parse(raw.decode("utf-8", "replace"))
        if parsed is None:
            return None
        return trace.set_span_in_context(trace.NonRecordingSpan(parsed))
    return None


@contextmanager
def message_span(
    name: str,
    headers: Sequence[tuple[str, bytes | None]] | None = None,
    **attributes: Any,
) -> Iterator[Span]:
    """One span per message, continuing whatever trace produced it."""
    with tracer().start_as_current_span(
        name, context=extract(headers), kind=trace.SpanKind.CONSUMER
    ) as span:
        for key, value in attributes.items():
            if value is not None:
                span.set_attribute(key, value)
        yield span
