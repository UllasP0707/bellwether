"""Print Jaeger traces as a waterfall, and fail if one did not span the pipeline.

A separate file rather than a heredoc inside `trace_demo.sh`: quoting f-strings
through two levels of shell escaping is a way to spend an afternoon, and this
is the part of the demo that has to be readable.

Reads a Jaeger `/api/traces` response on stdin.
"""

from __future__ import annotations

import json
import sys

EXPECTED_SERVICES = 4
"""producer, normalizer, scorer, intervention -- one trace has to reach all of them."""


def main() -> int:
    traces = json.load(sys.stdin).get("data", [])
    if not traces:
        print("no traces reached jaeger", file=sys.stderr)
        return 1

    complete = 0
    for trace in sorted(traces, key=lambda t: t["spans"][0]["startTime"]):
        service = {key: value["serviceName"] for key, value in trace["processes"].items()}
        spans = sorted(trace["spans"], key=lambda s: s["startTime"])
        names = {service[s["processID"]] for s in spans}
        complete += len(names) >= EXPECTED_SERVICES

        print(f"\ntrace {trace['traceID']}  {len(spans)} spans across {len(names)} services")
        for span in spans:
            tags = {t["key"]: t["value"] for t in span["tags"]}
            note = tags.get("bellwether.signal") or tags.get("bellwether.outcome") or ""
            topic = tags.get("topic") or tags.get("messaging.destination.name") or ""
            print(
                f"  {service[span['processID']]:26s} {span['operationName']:20s} "
                f"{span['duration'] / 1000:7.1f}ms  {note:28s} {topic}"
            )

    print(f"\n{complete} of {len(traces)} traces span all {EXPECTED_SERVICES} services.")
    if not complete:
        # The whole point. Four disconnected traces look fine in the UI and
        # answer nothing, so the demo has to assert the join rather than
        # display it and hope somebody notices.
        print("the traceparent did not survive a hop", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
