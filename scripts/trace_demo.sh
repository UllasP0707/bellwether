#!/usr/bin/env bash
# One incident, followed across three Kafka topics and four processes.
#
# The claim this checks is the one that makes tracing worth having here: a
# connector fetching a record and the intervention that record eventually
# causes are separated by three topics and four processes that never overlap
# in time. If the W3C traceparent does not survive the message headers, you get
# four unrelated traces and no mechanical answer to "why did this person get
# this message".
#
#   make observe && ./scripts/trace_demo.sh
set -euo pipefail

export BELLWETHER_OTLP_ENDPOINT="${BELLWETHER_OTLP_ENDPOINT:-http://localhost:4318}"
PY="${PY:-.venv/bin/python}"
EMPLOYEE="${EMPLOYEE:-E0042}"
JAEGER="${JAEGER:-http://localhost:16686}"

if ! curl -fsS -m 5 "$JAEGER/" >/dev/null 2>&1; then
  echo "jaeger is not up on $JAEGER -- run: make observe" >&2
  exit 1
fi

echo "1/5  injecting the phishing chain for $EMPLOYEE"
$PY -m bellwether.cli generate incident --employee "$EMPLOYEE" --to kafka | sed 's/^/     /'

for stage in "normalize:2" "score-stream:3" "intervene:4"; do
  name="${stage%%:*}"
  echo "$((${stage##*:}))/5  $name"
  $PY -m bellwether.cli "$name" --idle-timeout 8 --metrics-port 0 \
    ${name:+$([ "$name" = intervene ] && echo --copy template)} >/dev/null 2>&1 || true
done

echo "5/5  reading the traces back out of jaeger"
# Spans are batched, so give the exporter a moment to flush before asking.
sleep 6

curl -fsS -m 15 "$JAEGER/api/traces?service=bellwether-producer&limit=20&lookback=1h" \
  | $PY scripts/show_traces.py

echo
echo "open $JAEGER/search?service=bellwether-producer"
