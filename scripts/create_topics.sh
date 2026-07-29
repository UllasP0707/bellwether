#!/usr/bin/env bash
# Create Bellwether's topics. Idempotent — re-running is safe.
#
# Partition counts are chosen from the consumer side: partitions cap how many
# scorer instances can work in parallel, and events are keyed by employee, so
# 12 partitions on the normalized topic means up to 12 scorers. Raw needs less
# because normalizing is cheap; interventions needs least because the send path
# is rate-limited by design, not by throughput.
set -euo pipefail

compose="docker compose"
rpk="$compose exec -T redpanda rpk"

create() {
  local topic=$1 partitions=$2
  shift 2
  echo "  $topic (${partitions}p) $*"
  $rpk topic create "$topic" --partitions "$partitions" "$@" 2>&1 |
    grep -vE 'TOPIC_ALREADY_EXISTS|already exists' || true
}

echo "creating topics..."

create bellwether.events.raw 6 \
  --topic-config retention.ms=604800000

create bellwether.events.normalized 12 \
  --topic-config retention.ms=2592000000

# Compacted: the latest score per employee is a snapshot we want to keep
# indefinitely without keeping every intermediate score forever. Tombstones
# also give employee deletion somewhere to land.
create bellwether.risk.scores 6 \
  --topic-config cleanup.policy=compact \
  --topic-config min.cleanable.dirty.ratio=0.1

create bellwether.interventions 3 \
  --topic-config retention.ms=2592000000

echo
$rpk topic list
