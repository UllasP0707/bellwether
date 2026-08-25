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
#
# min.compaction.lag.ms is what makes this topic safe to *trigger* from, and it
# is not optional. A compacted topic is a snapshot, not a log: the cleaner is
# free to delete the intermediate record in which an employee crossed from
# elevated into high, keeping only their latest score. The intervention stage
# reads this topic to find exactly those crossings, so a consumer that fell
# behind — a deploy, a rebalance, a long weekend — could have the evidence
# deleted underneath it and silently never act on it. Holding records
# uncompactable for a week means no consumer inside that week can be outrun.
create bellwether.risk.scores 6 \
  --topic-config cleanup.policy=compact \
  --topic-config min.cleanable.dirty.ratio=0.1 \
  --topic-config min.compaction.lag.ms=604800000

create bellwether.interventions 3 \
  --topic-config retention.ms=2592000000

# Dead letters are kept longer than the data that produced them: you find out
# you had a parsing bug well after the messages stopped arriving.
create bellwether.events.dlq 3 \
  --topic-config retention.ms=7776000000

echo
$rpk topic list
