#!/usr/bin/env bash
# Create Bellwether's topics. Idempotent — re-running is safe, and re-running
# after changing a config below actually applies the change, which `topic
# create` alone does not do for a topic that already exists.
#
# Partition counts are chosen from the consumer side: partitions cap how many
# scorer instances can work in parallel, and events are keyed by employee, so
# 12 partitions on the normalized topic means up to 12 scorers. Raw needs less
# because normalizing is cheap; interventions needs least because the send path
# is rate-limited by design, not by throughput.
set -euo pipefail

compose="docker compose"
rpk="$compose exec -T redpanda rpk"

# Every requested config is read back after it is applied.
#
# This is not paranoia. Redpanda accepts `min.cleanable.dirty.ratio` and
# `min.compaction.lag.ms` at create time, reports OK, and stores neither — so
# this script claimed a compaction policy it never had, and nothing said
# otherwise. A broker that rejects a config you cannot honour is fine. One that
# accepts it and drops it is how a cluster ends up quietly not doing what its
# infrastructure code says it does.
verify() {
  local topic=$1 actual kv key value
  shift
  actual=$($rpk topic describe "$topic" -c 2>/dev/null || true)
  for kv in "$@"; do
    key=${kv%%=*}
    value=${kv#*=}
    if ! grep -qE "^${key//./\\.}[[:space:]]+${value}([[:space:]]|$)" <<<"$actual"; then
      echo "    ! $key did not stick (wanted $value) — this broker ignores it"
    fi
  done
}

create() {
  local topic=$1 partitions=$2 kv
  shift 2
  local args=()
  for kv in "$@"; do args+=(--topic-config "$kv"); done

  echo "  $topic (${partitions}p) $*"
  $rpk topic create "$topic" --partitions "$partitions" "${args[@]}" 2>&1 |
    grep -vE 'TOPIC_ALREADY_EXISTS|already exists' || true

  # Applied separately so an existing topic converges on the config in this
  # file rather than keeping whatever it was created with months ago.
  for kv in "$@"; do
    $rpk topic alter-config "$topic" --set "$kv" >/dev/null 2>&1 || true
  done

  verify "$topic" "$@"
}

echo "creating topics..."

create bellwether.events.raw 6 \
  retention.ms=604800000

create bellwether.events.normalized 12 \
  retention.ms=2592000000

# Scores are a log, not a snapshot, and the two want opposite settings.
#
# This topic was compacted, on the reasoning that the latest score per employee
# is worth keeping cheaply. That is a fine thing to want and the wrong place to
# put it: the intervention stage reads this topic to find the record in which
# somebody crossed from elevated into high, and compaction is free to delete
# exactly that record while keeping their current score. A consumer that fell
# behind for any ordinary reason would never learn it had missed a crossing.
#
# Kafka's answer is min.compaction.lag.ms, which Redpanda accepts and ignores.
# Rather than build around a broker's silence, the conflict is removed: the log
# stays a log, and the latest-score snapshot lives in Redis, where the read path
# wants it anyway and where a point lookup is a single round trip instead of a
# topic scan.
create bellwether.risk.scores 6 \
  retention.ms=2592000000

create bellwether.interventions 3 \
  retention.ms=2592000000

# Dead letters are kept longer than the data that produced them: you find out
# you had a parsing bug well after the messages stopped arriving.
create bellwether.events.dlq 3 \
  retention.ms=7776000000

echo
$rpk topic list
