#!/usr/bin/env bash
# Put the stack into the state the demo narrative assumes.
#
# This exists because the first recording rehearsal did not work. Days of
# testing had left the demo employee pinned at 100.0 from 56 events, so the
# incident moved nothing and "watch the band change" was simply false; and the
# intervention stage, fed by accumulated load-test traffic, sent 192 messages
# where the story says one.
#
# Neither was a bug. Both are what a long-lived environment looks like, and a
# demo that only works on a machine nobody has used is a demo that fails in
# front of someone. So: wipe the derived state, keep the population, rebuild.
#
# Nothing here is destructive of anything that matters. Every store it clears
# is derived -- topics, the online projection, the ledger -- and the employee
# dimension, which is the only thing that is not, is left alone.
set -euo pipefail

PY="${PY:-.venv/bin/python}"
COMPOSE="${COMPOSE:-docker compose}"
EMPLOYEE="${EMPLOYEE:-E0042}"
DAYS="${DAYS:-30}"

echo "resetting demo state (employee $EMPLOYEE, ${DAYS}d of history)"

echo "  1/6  topics"
# Deleted and recreated rather than drained: a consumer group's committed
# offsets survive a drain, so the stages would start from the end and the
# rebuild below would be invisible to them.
$COMPOSE exec -T redpanda rpk topic delete \
  bellwether.events.raw bellwether.events.normalized bellwether.risk.scores \
  bellwether.interventions bellwether.events.dlq >/dev/null 2>&1 || true
sleep 2
./scripts/create_topics.sh >/dev/null 2>&1

echo "  2/6  online store"
$COMPOSE exec -T redis redis-cli FLUSHDB >/dev/null

echo "  3/6  intervention ledger"
# The one deletion worth pausing on. In production this table is the record of
# what was sent to whom and must never be truncated -- the cooldowns would
# forget and the next run would message everybody again. Here it is demo data,
# and the reset is the whole point.
$COMPOSE exec -T postgres psql -U bellwether -q -c \
  "TRUNCATE intervention; TRUNCATE score_read_audit;" >/dev/null 2>&1 || true

echo "  4/6  employee dimension"
$PY -m bellwether.cli load-dimension >/dev/null

echo "  5/6  ${DAYS}d of behaviour"
$PY -m bellwether.cli generate backfill --days "$DAYS" --to both >/dev/null

echo "  6/6  normalize and score"
$PY -m bellwether.cli normalize    --idle-timeout 10 --metrics-port 0 >/dev/null 2>&1
$PY -m bellwether.cli score-stream --idle-timeout 10 --metrics-port 0 >/dev/null 2>&1

# Let the intervention stage consume the backfill and refuse it, so the demo
# starts from a committed offset rather than a backlog. Every trigger in
# thirty days of history is older than the 48-hour recency gate, so this sends
# nothing -- which is the property the demo's last beat demonstrates, done
# here quietly so the beat is about one fresh incident instead.
$PY -m bellwether.cli intervene --idle-timeout 10 --metrics-port 0 --copy template >/dev/null 2>&1

echo
$PY -m bellwether.cli scores --employee "$EMPLOYEE" 2>/dev/null || true
echo

# --- verify the state this script exists to produce ---------------------------
#
# This used to print "ready" unconditionally, and it was wrong often enough to
# matter. `generate backfill` seeds the *population* -- who has which persona --
# but not the behaviour draw, so thirty days of history is a fresh roll every
# time. One roll leaves E0042 at 51.79 and elevated, which is the narrative; the
# next hands them a `phish_credentials_submitted` and they start at 83.36 and
# critical, which is not.
#
# The failure was circular and would only ever be discovered with a camera
# running: this script ended with "now run ./scripts/demo.sh", and demo.sh's
# preflight answered "run ./scripts/demo_reset.sh first". A reset that tells you
# to run a demo that tells you to run the reset is worse than no reset at all.
#
# So the outcome is checked, and when the draw is unusable the script finds a
# subject it can vouch for rather than failing and leaving the operator to read
# a ranking. Preference goes to someone already driven by phishing, because the
# incident is a phishing chain and the "driven by" line is on screen while it
# happens.
band=$($PY -m bellwether.cli scores --employee "$EMPLOYEE" 2>/dev/null |
  grep -oE 'band [a-z]+' | head -1 | cut -d' ' -f2 || true)

if [ "$band" = "elevated" ] || [ "$band" = "moderate" ] || [ "$band" = "low" ]; then
  echo "ready. now run: ./scripts/demo.sh"
  exit 0
fi

# Highest scorer still inside `elevated`: closest to a crossing, so the incident
# moves them a visible distance rather than nudging a zero.
pick() {
  $PY -m bellwether.cli scores --top 300 2>/dev/null |
    awk -F'│' -v want="$1" 'NF>=6 {
        for (i = 2; i <= 5; i++) gsub(/^[ \t]+|[ \t]+$/, "", $i)
        if ($2 ~ /^E[0-9]+$/ && $4 == "elevated" && (want == "" || $5 == want)) print $3"\t"$2
      }' | sort -rn | head -1 | cut -f2
}

alt=$(pick phishing_susceptibility)
[ -n "$alt" ] || alt=$(pick "")

if [ -z "$alt" ]; then
  echo "$EMPLOYEE came out $band, and no employee landed in 'elevated' either." >&2
  echo "re-run this script: the behaviour draw is random and the next roll will differ." >&2
  exit 1
fi

echo "$EMPLOYEE came out $band this time -- the incident would not move them."
echo "the behaviour draw is random; $alt landed in 'elevated' and works."
echo
echo "ready. now run: EMPLOYEE=$alt ./scripts/demo.sh"
