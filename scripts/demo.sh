#!/usr/bin/env bash
# The 90-second narrative, end to end, one command.
#
# One employee, three events, ninety seconds of real time: a phishing
# simulation is delivered, clicked, and credentials are submitted. Their score
# moves, their band changes, and the message they end up reading is about the
# thing that just happened rather than the thing that happened first.
#
# That last clause is the whole of beat 5, and it is the part rehearsing this
# script fixed. Two messages go out, not one: the click crosses a band and
# sends a routine note, and the credential submission a minute later overrides
# the spacing gate because a routine message must not be able to block an
# urgent one.
#
# Paced for recording. Each beat prints what it is about to do, does it, and
# pauses long enough to be read. Set PACE=0 to run it flat out.
#
#   make up && make topics && make seed && make demo
set -euo pipefail

PY="${PY:-.venv/bin/python}"
EMPLOYEE="${EMPLOYEE:-E0042}"
PACE="${PACE:-2}"
KEY="${KEY:-localdev}"
API="${API:-http://localhost:8800}"

bold() { printf '\n\033[1m%s\033[0m\n' "$1"; }
dim() { printf '\033[2m%s\033[0m\n' "$1"; }
beat() { sleep "$PACE"; }

# --- 0. preflight -------------------------------------------------------------
#
# Checked up front rather than failing on beat four. A demo that dies halfway
# is worse than one that refuses to start.
for check in "redpanda:9092" "postgres:5433" "redis:6379"; do
  host="${check%%:*}"
  port="${check##*:}"
  if ! nc -z localhost "$port" 2>/dev/null; then
    echo "$host is not listening on $port -- run: make up" >&2
    exit 1
  fi
done

# The narrative assumes a specific starting state, and a long-lived
# environment will not have it. Checked rather than hoped for: the first
# rehearsal ran against an employee already pinned at 100.0 from days of
# testing, so "watch the band change" was false and the intervention beat
# reported 192 messages where the story says one.
current=$($PY -m bellwether.cli scores --employee "$EMPLOYEE" 2>/dev/null | grep -oE 'band [a-z]+' | head -1 | cut -d' ' -f2 || true)
if [ "$current" = "critical" ] || [ "$current" = "high" ]; then
  echo "$EMPLOYEE is already $current -- the incident below will not move them." >&2
  echo "run ./scripts/demo_reset.sh first, or set EMPLOYEE= to somebody lower." >&2
  exit 1
fi
if [ -z "$current" ]; then
  dim "no score for $EMPLOYEE yet; run ./scripts/demo_reset.sh for the full narrative."
fi

bold "BELLWETHER -- ninety seconds"
dim  "ingest employee security behaviour, score it continuously, intervene once."
beat

# --- 1. where they start ------------------------------------------------------

bold "1. $EMPLOYEE, before anything happens"
dim  "the score is a decayed weighted sum over a 30-day window. explainable on purpose:"
dim  "a security team will not act on a number it cannot interrogate."
$PY -m bellwether.cli scores --employee "$EMPLOYEE" 2>/dev/null || \
  dim "  (no score yet -- the incident below will create one)"
beat

# --- 2. the incident ----------------------------------------------------------

bold "2. a phishing simulation lands"
dim  "delivered, clicked, credentials submitted. 112 seconds of simulated time."
$PY -m bellwether.cli generate incident --employee "$EMPLOYEE" --to kafka
beat

# --- 3. through the pipeline --------------------------------------------------

bold "3. normalize, then score"
dim  "raw is keyed by the vendor's record id; normalized is re-keyed onto the person,"
dim  "so all of one employee's behaviour lands on one partition and scoring needs"
dim  "no cross-partition coordination."
$PY -m bellwether.cli normalize   --idle-timeout 8 --metrics-port 0 2>&1 | grep -E 'emitted|dead' || true
$PY -m bellwether.cli score-stream --idle-timeout 8 --metrics-port 0 2>&1 | grep -E 'scored|band changes' || true
beat

bold "4. the same employee now"
dim  "the band moved. the driver is named, with its contribution."
$PY -m bellwether.cli scores --employee "$EMPLOYEE" 2>/dev/null || true
beat

# --- 5. the intervention ------------------------------------------------------

bold "5. deciding whether to say anything"
dim  "five gates, all defaulting to silence: recency, spacing, cooldown, weekly cap,"
dim  "and an escalation ladder. most of this code exists to NOT send a message."
dim  ""
dim  "two go out here, and the second one is the point. the click crossed a band and"
dim  "sent a routine note; the credential submission a minute later would normally"
dim  "hit the 24-hour spacing gate. four signals are allowed to override it, once,"
dim  "because a routine message must not be able to block an urgent one."
$PY -m bellwether.cli intervene --idle-timeout 8 --metrics-port 0 2>&1 | \
  grep -E 'sent|suppressed|nudge|training|copy:' || true
beat

bold "6. what the person actually receives"
dim  "the most recent one: about the credentials, not about the file shares."
$PY -m bellwether.cli interventions --employee "$EMPLOYEE" --limit 1 2>/dev/null || true
dim  ""
dim  "written from the catalog's own descriptions and a placeholder name -- the prompt"
dim  "carries no personal data at all. validated before sending: no blame, no threat,"
dim  "and above all no claim that the account was compromised, which is the leap a"
dim  "fluent model makes when asked to convey urgency."
beat

# --- 7. the claim the project exists for --------------------------------------

bold "7. the constraint the whole design is organised around"
dim  "one signal catalog, two execution engines. the streaming scorer and the Spark"
dim  "batch scorer call the same pure function, so they cannot silently disagree."
dim  ""
dim  "  make parity  ->  120 employees, 1,986 events, 0 disagreements, delta 0.0"
dim  "                   exact equality, not a tolerance."
beat

bold "8. replay the whole history"
dim  "the safety property. reprocessing thirty days of behaviour rescores everyone"
dim  "and must message nobody, because every crossing on the way up is an artefact"
dim  "of ingestion order rather than something that just happened."
$PY -m bellwether.cli intervene --group "demo-replay-$(date +%s)" \
   --idle-timeout 8 --metrics-port 0 2>&1 | grep -E 'sent|trigger_too_old' || true
beat

bold "done"
dim  "dashboard   $API/?key=$KEY"
dim  "grafana     http://localhost:3000/d/bellwether-overview   (make observe)"
dim  "one trace   ./scripts/trace_demo.sh"
