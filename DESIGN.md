# Bellwether: design

## Problem

An enterprise wants to know which of its employees are currently risky, why,
and what to do about it — within seconds of the risky behavior, not on next
quarter's training report.

That splits into two workloads with incompatible shapes:

- **Online.** An employee clicks a phishing link. Their score must move and an
  intervention must fire while the moment is still salient — single-digit
  seconds, per-employee state, unbounded event stream.
- **Offline.** A security team wants department-level trends, cohort analysis,
  and the training data for tomorrow's model. Full history, arbitrary
  re-aggregation, correctness over latency.

The naive solution builds these twice. That is the failure mode this project is
organized to avoid.

## Core constraint: one signal catalog

Every behavior Bellwether understands is declared once, in
[`bellwether/scoring/catalog.py`](bellwether/scoring/catalog.py), as a `SignalSpec`:
weight, direction, half-life, risk category.

The streaming scorer and the Spark batch scorer are two evaluation strategies
over the same catalog and the same pure `score_events()` function. A weight
change is one edit that both paths pick up, and `tests/test_score_parity.py`
replays a fixed event log through both and asserts the scores agree.

This is the standard train/serve skew problem wearing a different hat, and the
answer is the same: the definition must live in one place that both paths
import, not in two places that a human keeps in sync.

**Cost of the choice.** The scoring function is constrained to what both a
Python stream consumer and a Spark executor can run — no per-event database
lookups, no incremental state the batch path can't reconstruct. Scoring is
therefore a pure function of `(employee dimension, event window)`. Anything
needing richer state has to earn its way in.

## Frequency beats severity in a decayed sum, and that is a trap

The first version of the catalog priced "shared a file with an external address"
at weight 2.0 — plausible in isolation. It arrives about 0.4 times per employee
per day. Integrated over a 30-day window with a 14-day half-life, that one
routine signal contributed ~12.5 of raw score, more than a phishing credential
submission (weight 25, but arriving perhaps twice a year).

The result: a median employee scored 57.6 out of 100, the whole population sat in
the "high" band, and the people who had actually handed over credentials were
indistinguishable from everyone else. The scoring code was correct. The model was
useless.

Any decayed-sum scorer has this property: contribution scales with
`weight × arrival rate`, not weight. So routine behaviors must be priced near
zero even when they feel risky, because their frequency does the multiplying.
Two consequences:

- Routine signals (external sharing, denied MFA pushes, USB mounts) carry weights
  under 1.5. They are useful as *baselines* and in *combination*, not as risk in
  themselves.
- The real fix, which v1 does not implement, is to score high-frequency behaviors
  as deviations from each employee's own baseline rather than as absolute counts.
  "Shared 40 files externally today" means nothing without "normally shares 3."
  That needs a per-employee rolling baseline in the feature store, and it is the
  first thing I would add.

After rebalancing: median 19.9, p90 71.0, 4.6% of the population in the critical
band. The distribution now has a tail to act on.

## Does it work?

The generator assigns each employee a hidden persona — `vigilant`, `typical`,
`onboarding`, `hurried`, `targeted`, `shadow_it` — that drives their behavior.
The persona never appears on an event and the scorer never sees it.

Mean score by persona, 500 employees over 30 days:

| Persona | n | Mean score |
| --- | --- | --- |
| shadow_it | 16 | 71.5 |
| targeted | 65 | 54.5 |
| hurried | 90 | 46.8 |
| onboarding | 34 | 30.2 |
| typical | 213 | 16.8 |
| vigilant | 82 | 2.1 |

The ranking is recovered exactly, from behavior alone, with clear separation
between adjacent groups. That is the closest thing to a ground-truth check
available without real labeled incident data, and it is what the parity test and
the load test are measured against.

The honest caveat: the generator and the scorer were written by the same person
against the same mental model, so this validates internal consistency, not that
the weights match reality. Real validation needs outcome labels — which employees
actually got compromised — and that is exactly the data this system's first year
in production would produce.

## Event contract

Connectors are the only components that know what an Okta log line looks like.
They emit `BehaviorEvent` (see [`bellwether/events/schema.py`](bellwether/events/schema.py))
and everything downstream speaks only that.

Decisions worth defending:

- **`occurred_at` and `ingested_at` are separate fields.** Source systems
  deliver late and out of order; a batch job that windows on ingest time will
  silently produce different answers than a stream that windows on event time.
  Both timestamps travel with the event so either path can choose.
- **`raw_ref` points into the raw lake.** Normalized events stay small, but
  every one can be traced back to the exact source payload. When a connector's
  parsing turns out to be wrong, the fix is a reprocess, not a data loss.
- **`employee_id` is a tenant-scoped token, never an email.** PII lives in one
  place (the employee dimension) so retention and deletion have one enforcement
  point. See [Handling employee data](#handling-employee-data).
- **`schema_version` is on every event.** Consumers must tolerate versions they
  don't recognize rather than crash-loop the partition.

Events are keyed by `employee_id`, so all of one person's behavior lands on one
partition and the per-employee scorer needs no cross-partition coordination.
The tradeoff is that a single pathological employee — an admin account
generating audit spam — can hot-spot a partition.

## Topics

| Topic | Key | Retention | Why |
| --- | --- | --- | --- |
| `bellwether.events.raw` | source event id | 7d | Replay buffer for connector bugs. |
| `bellwether.events.normalized` | employee_id | 30d | The event-sourced spine. Rebuild any downstream state from here. |
| `bellwether.risk.scores` | employee_id | compacted | Latest score per employee; compaction makes it a queryable snapshot. |
| `bellwether.interventions` | employee_id | 30d | Emitted actions, audited. |

## Delivery semantics

At-least-once throughout, with idempotent consumers, rather than a
transactional exactly-once configuration.

Justification: the two things duplicates could corrupt are scores and
interventions. Scores are recomputed from a windowed event set keyed by
`event_id`, so replaying a duplicate is a no-op. Interventions are deduplicated
on `(employee_id, intervention_type, cooldown_window)` in Postgres before send.
Exactly-once machinery would add coordination cost to buy a property the
consumer design already provides.

## Interventions must not spam a human

The part of this system with real-world consequences is the one that messages
employees. Three gates before anything sends:

1. **Cooldown.** One intervention per employee per type per window (default 72h).
2. **Global rate limit.** A cap per employee per week regardless of type, so a
   bad day doesn't produce eleven notifications.
3. **Escalation ladder.** Nudge → training → manager notification. Severity
   climbs only on repeat behavior, and manager notification requires a policy
   flag, because escalating to someone's boss is not a reversible action.

Copy is generated by Claude from the employee's actual signals, then validated
against a guardrail check (no accusatory framing, no PII beyond first name,
length bounded, must contain the concrete action to take). Generation failure
falls back to a static template — the system degrades to boring, never to
silent.

## Handling employee data

This is behavioral data about identifiable people, which is the most sensitive
category the system could hold.

- PII (email, name, manager) lives only in the `employees` table. Events carry
  the token.
- Retention: raw payloads 30d, normalized events 400d, aggregates indefinitely.
  Enforced by a scheduled job, not by convention.
- Deletion: one function resolves a token, purges the dimension row, and
  tombstones the compacted score topic.
- The read API is tenant-scoped at the query layer, and every score read is
  written to an audit log — who looked at whose risk score is itself sensitive.

## What I would do differently at real scale

Honest list, since these are the questions an interviewer should ask.

- **Python stream consumers won't hold.** Fine at thousands of events/sec on one
  partition set; at a hundred thousand I would move the scorer to Flink for real
  windowing, checkpointed state, and event-time watermarks instead of the
  hand-rolled window this uses.
- **Postgres is doing three jobs** (dimension, dedup ledger, serving). At scale
  those separate: dimension stays relational, dedup moves to Redis with TTLs,
  serving moves behind a read replica or a purpose-built store.
- **Iceberg over raw Parquet.** Parquet plus a directory convention is enough for
  a demo; schema evolution and time-travel are what make reprocessing safe, and
  those want a table format.
- **The score is a weighted sum, deliberately.** It is explainable and
  debuggable, which matters more than accuracy at v1 — a security team will not
  act on a score it cannot interrogate. A learned model is the successor, and it
  needs the labeled outcome data this system's first year would produce.

## Open questions

- Windowed scoring currently recomputes over a 30-day lookback on every event.
  Correct and simple, but O(events in window) per event. Incremental decay
  update is the obvious fix; it complicates batch parity.
- No backpressure story yet between connectors and the raw topic.
- Score parity between stream and batch is asserted on a fixed fixture. It
  should be a continuous production check.
