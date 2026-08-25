# Bellwether: design

> **How to read this.** Bellwether is being built in stages, and this document
> describes the whole design rather than only what exists today. Every section
> carries a status:
>
> - **`[built]`** — implemented, tested, running. Claims here are things you can
>   verify by cloning the repo.
> - **`[partly built]`** — some of it exists; the section says which part.
> - **`[designed]`** — decided and specified here, not yet written.
>   [docs/ROADMAP.md](docs/ROADMAP.md) says which day it lands.
>
> Nothing marked `[built]` depends on anything marked `[designed]`. Measurements
> quoted in this document come from code that exists.

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

## Core constraint: one signal catalog — `[partly built]`

*Built: the catalog, the shared scoring function, and the streaming path that
calls it. Not yet built: the batch path, and therefore the parity test.*

Every behavior Bellwether understands is declared once, in
[`bellwether/scoring/catalog.py`](bellwether/scoring/catalog.py), as a `SignalSpec`:
weight, direction, half-life, risk category.

The streaming scorer and the Spark batch scorer are intended as two evaluation
strategies over the same catalog and the same pure `score_events()` function, so
that a weight change is one edit both paths pick up. A `test_score_parity` case
will replay a fixed event log through both and assert the scores agree.

**The streaming path exists; the batch path does not.** Until it and the parity
test land (day 6), this section is an argument for a design rather than a
description of a working guarantee. It remains the most important thing left to
prove, because it is the reason the project is structured the way it is.

What makes it reachable is that scoring reads a *structural* type rather than a
concrete one. `ScorableEvent` is three fields — `employee_id`, `signal`,
`occurred_at` — so the stream consumer's parsed models, the Redis window's
compact projection, and Spark's `Row` objects all satisfy it without conversion.
Had `score_events` kept its original `Iterable[BehaviorEvent]` signature, the
batch path would have had to materialise millions of Pydantic models per run,
and the obvious remedy would have been a second implementation in Spark — which
is precisely the outcome this design is meant to prevent.

One early signal that it holds: the streaming path scores the population into
23 critical / 41 high / 79 elevated / 109 moderate / 247 low, and the day-1
offline computation over the lake put 4.6% critical. Same answer, two callers.

This is the standard train/serve skew problem wearing a different hat, and the
answer is the same: the definition must live in one place that both paths
import, not in two places that a human keeps in sync.

**Cost of the choice.** The scoring function is constrained to what both a
Python stream consumer and a Spark executor can run — no per-event database
lookups, no incremental state the batch path can't reconstruct. Scoring is
therefore a pure function of `(employee dimension, event window)`. Anything
needing richer state has to earn its way in.

## Frequency beats severity in a decayed sum, and that is a trap — `[built]`

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

## Does it work? — `[built]`

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
available without real labeled incident data, and it is the fixture the parity
test and the load test will be measured against once they exist.

The honest caveat: the generator and the scorer were written by the same person
against the same mental model, so this validates internal consistency, not that
the weights match reality. Real validation needs outcome labels — which employees
actually got compromised — and that is exactly the data this system's first year
in production would produce.

## Event contract — `[built]`

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

## Topics — `[partly built]`

All are created by [`scripts/create_topics.sh`](scripts/create_topics.sh) with
the partition counts and retention below.

| Topic | Key | Retention | Producer | Why |
| --- | --- | --- | --- | --- |
| `bellwether.events.raw` | source event id | 7d | connectors | Replay buffer for connector bugs. |
| `bellwether.events.normalized` | employee_id | 30d | normalizer | The event-sourced spine. Rebuild any downstream state from here. |
| `bellwether.events.dlq` | employee_id | 90d | any stage | What a stage could route but not trust. |
| `bellwether.risk.scores` | employee_id | 30d | scorer | Every score, including the ones where a band changed. |
| `bellwether.interventions` | employee_id | 30d | intervention stage | Emitted actions, audited. |

**`risk.scores` was compacted, and that was wrong.** Keeping only the latest
score per employee is a reasonable thing to want, and a terrible property for
the topic the intervention stage triggers from: the record in which somebody
crossed from elevated into high is exactly what the cleaner is entitled to
delete, and a consumer that fell behind for any ordinary reason would never
learn it had missed a crossing.

Kafka's answer is `min.compaction.lag.ms`. Redpanda accepts it, reports OK, and
stores nothing — as it also does for `min.cleanable.dirty.ratio`, which this
repo had claimed to set since day 2 and never had. That only surfaced by reading
the config back after writing it, so `scripts/create_topics.sh` now does exactly
that and prints a warning for any setting the broker quietly dropped.

The fix was not to work around the broker but to stop asking one topic to be two
things. A log stays a log, and the latest-score snapshot lives in Redis, where
the read path wants it anyway and a point lookup is one round trip rather than a
topic scan.

**The two event topics are keyed differently on purpose, and that is the reason
there are two of them.** Raw is keyed by the vendor's record id: it spreads
connector output evenly and lets a connector republish a record without knowing
or caring whose it is. Per-employee stateful scoring needs the opposite —
everything about one person on one partition — so the normalizer re-keys onto
`employee_id`. A single topic could not satisfy both.

Partition counts are chosen from the consumer side: events are keyed by
employee, so 12 partitions on `normalized` caps the scorer at 12 parallel
instances. Verified on the raw topic — a 30-day backfill of 8,606 events spread
1284–1589 across its 6 partitions, which is what employee-key hashing should
give.

## Delivery semantics — `[built]`

At-least-once throughout, with idempotent consumers, rather than a
transactional exactly-once configuration.

Three places enforce it, and all of them order their writes the same way — do
the work, then acknowledge the input:

- A **connector** commits its cursor after the page's events are emitted. A
  crash in between redelivers the page.
- The **normalizer** and the **scorer** flush their producers before committing
  consumer offsets. A crash in between redelivers the messages.
- The **intervention stage** claims its ledger row before publishing. A crash in
  between records a message nobody received.

Either ordering reversed would silently lose data, which is far worse than
duplicating it. Redelivery is then absorbed by `event_id`, which connectors
derive as `uuid5(source, source_event_id)` — the same vendor record always
produces the same id, however many times it is reprocessed. The normalizer
suppresses repeats against a shared Redis set, and the scorer is inert to them
for a different reason: its window is a sorted set keyed by `event_id`, so
re-adding an event it already holds changes nothing.

Measured rather than assumed. Adding a second scorer to a running consumer group
redelivered 430 uncommitted messages out of 7,789, and every published score was
byte-identical afterwards. At-least-once is only a safe choice if reprocessing is
demonstrably inert.

Justification: the two things duplicates could corrupt are scores and
interventions. Scores are recomputed from a windowed event set keyed by
`event_id`, so replaying a duplicate is a no-op, and `score_events()`'s
order-independence is tested. Interventions are fenced by a unique index on
`(tenant, employee, trigger_event_id)` in Postgres. Exactly-once machinery would
add coordination cost to buy a property the consumer design provides on its own.

The intervention fence is measured too, and separately from the rate limits that
sit in front of it. Under the default policy a full replay of the score topic is
suppressed long before it reaches the database, which would leave the fence
itself untested — so with every rate gate switched off and the whole topic
replayed, 137 attempts came back `already_sent` and the ledger did not move.

**A consumer must also survive input it cannot parse.** One poisoned message
that raises will crash-loop its partition and block every well-formed event
behind it, which turns a single bad record into an outage. So the normalizer
routes rather than raises: garbage and invalid-at-a-known-version go to the
dead-letter topic; an unrecognised *future* `schema_version` is forwarded
unvalidated, because the routing fields are stable by contract and a newer
consumer downstream may understand what this one doesn't.

## Interventions must not spam a human — `[built]`

*The `bellwether.interventions` topic is an **outbox**. Nothing here talks to
Slack or an SMTP server; a delivery worker consuming that topic would be the
thing that can actually fail to reach somebody, and it is deliberately not part
of this repo.*

The part of this system with real-world consequences is the one that messages
employees, so most of the code in the package is about not doing it.

**What fires.** An upward band crossing, or one of four signals that get a
response on their own merits: credential submission, an address appearing in a
breach dump, an external mail-forwarding rule, an MFA push flood. Those four
deliberately ignore the band threshold, and that turns out to be where most of
the value is — 68 of 137 interventions in the verification run went to people in
the *low or moderate* bands. They have no accumulated history to push them over
a threshold, a crossing-only policy would never have contacted one of them, and
four of them had just handed credentials to a phishing page.

`mfa_push_flood` is in that set for a different reason than the rest. It is not
the employee's mistake at all — somebody is attacking them, and "do not approve
the next one" is worth saying to an employee whose score is otherwise spotless.

**What stops it.** Five gates, all defaulting toward silence:

1. **Recency.** The behaviour that caused the rescore must be under 48h old.
2. **Minimum spacing.** 24h between any two messages to one person, of any type.
3. **Cooldown.** 72h per employee *per type*.
4. **Weekly cap.** Three per employee per rolling week, across all types.
5. **Escalation ladder.** Nudge → training → manager notification, one rung at a
   time. The last needs a deployment flag, because telling somebody's manager is
   the only action here that cannot be walked back. When it is off, the ladder
   clamps to the highest permitted rung rather than going silent — otherwise
   disabling the strongest action would also lose the second-strongest.

Two of those are less obvious than they look. **Minimum spacing** exists because
a per-type cooldown and an escalation ladder make every rung free: an employee
nudged this morning escalates to training on their next trigger, training's own
cooldown has never been touched, and the second message lands hours later.

**Recency** exists because a 32-day-old credential submission — already outside
the scoring lookback, contributing exactly zero to the score it was attached to
— produced a message telling someone to reset their password *now*. It also
makes reprocessing safe without a separate operating mode, which matters more: a
backfill rescores a month of behaviour with `as_of` set to now, so every crossing
it produces is an artefact of ingestion order. Replaying the whole log now sends
nothing — 7,789 scores in, 212 stale triggers refused, zero messages — and
injecting one live incident produces exactly one nudge.

**Idempotency.** A unique index on `(tenant, employee, trigger_event_id)`
encodes *one behaviour, one message*. The type is deliberately not in the key: it
was, and a redelivered score then found one more prior in the ledger, climbed a
rung, and inserted cleanly as a different type — the same click producing a nudge
and then a training assignment. A score carrying no trigger id is refused
outright, because being able to do something exactly once is a precondition for
doing it at all.

The ledger row is written **before** the message is published. A crash between
them records an intervention nobody received; the reverse ordering sends a second
message to somebody who already got one. Only one of those is visible to the
person.

**Copy.** Generated from the catalog's own plain-English descriptions and a first
name — never an email, a surname, a token or a signal identifier, so the prompt
cannot leak what the copy is forbidden to contain. Output is validated before it
is sent, and any violation falls back to a static template. The validator runs
over the templates too, and a parametrised sweep asserts every one of them clears
every rule for every combination of rung and cause: a fallback that could not
pass its own guardrails would be a second unchecked path wearing a safe name.

The guardrail specific to this product is the one against **overclaiming**.
Bellwether observes behaviour — a click, a submission on a *simulated* page, an
address in someone else's breach dump. None of that establishes that an account
is in another person's hands, and telling an employee they have been breached
when they have not is a false statement that causes real alarm. It is also
exactly the leap a fluent model makes when asked to convey urgency.

**Not built.** Acknowledgement feedback. The catalog has
`intervention_acknowledged` and `intervention_ignored` and the ladder would be
better for reading them, but that needs a consumer writing engagement back into
the ledger. The ladder currently approximates it from whether the employee's
security-engagement signals are net aggravating.

## Handling employee data — `[partly built]`

This is behavioral data about identifiable people, which is the most sensitive
category the system could hold.

- **Built.** PII (email, name, manager) exists only on the `Employee`
  dimension; events carry the token. A test asserts `BehaviorEvent` has no PII
  fields, because the tempting mistake is denormalizing an email onto the event
  for a nicer dashboard — which silently moves PII into a topic with different
  retention than the table it was supposed to live in.
- **Built.** An address that matches more than one employee resolves to nobody.
  Vendors identify people by email and the dimension is the only thing that can
  turn that into a token, so a duplicate address means the platform cannot say
  whose behaviour it is looking at. Guessing would attribute one employee's
  phishing click to a colleague and produce a score that looks entirely
  plausible — the worst available failure for a product whose whole output is
  *which person*. This was a real bug, not a hypothetical: the synthetic
  population issued colliding addresses and silently merged 185 people into
  others' scores before anything noticed.
- **Designed.** Retention: raw payloads 30d, normalized events 400d, aggregates
  indefinitely, enforced by a scheduled job rather than by convention (day 10).
- **Designed.** Deletion: one function resolves a token, purges the dimension
  row, drops their Redis projection, and lets the score log age out under its
  own retention (day 10).
- **Built.** The read API takes its tenant from the credential and not from the
  request, so there is no parameter a caller can set to reach across one. An
  employee belonging to another tenant returns 404 with a body byte-identical to
  a genuinely missing one; a 403 would confirm the person exists, which is the
  thing tenancy is supposed to hide, and a test asserts the two cannot be told
  apart.
- **Built.** A privacy gradient across the read path. Ranking the population
  returns tokens and scores with no names, so browsing is pseudonymous and is
  not audited. Looking one person up returns their name and writes a row to the
  read audit log, synchronously, before the response is built — including when
  that person turns out to have no score, because the look happened either way.

  Both halves of that are deliberate. Who looked at whose risk score is itself
  sensitive: a tool that sorts colleagues by how much of a liability they are
  will be opened for reasons that have nothing to do with security, and a record
  of every look is the only thing that makes that answerable afterwards. But
  auditing the *ranking* too would bury those reads under a row for every
  dashboard refresh, which is how an audit log becomes unreadable and therefore
  useless.

## What I would do differently at real scale

Honest list, since these are the questions an interviewer should ask.

- **Python stream consumers won't hold.** Fine at thousands of events/sec on one
  partition set; at a hundred thousand I would move the scorer to Flink for real
  windowing, checkpointed state, and event-time watermarks instead of the
  hand-rolled window this design calls for. The load test on day 9 is what will
  tell me where the actual ceiling is, rather than my guess at it.
- **The read path serves live state and nothing historical.** Per-employee reads
  and the ranking are point and range queries against the Redis projection the
  scorer writes; the department rollup folds live over that projection, which is
  fine for one company's headcount and the wrong shape for a trend or a cohort.
  Anything spanning more than the current instant belongs in the marts, because
  an online store that gets scanned stops being fast for the queries it exists
  for. The API also runs one Postgres connection per store rather than a pool —
  psycopg serialises concurrent use, so it is correct but not concurrent.
- **Postgres is doing four jobs** — the employee dimension, connector cursors,
  the intervention ledger and the read audit log. Those separate under load:
  the dimension stays relational, the audit log becomes append-only storage
  nothing reads in a request path, and the ledger's uniqueness check is the only
  one that genuinely needs a transactional store.
- **Iceberg over raw Parquet.** Parquet plus a directory convention is enough for
  a demo; schema evolution and time-travel are what make reprocessing safe, and
  those want a table format.
- **The score is a weighted sum, deliberately.** It is explainable and
  debuggable, which matters more than accuracy at v1 — a security team will not
  act on a score it cannot interrogate. A learned model is the successor, and it
  needs the labeled outcome data this system's first year would produce.

## Open questions

- Windowed scoring recomputes over a 30-day lookback on every call. Correct and
  simple, but O(events in window) per event, which will matter once a stream
  consumer is calling it per message rather than a CLI calling it once.
  Incremental decay update is the obvious fix, and it complicates batch parity —
  which is exactly the tension the parity test exists to hold.
- No backpressure story between connectors and the raw topic.
- Parity will be asserted on a fixed fixture. That catches regressions but not
  drift in production; it should eventually be a continuous check comparing the
  two paths on live data.
- The high-value-target multiplier (1.4x) and the saturation constant (30) are
  judgment calls with no empirical basis. They are the two numbers I would most
  want real outcome data to correct.
