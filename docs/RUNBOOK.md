# Runbook

What breaks, how you find out, and what to do about it.

Organised by **symptom**, because that is what you have at 3am. Every entry
names the alert that fires (from [`docker/alerts.yml`](../docker/alerts.yml)),
what to check, and what to do — in that order, and with the diagnosis
separated from the fix so you are not applying a remedy to a problem you have
not confirmed.

**Before anything else, two rules specific to this system.**

1. **A message cannot be unsent.** If interventions are going out wrongly, stop
   the intervention stage first and diagnose second. Every other component can
   be left running while you think; that one cannot.
2. **Nothing here is lost by stopping.** Every stage is a Kafka consumer with
   committed offsets and an idempotent handler. Stopping one costs latency and
   nothing else, and restarting reprocesses safely — measured, not assumed:
   adding a consumer mid-run redelivered 430 messages and every score came back
   byte-identical.

---

## Stop the pipeline

The thing to know before you need it.

```bash
kubectl -n bellwether scale deploy/intervention --replicas=0   # first, always
kubectl -n bellwether scale deploy/scorer       --replicas=0
kubectl -n bellwether scale deploy/normalizer   --replicas=0
```

Locally: Ctrl-C. The stage flushes its producer and commits before exiting.

Scores queue on `events.normalized`, which retains 30 days. Interventions queue
on `risk.scores`, which retains 30 days. **A pipeline stopped for a weekend
loses nothing** — it comes back and catches up. Restarting the intervention
stage after a long pause is the one case that needs thought: the 48-hour
recency gate will refuse every trigger older than that, which is correct
behaviour and will look like the stage doing nothing.

---

## Scores have stopped moving

**Fires:** `StageDown`, or `ConsumerLagGrowing`.

The most common incident, and the two causes look identical from a dashboard.

**Check, in order:**

```bash
# 1. Is anything running?
kubectl -n bellwether get pods
# up == 0 for a component means Prometheus cannot scrape it, which means the
# process is gone. A dead consumer produces no error -- only silence.

# 2. Is it running and behind, or running and idle?
curl -s localhost:9103/metrics | grep bellwether_consumer_lag_messages
```

**Lag high and rising** → the scorer cannot keep up. It tops out around 736
events/sec per instance ([LOAD_TEST.md](LOAD_TEST.md)), so either input spiked
or replicas dropped. KEDA scales on lag with a 5,000-message threshold and a
5-minute cooldown; if it has not reacted, check the ScaledObject.

```bash
kubectl -n bellwether get scaledobject scorer -o wide
kubectl -n bellwether scale deploy/scorer --replicas=8   # manual override
```

Do not scale past 12. `events.normalized` has 12 partitions and a thirteenth
consumer holds no assignment.

**Lag flat at zero and no scores** → nothing is arriving. Walk upstream: is the
normalizer emitting, is `events.raw` growing, are the connectors running? A
connector whose credential expired fails its poll and logs it, and the pipeline
below it looks perfectly healthy processing nothing.

**Lag high and *not* rising** → a backlog draining normally. Leave it. 5,000
events clear in about 7 seconds per instance; the alert deliberately requires
lag to be both high and not falling, so this should not have paged you.

---

## Employees are getting too many messages

**Fires:** `InterventionVolumeSpike`.

The failure this system is most obliged to avoid, and the only one where the
damage is external and permanent.

**Stop the stage first.**

```bash
kubectl -n bellwether scale deploy/intervention --replicas=0
```

**Then find out which gate stopped working.** Five gates all default toward
silence — recency (48h), spacing (24h), per-type cooldown (72h), weekly cap
(3), and the escalation ladder — so a spike means one was widened or one is not
being reached.

```sql
-- What went out, and under which trigger.
SELECT date_trunc('hour', created_at) AS hour, type, count(*)
FROM intervention
WHERE created_at > now() - interval '24 hours'
GROUP BY 1, 2 ORDER BY 1 DESC;

-- Anyone over the weekly cap is the clearest signal the cap is not applying.
SELECT employee_id, count(*)
FROM intervention
WHERE created_at > now() - interval '7 days'
GROUP BY 1 HAVING count(*) > 3 ORDER BY 2 DESC;
```

**The most likely cause is a replay being treated as live traffic.** Someone
reset a consumer group or reprocessed history. Under the default policy this is
already safe — replaying the entire score topic sends **zero** messages,
because the recency gate refuses triggers older than 48 hours — so a spike from
a replay means `--max-trigger-age-hours` was raised. Check the deployment args
before looking anywhere else.

**Recovery.** Interventions already published cannot be recalled; the topic is
an outbox and a delivery worker downstream has them. What you can do is stop
the source, fix the policy, and leave the ledger intact — it is the record of
what was sent, and deleting it would make the cooldowns forget and the next run
send everything again.

---

## Nobody is getting messages

**Fires:** `NoInterventionsAtAll`.

Quieter, more embarrassing, and much easier to miss. A broken trigger, a
cooldown misconfigured to a month, and a scorer that stopped publishing all
look identical from outside: silence.

```bash
# Is the stage seeing scores at all?
curl -s localhost:9104/metrics | grep bellwether_stage_messages_total

# What is it deciding?
curl -s localhost:9104/metrics | grep bellwether_interventions_suppressed_total
```

**Suppression is the expected outcome for most scores** — roughly 98% of them.
The question is *which reason dominates*:

| Dominant reason | What it means |
| --- | --- |
| `no_trigger` | Normal. Most scores are not band crossings. |
| `trigger_too_old` | Everything is stale. A backlog just drained, or a replay is running. |
| `cooldown` / `too_soon` / `weekly_cap` | The gates are working. Check they are not misconfigured to a month. |
| `already_sent` | A replay. The uniqueness fence is doing its job; nothing is wrong. |
| `unknown_employee` | The dimension is empty or stale. Run `load-dimension`. |
| `no_trigger_id` | Scores are being published without a trigger. Upstream bug — investigate the scorer. |

If `sent` is non-zero but nothing arrives with people, the problem is
downstream of this repo: the interventions topic is an outbox, and the delivery
worker that consumes it is deliberately not part of this system.

---

## A data contract is failing

**Fires:** `DataQualityContractFailing`.

Nothing is malformed and every dbt test is green — that is what these checks
are for. They compare a day against the fourteen before it.

```bash
python -m bellwether.cli quality check --as-of 2026-08-25
python -m bellwether.cli quality history
```

| Check | Almost always means |
| --- | --- |
| `signal_mix_drift` | A connector stopped returning one record type. The detail names the signal that moved. |
| `volume_shift` | A cursor stuck, or a credential expired. Compare against the trailing median in the output, not against yesterday. |
| `null_employee_id` | Identity resolution is failing — usually a vendor renamed a field. |
| `late_arrival_rate` | A source is backfilling. Any window computed before it finishes is wrong; wait, then reprocess the day. |

**Reprocessing a day is safe and is the usual fix.** Every task replaces its
day rather than appending, verified by running the same date twice and diffing
six tables.

```bash
docker compose --profile orchestration run --rm airflow \
  airflow dags test bellwether_daily 2026-08-25
```

**A day with zero rows is a finding, not a pass.** All four checks pass
trivially on an empty table, which is exactly how a silent ingestion failure
gets a green run — so the command exits non-zero and says so.

---

## Generated copy has stopped, or has gone wrong

**Fires:** `CopyFallingBackToTemplates` (info), `GuardrailsRejectingModelOutput`
(warning).

These are different problems and the metric distinguishes them, which it did
not originally: the first live run reported 163 "generation failures" that were
all rate limiting.

```bash
curl -s localhost:9104/metrics | grep bellwether_copy_failures_total
```

| `kind` | Fix |
| --- | --- |
| `rate_limited` | A billing page, not a code change. Templates are carrying traffic meanwhile. |
| `timeout` | Generation is slow (8–40s measured). Raise `BELLWETHER_COPY_TIMEOUT_SECONDS`, or accept templates. |
| `unreachable` | Network or endpoint. Check `BELLWETHER_COPY_BASE_URL`. |
| `rejected` | **The interesting one.** The model wrote something the guardrails refused. |
| `empty` | A reasoning model spent its whole budget thinking. Raise `max_tokens`. |

**Falling back to templates is not an outage.** The static path is validated by
the same guardrails and is a supported way to run this. Severity is `info` on
purpose.

**A rising `rejected` rate is different** and is about the model rather than
the infrastructure. Check which rule:

```bash
curl -s localhost:9104/metrics | grep bellwether_copy_guardrail_rejections_total
```

`overclaiming` is the one to take seriously. It means generated copy is
asserting that an account has been compromised, which Bellwether does not know
and which causes real alarm in someone whose account is fine. If it is rising,
switch to templates while you fix the prompt:

```bash
BELLWETHER_COPY_PROVIDER=template  # in the deployment, then restart
```

---

## Someone must be erased

Not a failure — a request, usually with a deadline.

```bash
python -m bellwether.cli privacy erase --employee E0042              # dry run
python -m bellwether.cli privacy erase --employee E0042 --no-dry-run --yes
python -m bellwether.cli privacy verify --employee E0042             # independent
```

**Three things to know before you run it.**

**It is not instantaneous.** The employee dimension is cached in every running
process so the scorer does not query Postgres per message. That cache expires
after 300 seconds (`BELLWETHER_DIMENSION_STALE_AFTER_SECONDS`), so erasure is
complete within five minutes, not immediately. Restart the consumers if you
need it sooner.

**Some things are kept on purpose** and the command lists them every time. The
read audit log stays: rows saying who looked at this person are a record about
the *actor*. If a deployment requires otherwise, `--purge-audit`.

**Kafka and the raw archive are not touched.** The topics are append-only and
age out on their own horizon holding tokens; the archive holds vendor payloads
with real addresses and is pruned at 30 days. If a request cannot wait for
that, the archive object has to be deleted directly, and that is a decision to
make explicitly.

---

## Restoring from nothing

Order matters, and only one step is not recomputable.

1. **Postgres.** Restore from the automated snapshot. This is the only
   irreplaceable store: the intervention ledger is the record of what was sent
   to whom, and nothing can rebuild it. Everything below can be recomputed.
2. **The dimension.** `python -m bellwether.cli load-dimension` if the
   snapshot predates the current population.
3. **Redis.** Do nothing. The window rebuilds as events arrive and the batch
   job recomputes the whole population nightly; a manual rebuild would mean
   replaying 30 days of `events.normalized` before the first score.
4. **The warehouse.** Replay the daily DAG over the affected dates. Every task
   replaces its day, so this is safe to run for a range.
5. **The lake.** Recoverable from `events.normalized` within its 30-day
   retention. Beyond that it is gone, which is what the retention policy says.

---

## Things that look like incidents and are not

Worth listing, because each of these has been mistaken for a bug.

**A backfill shows p50 latency in days.** `behaviour → score` measures the age
of the event, and on a backfill that is the age of the history. The `ingest →
score` line is the SLO.

**Most scores produce no intervention.** ~98% suppression is the design. A run
where *nothing* was suppressed is the anomaly, and the CLI warns about it.

**Two thirds of interventions go to low and moderate bands.** 68 of 137 in the
verification run. Four critical signals fire regardless of band, and those
people have no accumulated history to push them over one — reaching them is
most of the value.

**A replay sends nothing.** Correct. 7,789 scores in, 212 stale triggers
refused, zero messages.

**Prometheus shows four targets down locally.** The stages are CLI invocations,
so they are only up while running. `up == 0` is the intended signal.
