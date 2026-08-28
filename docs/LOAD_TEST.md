# Load test

**Question:** where does this break, and what breaks first?

A single throughput number would not answer it. Every phase is isolated so a
slow number can be attributed to something, and the two most useful results
came from swapping one component out and rerunning — which is the only way to
turn "the scorer is slow" into "the scorer spends 92% of its budget on Redis
round trips".

Reproduce with `make loadtest`, or a phase at a time:

```bash
python -m bellwether.cli load scoring                  # no infrastructure
python -m bellwether.cli load window   --store redis
python -m bellwether.cli load pipeline --store redis
python -m bellwether.cli load api      --concurrency 16
```

**Measured on** an Apple M-series laptop, arm64, 8 cores, with Redpanda,
Postgres and Redis in Docker Desktop. Everything runs on one machine, so these
are *relative* costs on shared hardware, not capacity numbers for a cluster.
The ratios are what transfer.

---

## Summary

| | |
| --- | --- |
| **Ceiling** | **736 events/sec** per scorer instance |
| **What sets it** | Three Redis round trips per message — **92%** of the budget |
| Not what sets it | The O(window) rescore. It costs 1.35 µs/event and would need a ~100× larger window to matter |
| Projected with 12 partitions | ~8,800 events/sec, one consumer per partition |
| Read path, point lookup | 957 req/s, p99 40 ms at 16 clients |
| Read path, department rollup | **60 req/s, flat from 2 clients up** |
| Backlog drain | 5,000 events in 6.8 s |

**One-line conclusion.** The pipeline is fast everywhere except the scorer, the
scorer is slow for a reason that has nothing to do with scoring, and the fix is
a Redis pipeline rather than a rewrite.

---

## 1. Scoring is not the problem

`score_events` against windows of increasing size, with no broker, no Redis and
no network. Whatever this shows is a property of the algorithm.

| Window | Median rescore | Per event |
| ---: | ---: | ---: |
| 10 | 0.037 ms | 3.70 µs |
| 50 | 0.113 ms | 2.25 µs |
| 100 | 0.188 ms | 1.88 µs |
| 500 | 0.694 ms | 1.39 µs |
| 1,000 | 1.318 ms | 1.32 µs |
| 5,000 | 6.749 ms | 1.35 µs |
| 10,000 | 13.678 ms | 1.37 µs |

Per-event cost is **flat at ~1.35 µs** beyond a few hundred events, so the work
is linear in window size with a small constant. The curve settles a question
that DESIGN.md has been carrying as an open item since day 3: recomputing the
whole window on every message is O(window), and *it does not matter at this
scale*. A realistic employee carries about 15 events in a 30-day window, which
is 0.05 ms — under 4% of the per-message budget.

**Where it would matter.** The scale is set by the noisiest employee rather
than the average one, and two effects compound. Events are keyed by
`employee_id`, so one pathological account — a service account writing audit
spam — both hot-spots its partition *and* has the most expensive rescore on it.
At 10,000 events in window, that partition drops to 73 rescores/sec while the
other eleven are unaffected. The mitigation is a cap on window size per
employee, not a faster scorer.

## 2. What the online store costs

Two round trips per message in the read path of the scorer — add the event to
the sorted set, then read the window back — plus a third to project the score.

| Operation | p50 | p95 | p99 | Rate |
| --- | ---: | ---: | ---: | ---: |
| `window.add` → Redis | 0.24 ms | 0.60 ms | 0.92 ms | 3,500/s |
| `window.events` → Redis | 0.34 ms | 0.52 ms | 0.95 ms | 2,750/s |
| `window.add` → memory | 0.00 ms | 0.00 ms | 0.02 ms | 201,000/s |
| `window.events` → memory | 0.00 ms | 0.00 ms | 0.00 ms | 1,712,000/s |

Redis latency is stable across runs — the p50s above moved by less than
0.02 ms over four repetitions — so it is a floor, not noise.

## 3. The pipeline, end to end

5,000 events through the real broker, one consumer per stage, on isolated
topics. The same run twice, changing only the online store:

| Stage | Redis | In memory |
| --- | ---: | ---: |
| produce → `events.raw` | 9,352/s | 9,571/s |
| normalize | 19,850/s | 20,853/s |
| **score** | **736/s** | **9,282/s** |
| Backlog drain, 5,000 events | 6.8 s | 0.54 s |

**12.6×.** Per message the scorer has a 1.359 ms budget with Redis and 0.108 ms
without it, so **1.25 ms — 92% — is the three round trips.** Deserialisation,
the dimension lookup, the scoring itself, serialisation and the produce call
together account for the remaining 8%.

That is the whole finding. The scorer is not compute-bound and it is not
broker-bound; it is bound by talking to Redis three times per message, on
localhost, where a round trip is as cheap as it will ever be.

**The fix, in order of effort.** `add` and `events` are adjacent and their
results are independent, so they combine into one pipelined call — `ZADD`,
`ZREMRANGEBYSCORE` and `ZRANGE` in one round trip, which is roughly a third of
the cost gone. The `record` write happens after scoring and cannot merge with
them, but it can be fire-and-forget, since the projection is a cache of the
topic and is rebuildable from it. Both together should land near 1,500/s per
instance. Neither requires touching `score_events`, which is the point of
measuring before optimising.

## 4. The read path

500 requests per endpoint, 16 concurrent clients, against the running API.

| Endpoint | Rate | p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: |
| `GET /v1/employees/{id}/score` | 957/s | 15 ms | 21 ms | 40 ms |
| `GET /v1/population/ranking` | 535/s | 22 ms | 75 ms | 135 ms |
| `GET /v1/population/departments` | **77/s** | 191 ms | 360 ms | 510 ms |

The point lookup being the *fastest* is worth noting, because it does the most:
Redis for the score, Postgres for the dimension, and a synchronous audit write
before the response is built. The audit row is not the bottleneck anybody would
guess it to be.

**The department rollup does not scale with clients at all.** Sweeping
concurrency:

| Clients | 1 | 2 | 4 | 8 | 16 | 32 | 64 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Ranking, req/s | 244 | 516 | 505 | 599 | 536 | 383 | 417 |
| Ranking, p50 ms | 3.5 | 3.7 | 7.2 | 12.7 | 28.1 | 75.9 | 136.3 |
| **Departments, req/s** | 46 | 77 | 62 | 62 | 59 | 58 | **75** |
| **Departments, p50 ms** | 17 | 22 | 57 | 126 | 270 | 523 | **753** |

Throughput is flat at ~60/s from two clients onward while latency grows
linearly — the signature of a serialised resource. Little's Law predicts it
almost exactly: at 64 clients and ~75 req/s, `L/λ` gives 0.85 s and the measured
p50 is 0.75 s. Adding clients buys nothing and costs latency proportionally.

The cause is not the database. The endpoint pages the entire scored population
out of Redis and folds it in Python, in one process, under the GIL. It is the
limitation DESIGN.md already described — "fine for one company's headcount and
the wrong shape for a trend" — with a number attached. The answer is not a
connection pool; it is that this query belongs in the marts, which is where the
equivalent already exists.

## 5. Cost

An estimate with stated assumptions, not a bill — nothing here has run on AWS.

At 736 events/sec per instance and 12 partitions, a sustained 5,000 events/sec
needs 7 scorer instances. On `m7g.large` at roughly $0.08/hr on-demand, that is
about $0.56/hr of scorer, or **$0.031 per million events**. MSK, ElastiCache
and S3 dominate at that volume: a three-broker `kafka.m7g.large` cluster is
around $0.75/hr regardless of throughput, so the marginal cost per event is
close to zero and the fixed cost is nearly all of it. The conclusion that
matters for capacity planning is that this workload is small — 5,000 events/sec
is 432 million events/day, roughly a 100,000-person company at Bellwether's
per-employee event rate — and the whole thing fits in a footprint costing under
$1,500/month.

## What this test does not cover

Stated because a load test that implies more coverage than it has is worse than
none.

- **One machine.** Producer, three brokers' worth of Redpanda, Postgres, Redis
  and the consumers all contend for 8 cores. Real deployments do not, so the
  absolute numbers are pessimistic and the ratios are what to trust.
- **No sustained soak.** Runs are seconds to a few minutes. Nothing here would
  catch a memory leak, an unbounded Redis key or a connection pool exhausting
  itself overnight.
- **Consumers are not scaled out.** The 12-partition projection is arithmetic,
  not a measurement. Rebalancing behaviour under load is untested; day 3 tested
  it for *correctness* — 430 messages redelivered, scores byte-identical — but
  not for throughput.
- **The intervention stage is excluded.** Its ceiling is not throughput but the
  copy call, which is 8 to 40 seconds against a hosted model and is why drafts
  are cached by brief shape. Interventions are 1.8% of scores, so it does not
  gate the pipeline.

## Two measurement bugs found while writing this

Both worth recording, because a load test's own correctness is the thing
readers cannot check.

**Negative latency.** The first end-to-end run reported an ingest-to-score p50
of *minus 24 seconds*. The accelerated simulator emits a phishing chain whose
later steps are dated up to 90 minutes ahead of the wall clock, so
`now - ingested_at` was negative for most events. Two consequences: the load
test now builds its own events with an honest `ingested_at`, and the scorer
counts and clamps future-dated events instead of feeding negatives into a
histogram whose first bucket starts at zero — where they vanish silently and
the SLO reads better than it is.

**Unreproducible throughput.** Five identical runs reported 807, 260, 783, 737
and 389 events/sec. Two causes, both in the harness rather than the system.
Timing a consumer from process start to exit included the consumer-group join,
which takes anywhere from a fraction of a second to several against a freshly
created topic; that is now measured from the first handled message to the last.
And the Redis window was not cleared between runs, so each run scored windows
5,000 events larger than the last and got faithfully slower — the O(window)
curve from section 1, showing up where it was not wanted. With both fixed,
three consecutive runs gave 804, 845 and 834.
