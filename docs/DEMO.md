# Demo

A 90-second walkthrough: one employee, three events, one message.

`./scripts/demo.sh` runs the whole narrative end to end, paced for recording.
This document is the shot list and the script — what to show, in what order,
and what to say over it.

```bash
make up && make topics && make seed && make backfill-kafka
make normalize && make score-stream     # so there is history to move against
./scripts/demo.sh                       # PACE=4 to slow it down
```

---

## Before you record

The demo is much better with history behind it. A fresh database produces an
employee whose first-ever score is their only score, and the interesting claim
— that a band *moved* — needs something to move from.

```bash
make backfill-kafka   # 30 days into the lake and the raw topic
make normalize
make score-stream     # ~500 employees scored, ~8k events
make observe          # optional: grafana and jaeger for the last two shots
```

Terminal at 100×30 or so. Larger and the tables wrap; smaller and the rich
output truncates columns.

---

## The script

**0:00–0:10 — the problem.**

> Most enterprise breaches start with a person. The standard response is annual
> training and quarterly phishing tests, which are disconnected in both time
> and content from the moment the risk was created. This scores behaviour
> continuously and intervenes within seconds of it.

**0:10–0:20 — where the employee starts.**

Show `scores --employee E0042`.

> A decayed weighted sum over a thirty-day window. Explainable on purpose —
> every contribution is attributable to a signal, because a security team will
> not act on a number it cannot interrogate.

**0:20–0:32 — the incident.**

Show `generate incident`, three lines of output.

> A phishing simulation is delivered, clicked, and credentials are submitted.
> A hundred and twelve seconds.

**0:32–0:45 — through the pipeline.**

Show normalize and score.

> Raw is keyed by the vendor's record id. Normalized is re-keyed onto the
> person, so everything about one employee lands on one partition and the
> scorer needs no cross-partition coordination.

Then the new score.

> The band moved, and the driver is named with its contribution.

**0:45–1:05 — the intervention. This is the part to spend time on.**

Show the suppression table — the large `suppressed` count next to `sent: 1`.

> Most of the code in this package exists to *not* send a message. Five gates,
> all defaulting to silence: the behaviour has to be under forty-eight hours
> old, twenty-four hours between any two messages, a per-type cooldown, three a
> week maximum, and an escalation ladder that only climbs one rung at a time.
> A human-risk platform whose failure mode is messaging people too much stops
> being used, and then it protects nobody.

Then the message itself.

> Written from the signal catalog's own plain-English descriptions and a
> placeholder name — the prompt carries no personal data at all. And validated
> before anyone reads it: no blame, no threat, and above all no claim that the
> account has been compromised. That last one is the guardrail specific to this
> product, because we observe a click on a *simulated* page, and telling
> somebody they have been breached when they have not is a false statement that
> causes real alarm. It is also exactly the leap a fluent model makes when
> asked to convey urgency.

**1:05–1:20 — the constraint the project is organised around.**

Show `make parity` output, or the recorded numbers.

> One signal catalog defines every behaviour's weight and decay. The streaming
> scorer and the Spark batch scorer are two evaluation strategies over the same
> pure function, so they cannot silently disagree — and that is measured rather
> than asserted. One fixed event log through both paths: 1,986 events, 120
> employees, zero disagreements, largest delta zero. Exact equality, not a
> tolerance.

**1:20–1:30 — the safety property.**

Show the replay: `sent: 0`.

> Replaying the entire history rescores everyone and messages nobody. Every
> band crossing on the way up is an artefact of ingestion order rather than
> something that just happened, and the recency gate refuses all of them.
> That is what makes reprocessing safe without a separate operating mode.

---

## Optional closing shots

Two more, if the recording has room.

**One trace across three topics.** `./scripts/trace_demo.sh`, then Jaeger.

> A connector fetching a record and the intervention it eventually causes are
> four processes and three Kafka topics apart, and never overlap in time. The
> W3C traceparent rides in the message headers, so this is one trace: produced,
> normalized, scored, sent.

**Erasure.** `python -m bellwether.cli privacy erase --employee E0208`.

> Deleting a person is one row and a projection drop, and it is that small only
> because events carry a token and PII lives in one table. What it *keeps* is
> reported every time — the read audit log stays, because a row saying who
> looked at this person is a record about the person who looked.

---

## What not to show

- **The dashboard as the opening shot.** It is the least interesting part and
  it looks like every other dashboard. Lead with the score moving.
- **The full 500-employee ranking.** A wall of tokens says nothing. One
  employee, followed through, says the same thing better.
- **Grafana with no traffic.** Empty panels read as broken. Either drive load
  first or leave it out.

---

## Recording notes

The video is not in this repository. `scripts/demo.sh` exists so recording it
is a screen capture rather than a performance: the script does the typing, the
pacing is a variable, and every number that appears comes from the system
rather than from a slide.
