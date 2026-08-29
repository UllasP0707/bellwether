# Infrastructure

Terraform for the AWS footprint and Kubernetes manifests for the consumers.

## What is verified, and what is not

Stated first, because infrastructure code is the easiest thing in a repository
to present as more finished than it is.

| | |
| --- | --- |
| `terraform validate` / `fmt -check` | **pass**, and run in CI on every push |
| **`terraform apply`** | **run against a real AWS account.** 102 resources created, `Apply complete!`, then destroyed |
| MSK, EKS, RDS, ElastiCache | all reached `ACTIVE`/`available` |
| Per-topic Kafka IAM | **11/11** allow *and* deny decisions correct under `iam simulate-principal-policy` |
| The archive claim | scorer, API and batch all **denied** `s3:GetObject` on the raw archive |
| IRSA | trust policy bound to the live OIDC issuer, `sub` **and** `aud` conditions both present |
| **`kubectl apply`** | **still never run.** The API endpoint is private, and opening it for a demo was not worth undoing the argument in `eks.tf` |

Everything above is one environment, in one region, at one moment. It proves
the configuration converges and that the permission boundaries are real. It
does not prove this survives an upgrade, a failover, or a year of drift.

### What `validate` could not have caught

The first apply failed, and the failure is the argument for doing it. Creating
the MSK broker log group returned:

```
AccessDeniedException: The specified KMS key does not exist or is not allowed
to be used with Arn '...:log-group:/aws/msk/bellwether-dev'
```

`aws_kms_key.data` had no `policy` argument, so it took the AWS default, which
delegates authorization to IAM **for principals in this account**. Every other
consumer of that key — RDS storage and Performance Insights, EKS envelope
encryption, MSK at rest, S3, ElastiCache — reaches it through an IAM role, so
five of six worked. CloudWatch Logs encrypts on its own behalf as a *service*
principal, and service principals are not covered by that delegation. It has to
be named in the key policy.

This file previously predicted the class of failure without being able to find
the instance: *"an IAM condition key could be spelled plausibly and wrongly."*
That is exactly what it was.

The fix in `kms.tf` also carries a constraint worth knowing about. The natural
way to scope the grant is a condition on `aws_cloudwatch_log_group.msk.arn`,
and that is a dependency cycle — the log group cannot be created until the key
permits it, and the key policy cannot be written until the log group exists. So
the ARN is composed from `var.region` and the account id instead, which is
looser than a direct reference and the reason the condition is `ArnEquals` on
one exact log group rather than a prefix match.

## Layout

```
terraform/
  versions.tf      provider pins, S3 backend (config supplied per environment)
  variables.tf     capacity sized from docs/LOAD_TEST.md; retention mirroring
                   bellwether/warehouse/retention.py
  network.tf       VPC, private subnets, one NAT, S3 gateway endpoint,
                   security groups that reference groups rather than CIDRs
  kms.tf           two keys: data at rest, and the destroyable token key
  storage.tf       lake and raw archive as separate buckets
  msk.tf           3 brokers, IAM auth, TLS only, auto-create off
  rds.tf           Postgres 16, encrypted, deletion protection, no password in state
  elasticache.tf   Redis, replicated, no persistence, noeviction
  eks.tf           private-endpoint cluster, IRSA, one node group
  iam.tf           one role per component, per-topic Kafka permissions
  alarms.tf        the four platform failures Prometheus cannot see
  outputs.tf       endpoints and role ARNs. No credentials
k8s/
  00..09           namespace through PodDisruptionBudgets
```

## The decisions worth arguing about

**Two KMS keys, and one of them never rotates.** `kms.tf` creates a data key
(rotating) and a token key (not rotating). Tokenization is deterministic — the
same address must produce the same token forever or identity resolution breaks
across every historical partition — so rotating that key would create a new
token space. Its value is instead that destroying it crypto-shreds every
tokenized field derived from it, in Parquet, in Kafka segments, in a snapshot
from last March. That is the only erasure that reaches a data lake, and it is
far too blunt for one person's request; see
[`bellwether/privacy/`](../bellwether/privacy/) for the per-person path.

**One IAM role per component, not one for the platform.** The scorer reads
`events.normalized` and writes `risk.scores`. It cannot read the raw archive,
which is the only store holding vendor payloads with addresses in them. This is
more Terraform than a shared role, and that cost is the argument: an
environment where least privilege is inconvenient is one where somebody
attaches an admin policy at 2am.

**The scorer scales on consumer lag, not CPU.** The load test measured 736
events/sec per instance, bound by Redis round trips rather than compute — so a
scorer falling behind sits at low CPU while lag climbs, and a CPU-based HPA
would never scale it out. That is the reason for KEDA, and the reason this runs
on Kubernetes at all rather than on an autoscaling group.

**`maxReplicaCount: 12`** because `events.normalized` has twelve partitions and
a thirteenth consumer in the group would hold no assignment and do nothing.

**The intervention stage is a singleton with `Recreate`.** Not for throughput —
interventions are 1.8% of scores. It is the only stage whose side effect
reaches a person, and two instances mid-rollout means a rebalance and
redelivery. The unique index on `(tenant, employee, trigger_event_id)` makes
that safe, but "safe because a database constraint catches it" is a worse place
to be than "cannot happen" for the component that emails people.

**Default-deny NetworkPolicy.** Without it, every pod can reach every other
pod, and the IAM split becomes half a control: the scorer could not read the
archive through IAM, but could reach the connector's pod and ask it to.

**One NAT gateway.** A deliberate cost/resilience trade and the wrong one for
production: losing that AZ costs egress for the whole VPC. At roughly $32/month
each the three-AZ version is not expensive, and it is the first line to change
when an environment stops being a demo. Flagged rather than quietly shipped.

## Running it

```bash
cd infra/terraform
terraform init -backend=false     # what CI does
terraform validate
terraform fmt -check -recursive
```

Against a real account. The state bucket has to exist first — state cannot
bootstrap itself — and `env/*.backend.hcl` is gitignored because the bucket
name embeds the account id; copy `env/example.backend.hcl` to start.

```bash
terraform init -backend-config=env/dev.backend.hcl
terraform plan  -var environment=dev
terraform apply -var environment=dev
```

Tearing it down needs two overrides, and they are variables rather than
literals for a reason. `rds_deletion_protection` defaults to `true` in every
environment including dev, which is correct and which makes `terraform destroy`
fail partway — leaving an operator with a half-destroyed environment, a running
meter, and instructions to go edit `rds.tf`. That is the moment people give up
and leave infrastructure running, so the escape hatch is explicit and visible
in shell history:

```bash
terraform destroy -var environment=dev \
  -var rds_deletion_protection=false \
  -var rds_skip_final_snapshot=true
```

Budget roughly **$1.35/hour** for the full environment, two thirds of it MSK
and ElastiCache. Neither is sized for throughput — the load test found 736
events/sec bounded by Redis round trips, which a far smaller node would serve.
Three brokers is one per availability zone and the cache node is sized for one
30-day window per employee. This is a high-availability bill, not a
throughput bill.

Set a budget alarm **before** the first apply, not after. AWS cost data lags
8–12 hours, so an alarm is a backstop and not a circuit breaker; the thing that
actually protects you is confirming `destroy` reached `Destroy complete!`
rather than assuming it did.

Then substitute the outputs into the manifests — `ACCOUNT_ID`, `REGION`,
`ENV`, `VERSION`, `MSK_BOOTSTRAP`, `REDIS_ENDPOINT` — and apply. Placeholders
rather than committed values, because the account id is the one piece of this
that should not be in a public repository, and because a manifest that
hardcodes an environment is a manifest that gets copied and half-edited.

## Deliberately absent

- **A load balancer or Ingress for the API.** Exposing the read path is a
  decision that should be visible in a diff rather than implied by a module
  default.
- **Secret *values*.** Terraform creates the Secrets Manager container; RDS
  rotates the database password into it; the tokenization secret is set out of
  band. A secret in a variable is a secret in state, and state is a file in a
  bucket that `terraform show` will print.
- **Cross-region replication.** This is behavioural data about identifiable
  people and is subject to residency rules, so copying it to a second region is
  a decision, not a default.
- **A Helm chart.** Nine numbered manifests with comments are more readable
  than a chart with a `values.yaml`, and this deploys one application to one
  namespace. A chart earns its place when the same thing ships to people who
  need to configure it differently.
