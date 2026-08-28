# Infrastructure

Terraform for the AWS footprint and Kubernetes manifests for the consumers.

## What is verified, and what is not

Stated first, because infrastructure code is the easiest thing in a repository
to present as more finished than it is.

| | |
| --- | --- |
| `terraform validate` | **passes**, and runs in CI on every push |
| `terraform fmt -check` | **passes**, and runs in CI |
| 51 resources across 9 files | parse, type-check and reference each other correctly |
| Kubernetes: 23 objects | parse and carry `apiVersion`/`kind` |
| **`terraform apply`** | **never run.** No AWS account is attached to this project |
| **`kubectl apply`** | **never run.** No cluster |

So: this is a design expressed in HCL rather than in prose, and `validate`
proves it is internally consistent — every reference resolves, every type
matches, no resource names a field that does not exist. It does not prove AWS
would accept it. An instance type could be unavailable in a region, a service
quota could refuse the cluster, an IAM condition key could be spelled
plausibly and wrongly. Treat the reasoning as the deliverable and the HCL as
its precise form.

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

# Against a real account:
terraform init -backend-config=env/prod.backend.hcl
terraform plan  -var environment=prod
```

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
