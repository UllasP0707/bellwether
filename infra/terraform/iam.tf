# One role per component, and the reason it is not one role for everything.
#
# The blast radius of a compromised consumer should be that consumer's job. The
# scorer reads `events.normalized` and writes `risk.scores`; it has no reason to
# read the raw archive, which is the only store holding vendor payloads with
# addresses in them, and here it cannot. The intervention stage writes the
# ledger and is the only thing that does.
#
# This is more Terraform than a single shared role would be, and that cost is
# the point: an environment where least privilege is inconvenient is one where
# somebody attaches an admin policy at 2am and nobody notices.

locals {
  # `sub` binds a role to one Kubernetes service account, so a pod cannot
  # assume another component's role by guessing an ARN.
  oidc_host = replace(aws_iam_openid_connect_provider.this.url, "https://", "")

  components = ["connector", "normalizer", "scorer", "intervention", "api", "batch"]
}

data "aws_iam_policy_document" "irsa_assume" {
  for_each = toset(local.components)

  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.this.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_host}:sub"
      values   = ["system:serviceaccount:bellwether:${each.value}"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_host}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "component" {
  for_each = toset(local.components)

  name               = "${local.name}-${each.value}"
  assume_role_policy = data.aws_iam_policy_document.irsa_assume[each.value].json
  tags               = { Component = each.value }
}

# --- Kafka ---------------------------------------------------------------------
#
# MSK IAM auth is per topic and per consumer group, which is what makes the
# split above real rather than notional.

locals {
  cluster_arn = aws_msk_cluster.this.arn
  topic_arn   = replace(local.cluster_arn, ":cluster/", ":topic/")
  group_arn   = replace(local.cluster_arn, ":cluster/", ":group/")

  # Which topics each component may read and write. A component absent from
  # one side of this map cannot touch that topic at all.
  kafka_access = {
    connector    = { read = [], write = ["bellwether.events.raw"] }
    normalizer   = { read = ["bellwether.events.raw"], write = ["bellwether.events.normalized", "bellwether.events.dlq"] }
    scorer       = { read = ["bellwether.events.normalized"], write = ["bellwether.risk.scores"] }
    intervention = { read = ["bellwether.risk.scores"], write = ["bellwether.interventions"] }
    api          = { read = [], write = [] }
    batch        = { read = [], write = [] }
  }
}

data "aws_iam_policy_document" "kafka" {
  for_each = { for k, v in local.kafka_access : k => v if length(v.read) + length(v.write) > 0 }

  statement {
    actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster"]
    resources = [local.cluster_arn]
  }

  dynamic "statement" {
    for_each = length(each.value.read) > 0 ? [1] : []
    content {
      actions   = ["kafka-cluster:ReadData", "kafka-cluster:DescribeTopic"]
      resources = [for topic in each.value.read : "${local.topic_arn}/${topic}"]
    }
  }

  dynamic "statement" {
    for_each = length(each.value.write) > 0 ? [1] : []
    content {
      actions   = ["kafka-cluster:WriteData", "kafka-cluster:DescribeTopic"]
      resources = [for topic in each.value.write : "${local.topic_arn}/${topic}"]
    }
  }

  # Group names are prefixed by component, so one consumer cannot join
  # another's group and steal its partitions.
  dynamic "statement" {
    for_each = length(each.value.read) > 0 ? [1] : []
    content {
      actions   = ["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"]
      resources = ["${local.group_arn}/bellwether-${each.key}*"]
    }
  }
}

resource "aws_iam_role_policy" "kafka" {
  for_each = data.aws_iam_policy_document.kafka

  name   = "kafka"
  role   = aws_iam_role.component[each.key].id
  policy = each.value.json
}

# --- S3 -------------------------------------------------------------------------

locals {
  # The connector writes the archive and nothing reads it in normal operation.
  # Spark reads the lake. Notably absent: the scorer and the API, neither of
  # which has any reason to touch object storage.
  s3_access = {
    connector = { write = [aws_s3_bucket.archive.arn, aws_s3_bucket.lake.arn], read = [] }
    batch     = { write = [aws_s3_bucket.lake.arn], read = [aws_s3_bucket.lake.arn] }
  }
}

data "aws_iam_policy_document" "s3" {
  for_each = local.s3_access

  dynamic "statement" {
    for_each = length(each.value.write) > 0 ? [1] : []
    content {
      actions   = ["s3:PutObject", "s3:AbortMultipartUpload"]
      resources = [for arn in each.value.write : "${arn}/*"]
    }
  }

  dynamic "statement" {
    for_each = length(each.value.read) > 0 ? [1] : []
    content {
      actions   = ["s3:GetObject", "s3:ListBucket"]
      resources = concat(each.value.read, [for arn in each.value.read : "${arn}/*"])
    }
  }

  statement {
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.data.arn]
  }
}

resource "aws_iam_role_policy" "s3" {
  for_each = data.aws_iam_policy_document.s3

  name   = "s3"
  role   = aws_iam_role.component[each.key].id
  policy = each.value.json
}

# --- secrets ---------------------------------------------------------------------
#
# The tokenization secret reaches the connector, which resolves identity, and
# nothing else. In particular not the API: a read path that can compute tokens
# can correlate an erased person's history across the lake.

data "aws_iam_policy_document" "tokenization" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.tokenization.arn]
  }
  statement {
    actions   = ["kms:Decrypt"]
    resources = [aws_kms_key.tokens.arn]
  }
}

resource "aws_iam_role_policy" "tokenization" {
  name   = "tokenization"
  role   = aws_iam_role.component["connector"].id
  policy = data.aws_iam_policy_document.tokenization.json
}

# The database credential RDS rotates. Every component that speaks to Postgres
# needs it; the connector, scorer, intervention stage and API all do.
data "aws_iam_policy_document" "database" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_db_instance.this.master_user_secret[0].secret_arn]
  }
  statement {
    actions   = ["kms:Decrypt"]
    resources = [aws_kms_key.data.arn]
  }
}

resource "aws_iam_role_policy" "database" {
  for_each = toset(["connector", "scorer", "intervention", "api", "batch"])

  name   = "database"
  role   = aws_iam_role.component[each.value].id
  policy = data.aws_iam_policy_document.database.json
}
