# The encryption keys, and why there are two of them.
#
# One key would be simpler and wrong. `data` protects the stores that hold
# behavioural data; `tokens` is the key material behind field-level
# tokenization, and its whole purpose is to be *destroyable*.
#
# Destroying the token key crypto-shreds every tokenized field derived from it
# -- in Parquet, in Kafka segments, in a snapshot taken last March -- which is
# the only erasure that reaches a data lake. Destroying the data key would
# instead make the entire platform unreadable. Two keys, two blast radiuses.

data "aws_caller_identity" "current" {}

# The data key's policy, and the reason it has to be written out.
#
# Found by the first real `terraform apply`, not by `validate`: creating the
# MSK broker log group failed with
#
#   AccessDeniedException: The specified KMS key does not exist or is not
#   allowed to be used with Arn '...log-group:/aws/msk/bellwether-dev'
#
# With no `policy` argument a KMS key gets the AWS default, which delegates to
# IAM for principals *in this account*. Every other consumer of this key --
# RDS, EKS envelope encryption, MSK at rest, S3, ElastiCache -- reaches it
# through an IAM role, so all of them worked and this one did not.
# CloudWatch Logs is different in kind: it encrypts on its own behalf as a
# service principal, and a service principal is not covered by IAM delegation.
# It has to be named in the key policy itself.
#
# This is exactly the failure mode `infra/README.md` warned about before there
# was an account to test against -- an IAM condition spelled plausibly and
# wrongly -- and it is the argument for applying rather than validating.
data "aws_iam_policy_document" "data_key" {
  # Without this statement the key becomes unmanageable the moment a policy is
  # attached: an explicit policy replaces the default wholesale, and a KMS key
  # nobody can administer cannot even be scheduled for deletion.
  statement {
    sid    = "AccountAdministration"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
    }

    actions   = ["kms:*"]
    resources = ["*"]
  }

  statement {
    sid    = "CloudWatchLogsEncryption"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["logs.${var.region}.amazonaws.com"]
    }

    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:DescribeKey",
    ]

    resources = ["*"]

    # Scoped to this one log group rather than left open. `resources = ["*"]`
    # above is the key itself and cannot be narrowed; this condition is what
    # actually bounds the grant, and without it the statement would let
    # CloudWatch Logs encrypt *any* log group in the account under the key that
    # protects the warehouse.
    #
    # The ARN is written out rather than referenced from
    # `aws_cloudwatch_log_group.msk.arn`, which would be tighter and is a
    # dependency cycle: the log group cannot be created until the key permits
    # it, and the key cannot be written until the log group exists.
    condition {
      test     = "ArnEquals"
      variable = "kms:EncryptionContext:aws:logs:arn"
      values = [
        "arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/msk/${local.name}",
      ]
    }
  }
}

resource "aws_kms_key" "data" {
  description             = "bellwether ${var.environment}: data at rest"
  enable_key_rotation     = true
  deletion_window_in_days = 30
  policy                  = data.aws_iam_policy_document.data_key.json

  tags = { Name = "${local.name}-data" }
}

resource "aws_kms_alias" "data" {
  name          = "alias/${local.name}-data"
  target_key_id = aws_kms_key.data.key_id
}

resource "aws_kms_key" "tokens" {
  description = "bellwether ${var.environment}: tokenization key material. Destroying this unlinks every token."

  # Rotation *off*, unlike the data key, and this is the important line in the
  # file. Tokenization is deterministic: the same address must produce the same
  # token forever, or identity resolution breaks across every historical
  # partition. A rotated key is a new token space.
  enable_key_rotation = false

  # The maximum AWS allows. This key is the erasure lever for a whole tenant,
  # so the window before destruction becomes irreversible is as long as it can
  # be.
  deletion_window_in_days = 30

  tags = {
    Name    = "${local.name}-tokens"
    Purpose = "crypto-shredding"
  }
}

resource "aws_kms_alias" "tokens" {
  name          = "alias/${local.name}-tokens"
  target_key_id = aws_kms_key.tokens.key_id
}

# The tokenization secret itself, encrypted under the key above. Terraform
# creates the container and never the value: a secret in state is a secret in
# a bucket, and `terraform show` prints it.
resource "aws_secretsmanager_secret" "tokenization" {
  name        = "${local.name}/tokenization-secret"
  description = "HMAC secret for field-level tokenization. Rotating it invalidates every existing token."
  kms_key_id  = aws_kms_key.tokens.arn

  recovery_window_in_days = 30

  tags = { Name = "${local.name}-tokenization" }
}
