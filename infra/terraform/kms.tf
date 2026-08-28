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

resource "aws_kms_key" "data" {
  description             = "bellwether ${var.environment}: data at rest"
  enable_key_rotation     = true
  deletion_window_in_days = 30

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
