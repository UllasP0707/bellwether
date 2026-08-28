# The lake and the raw archive, as two buckets rather than two prefixes.
#
# They have different contents and therefore different rules. The lake holds
# canonical events, which carry a token and no PII. The archive holds vendor
# payloads verbatim, which *do* contain addresses -- it is the one store where
# the token/PII split does not protect anything, and it is kept only so a
# parser bug can be debugged against exactly what the vendor sent.
#
# Same horizon, different reasons, and separate buckets so the archive's rules
# can tighten without touching the lake.

resource "aws_s3_bucket" "lake" {
  bucket = "${local.name}-lake"
  tags   = { Name = "${local.name}-lake", Contains = "tokenized-events" }
}

resource "aws_s3_bucket" "archive" {
  bucket = "${local.name}-raw-archive"
  tags   = { Name = "${local.name}-raw-archive", Contains = "vendor-payloads-with-pii" }
}

locals {
  buckets = {
    lake    = { id = aws_s3_bucket.lake.id, days = var.lake_retention_days }
    archive = { id = aws_s3_bucket.archive.id, days = var.archive_retention_days }
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each = local.buckets

  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = local.buckets

  bucket = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.data.arn
    }
    # Cuts KMS API calls by reusing a data key across objects in a prefix.
    # Without it, a Spark job writing thousands of Parquet parts makes a KMS
    # call per part and gets throttled -- a real failure mode, not a cost note.
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_versioning" "this" {
  for_each = local.buckets

  bucket = each.value.id
  versioning_configuration { status = "Enabled" }
}

# Retention. These durations mirror `bellwether/warehouse/retention.py`, and
# that duplication is the risk: two systems enforcing one horizon can disagree,
# and the direction they disagree in is data outliving a policy. The lifecycle
# rule is the backstop -- it runs whether or not the Airflow DAG did.
resource "aws_s3_bucket_lifecycle_configuration" "this" {
  for_each = local.buckets

  bucket = each.value.id

  rule {
    id     = "expire"
    status = "Enabled"
    filter {}

    expiration { days = each.value.days }

    # Versioning is on, so an expired object leaves a noncurrent version
    # behind. Without this rule the bucket keeps every "deleted" object
    # forever and the retention policy is decorative.
    noncurrent_version_expiration { noncurrent_days = 7 }

    abort_incomplete_multipart_upload { days_after_initiation = 3 }
  }
}

# TLS-only. S3 accepts plaintext HTTP by default and nothing warns about it.
resource "aws_s3_bucket_policy" "tls_only" {
  for_each = local.buckets

  bucket = each.value.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource = [
        "arn:aws:s3:::${each.value.id}",
        "arn:aws:s3:::${each.value.id}/*",
      ]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}
