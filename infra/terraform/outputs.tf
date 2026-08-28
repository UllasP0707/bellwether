# What the application needs, and nothing it does not.
#
# No credentials here. The database password and the tokenization secret live
# in Secrets Manager and are read at runtime through the IRSA roles in iam.tf;
# an output is written to state, and state is a file in a bucket.

output "kafka_bootstrap" {
  value       = aws_msk_cluster.this.bootstrap_brokers_sasl_iam
  description = "BELLWETHER_KAFKA_BOOTSTRAP. IAM-authenticated TLS listener."
}

output "postgres_host" {
  value = aws_db_instance.this.address
}

output "postgres_secret_arn" {
  value       = aws_db_instance.this.master_user_secret[0].secret_arn
  description = "Where the rotated credential lives. Not the credential."
}

output "redis_endpoint" {
  value = aws_elasticache_replication_group.this.primary_endpoint_address
}

output "lake_bucket" {
  value = aws_s3_bucket.lake.id
}

output "archive_bucket" {
  value = aws_s3_bucket.archive.id
}

output "eks_cluster_name" {
  value = aws_eks_cluster.this.name
}

output "component_role_arns" {
  value       = { for name, role in aws_iam_role.component : name => role.arn }
  description = "Annotate each Kubernetes service account with the matching ARN."
}

output "tokenization_secret_arn" {
  value       = aws_secretsmanager_secret.tokenization.arn
  description = "Container only. Terraform never sets the value."
}

output "tokenization_key_arn" {
  value       = aws_kms_key.tokens.arn
  description = "Destroying this key unlinks every token for this environment. See docs/RUNBOOK.md."
}
