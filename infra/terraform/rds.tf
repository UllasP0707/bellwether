# Postgres: the employee dimension, connector cursors, the intervention ledger,
# the read audit log and the warehouse.
#
# Five jobs in one database, which DESIGN.md is explicit about being a
# scale-model decision rather than a good one. The two that separate first are
# the audit log, which is append-only and read by nothing in a request path,
# and the ledger, whose uniqueness check is the only part that genuinely needs
# a transactional store.

resource "aws_db_subnet_group" "this" {
  name       = local.name
  subnet_ids = aws_subnet.private[*].id
  tags       = { Name = local.name }
}

resource "aws_db_parameter_group" "this" {
  name   = local.name
  family = "postgres16"

  parameter {
    name  = "rds.force_ssl"
    value = "1"
  }

  # Log anything slower than a second. The department rollup measured 191ms at
  # p50 under load and the marts exist so it never becomes a second, so a
  # query crossing that line is a regression worth a log line.
  parameter {
    name  = "log_min_duration_statement"
    value = "1000"
  }
}

resource "aws_db_instance" "this" {
  identifier     = local.name
  engine         = "postgres"
  engine_version = "16.4"
  instance_class = var.rds_instance_class

  allocated_storage     = 100
  max_allocated_storage = 1000
  storage_type          = "gp3"
  storage_encrypted     = true
  kms_key_id            = aws_kms_key.data.arn

  db_name  = "bellwether"
  username = "bellwether"
  # Rotated by RDS into Secrets Manager rather than set here. A password in a
  # Terraform variable is a password in state.
  manage_master_user_password   = true
  master_user_secret_kms_key_id = aws_kms_key.data.arn

  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.data.id]
  parameter_group_name   = aws_db_parameter_group.this.name
  publicly_accessible    = false

  multi_az                = var.environment == "prod"
  backup_retention_period = var.backup_retention_days
  backup_window           = "03:00-04:00"
  maintenance_window      = "sun:04:30-sun:05:30"

  # Defaults on in every environment including dev. The cost of an accidental
  # destroy in dev is a rebuild; the cost of learning the flag was off in prod
  # is the intervention ledger, which is the one table here that cannot be
  # recomputed from anything.
  #
  # A variable rather than a literal, because the first real teardown showed
  # what a literal costs: `terraform destroy` fails on the protected instance
  # after destroying part of the environment, and the fix is to edit this file,
  # apply, and destroy again. An operator holding a half-destroyed environment
  # and a billing meter is being asked to edit source, which is the moment
  # people give up and leave it running. The default is unchanged; the escape
  # hatch is now explicit and auditable in shell history:
  #
  #   terraform destroy -var environment=dev -var rds_deletion_protection=false
  deletion_protection = var.rds_deletion_protection

  # Snapshot on the way out. `skip_final_snapshot = true` is the default in
  # most examples and it turns a mistaken `terraform destroy` into permanent
  # loss. Same override reasoning: a demo environment that has never held real
  # data leaves a snapshot behind that bills quietly and forever.
  skip_final_snapshot       = var.rds_skip_final_snapshot
  final_snapshot_identifier = var.rds_skip_final_snapshot ? null : "${local.name}-final"

  performance_insights_enabled    = true
  performance_insights_kms_key_id = aws_kms_key.data.arn
  enabled_cloudwatch_logs_exports = ["postgresql"]

  tags = { Name = local.name }
}
