# Redis: the online feature store.
#
# Two things live here and they have different durability needs, which is why
# this is not simply "a cache". The per-employee window is rebuildable by
# replaying `events.normalized`, but that replay is thirty days of messages
# before the first score comes out. The score projection the API serves is
# rebuildable from `risk.scores` in seconds.
#
# So: replication on, and persistence off. Losing a node costs a failover
# rather than a thirty-day replay; keeping an AOF on a store that is entirely
# rebuildable buys latency on every write for a recovery path nobody would use.

resource "aws_elasticache_subnet_group" "this" {
  name       = local.name
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_elasticache_parameter_group" "this" {
  name   = local.name
  family = "redis7"

  # The window has a TTL and the sorted sets are bounded, so eviction should
  # never fire. `noeviction` makes that assumption loud: if memory does fill,
  # writes fail visibly rather than the cache silently dropping the windows of
  # whichever employees were least recently active -- who are precisely the
  # people whose scores would then be wrong.
  parameter {
    name  = "maxmemory-policy"
    value = "noeviction"
  }
}

resource "aws_elasticache_replication_group" "this" {
  replication_group_id = local.name
  description          = "bellwether ${var.environment} online feature store"

  engine         = "redis"
  engine_version = "7.1"
  node_type      = var.cache_node_type
  port           = 6379

  num_node_groups            = 1
  replicas_per_node_group    = var.environment == "prod" ? 2 : 1
  automatic_failover_enabled = true
  multi_az_enabled           = var.environment == "prod"

  subnet_group_name    = aws_elasticache_subnet_group.this.name
  security_group_ids   = [aws_security_group.data.id]
  parameter_group_name = aws_elasticache_parameter_group.this.name

  at_rest_encryption_enabled = true
  kms_key_id                 = aws_kms_key.data.arn
  transit_encryption_enabled = true

  # No snapshots, deliberately. See the header: everything here is
  # reconstructible from a topic, and a nightly snapshot of an online store is
  # a copy of employee risk scores sitting in a bucket for no reason.
  snapshot_retention_limit = 0

  maintenance_window = "sun:05:30-sun:06:30"

  tags = { Name = local.name }
}
