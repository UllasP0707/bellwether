# Alarms on the things Prometheus cannot see.
#
# `docker/alerts.yml` covers the application: consumer lag, scoring latency,
# intervention rate. These are the ones that fire when the *platform* is
# unhealthy in a way the application never observes -- a broker disk filling
# has no symptom in the pipeline right up until it has every symptom.

locals {
  alarm_actions = var.alarm_topic_arn == "" ? [] : [var.alarm_topic_arn]
}

resource "aws_cloudwatch_metric_alarm" "broker_disk" {
  count = var.kafka_broker_count

  alarm_name          = "${local.name}-broker-${count.index}-disk"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "KafkaDataLogsDiskUsed"
  namespace           = "AWS/Kafka"
  period              = 300
  statistic           = "Maximum"
  # 75, not 90. A full broker disk stops every producer, and expanding storage
  # takes longer than the gap between 90 and 100 allows.
  threshold         = 75
  alarm_description = "Broker log disk above 75%. Expand storage before it stops accepting writes."

  dimensions = {
    "Cluster Name" = aws_msk_cluster.this.cluster_name
    "Broker ID"    = count.index + 1
  }

  alarm_actions = local.alarm_actions
  tags          = { Name = "${local.name}-broker-disk" }
}

resource "aws_cloudwatch_metric_alarm" "rds_storage" {
  alarm_name          = "${local.name}-rds-storage"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 2
  metric_name         = "FreeStorageSpace"
  namespace           = "AWS/RDS"
  period              = 300
  statistic           = "Minimum"
  threshold           = 10737418240 # 10 GiB
  alarm_description   = "Under 10 GiB free. Autoscaling should have handled this, so it firing means autoscaling did not."

  dimensions    = { DBInstanceIdentifier = aws_db_instance.this.id }
  alarm_actions = local.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "redis_memory" {
  alarm_name          = "${local.name}-redis-memory"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "DatabaseMemoryUsagePercentage"
  namespace           = "AWS/ElastiCache"
  period              = 300
  statistic           = "Average"
  threshold           = 80
  # The parameter group sets `noeviction`, so this is the alarm before writes
  # start failing rather than before data starts silently disappearing. That
  # trade is deliberate; see elasticache.tf.
  alarm_description = "Online store above 80% memory. maxmemory-policy is noeviction, so the next stop is failed writes."

  dimensions    = { ReplicationGroupId = aws_elasticache_replication_group.this.id }
  alarm_actions = local.alarm_actions
}
