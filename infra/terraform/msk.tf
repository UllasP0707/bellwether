# The broker.
#
# MSK provisioned rather than Serverless, for one reason that matters here:
# Serverless does not let you set `num.partitions` or per-topic retention, and
# this design depends on both. Partition count on `events.normalized` is what
# caps scorer parallelism, because events are keyed by employee.

resource "aws_msk_configuration" "this" {
  name           = "${local.name}-config"
  kafka_versions = ["3.6.0"]
  description    = "Partition and retention defaults matching scripts/create_topics.sh."

  # These mirror the local topic script. Retention is per topic there and set
  # by the creating client; this is the floor for anything created without one.
  #
  # Auto-creation is off. A typo in a topic name should fail loudly, not
  # silently create a topic nobody consumes and lose every message written to
  # it -- which is exactly what auto-creation does.
  server_properties = <<-PROPERTIES
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=${var.normalized_partitions}
    log.retention.hours=720
    unclean.leader.election.enable=false
  PROPERTIES
}

resource "aws_msk_cluster" "this" {
  cluster_name           = local.name
  kafka_version          = "3.6.0"
  number_of_broker_nodes = var.kafka_broker_count

  broker_node_group_info {
    instance_type   = var.kafka_broker_type
    client_subnets  = aws_subnet.private[*].id
    security_groups = [aws_security_group.data.id]

    storage_info {
      ebs_storage_info {
        volume_size = 200

        # Storage grows on its own rather than paging somebody at 3am. A full
        # broker disk stops the whole pipeline, and the recovery is slower than
        # the expansion would have been.
        provisioned_throughput { enabled = false }
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = aws_kms_key.data.arn

    encryption_in_transit {
      # TLS only. `TLS_PLAINTEXT` is the default and it means a
      # misconfigured client silently connects unencrypted, which is worse
      # than one that fails to connect.
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  client_authentication {
    sasl { iam = true }
  }

  # Broker logs to CloudWatch and metrics to Prometheus. The exporters matter:
  # consumer lag is the number this system is watched by, and it comes from the
  # broker rather than from any consumer.
  open_monitoring {
    prometheus {
      jmx_exporter { enabled_in_broker = true }
      node_exporter { enabled_in_broker = true }
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }

  tags = { Name = local.name }
}

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${local.name}"
  retention_in_days = 30
  kms_key_id        = aws_kms_key.data.arn
}
