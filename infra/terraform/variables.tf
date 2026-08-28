variable "region" {
  type        = string
  description = "AWS region. One region: this data is subject to residency rules and replication is a decision, not a default."
  default     = "us-east-1"
}

variable "environment" {
  type        = string
  description = "Environment name, used in every resource name."

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging or prod."
  }
}

variable "vpc_cidr" {
  type    = string
  default = "10.40.0.0/16"
}

variable "availability_zones" {
  type        = list(string)
  description = "Three AZs. MSK wants one broker per AZ and RDS multi-AZ needs at least two."
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]

  validation {
    condition     = length(var.availability_zones) >= 3
    error_message = "MSK requires at least three availability zones."
  }
}

# --- capacity, sized from docs/LOAD_TEST.md ----------------------------------
#
# The load test measured 736 events/sec per scorer instance, bounded by Redis
# round trips rather than by CPU. These defaults follow from that rather than
# from a shrug: the scorer scales out, and the broker and cache do not need to
# be large to keep up with it.

variable "kafka_broker_type" {
  type    = string
  default = "kafka.m7g.large"
}

variable "kafka_broker_count" {
  type        = number
  default     = 3
  description = "One per AZ."

  validation {
    condition     = var.kafka_broker_count % 3 == 0
    error_message = "broker count must be a multiple of the AZ count for even placement."
  }
}

variable "normalized_partitions" {
  type        = number
  default     = 12
  description = "Caps scorer parallelism, because events are keyed by employee. 12 x 736/s is roughly 8,800 events/sec."
}

variable "rds_instance_class" {
  type    = string
  default = "db.t4g.medium"
}

variable "cache_node_type" {
  type        = string
  default     = "cache.m7g.large"
  description = "Memory sized for one 30-day window per employee, not for throughput."
}

# --- retention ----------------------------------------------------------------
#
# These mirror `bellwether/warehouse/retention.py`. Two systems enforcing one
# horizon is a real risk -- an S3 lifecycle rule and a Python job disagreeing
# means data survives past a policy nobody thinks it survived -- so the numbers
# live here as variables and the runbook says to change both together.

variable "lake_retention_days" {
  type        = number
  default     = 30
  description = "Raw lake partitions: a replay buffer for connector bugs."
}

variable "archive_retention_days" {
  type        = number
  default     = 30
  description = "Vendor payloads. The one store that holds addresses rather than tokens."
}

variable "backup_retention_days" {
  type    = number
  default = 14
}

variable "alarm_topic_arn" {
  type        = string
  default     = ""
  description = "SNS topic for alarms. Empty means alarms are created but notify nobody, which is worse than no alarms -- set it."
}
