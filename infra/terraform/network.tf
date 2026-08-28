# The VPC everything else sits in.
#
# One decision runs through this file: **nothing Bellwether runs is reachable
# from the internet.** Every data store and every consumer is in a private
# subnet, egress goes through NAT, and the only ingress is whatever load
# balancer a cluster operator puts in front of the API -- which is not created
# here, because exposing the read path is a decision that should be visible in
# a diff rather than implied by a module default.

locals {
  name = "bellwether-${var.environment}"

  # /20 per AZ for pods -- EKS assigns a VPC IP per pod, so the subnet has to
  # be sized for pod count and not for node count. This is the mistake that
  # only shows up when a cluster stops scheduling at 250 pods.
  private_cidrs = [for index in range(length(var.availability_zones)) : cidrsubnet(var.vpc_cidr, 4, index)]
  public_cidrs  = [for index in range(length(var.availability_zones)) : cidrsubnet(var.vpc_cidr, 8, index + 200)]
}

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = local.name }
}

resource "aws_subnet" "private" {
  count             = length(var.availability_zones)
  vpc_id            = aws_vpc.this.id
  cidr_block        = local.private_cidrs[count.index]
  availability_zone = var.availability_zones[count.index]

  tags = {
    Name = "${local.name}-private-${var.availability_zones[count.index]}"
    # EKS reads these tags to decide where to place internal load balancers.
    "kubernetes.io/role/internal-elb"     = "1"
    "kubernetes.io/cluster/${local.name}" = "shared"
  }
}

resource "aws_subnet" "public" {
  count             = length(var.availability_zones)
  vpc_id            = aws_vpc.this.id
  cidr_block        = local.public_cidrs[count.index]
  availability_zone = var.availability_zones[count.index]

  # Explicitly false. The default is false too, but a public subnet that does
  # not auto-assign is the difference between "a NAT gateway lives here" and
  # "anything launched here is on the internet".
  map_public_ip_on_launch = false

  tags = {
    Name                                  = "${local.name}-public-${var.availability_zones[count.index]}"
    "kubernetes.io/role/elb"              = "1"
    "kubernetes.io/cluster/${local.name}" = "shared"
  }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = local.name }
}

# One NAT gateway rather than one per AZ. That is a deliberate cost/resilience
# trade and it is the wrong one for prod: losing this AZ costs egress for the
# whole VPC. At roughly $32/month each the three-AZ version is not expensive,
# and this is the first line to change when the environment stops being a demo.
resource "aws_eip" "nat" {
  domain = "vpc"
  tags   = { Name = "${local.name}-nat" }
}

resource "aws_nat_gateway" "this" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id
  depends_on    = [aws_internet_gateway.this]

  tags = { Name = local.name }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = { Name = "${local.name}-public" }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.this.id
  }

  tags = { Name = "${local.name}-private" }
}

resource "aws_route_table_association" "public" {
  count          = length(aws_subnet.public)
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private" {
  count          = length(aws_subnet.private)
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# S3 traffic is the highest-volume egress in the system -- every raw payload
# and every Parquet file. A gateway endpoint keeps it off the NAT gateway,
# which is both cheaper and one less thing between Spark and the lake.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]

  tags = { Name = "${local.name}-s3" }
}

# --- security groups ----------------------------------------------------------
#
# Every rule references another security group rather than a CIDR. A CIDR rule
# says "anything in this address range", which over the life of a VPC comes to
# mean anything at all; a group reference says "the scorer", and stays true.

resource "aws_security_group" "workload" {
  name        = "${local.name}-workload"
  description = "Bellwether consumers and the API."
  vpc_id      = aws_vpc.this.id

  egress {
    description = "Outbound to vendor APIs and AWS services."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name}-workload" }
}

resource "aws_security_group" "data" {
  name        = "${local.name}-data"
  description = "MSK, RDS and ElastiCache. Reachable only from the workload group."
  vpc_id      = aws_vpc.this.id

  tags = { Name = "${local.name}-data" }
}

locals {
  data_ports = {
    kafka_tls = 9094
    postgres  = 5432
    redis     = 6379
  }
}

resource "aws_vpc_security_group_ingress_rule" "data_from_workload" {
  for_each = local.data_ports

  security_group_id            = aws_security_group.data.id
  referenced_security_group_id = aws_security_group.workload.id
  ip_protocol                  = "tcp"
  from_port                    = each.value
  to_port                      = each.value
  description                  = "${each.key} from the workload group only"
}

resource "aws_vpc_security_group_egress_rule" "data_out" {
  security_group_id = aws_security_group.data.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
  description       = "Managed services need egress for backups and patching."
}
