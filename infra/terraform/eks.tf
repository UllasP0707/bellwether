# The cluster the consumers run on.
#
# EKS rather than ECS or plain EC2 because the workload is a set of long-lived
# consumers that need to scale independently and rebalance cleanly, and because
# the scaling signal is consumer lag rather than CPU -- which wants KEDA, and
# KEDA wants Kubernetes.

data "aws_iam_policy_document" "eks_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "cluster" {
  name               = "${local.name}-cluster"
  assume_role_policy = data.aws_iam_policy_document.eks_assume.json
}

resource "aws_iam_role_policy_attachment" "cluster" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_eks_cluster" "this" {
  name     = local.name
  role_arn = aws_iam_role.cluster.arn
  version  = "1.31"

  vpc_config {
    subnet_ids = concat(aws_subnet.private[*].id, aws_subnet.public[*].id)
    # Private endpoint only. A public API server endpoint is reachable from
    # the internet even with authentication in front of it, and nothing here
    # needs to be.
    endpoint_private_access = true
    endpoint_public_access  = false
    security_group_ids      = [aws_security_group.workload.id]
  }

  # Kubernetes Secrets are base64 in etcd by default, which is not encryption.
  # This is where the tokenization secret and the database credential land.
  encryption_config {
    provider { key_arn = aws_kms_key.data.arn }
    resources = ["secrets"]
  }

  enabled_cluster_log_types = ["api", "audit", "authenticator"]

  depends_on = [aws_iam_role_policy_attachment.cluster]

  tags = { Name = local.name }
}

# --- IRSA ---------------------------------------------------------------------
#
# The OIDC provider is what makes per-workload IAM roles possible. Without it
# every pod inherits the node's role, which means the API can read the raw
# archive and a connector can write the score topic -- and the least-privilege
# split in iam.tf becomes decoration.

data "tls_certificate" "oidc" {
  url = aws_eks_cluster.this.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "this" {
  url             = aws_eks_cluster.this.identity[0].oidc[0].issuer
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.oidc.certificates[0].sha1_fingerprint]
}

# --- nodes --------------------------------------------------------------------

data "aws_iam_policy_document" "node_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "node" {
  name               = "${local.name}-node"
  assume_role_policy = data.aws_iam_policy_document.node_assume.json
}

resource "aws_iam_role_policy_attachment" "node" {
  for_each = toset([
    "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
    "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
  ])

  role       = aws_iam_role.node.name
  policy_arn = each.value
}

resource "aws_eks_node_group" "stream" {
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "stream"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = aws_subnet.private[*].id

  # The load test says the scorer is bound by Redis round trips rather than
  # CPU, so this scales on count and not on instance size. Twelve partitions
  # on `events.normalized` is the ceiling on useful scorer replicas.
  instance_types = ["m7g.large"]
  ami_type       = "AL2023_ARM_64_STANDARD"

  scaling_config {
    min_size     = 2
    desired_size = 3
    max_size     = var.normalized_partitions
  }

  # One at a time. A consumer group rebalances on every membership change, and
  # replacing several nodes at once turns a rolling update into a stop.
  update_config { max_unavailable = 1 }

  depends_on = [aws_iam_role_policy_attachment.node]

  tags = { Name = "${local.name}-stream" }
}
