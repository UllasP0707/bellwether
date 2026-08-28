# Pinned, not floated.
#
# `~> 5.0` on the provider rather than no constraint: AWS provider majors
# rename resource attributes, and a `terraform apply` that behaves differently
# because somebody ran it on a Tuesday is the failure mode infrastructure-as-
# code exists to remove.
terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    # Used once, for the EKS OIDC thumbprint. Declared rather than left to
    # implicit resolution: an undeclared provider resolves to whatever is
    # latest at `init` time, which is exactly the non-determinism the version
    # pins above exist to remove.
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }

  # Filled in per environment. Deliberately not committed with a bucket name:
  # a shared state file is the one piece of this that cannot be recreated, and
  # pointing two environments at the same key is how it gets destroyed.
  #
  #   terraform init -backend-config=env/prod.backend.hcl
  backend "s3" {}
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = "bellwether"
      Environment = var.environment
      ManagedBy   = "terraform"
      # Everything here holds behavioural data about identifiable people. The
      # tag is what makes that findable by an auditor who does not know the
      # architecture.
      DataClass = "employee-behavioural"
    }
  }
}
