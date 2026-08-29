# Copy to `<environment>.backend.hcl` and fill in. The real files are
# gitignored because the bucket name embeds an account id.
#
#   terraform init -backend-config=env/dev.backend.hcl
#
# The bucket must exist before Terraform runs -- state cannot bootstrap itself.
# `scripts/tf_bootstrap.sh` creates it, versioned and encrypted. Versioning is
# not optional: it is the only thing standing between a corrupted state write
# and rebuilding the whole environment by hand.

bucket = "bellwether-tfstate-<account-id>"
key    = "<environment>/terraform.tfstate"
region = "us-east-1"

profile = "bellwether"
encrypt = true

# S3-native locking; requires Terraform >= 1.10 and no DynamoDB table.
use_lockfile = true
