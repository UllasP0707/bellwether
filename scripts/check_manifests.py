"""Check the Kubernetes manifests parse and say what they claim to.

No cluster is attached to this project, so `kubectl apply --dry-run=server`
is not available and this is the honest ceiling: every document parses, every
object carries the fields Kubernetes requires to route it, and the set is
complete. It cannot tell you the API server would accept a field name.

Run by CI and by `make manifests`.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

MANIFESTS = Path(__file__).resolve().parent.parent / "infra" / "k8s"
REQUIRED = {"apiVersion", "kind", "metadata"}

# Every component that runs as a workload must have all four, or it is
# deployed without an identity, without configuration, or without being
# scraped -- each of which fails quietly rather than loudly.
COMPONENTS = {"scorer", "normalizer", "intervention", "api"}


def main() -> int:
    objects: list[dict[str, object]] = []

    for path in sorted(MANIFESTS.glob("*.yaml")):
        for index, document in enumerate(yaml.safe_load_all(path.read_text())):
            if document is None:
                continue
            missing = REQUIRED - set(document)
            if missing:
                print(f"{path.name} doc {index}: missing {sorted(missing)}", file=sys.stderr)
                return 1
            objects.append(document)
        print(f"  {path.name}")

    kinds = [str(obj["kind"]) for obj in objects]
    print(f"{len(objects)} objects across {len(set(kinds))} kinds")

    accounts = {
        str(obj["metadata"].get("name"))  # type: ignore[union-attr]
        for obj in objects
        if obj["kind"] == "ServiceAccount"
    }
    if not accounts >= COMPONENTS:
        print(f"no service account for {sorted(COMPONENTS - accounts)}", file=sys.stderr)
        return 1

    # Every deployment must name a service account. The default one has no IAM
    # role attached, so a deployment that omits it gets the node's identity --
    # which is broader than any component's and would silently undo the
    # per-component split in infra/terraform/iam.tf.
    for obj in objects:
        if obj["kind"] != "Deployment":
            continue
        spec = obj["spec"]["template"]["spec"]  # type: ignore[index]
        name = obj["metadata"]["name"]  # type: ignore[index]
        if not spec.get("serviceAccountName"):
            print(f"deployment {name} has no serviceAccountName", file=sys.stderr)
            return 1
        for container in spec["containers"]:
            security = container.get("securityContext", {})
            if security.get("allowPrivilegeEscalation") is not False:
                print(f"{name}: allowPrivilegeEscalation must be false", file=sys.stderr)
                return 1
            if not security.get("readOnlyRootFilesystem"):
                print(f"{name}: readOnlyRootFilesystem must be true", file=sys.stderr)
                return 1

    if "NetworkPolicy" not in kinds:
        print("no NetworkPolicy: every pod could reach every other pod", file=sys.stderr)
        return 1

    print("service accounts, security contexts and network policies all present")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
