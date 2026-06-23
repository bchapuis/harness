#!/bin/bash
# Bring up the three-node harness-standalone StatefulSet on whatever cluster
# kubectl currently points at: build the image, make it reachable, create the
# API-key secret, apply the manifest, wait for all three pods, print how to
# attach. The manual walkthrough this automates lives in k8s/README.md.
set -euo pipefail
cd "$(dirname "$0")/.."   # build context is the whole workspace

IMAGE=${HARNESS_IMAGE:-harness-standalone:latest}

if [ -z "${ANTHROPIC_API_KEY:-}" ]; then
  echo "Set ANTHROPIC_API_KEY first:  export ANTHROPIC_API_KEY=sk-ant-…" >&2
  exit 1
fi

# A wrong-context apply lands in someone else's cluster — fail loud if kubectl
# can't reach one, and show which it is so the target is never a surprise.
if ! kubectl version >/dev/null 2>&1 || ! kubectl get nodes >/dev/null 2>&1; then
  echo "kubectl can't reach a cluster. Point it at one (kind/minikube/OrbStack/Docker Desktop)" >&2
  echo "and retry. Current context: $(kubectl config current-context 2>/dev/null || echo none)" >&2
  exit 1
fi
CONTEXT=$(kubectl config current-context)
echo "▸ target cluster: $CONTEXT"

echo "▸ building $IMAGE"
docker build -q -t "$IMAGE" . >/dev/null

# The cluster pulls with IfNotPresent, so the image has to be in ITS store, not
# just the host daemon's. kind and minikube keep separate stores and need an
# explicit load; OrbStack and Docker Desktop share the daemon's, so the build
# above is enough. Override the heuristic with HARNESS_IMAGE_LOADER if needed
# (one of: kind, minikube, none) — e.g. for a named kind cluster.
loader=${HARNESS_IMAGE_LOADER:-}
if [ -z "$loader" ]; then
  case "$CONTEXT" in
    kind-*)                  loader=kind ;;
    minikube)                loader=minikube ;;
    orbstack|docker-desktop) loader=none ;;
    *)                       loader=unknown ;;
  esac
fi
case "$loader" in
  kind)     echo "▸ loading image into kind";     kind load docker-image "$IMAGE" --name "${CONTEXT#kind-}" ;;
  minikube) echo "▸ loading image into minikube"; minikube image load "$IMAGE" ;;
  none)     echo "▸ image already visible to the cluster (shared daemon store)" ;;
  *)        echo "▸ WARNING: unknown cluster type for '$CONTEXT' — assuming a shared store." >&2
            echo "  If pods report ErrImagePull, push $IMAGE to a registry, set image: in" >&2
            echo "  k8s/harness.yaml, and re-run; or set HARNESS_IMAGE_LOADER=kind|minikube|none." >&2 ;;
esac

# Idempotent: the secret is kept out of the manifest on purpose (never commit
# it), so create it here only if it is missing — re-runs leave it untouched.
if kubectl get secret harness-anthropic >/dev/null 2>&1; then
  echo "▸ secret harness-anthropic already exists — leaving it as is"
else
  echo "▸ creating secret harness-anthropic"
  kubectl create secret generic harness-anthropic \
    --from-literal=api-key="$ANTHROPIC_API_KEY" >/dev/null
fi

echo "▸ applying manifest"
kubectl apply -f k8s/harness.yaml

# Each node blocks until it has discovered every peer before going Ready, so
# this waits for the whole cluster to form, not just for containers to start.
echo "▸ waiting for the rollout (all three pods Ready)…"
kubectl rollout status statefulset/harness --timeout=180s

cat <<EOF

  cluster up — three pods: harness-0 (node 1), harness-1 (node 2), harness-2 (node 3)

  Attach a REPL (any pod works — placement routes to the session's owner):
    kubectl exec -it harness-0 -- harness-standalone repl 127.0.0.1:7501

  Watch it form / find a session's owner:
    kubectl logs harness-0 | tail            # "cluster ready (leader elected)"

  Failure drill (a session outlives its pod):
    kubectl delete pod harness-1 --grace-period=5   # then :retry on a survivor

  Tear down:
    kubectl delete -f k8s/harness.yaml
    kubectl delete pvc -l app=harness        # PVCs outlive the StatefulSet
    kubectl delete secret harness-anthropic

EOF
