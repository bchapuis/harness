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

# The cluster secret guards the transport handshake; the nodes and the gateway
# pods all read it from this Secret, so they admit one another and reject anyone
# else. Random and opaque; provisioned once, idempotently.
if kubectl get secret harness-cluster >/dev/null 2>&1; then
  echo "▸ secret harness-cluster already exists — leaving it as is"
else
  echo "▸ creating secret harness-cluster (the transport cluster secret)"
  secret=$(head -c 30 /dev/urandom | base64 | tr -dc 'A-Za-z0-9')
  kubectl create secret generic harness-cluster \
    --from-literal=secret="$secret" >/dev/null
fi

if kubectl get secret harness-auth >/dev/null 2>&1; then
  echo "▸ secret harness-auth already exists — leaving it as is"
else
  echo "▸ creating secret harness-auth (one tenant: demo, with a random token)"
  # An opaque high-entropy token the gateway verifies. Pull it back from the
  # mounted Secret when you curl. Add more `<principal> <token>` lines for more
  # tenants.
  token=$(head -c 30 /dev/urandom | base64 | tr -dc 'A-Za-z0-9')
  tenants=$(mktemp)
  printf 'demo %s\n' "$token" > "$tenants"
  kubectl create secret generic harness-auth --from-file=tenants="$tenants" >/dev/null
  rm -f "$tenants"
fi

echo "▸ applying manifest"
kubectl apply -f k8s/harness.yaml

# Each node blocks until it has discovered every peer before going Ready, so
# this waits for the whole cluster to form, not just for containers to start.
echo "▸ waiting for the rollout (all three nodes + both gateways Ready)…"
kubectl rollout status statefulset/harness --timeout=180s
kubectl rollout status statefulset/harness-gw --timeout=120s

cat <<'EOF'

  cluster up — three node pods (harness-0/1/2) and two gateway pods (harness-gw-0/1)

  Drive sessions over HTTP through the gateway (the only tenant-facing path). The
  bearer token is verified against the tenants Secret; pull the demo token:
    token=$(kubectl get secret harness-auth -o jsonpath='{.data.tenants}' | base64 -d | awk 'NR==1{print $2}')
    kubectl port-forward service/harness-gateway 8080:8080            # another terminal
    curl -N -X POST http://127.0.0.1:8080/v1/assistant/demo/prompt \
      -H "Authorization: Bearer $token" -H "Content-Type: application/json" \
      -H "Accept: text/event-stream" \
      -d '{"turn":"t-1","content":"Create numbers.txt holding 1..10 and sum them."}'

  Watch it form / find a session's owner:
    kubectl logs harness-0 | tail            # "cluster ready (leader elected)"
    kubectl logs harness-gw-0 | tail         # "joined the cluster (client of nodes 1..=3)"

  Failure drill (a session outlives its pod):
    kubectl delete pod harness-1 --grace-period=5   # re-issue the same prompt;
                                                    # placement re-runs it on a survivor

  Tear down:
    kubectl delete -f k8s/harness.yaml
    kubectl delete pvc -l app=harness        # PVCs outlive the StatefulSet
    kubectl delete secret harness-anthropic harness-auth harness-cluster

EOF
