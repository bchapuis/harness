# harness-standalone on Kubernetes

A three-node `harness-standalone` cluster as a **StatefulSet**: three pods,
each with its own data volume and stable DNS name. Unlike the single-host
`demo.sh`, the nodes run on different pods (and so, potentially, different
machines) and share nothing on disk — the journal replicates over the
transport, a quorum append per grain (spec §7.2).

```
        ┌── Service: harness (headless) ──┐   DNS: harness-N.harness
        │                                 │
   ┌────┴─────┐      ┌──────────┐      ┌──┴───────┐
   │ harness-0│◄────►│ harness-1│◄────►│ harness-2│   transport 7401/02/03
   │  node 1  │      │  node 2  │      │  node 3  │   control   7501/02/03
   │ PVC data │      │ PVC data │      │ PVC data │   own volume per pod
   └──────────┘      └──────────┘      └──────────┘
```

## Prerequisites

- A cluster: `kind`, `minikube`, Docker Desktop, or a real one.
- `kubectl` pointed at it.
- An Anthropic API key in `ANTHROPIC_API_KEY`.

## 1. Build the image

From the repository root (the build context is the whole workspace):

```sh
docker build -t harness-standalone:latest .
```

Make the image reachable by the cluster:

```sh
# kind:
kind load docker-image harness-standalone:latest
# minikube:
minikube image load harness-standalone:latest
# remote cluster: push to a registry and set `image:` in harness.yaml
docker tag harness-standalone:latest <registry>/harness-standalone:latest
docker push <registry>/harness-standalone:latest
```

## 2. Create the API-key secret

Kept out of the manifest on purpose — never commit it:

```sh
kubectl create secret generic harness-anthropic \
  --from-literal=api-key="$ANTHROPIC_API_KEY"
```

## 3. Deploy

```sh
kubectl apply -f k8s/harness.yaml
kubectl rollout status statefulset/harness        # waits for all three pods
kubectl logs harness-0 | tail                      # "all 3 hosts discovered" / "cluster ready"
```

All three pods start at once (`podManagementPolicy: Parallel`) because each
node blocks until it has discovered every peer before opening its control
port — a one-at-a-time rollout would deadlock.

## 4. Talk to it

Attach a REPL to any pod (placement routes the request to the session's owner,
so the entry point does not matter):

```sh
kubectl exec -it harness-0 -- harness-standalone repl 127.0.0.1:7501
```

Each pod's control port is `7500 + node_id`, i.e. `7501 + ordinal`: `harness-0`
→ `7501`, `harness-1` → `7502`, `harness-2` → `7503`.

```
assistant/demo> Create a file named numbers.txt that holds 1..10, then tell me their sum.
assistant/demo> :tail
assistant/demo> :quit
```

Prefer a local REPL binary? Forward the control port instead:

```sh
kubectl port-forward harness-0 7501:7501
harness-standalone repl 127.0.0.1:7501          # in another terminal
```

## 5. The failure drill

The reason the deployment exists: a session survives the pod that ran it.

```sh
# Submit a longish turn, find its owner in the logs, then delete that pod:
kubectl delete pod harness-1 --grace-period=5

# Survivors notice within a few seconds (SWIM: 1s probes, 3s suspicion):
kubectl logs harness-0 | grep -E 'Suspected|Unreachable'

# Re-attach to a survivor and :retry — the new owner recovers the grain's head
# from a quorum, folds the journal, and resumes from the last committed record:
kubectl exec -it harness-0 -- harness-standalone repl 127.0.0.1:7501
```

The StatefulSet recreates `harness-1` with the **same** name, DNS, and PVC, so
it rejoins as the same node and new sessions place onto it again.

## Sandbox: `local` here, container/microVM for a per-session boundary

The manifest runs `--sandbox local`: the model's `shell` tool runs **inside
the node's own pod container**. The pod is the isolation boundary — good
enough for a demo, since a pod is already a confined, network-policy-governed
unit — but it is one boundary per node, not one per session, and it offers
`shell` only (no `run_js`; that is the hermetic QuickJS Compute tier, which
the confined modes provide).

For a per-session boundary (and `run_js`), switch to `--sandbox docker` or
`--sandbox firecracker`. Both need extra in-cluster plumbing that is
deliberately left out of this starter manifest:

- **docker** — the node shells out to a Docker daemon. In Kubernetes that
  means a Docker-in-Docker sidecar (a `docker:dind` container, `privileged:
  true`, sharing a volume), with `--container-cli docker` pointed at it. Also
  add `--sandbox-image python:3.12-slim` and pre-pull it.
- **firecracker** — one microVM per session; needs `/dev/kvm` on the node and
  a privileged context. Best on bare-metal node pools.

Keep `local` unless you are feeding the model untrusted input; then a
per-session container or microVM is the right boundary.

## Scaling and limits

- **The roster is fixed at three.** `--nodes`, the `--peer` list, and
  `replicas` must agree; growing the cluster means editing all three (dynamic
  shard split/merge is deferred, spec §7.7). `kubectl scale` alone will not
  form a larger cluster.
- **The transport is plaintext, guarded by `--secret`** (default
  `harness-standalone`; set a real one via a flag/secret for a shared
  cluster). Fine within a trusted cluster network; provision TLS before
  crossing untrusted links.
- **`storage: 1Gi` per pod** is arbitrary — size it to your sessions.

## Tear down

```sh
kubectl delete -f k8s/harness.yaml
kubectl delete pvc -l app=harness        # PVCs outlive the StatefulSet — delete to reclaim
kubectl delete secret harness-anthropic
```
