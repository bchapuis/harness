# harness-standalone on Kubernetes

A three-node `harness-standalone` cluster (silos) as a **StatefulSet**, fronted
by a **`harness-gateway` StatefulSet** that joins the same cluster as a
non-voting, non-hosting **client** (the Orleans cluster-client pattern). Each pod
has its own data volume and stable DNS name. Unlike the single-host `demo.sh`,
the nodes run on separate pods, and so potentially separate machines, and share
nothing on disk. The journal replicates over the transport, a quorum append per
grain (spec §7.2).

```
   HTTP  ─►  Service: harness-gateway  ─►  StatefulSet harness-gw (N clients)
                                              │  actor transport (cluster secret)
        ┌── Service: harness (headless) ──────┴──┐   DNS: harness-N.harness
        │                                        │
   ┌────┴─────┐      ┌──────────┐      ┌─────────┴┐
   │ harness-0│◄────►│ harness-1│◄────►│ harness-2│   transport 7401/02/03
   │  node 1  │      │  node 2  │      │  node 3  │   no client-facing listener
   │ PVC data │      │ PVC data │      │ PVC data │   own volume per pod
   └──────────┘      └──────────┘      └──────────┘
```

The gateway verifies a caller's bearer token to a tenant (it terminates auth),
then addresses the session's grain **directly** over the actor transport — no
control protocol, no forwarding hop. Because it holds `GrainRef`s and rides the
receptionist gossip, the gateway sits **inside** the cluster's trust boundary
(it presents the cluster secret); untrusted callers reach it only over HTTP with
a bearer token. The session-key scope is what keeps tenants isolated.

The gateway is a StatefulSet, not a Deployment, because a cluster client needs a
stable transport identity: gateway pod *N* takes the non-voting node id `100+N`
at the stable DNS name `harness-gw-N.harness-gw`, which the nodes admit with
`--client <id>=<host>`.

## Prerequisites

- A cluster: `kind`, `minikube`, Docker Desktop, OrbStack, or a real one.
- `kubectl` pointed at it.
- An Anthropic API key in `ANTHROPIC_API_KEY`.

## Quick start

`start.sh` does steps 1–3 in one go: it builds the image, makes it reachable
(detecting `kind`/`minikube` versus a shared daemon store from the current
context), creates the three Secrets if they are missing (the API key, the
cluster secret, and a tenant token), applies the manifest, and waits for the
nodes and the gateways.

```sh
export ANTHROPIC_API_KEY=sk-ant-…
k8s/start.sh
```

It is idempotent: re-run it after editing the manifest, and it leaves existing
Secrets untouched. Override the image name with `HARNESS_IMAGE`, or force the
load step with `HARNESS_IMAGE_LOADER=kind|minikube|none`.

The rest of this page is the manual path the script automates, plus the parts it
leaves to you: talking to the cluster, the failure drill, the sandbox choice,
and tear-down.

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

## 2. Create the Secrets

All three stay out of the manifest on purpose; never commit them.

The API key:

```sh
kubectl create secret generic harness-anthropic \
  --from-literal=api-key="$ANTHROPIC_API_KEY"
```

The cluster secret guards the transport handshake; the nodes and the gateway
pods all present it, so they admit one another and reject anyone else:

```sh
secret=$(head -c 30 /dev/urandom | base64 | tr -dc 'A-Za-z0-9')
kubectl create secret generic harness-cluster --from-literal=secret="$secret"
```

A tenant token the gateway verifies (one principal, `demo`, with a random opaque
token):

```sh
token=$(head -c 30 /dev/urandom | base64 | tr -dc 'A-Za-z0-9')
printf 'demo %s\n' "$token" > tenants
kubectl create secret generic harness-auth --from-file=tenants
rm tenants            # the Secret is the source of truth now
```

## 3. Deploy

```sh
kubectl apply -f k8s/harness.yaml
kubectl rollout status statefulset/harness      # waits for all three nodes
kubectl rollout status statefulset/harness-gw   # and both gateways
kubectl logs harness-0 | tail                   # "cluster ready (leader elected)"
kubectl logs harness-gw-0 | tail                # "joined the cluster (client of nodes 1..=3)"
```

All node and gateway pods start at once (`podManagementPolicy: Parallel`): each
waits to discover its peers before reporting ready, so a one-at-a-time rollout
would deadlock.

## 4. Talk to it

The gateway's HTTP API is the only tenant-facing path — the nodes have no
client-facing listener. Forward the gateway Service, pull a tenant token from
the Secret, and stream a turn as Server-Sent Events:

```sh
token=$(kubectl get secret harness-auth -o jsonpath='{.data.tenants}' | base64 -d | awk 'NR==1{print $2}')
kubectl port-forward service/harness-gateway 8080:8080            # another terminal
curl -N -X POST http://127.0.0.1:8080/v1/assistant/demo/prompt \
  -H "Authorization: Bearer $token" -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -d '{"turn":"t-1","content":"Create a numbers.txt holding 1..10, then sum them."}'
```

Drop the `Accept` header to block and get the final outcome as JSON. Other
endpoints: `GET /v1/{kind}/{session}/records`, `GET …/stream`, `POST …/cancel`,
`GET /v1/sessions?kind=assistant`. Each carries `Authorization: Bearer <token>`;
sessions are scoped to the tenant, so one tenant never sees another's.

## 5. The failure drill

The reason the deployment exists: a session survives the pod that ran it.

```sh
# Submit a longish turn, find its owner in the logs, then delete that pod:
kubectl delete pod harness-1 --grace-period=5

# Survivors notice within a few seconds (SWIM: 1s probes, 3s suspicion):
kubectl logs harness-0 | grep -E 'Suspected|Unreachable'

# Re-issue the SAME prompt (same turn id) through the gateway. The new owner
# recovers the grain's head from a quorum, folds the journal, and resumes from
# the last committed record — the run completes as if nothing happened:
curl -N -X POST http://127.0.0.1:8080/v1/assistant/demo/prompt \
  -H "Authorization: Bearer $token" -H "Content-Type: application/json" \
  -d '{"turn":"t-1","content":"…the same turn…"}'
```

The StatefulSet recreates `harness-1` with the **same** name, DNS, and PVC, so
it rejoins as the same node and new sessions place onto it again.

## Sandbox boundary

The manifest runs `--sandbox local`: the model's `shell` tool runs **inside the
node's own pod container**. The pod is the isolation boundary, good enough for a
demo, since a pod is already a confined, network-policy-governed unit. But it is
one boundary per node, not one per session, and it offers `shell` only (no
`run_js`, the hermetic QuickJS Compute tier the confined modes provide).

For a per-session boundary (and `run_js`), switch to `--sandbox docker` or
`--sandbox firecracker`. Both need extra in-cluster plumbing this starter
manifest leaves out:

- **docker**: the node shells out to a Docker daemon. In Kubernetes that means a
  Docker-in-Docker sidecar (a `docker:dind` container, `privileged: true`,
  sharing a volume), with `--container-cli docker` pointed at it. Add
  `--sandbox-image python:3.12-slim` and pre-pull it.
- **firecracker**: one microVM per session. Needs `/dev/kvm` on the node and a
  privileged context. Best on bare-metal node pools.

Keep `local` unless you are feeding the model untrusted input; then a
per-session container or microVM is the right boundary.

## Scaling and limits

- **The roster is fixed at three.** `--nodes`, the `--peer` list, and `replicas`
  must agree; growing the cluster means editing all three (dynamic shard
  split/merge is deferred, spec §7.7). `kubectl scale` alone will not form a
  larger cluster.
- **Scale the gateways by id, not just by replicas.** Each gateway pod needs a
  distinct non-voting node id the nodes admit, so adding capacity means raising
  `harness-gw`'s `replicas` **and** extending the nodes' `--client` flags (ids
  `102`, `103`, …) to admit the new pods. The gateway holds no durable state, so
  beyond admission it scales freely. Each line of the tenants file
  (`harness-auth`) is one tenant; rotate tokens by editing the Secret and
  restarting.
- **Public TLS terminates at the edge, not the gateway.** The gateway Service is
  `ClusterIP`; expose it with an Ingress or `LoadBalancer` and terminate TLS
  there.
- **The transport is plaintext, guarded by the cluster secret** (`harness-cluster`).
  The gateway joins it inside the trust boundary, so it must be a trusted
  component — which it is, being where tenant auth terminates. Fine within a
  trusted cluster network; provision transport TLS (`TlsConfig`, on both the
  nodes and the gateways) before crossing untrusted links.
- **`storage: 1Gi` per pod** is arbitrary; size it to your sessions.

## Tear down

```sh
kubectl delete -f k8s/harness.yaml
kubectl delete pvc -l app=harness        # PVCs outlive the StatefulSet; delete to reclaim
kubectl delete secret harness-anthropic harness-auth harness-cluster
```
