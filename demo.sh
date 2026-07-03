#!/bin/bash
# Your agentic distributed harness in two minutes: build, boot a three-node
# cluster (three OS processes, each its own journal, replicated over the
# transport), and start the HTTP gateway in front of it.
#
# The gateway is the single public edge: an Orleans-style cluster *client* that
# joins the transport as a non-voting, non-hosting member and addresses session
# grains directly. The nodes have no client-facing listener — they host grains
# and vote in Raft; each admits the gateway with `--client`.
#
# The gateway runs in INSECURE dev mode here (no --auth-tokens): the bearer token
# is taken as the tenant, unverified. That keeps this a one-command demo, and is
# allowed only because the public bind is loopback. For the authenticated,
# network-facing edge — opaque tokens — see k8s/. Drive sessions with `curl`
# (below) or any HTTP client.
set -euo pipefail
cd "$(dirname "$0")"

if [ -z "${ANTHROPIC_API_KEY:-}" ]; then
  echo "Set ANTHROPIC_API_KEY first:  export ANTHROPIC_API_KEY=sk-ant-…" >&2
  exit 1
fi

echo "▸ building"
cargo build -q -p harness-standalone -p harness-gateway
BIN=target/debug/harness-standalone
GATEWAY_BIN=target/debug/harness-gateway
# The cluster secret guards the transport handshake; the nodes and the gateway
# must agree on it. Both default to "harness-standalone"; override here to share.
SECRET=${HARNESS_SECRET:-harness-standalone}
# The gateway joins as a non-voting cluster client with this id, OUTSIDE the
# nodes' 1..=3 voter roster; each node admits it with --client.
GATEWAY_ID=${HARNESS_GATEWAY_ID:-100}
DATA=${HARNESS_DATA:-./harness-data}
API_URL=${HARNESS_API_URL:-https://api.anthropic.com}
# python:3.12-slim rather than bare alpine: it carries python3, bash, and
# the GNU coreutils, so the model can actually run what a newcomer asks for.
# Override with HARNESS_SANDBOX_IMAGE for a leaner (alpine:3.20) or richer
# (a polyglot image) container.
IMAGE=${HARNESS_SANDBOX_IMAGE:-python:3.12-slim}
mkdir -p "$DATA"

# A real model composes the shell commands this demo runs, so they execute
# confined in a per-session container — never as your user. The container's
# workspace is backed by a durable filesystem grain, so a session's files
# survive hibernation and migration.
if ! docker version >/dev/null 2>&1; then
  echo "The demo runs \`shell\` inside docker containers; start Docker (or colima) first." >&2
  exit 1
fi
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "▸ pulling $IMAGE (one-time; the first shell call would otherwise eat its timeout)"
  docker pull -q "$IMAGE"
fi

# A node from an earlier run would silently join this cluster (same ports,
# same secret) and confuse the demo — refuse to start over one. 7401-7403 are
# the node transports, 7500 the gateway's (port_base + GATEWAY_ID - 1), 8080
# the gateway's public HTTP.
for p in 7401 7402 7403 7500 8080; do
  if (echo > "/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
    echo "Port $p is busy — an old demo still running?  pkill -f 'harness-'" >&2
    exit 1
  fi
done

PIDS=()
cleanup() { kill "${PIDS[@]}" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

echo "▸ booting three nodes (logs in $DATA/node*.log)"
for i in 1 2 3; do
  # Each node keeps its own data dir: the journal replicates over the
  # transport (a quorum append per grain), so nodes share nothing on disk.
  # They find each other on loopback here; --bind-host and --peer span hosts.
  # --client admits the gateway (id $GATEWAY_ID) as a non-voting member.
  "$BIN" node --id "$i" --data "$DATA/node$i" --api-url "$API_URL" \
    --sandbox docker --sandbox-image "$IMAGE" --secret "$SECRET" \
    --client "$GATEWAY_ID=127.0.0.1" \
    > "$DATA/node$i.log" 2>&1 &
  PIDS+=($!)
done

# Each node's transport comes up immediately; the gateway discovers the hosts
# over the receptionist gossip once it joins.
for i in 0 1 2; do
  port=$((7401 + i))
  until (echo > "/dev/tcp/127.0.0.1/$port") 2>/dev/null; do sleep 0.2; done
done

# The gateway joins the cluster as client id $GATEWAY_ID and serves HTTP/SSE on
# 8080. No --auth-tokens, so it runs INSECURE (loopback only) — the bearer token
# is the tenant.
echo "▸ starting the HTTP gateway on 127.0.0.1:8080 (logs in $DATA/gateway.log)"
"$GATEWAY_BIN" --bind 127.0.0.1:8080 --secret "$SECRET" \
  --node-id "$GATEWAY_ID" --nodes 3 \
  > "$DATA/gateway.log" 2>&1 &
PIDS+=($!)
until (echo > "/dev/tcp/127.0.0.1/8080") 2>/dev/null; do sleep 0.2; done

cat <<EOF

  cluster up — node 1: pid ${PIDS[0]}, node 2: pid ${PIDS[1]}, node 3: pid ${PIDS[2]}, gateway: pid ${PIDS[3]}

  Drive it over HTTP through the gateway. The bearer token is the tenant; stream
  the run as Server-Sent Events:

    curl -N -X POST http://127.0.0.1:8080/v1/assistant/http-demo/prompt \\
      -H 'Authorization: Bearer alice' -H 'Content-Type: application/json' \\
      -H 'Accept: text/event-stream' \\
      -d '{"turn":"t-1","content":"Create numbers.txt holding 1..10, then tell me their sum."}'

  Other endpoints (same Bearer header):
    GET  /v1/sessions?kind=assistant                 this tenant's sessions
    GET  /v1/assistant/http-demo/records?from=0      the journal IS the session
    GET  /v1/assistant/http-demo/stream?turn=t-1     observe a run live (SSE)
    POST /v1/assistant/http-demo/cancel?turn=t-1     cancel a run

  A different bearer token is a wholly separate tenant — its sessions never mix:
    curl ... -H 'Authorization: Bearer bob' ...

  Failure drill (a session outlives its node):
    kill ${PIDS[0]}                  then re-issue the same prompt (same turn id);
                                  placement re-runs it on a survivor.

  Logs:    tail -f $DATA/gateway.log $DATA/node1.log
  Quit:    Ctrl-C  (tears the demo cluster down)

EOF

# No REPL to attach to: the gateway is the edge. Stay up until interrupted so the
# trap tears the cluster down on Ctrl-C.
wait
