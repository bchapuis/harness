#!/bin/bash
# Your agentic distributed harness in two minutes: build, boot a three-node
# cluster (three OS processes, one shared journal), attach a REPL.
set -euo pipefail
cd "$(dirname "$0")"

if [ -z "${ANTHROPIC_API_KEY:-}" ]; then
  echo "Set ANTHROPIC_API_KEY first:  export ANTHROPIC_API_KEY=sk-ant-…" >&2
  exit 1
fi

echo "▸ building"
cargo build -q -p harness-standalone
BIN=target/debug/harness-standalone
DATA=${HARNESS_DATA:-./harness-data}
API_URL=${HARNESS_API_URL:-https://api.anthropic.com}
mkdir -p "$DATA"

# A node from an earlier run would silently join this cluster (same ports,
# same secret) and confuse the demo — refuse to start over one.
for p in 7401 7402 7403 7501 7502 7503; do
  if (echo > "/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
    echo "Port $p is busy — an old demo still running?  pkill -f harness-standalone" >&2
    exit 1
  fi
done

NODE_PIDS=()
cleanup() { kill "${NODE_PIDS[@]}" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

echo "▸ booting three nodes (logs in $DATA/node*.log)"
for i in 1 2 3; do
  "$BIN" node --id "$i" --data "$DATA" --api-url "$API_URL" \
    > "$DATA/node$i.log" 2>&1 &
  NODE_PIDS+=($!)
done

# A node opens its control port only once it has discovered every peer.
for i in 0 1 2; do
  port=$((7501 + i))
  until (echo > "/dev/tcp/127.0.0.1/$port") 2>/dev/null; do sleep 0.2; done
done

cat <<EOF

  cluster up — node 1: pid ${NODE_PIDS[0]}, node 2: pid ${NODE_PIDS[1]}, node 3: pid ${NODE_PIDS[2]}

  Try:
    Create numbers.txt holding 1..10, then tell me their sum.
    :tail                              the journal IS the session
    kill ${NODE_PIDS[0]}                         (another terminal) then :retry here
    :quit                              tears the demo cluster down

EOF

"$BIN" repl 127.0.0.1:7501
