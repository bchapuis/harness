#!/bin/bash
# What a machine `create` costs, decomposed — the harness behind TODO.md's numbers.
#
#   ./machine-cost.sh [image-mib ...]
#
# Boots a cluster exactly as machine-demo.sh does (same ports, same secret, the fake
# machine binding), then times **two** creates against it and reports each. Two,
# because the first and the second answer different questions and conflating them is
# how this measurement went wrong the first time:
#
#   - the **first** create pays everything a cold cluster owes — control-plane
#     warm-up, the shard's first leader, the CLI's own connect and retry — and that
#     cost is the same whether the image is one block or a hundred;
#   - the **second**, against the same live cluster, is the path itself.
#
# Divide the first by the block count and you get a number that looks like a
# catastrophic per-block cost and is actually a fixed cost in disguise. The whole
# reason this script exists is that "four minutes to create a 512 MB machine" was
# read that way for a long time.
#
# Sweep the sizes to get a slope. Two sizes an octave apart, differenced, cancel the
# intercept exactly; a single size cannot separate the two no matter how carefully it
# is measured.
#
# Knobs worth turning, all environment variables:
#
#   NODES=1|2|3   replicas. The interesting comparison in the tree: 1 node has no
#                 quorum round at all, so `NODES=1` against `NODES=3` prices the
#                 fan-out and nothing else.
#   BIN=path      which binary. Default is release — a debug build costs ~2.4x and
#                 is not what a deployment runs.
#   FILL=zero     an all-zero image (every block after the first is a dedup hit)
#                 instead of the default random one (every block a cold put).
set -euo pipefail
cd "$(dirname "$0")"

NODES=${NODES:-3}
BIN=${BIN:-target/release/machine-standalone}
FILL=${FILL:-random}
SIZES=("$@")
[ ${#SIZES[@]} -gt 0 ] || SIZES=(16 64 128)

[ -x "$BIN" ] || { echo "no binary at $BIN — cargo build --release -p machine-standalone" >&2; exit 1; }

SECRET=machine-standalone
PORT_BASE=${MACHINE_PORT_BASE:-7601}
DOOR_BASE=${MACHINE_DOOR_BASE:-2222}
ADMIN_BASE=${MACHINE_ADMIN_BASE:-7701}

WORK=$(mktemp -d "${TMPDIR:-/tmp}/machine-cost.XXXXXX")
trap 'rm -rf "$WORK"' EXIT

echo "▸ $NODES node(s), $FILL image, $(basename "$BIN") build"

for mib in "${SIZES[@]}"; do
  IMAGE="$WORK/img-$mib"
  if [ "$FILL" = zero ]; then
    dd if=/dev/zero of="$IMAGE" bs=1048576 count="$mib" 2>/dev/null
  else
    dd if=/dev/urandom of="$IMAGE" bs=1048576 count="$mib" 2>/dev/null
  fi

  DATA="$WORK/run-$mib"
  mkdir -p "$DATA"
  KEY="$DATA/id_ed25519"
  ssh-keygen -q -t ed25519 -N '' -C 'machine-cost' -f "$KEY"

  ADMIN=()
  for i in $(seq 1 "$NODES"); do
    ADMIN+=(--admin "127.0.0.1:$((ADMIN_BASE + i - 1))")
  done

  PIDS=()
  for i in $(seq 1 "$NODES"); do
    "$BIN" node --id "$i" --nodes "$NODES" --data "$DATA/node$i" --secret "$SECRET" \
      --port-base "$PORT_BASE" --machine fake \
      --door "$((DOOR_BASE + i - 1))=cost-box" \
      --admin "127.0.0.1:$((ADMIN_BASE + i - 1))" \
      > "$DATA/node$i.log" 2>&1 &
    PIDS+=($!)
  done

  # The front-door line, not the transport: a node prints it only once a leader is
  # elected and it is hosting machine grains. Creating before that spends the CLI's
  # retry budget rather than measuring anything.
  for i in $(seq 1 "$NODES"); do
    waited=0
    until grep -q 'ssh front door' "$DATA/node$i.log" 2>/dev/null; do
      sleep 0.3
      waited=$((waited + 1))
      [ $waited -le 300 ] || { echo "node $i never opened its door:" >&2; tail -5 "$DATA/node$i.log" >&2; exit 1; }
    done
  done

  for n in 1 2; do
    start=$(python3 -c 'import time; print(time.time())')
    "$BIN" create "cost-box-$n" "${ADMIN[@]}" --base-image "$IMAGE" --key "$KEY.pub" \
      --vcpus 1 --mem-mib 512 --checkpoint-secs 30 --lease-secs 10 \
      > "$DATA/create$n.log" 2>&1
    end=$(python3 -c 'import time; print(time.time())')
    eval "t$n=\$(python3 -c \"print($end - $start)\")"
  done

  # `wait` reports each killed node as "Terminated", which is noise here: the kill is
  # this script's own teardown, not a failure worth printing between measurements.
  kill ${PIDS[@]+"${PIDS[@]}"} 2>/dev/null || true
  wait ${PIDS[@]+"${PIDS[@]}"} 2>/dev/null || true

  python3 -c "
mib, first, second = $mib, $t1, $t2
print(f'  {mib:4d} blocks   cold {first:7.2f}s ({first/mib*1000:8.1f} ms/blk)"'   '"warm {second:7.2f}s ({second/mib*1000:7.1f} ms/blk)')
"
done

cat <<'EOF'

  Read the warm column, and read it as a slope between two sizes rather than as a
  per-block figure at one. The cold column is there to be subtracted, not divided.
EOF
