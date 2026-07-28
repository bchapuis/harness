#!/bin/bash
# Persistent machines in two minutes: lightweight VMs you address by name, reach
# over SSH, and cannot lose.
#
# This boots a three-node cluster (three OS processes on your machine, each with
# its own journal, replicated over the transport), provisions a machine, and
# opens an SSH front door on each node. The machine is a *grain*: its whole
# rootfs is durable state, so it survives disconnection, idle hibernation, and
# the death of the node it was running on. See docs/machine-spec.md.
#
# Two modes, chosen for you:
#
#   --vm firecracker  Linux with /dev/kvm. A real microVM per machine; you get a
#                     shell inside it over SSH. The full story.
#   --vm fake         everywhere else (macOS included). No guest, so no shell —
#                     but every durable property is real: provisioning, the
#                     journal, captures, hibernation, failover. What a host
#                     without KVM can honestly show.
set -euo pipefail
cd "$(dirname "$0")"

DATA=${MACHINE_DATA:-./machine-data}
SECRET=${MACHINE_SECRET:-machine-standalone}
MACHINE=${MACHINE_NAME:-dev-box}
PORT_BASE=${MACHINE_PORT_BASE:-7601}
DOOR_BASE=${MACHINE_DOOR_BASE:-2222}
ADMIN_BASE=${MACHINE_ADMIN_BASE:-7701}
ASSETS=${HARNESS_MACHINE_ASSETS:-guest/machine-rootfs/out}
# Every node offers an admin socket, and the CLI is given all three: a command
# keeps working after the node it usually talks to is killed.
ADMIN=(--admin "127.0.0.1:$ADMIN_BASE" --admin "127.0.0.1:$((ADMIN_BASE + 1))" \
       --admin "127.0.0.1:$((ADMIN_BASE + 2))")

echo "▸ building"
cargo build -q -p machine-standalone
BIN=target/debug/machine-standalone

# --- Which binding can this host actually run? -----------------------------------
# Firecracker needs Linux, /dev/kvm, and the assets guest/machine-rootfs/build.sh
# produces. Anything missing drops us to the fake binding rather than failing:
# the durability half of the demo runs anywhere.
VM=fake
FC_ARGS=()
if [ "$(uname -s)" != "Linux" ]; then
  echo "  Not Linux, so there is no KVM and no microVM to boot: using --vm fake."
  echo "  Everything durable still runs; the SSH shell does not (see the end of this script)."
elif [ ! -e /dev/kvm ]; then
  echo "  Linux, but no /dev/kvm (a VM without nested virtualization, perhaps): using --vm fake."
elif [ ! -r /dev/kvm ] || [ ! -w /dev/kvm ]; then
  # Worth its own branch: this is the one failure a Linux user hits that is
  # entirely fixable, and silently degrading would hide the fix from them.
  echo "  /dev/kvm exists but this user cannot open it. Firecracker needs read+write:" >&2
  echo "      sudo usermod -aG kvm \$USER    # then log out and back in" >&2
  echo "  Falling back to --vm fake for now." >&2
else
  if [ ! -f "$ASSETS/firecracker" ] || [ ! -f "$ASSETS/vmlinux" ] || [ ! -f "$ASSETS/machine.ext4" ]; then
    echo "  /dev/kvm is usable but the guest assets are missing ($ASSETS)."
    if [ "${MACHINE_BUILD_ASSETS:-1}" = "1" ] && docker version >/dev/null 2>&1; then
      # Built at demo size, not the script's 1 GiB default: provisioning
      # imports the whole image into the disk facet as 1 MiB blocks and
      # replicates them, so image size is the dominant cost of `create`.
      echo "▸ building guest assets (docker; a few minutes the first time)"
      MACHINE_MB=${MACHINE_MB:-512} guest/machine-rootfs/build.sh
    else
      echo "  Run guest/machine-rootfs/build.sh to get a real microVM (needs docker)."
      echo "  Falling back to --vm fake."
    fi
  fi
  if [ -f "$ASSETS/firecracker" ] && [ -f "$ASSETS/vmlinux" ] && [ -f "$ASSETS/machine.ext4" ]; then
    VM=firecracker
    FC_ARGS=(--fc-binary "$ASSETS/firecracker" --fc-kernel "$ASSETS/vmlinux")
    BASE_IMAGE="$ASSETS/machine.ext4"
  fi
fi

mkdir -p "$DATA"
if [ "$VM" = "fake" ]; then
  # The fake binding has no guest, so the base image is only ever imported and
  # dirtied — any file of the right shape does. 64 MiB of zeros, made once.
  # `bs=1048576` rather than `bs=1m`/`bs=1M`: BSD and GNU dd disagree on the
  # suffix, and this script has to run on both.
  BASE_IMAGE="$DATA/base.img"
  [ -f "$BASE_IMAGE" ] || dd if=/dev/zero of="$BASE_IMAGE" bs=1048576 count=64 2>/dev/null
fi

# --- A key for the demo ----------------------------------------------------------
# The machine authorizes a key, not a password: the front door verifies
# possession itself and no key material ever enters the guest (machine §5.1).
# A demo-only key keeps your own out of it.
KEY="$DATA/id_ed25519"
if [ ! -f "$KEY" ]; then
  echo "▸ generating a demo ssh key ($KEY)"
  ssh-keygen -q -t ed25519 -N '' -C 'machine-demo' -f "$KEY"
fi

# A node from an earlier run would silently join this cluster (same ports, same
# secret) and confuse the demo — refuse to start over one.
for p in $((PORT_BASE)) $((PORT_BASE + 1)) $((PORT_BASE + 2)) \
         $((ADMIN_BASE)) $((ADMIN_BASE + 1)) $((ADMIN_BASE + 2)) \
         $((DOOR_BASE)) $((DOOR_BASE + 1)) $((DOOR_BASE + 2)); do
  if (echo > "/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
    echo "Port $p is busy — an old demo still running?  pkill -f machine-standalone" >&2
    exit 1
  fi
done

PIDS=()
# `${a[@]+…}` guards the expansion: bash 3.2 (what macOS ships) treats an empty
# array as unset, and `set -u` would abort before the first node is spawned.
cleanup() { kill ${PIDS[@]+"${PIDS[@]}"} 2>/dev/null || true; }
trap cleanup EXIT INT TERM

# --- The cluster -----------------------------------------------------------------
# Every node is identical: it hosts machine grains, votes in Raft, and opens an
# SSH front door for this machine on its own port. Three doors, not one, because
# a door dies with its node — and reconnecting through a survivor is the whole
# point of the failure drill below.
echo "▸ booting three nodes (logs in $DATA/node*.log)"
for i in 1 2 3; do
  "$BIN" node --id "$i" --nodes 3 --data "$DATA/node$i" --secret "$SECRET" \
    --port-base "$PORT_BASE" --vm "$VM" ${FC_ARGS[@]+"${FC_ARGS[@]}"} \
    --door "$((DOOR_BASE + i - 1))=$MACHINE" \
    --admin "127.0.0.1:$((ADMIN_BASE + i - 1))" \
    > "$DATA/node$i.log" 2>&1 &
  PIDS+=($!)
done

# Wait for the doors, not the transport: a node prints its front-door line only
# once the cluster has elected a leader and it is hosting machine grains.
# Provisioning before that just spends the CLI's retry budget waiting.
for i in 1 2 3; do
  until grep -q 'ssh front door' "$DATA/node$i.log" 2>/dev/null; do sleep 0.3; done
done

# --- The machine -----------------------------------------------------------------
# Provisioning is an ordinary journaled grain command (machine §3), not an admin
# API: the disk starts as the base image's checkpoint manifest and diverges by
# dirty blocks as the guest writes. The machine's SSH host key is born here too,
# so its identity survives every move it will make.
echo "▸ creating machine \`$MACHINE\`"
"$BIN" create "$MACHINE" "${ADMIN[@]}" --base-image "$BASE_IMAGE" --key "$KEY.pub" \
  --vcpus 1 --mem-mib 512 --checkpoint-secs 30 --lease-secs 10

STATUS_CMD="$BIN status $MACHINE ${ADMIN[*]}"
SSH_OPTS="-i $KEY -o UserKnownHostsFile=$DATA/known_hosts -o StrictHostKeyChecking=accept-new"

cat <<EOF

  cluster up — node 1: pid ${PIDS[0]}, node 2: pid ${PIDS[1]}, node 3: pid ${PIDS[2]}
  machine \`$MACHINE\` provisioned, vm binding: $VM

  Inspect it (the journal is the machine):

    $STATUS_CMD

EOF

if [ "$VM" = "firecracker" ]; then
cat <<EOF
  SSH in. The first connection activates the grain and boots the microVM against
  the rehydrated disk, so expect a few seconds; the username is recorded as the
  principal (machine §5.1), and the host key you pin is the machine's own
  journaled identity:

    ssh $SSH_OPTS -p $DOOR_BASE alice@127.0.0.1

  Write something into the rootfs — a file in /root, a user, a config. Then
  disconnect and reconnect: it is still there. That is the disk facet, not a
  container layer. Note the guest has NO network in this demo: egress needs the
  \`net\` feature and CAP_NET_ADMIN (machine §5.2), which this script does not
  wire, so \`apk add\` will not work. Everything local to the rootfs does.

  Failure drill (a machine outlives its node):

    kill ${PIDS[0]}        the node holding your session dies; the connection drops
    ssh $SSH_OPTS -p $((DOOR_BASE + 1)) alice@127.0.0.1

  The survivors detect the death, placement moves the machine, and the next
  attach boots a fresh microVM against the last *captured* disk. Your files are
  there, rewound at most to the last capture — 30s here (--checkpoint-secs), and
  the same host key, so ssh does not warn. Nothing forks and nothing corrupts.

  If a boot fails, the VMM's own console is the place to look — the guest's
  kernel and init write there, and nothing else in this stack sees them:

    cat \${TMPDIR:-/tmp}/harness-machine-*/console.log
EOF
else
cat <<EOF
  What you can drive here (--vm fake — no guest, so no shell):

  Hold a session open. SSH authenticates against the machine's journaled key,
  pins its host key, and journals the attachment with your username as the
  principal (M4) — all of that is real. \`-N\` opens no channel, so nothing has
  to bridge to a guest that is not there:

    ssh $SSH_OPTS -N -p $DOOR_BASE alice@127.0.0.1 &

  Then watch it, in another shell. \`attachments\` shows who is on, and
  \`captures\` climbs once per checkpoint interval while attached — each one a
  durable point a crash would rewind to (M3):

    $STATUS_CMD

  Failure drill (a machine outlives its node):

    kill ${PIDS[0]}                       then run status again

  The survivors detect the death, placement moves the machine to another node,
  and its disk comes back from the last committed capture — same image digest,
  no fork. That is the property SSH would be riding on.

  For the SSH half you need Linux with /dev/kvm:
    guest/machine-rootfs/build.sh     builds the guest rootfs, kernel, and VMM
    ./machine-demo.sh                 then picks --vm firecracker by itself
EOF
fi

cat <<EOF

  Logs:    tail -f $DATA/node1.log
  Quit:    Ctrl-C  (tears the demo cluster down; the journals under $DATA remain,
                    so re-running this script finds \`$MACHINE\` where you left it)

EOF

# Stay up until interrupted so the trap tears the cluster down on Ctrl-C.
wait
