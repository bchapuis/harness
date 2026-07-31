#!/bin/sh
# The container mechanism's init (`machine_host::container`, machine spec §2.1):
# give a container the machine's *own* rootfs, so the guest's writes land in the
# disk facet's image (grain §7.15) exactly as they do under Firecracker.
#
# The container's own image is scaffolding the user never sees. What they get a
# shell in is /machine/disk.img — the disk facet's materialization, bind-mounted
# from the host — loop-mounted at /rootfs and chrooted into. So:
#
# - **The image is still the durable thing.** Every guest write goes through the
#   loop device into that file, which is what a capture scans. `losetup
#   --direct-io` keeps the guest's blocks out of this kernel's page cache, so
#   the `sync` the mechanism runs before pausing (machine §4) leaves nothing
#   between the guest and the file the host reads.
# - **No init runs.** The guest agent is the container's main process, chrooted;
#   there is no kernel to boot and no openrc, so the rootfs's *services* do not
#   start. That is this mechanism's one departure from Firecracker, which boots
#   the user rootfs's own init (machine §5.1). Shells, the whole filesystem, and
#   persistence are unaffected.
# - **The privilege is real.** losetup and mount need `--privileged`, so this
#   mechanism gives shared-kernel isolation, not the microVM boundary: it is
#   what a host without KVM can honestly run for development. Firecracker is
#   the mechanism that isolates.
set -eu

# Where the disk is bound and where the agent's sockets go are the mechanism's
# to decide: `machine_host::container` passes both in, and the defaults below are
# only for running this image by hand. The mechanism dials
# <sock dir>/<port>.sock through `docker exec`, and expects that path to mean the
# same thing inside this container and inside the chroot — which the bind mount
# below arranges.
IMAGE=${MACHINE_DISK:-/machine/disk.img}
SOCK_DIR=${MACHINE_SOCK_DIR:-/run/guest}
# The port is the *consumer's* (machine_proto::AGENT_PORT), not the mechanism's.
AGENT_PORT=${MACHINE_AGENT_PORT:-62}
ROOT=/rootfs
SOCK=$SOCK_DIR/$AGENT_PORT.sock

test -f "$IMAGE" || { echo "machine runner: no disk image at $IMAGE" >&2; exit 1; }

# `--privileged` shares the host's /dev as it stood when the container started,
# so a loop index the host has not materialized yet has no node in here:
# losetup allocates the index through /dev/loop-control and then cannot open it
# ("device node /dev/loopN is lost"). Create the node before using it.
ensure_node() {
  test -b "$1" || mknod -m 0660 "$1" b 7 "${1#/dev/loop}" 2>/dev/null || true
}

# An activation that was killed leaves its loop device attached to this image:
# the container's death frees the mount, not the device. Detach it before
# attaching our own — this mechanism's version of the control-directory sweep a
# microVM launch does per boot.
for stale in $(losetup -j "$IMAGE" -O NAME --noheadings 2>/dev/null); do
  ensure_node "$stale"
  umount "$stale" 2>/dev/null || true
  losetup -d "$stale" 2>/dev/null || true
done

# Attach to a device we name ourselves, rather than letting `losetup -f --show`
# both pick and open one: the node may need creating first. The loop is for the
# race two machines booting at once would otherwise lose. --direct-io first,
# cached only if this kernel's losetup refuses it: with it on, the guest's
# blocks reach the image file as the guest writes them.
DEV=
for _ in 1 2 3 4 5 6 7 8; do
  # `losetup -f` names the next free device and appends " (lost)" when its node
  # is one of the missing ones above, so take the first field and nothing else.
  candidate=$(losetup -f 2>/dev/null | cut -d' ' -f1) || break
  test -n "$candidate" || break
  ensure_node "$candidate"
  if losetup --direct-io=on "$candidate" "$IMAGE" 2>/dev/null ||
    losetup "$candidate" "$IMAGE" 2>/dev/null; then
    DEV=$candidate
    break
  fi
done
test -n "$DEV" || { echo "machine runner: no loop device for $IMAGE" >&2; exit 1; }
mkdir -p "$ROOT"
mount -t ext4 "$DEV" "$ROOT"

test -x "$ROOT/usr/sbin/machine-agent" || {
  echo "machine runner: $IMAGE has no /usr/sbin/machine-agent — the guest agent is \
part of the rootfs (machine §5.1); build it with guest/machine-rootfs/build.sh" >&2
  exit 1
}

# The guest's /dev, built by hand rather than bind-mounted: --privileged gives
# this container the *host's* devices (its disks and loop devices included), and
# the chroot must inherit none of them. A PTY channel needs ptmx and a devpts
# instance; everything else here is what a shell expects to find.
mount -t tmpfs -o mode=0755,size=1m tmpfs "$ROOT/dev"
mknod -m 666 "$ROOT/dev/null" c 1 3
mknod -m 666 "$ROOT/dev/zero" c 1 5
mknod -m 666 "$ROOT/dev/full" c 1 7
mknod -m 666 "$ROOT/dev/random" c 1 8
mknod -m 666 "$ROOT/dev/urandom" c 1 9
mknod -m 666 "$ROOT/dev/tty" c 5 0
mkdir -m 755 "$ROOT/dev/pts"
mount -t devpts -o gid=5,mode=620,ptmxmode=666 devpts "$ROOT/dev/pts"
ln -sf pts/ptmx "$ROOT/dev/ptmx"
mount -t proc proc "$ROOT/proc"
mount -t sysfs -o ro sysfs "$ROOT/sys" 2>/dev/null || true
# /workspace is the workspace facet's volume (machine §3), the tmpfs the
# rootfs's own fstab mounts under Firecracker: the host pushes it at boot and
# pulls it at every quiescent point, so nothing in it is durable on its own.
mount -t tmpfs -o size=128m,mode=0777 tmpfs "$ROOT/workspace"
# One socket directory, two namespaces: the agent binds it inside the chroot,
# the relay reaches it from outside, and neither needs to know about the other.
mkdir -p "$SOCK_DIR" "$ROOT$SOCK_DIR"
mount --bind "$SOCK_DIR" "$ROOT$SOCK_DIR"

# `docker stop` is the graceful path: flush the guest's writes into the image
# the host is about to capture, then unwind. `docker rm -f` (SIGKILL) skips all
# of it, which is exactly the crash the capture cadence bounds (machine §4, M3).
stop() {
  kill "$AGENT" 2>/dev/null || true
  sync
  umount "$ROOT$SOCK_DIR" "$ROOT/workspace" "$ROOT/proc" "$ROOT/sys" "$ROOT/dev/pts" "$ROOT/dev" \
    2>/dev/null || true
  umount "$ROOT" 2>/dev/null || true
  losetup -d "$DEV" 2>/dev/null || true
  exit 0
}
trap stop TERM INT

# The agent is this container's main process (machine §5.1's broker, not pid 1
# of a booted system). Its unix-socket mode speaks the same `CONNECT 62` muxer
# handshake Firecracker performs host-side, so one agent binary serves either
# mechanism unchanged — reached here through `docker exec` rather than vsock,
# because a socket created in this namespace is not one the host can dial.
chroot "$ROOT" /usr/sbin/machine-agent --uds "$SOCK" &
AGENT=$!
echo "machine runner: $IMAGE on $DEV, agent on $ROOT$SOCK"
wait "$AGENT" || true
