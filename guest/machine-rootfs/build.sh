#!/bin/sh
# Build the persistent machine's base image and VM assets (machine spec §5.1)
# into guest/machine-rootfs/out/:
#
#   machine-agent   the guest channel broker (static musl), built + tested on Linux
#   machine.ext4    alpine + openrc + machine-agent as a boot service + a
#                   default user + openssh-sftp-server, assembled with mkfs.ext4 -d
#   vmlinux         a firecracker-ci kernel (vsock-enabled)
#   firecracker     the VMM release binary
#
# Unlike guest/fc-rootfs (the sandbox's host-owned agent-as-init image), this
# is the *user's own rootfs*: the agent is an ordinary service the rootfs's
# own init (openrc) starts, not pid 1 (machine §2.1). The whole image persists
# block-for-block as the disk facet (grain §7.15), so a machine provisioned
# from it diverges by dirty blocks as its guest writes.
#
# Docker is the only requirement, so this runs on macOS too — building needs
# no KVM. *Running* the assets (tests/firecracker_e2e.rs) needs Linux + /dev/kvm.
#
# Knobs (env):
#   ARCH        x86_64 | aarch64   (default: the docker engine's arch)
#   ALPINE      alpine image tag                       (default 3.20)
#   FC_VERSION  firecracker release tag                (default v1.10.1)
#   CI_VERSION  firecracker-ci kernel folder           (default v1.10)
#   KERNEL      kernel series prefix to pick           (default 5.10)
#   MACHINE_MB  image size in MiB                       (default 1024)
#   USER_NAME   the default login user                  (default dev)
set -eu
cd "$(dirname "$0")"

ALPINE=${ALPINE:-3.20}
FC_VERSION=${FC_VERSION:-v1.10.1}
CI_VERSION=${CI_VERSION:-v1.10}
KERNEL=${KERNEL:-5.10}
MACHINE_MB=${MACHINE_MB:-1024}
USER_NAME=${USER_NAME:-dev}
ARCH=${ARCH:-$(docker run --rm alpine:"$ALPINE" uname -m)}
case "$ARCH" in
  x86_64) PLATFORM=linux/amd64 ;;
  aarch64) PLATFORM=linux/arm64 ;;
  *) echo "unsupported ARCH $ARCH" >&2; exit 1 ;;
esac
echo "building for $ARCH ($PLATFORM)"
mkdir -p out

echo "--- machine-agent: build + test (rust:alpine, static musl)"
# An explicit --target keeps host artifacts (serde's proc-macro) off the
# crt-static RUSTFLAGS — a proc-macro dylib cannot be built static-crt, and
# cargo only separates host from target flags when a target is named.
docker run --rm --platform "$PLATFORM" \
  -v "$(cd ../machine-agent && pwd)":/src -w /src \
  -e CARGO_TARGET_DIR=/src/target-linux-"$ARCH" \
  -e RUSTFLAGS="-C target-feature=+crt-static" \
  rust:alpine sh -ec '
    apk add -q musl-dev
    TARGET="$(uname -m)-unknown-linux-musl"
    cargo test --release --target "$TARGET"
    cargo build --release --target "$TARGET"
  '
cp ../machine-agent/target-linux-"$ARCH"/"$ARCH"-unknown-linux-musl/release/machine-agent out/machine-agent

echo "--- machine.ext4: alpine + openrc + the agent service + sftp + a user"
docker run --rm --platform "$PLATFORM" \
  -v "$(pwd)/out":/out \
  -e MACHINE_MB="$MACHINE_MB" -e USER_NAME="$USER_NAME" \
  alpine:"$ALPINE" sh -ec '
    apk add -q e2fsprogs openrc openssh-server-common openssh-sftp-server shadow
    mkdir /rootfs
    for d in bin etc lib sbin usr root var home; do cp -a /$d /rootfs/ 2>/dev/null || true; done
    mkdir -p /rootfs/dev /rootfs/proc /rootfs/sys /rootfs/tmp /rootfs/run
    install -m 0755 /out/machine-agent /rootfs/usr/sbin/machine-agent

    # /workspace: a tmpfs the host syncs via WsPush/WsPull (machine spec §3).
    # The workspace facet is the durable truth; nothing here lands on the
    # disk image. Sized past the 64 MiB sync cap for tar/fs headroom.
    mkdir -p /rootfs/workspace
    echo "tmpfs /workspace tmpfs rw,size=128m,mode=0777 0 0" >> /rootfs/etc/fstab

    # A default unprivileged login user; the front door bridges to its shell.
    chroot /rootfs sh -ec "adduser -D $USER_NAME && passwd -u $USER_NAME || true"

    # The agent as an openrc service, started at boot (not pid 1, machine §2.1).
    cat > /rootfs/etc/init.d/machine-agent <<EOF
#!/sbin/openrc-run
description="Persistent machine guest agent (vsock channel broker)"
command=/usr/sbin/machine-agent
command_background=true
pidfile=/run/machine-agent.pid
depend() { need localmount; after bootmisc; }
EOF
    chmod +x /rootfs/etc/init.d/machine-agent
    chroot /rootfs rc-update add machine-agent default || true
    # Bring up the default runlevel on boot.
    chroot /rootfs rc-update add devfs sysinit || true
    chroot /rootfs rc-update add procfs boot || true
    # localmount honors fstab (the /workspace tmpfs); the agent needs it.
    chroot /rootfs rc-update add localmount boot || true

    truncate -s "${MACHINE_MB}M" /out/machine.ext4.tmp
    mkfs.ext4 -q -d /rootfs /out/machine.ext4.tmp
    mv /out/machine.ext4.tmp /out/machine.ext4
  '

echo "--- vmlinux: latest firecracker-ci $KERNEL kernel for $ARCH"
docker run --rm --platform "$PLATFORM" -v "$(pwd)/out":/out alpine:"$ALPINE" sh -ec '
  KEY=$(wget -qO- "https://s3.amazonaws.com/spec.ccfc.min?prefix=firecracker-ci/'"$CI_VERSION"'/'"$ARCH"'/vmlinux-'"$KERNEL"'&list-type=2" \
    | tr "<" "\n" | sed -n "s|^Key>||p" | grep -E "vmlinux-[0-9.]+$" | sort -V | tail -1)
  test -n "$KEY" || { echo "no kernel found; set CI_VERSION/KERNEL" >&2; exit 1; }
  echo "fetching $KEY"
  wget -q "https://s3.amazonaws.com/spec.ccfc.min/$KEY" -O /out/vmlinux
'

echo "--- firecracker $FC_VERSION"
docker run --rm --platform "$PLATFORM" -v "$(pwd)/out":/out alpine:"$ALPINE" sh -ec '
  wget -q "https://github.com/firecracker-microvm/firecracker/releases/download/'"$FC_VERSION"'/firecracker-'"$FC_VERSION"'-'"$ARCH"'.tgz" -O /tmp/fc.tgz
  tar -xzf /tmp/fc.tgz -C /tmp
  # Exact name: the release ships firecracker-<ver>-<arch> AND its .debug
  # sibling; a bare glob matches both and busybox install writes the last
  # match — the dynamically linked debug artifact, which segfaults at exec.
  install -m 0755 /tmp/release-*/firecracker-'"$FC_VERSION"'-'"$ARCH"' /out/firecracker
'

echo "--- done"
ls -lh out/
