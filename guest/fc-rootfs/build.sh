#!/bin/sh
# Build the microVM assets for harness-sandbox's `firecracker` feature into
# guest/fc-rootfs/out/:
#
#   fc-agent      the guest agent (static musl), built and *tested* on Linux
#   rootfs.ext4   alpine + /sbin/fc-agent, assembled with mkfs.ext4 -d
#   vmlinux       a firecracker-ci kernel (vsock-enabled)
#   firecracker   the VMM release binary
#
# Docker is the only requirement, so this runs on macOS too — building needs
# no KVM. *Running* the assets (tests/firecracker.rs, --sandbox firecracker)
# needs Linux with /dev/kvm.
#
# Knobs (env):
#   ARCH        x86_64 | aarch64   (default: the docker engine's arch)
#   ALPINE      alpine image tag for the rootfs      (default 3.20)
#   FC_VERSION  firecracker release tag              (default v1.10.1)
#   CI_VERSION  firecracker-ci kernel folder         (default v1.10)
#   KERNEL      kernel series prefix to pick         (default 5.10)
set -eu
cd "$(dirname "$0")"

ALPINE=${ALPINE:-3.20}
FC_VERSION=${FC_VERSION:-v1.10.1}
CI_VERSION=${CI_VERSION:-v1.10}
KERNEL=${KERNEL:-5.10}
ARCH=${ARCH:-$(docker run --rm alpine:"$ALPINE" uname -m)}
case "$ARCH" in
  x86_64) PLATFORM=linux/amd64 ;;
  aarch64) PLATFORM=linux/arm64 ;;
  *) echo "unsupported ARCH $ARCH" >&2; exit 1 ;;
esac
echo "building for $ARCH ($PLATFORM)"
mkdir -p out

echo "--- fc-agent: build + test (rust:alpine, static musl)"
docker run --rm --platform "$PLATFORM" \
  -v "$(cd ../fc-agent && pwd)":/src -w /src \
  -e CARGO_TARGET_DIR=/src/target-linux-"$ARCH" \
  -e RUSTFLAGS="-C target-feature=+crt-static" \
  rust:alpine sh -ec '
    apk add -q musl-dev
    cargo test --release
    cargo build --release
  '
cp ../fc-agent/target-linux-"$ARCH"/release/fc-agent out/fc-agent

echo "--- rootfs.ext4: the alpine container is the minirootfs"
docker run --rm --platform "$PLATFORM" \
  -v "$(pwd)/out":/out \
  alpine:"$ALPINE" sh -ec '
    apk add -q e2fsprogs
    mkdir /rootfs
    for d in bin etc lib sbin usr root var; do cp -a /$d /rootfs/; done
    mkdir -p /rootfs/dev /rootfs/proc /rootfs/sys /rootfs/tmp /rootfs/run /rootfs/workspace
    install -m 0755 /out/fc-agent /rootfs/sbin/fc-agent
    truncate -s 256M /out/rootfs.ext4.tmp
    mkfs.ext4 -q -d /rootfs /out/rootfs.ext4.tmp
    mv /out/rootfs.ext4.tmp /out/rootfs.ext4
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
