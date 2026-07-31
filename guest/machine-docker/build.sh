#!/bin/sh
# Build the container mechanism's runner image (machine spec §2.1). Docker is the
# only requirement — the image carries no repository artifact, because the guest
# agent ships inside the machine's own rootfs (guest/machine-rootfs/build.sh).
#
# Knobs (env):
#   MACHINE_RUNNER_IMAGE  tag to build (default harness-machine-runner:1)
set -eu
cd "$(dirname "$0")"

IMAGE=${MACHINE_RUNNER_IMAGE:-harness-machine-runner:1}
echo "--- $IMAGE"
docker build -q -t "$IMAGE" .
echo "--- done"
