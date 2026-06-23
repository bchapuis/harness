# Build harness-standalone and ship it on a small Python base.
#
# With `--sandbox local` (the Kubernetes manifest's default) the model's
# `shell` tool runs inside THIS container, so the runtime image carries
# python3, bash, and coreutils — the same tools demo.sh hands the model via
# python:3.12-slim — and the pod is the isolation boundary. Switch the
# manifest to `--sandbox docker`/`firecracker` for a per-session boundary
# (see k8s/README.md); that needs extra in-cluster plumbing.
#
# Build from the repository root:  docker build -t harness-standalone:latest .

FROM rust:1.88-bookworm AS build
WORKDIR /src
COPY . .
# edition 2024 needs Rust ≥ 1.85, but the tree uses let-chains (stable in 1.88).
RUN cargo build --release -p harness-standalone

FROM python:3.12-slim
# ca-certificates lets the node's rustls client verify api.anthropic.com.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/harness-standalone /usr/local/bin/harness-standalone
ENTRYPOINT ["harness-standalone"]
