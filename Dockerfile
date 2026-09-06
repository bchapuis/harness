# Build agent-standalone (the node/silo) and agent-gateway (the public
# HTTP/SSE edge, a cluster client) and ship both on a small Python base.
#
# The Kubernetes manifest runs `--sandbox durable`: the model gets the typed
# file tools over a durable workspace entity and no `shell` at all, so nothing
# the model composes ever executes in this container. The runtime base is
# python:3.12-slim anyway — the same tools demo-agent.sh hands the model — so
# the image is ready for an operator who adds an interpreter path later.
# Switch the manifest to `--sandbox docker`/`firecracker` for a shell behind a
# per-session boundary (see k8s/README.md); that needs extra in-cluster
# plumbing.
#
# Build from the repository root:  docker build -t agent-standalone:latest .

FROM rust:1.88-bookworm AS build
WORKDIR /src
COPY . .
# edition 2024 needs Rust ≥ 1.85, but the tree uses let-chains (stable in 1.88).
RUN cargo build --release -p agent-standalone -p agent-gateway

FROM python:3.12-slim
# ca-certificates lets the node's rustls client verify api.anthropic.com.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/agent-standalone /usr/local/bin/agent-standalone
# The gateway pod overrides the entrypoint to run this binary instead.
COPY --from=build /src/target/release/agent-gateway /usr/local/bin/agent-gateway
ENTRYPOINT ["agent-standalone"]
