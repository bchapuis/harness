//! `agent-standalone` as a library: one node (silo) of the standalone agent
//! deployment, the production runtime (tokio, TCP, a file journal replicated to
//! peers) wired to the agent runtime. The `main.rs` binary is a thin argument
//! parser over [`node`].
//!
//! A node hosts every kind's entity type and the tenancy `Directory`, votes in
//! Raft, and has no client-facing listener: the public edge is `agent-gateway`,
//! a cluster client the node admits with `--client`. It supplies the agent
//! runtime's two plug points: the model seam as `agent-anthropic`'s
//! `AnthropicModel` over this crate's `HttpsPost` (the `agent::http::HttpPost`
//! implementation, built on `agent-http`), and the sandbox seam as
//! `agent-sandbox`'s `TieredSandboxes`, in the mode `--sandbox` names: a
//! container or a microVM for a confined `shell`, or the typed file tools
//! alone. No mode runs native code unconfined. The kinds carry the provider's
//! own tool declarations, so every node agrees on the digest.
//!
//! See `docs/standalone-deployment.md` for the walkthrough, and
//! `docs/edge-spec.md` for the edge the nodes admit.

mod http;
pub mod node;
