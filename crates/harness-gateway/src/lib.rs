//! The harness gateway: the public HTTP/SSE edge, an Orleans-style cluster
//! **client** of the harness cluster.
//!
//! It is the public multi-tenant edge (design: `docs/multi-tenant-acp-design.md`).
//! A caller presents a bearer token; the gateway verifies it to a [`PrincipalId`]
//! (it **terminates** tenant auth), scopes the caller's session key under that
//! principal, and drives the session by addressing its grain **directly** — no
//! control protocol, no per-node listener, no forwarding hop.
//!
//! The gateway joins the actor transport as a non-voting, routing-only **member**
//! (`Harness::builder(..).route_all()` /
//! [`granary_client`](granary::GranaryExt::granary_client)): a full local actor
//! system that hosts no grains but is still a member — it holds `GrainRef`s
//! discovered through the receptionist gossip, routes `ask`/`subscribe` to the
//! shard leader, and spawns the ephemeral reply mailbox the host calls back. This
//! puts the gateway **inside** the cluster's trust boundary (it holds the cluster
//! secret and rides the receptionist bus) — the Akka-`ClusterClient` tradeoff we
//! accept, because the gateway is already where tenant auth terminates. Untrusted
//! callers reach it only over HTTP with a bearer token.
//!
//! Stateless: the gateway holds no durable state. Run N replicas behind a load
//! balancer; each joins the cluster as its own client id.

pub mod auth;
pub mod cluster;
pub mod error;
pub mod http;

use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use granary::Grain;
use granary::GrainName;
use granary::Granary;
use granary::GranaryExt;
use harness::GranaryConfig;
use harness::Harness;
use harness::HarnessSystem;
use harness::Kind;
use harness::Kinds;
use harness::SessionId;
use harness::SessionRef;
use tenancy::Directory;

use crate::auth::PrincipalId;
use crate::auth::TokenVerifier;
use crate::auth::scoped_session;

/// The kinds the gateway addresses. They MUST name the same kinds with the same
/// `GranaryConfig.shards` the nodes host (so a session name hashes to the same
/// shard); the model params and tools are ignored — the client never activates a
/// grain, it only addresses one. Keep this in step with `harness-standalone`'s
/// `node::kinds` (the `assistant`/`worker` pair on the default four shards).
pub fn client_kinds() -> Kinds {
    let grain = |k: Kind| k.grain(GranaryConfig::default());
    Kinds::new()
        .register("assistant", grain(Kind::new("")))
        .register("worker", grain(Kind::new("")))
}

/// The operational limits the edge enforces, so a client cannot exhaust the
/// gateway. Defaults are generous; the CLI (`--max-within-secs`,
/// `--max-body-bytes`) tightens them for a deployment.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Ceiling on a prompt's `within_secs`: a client cannot park a run — and the
    /// resources behind it — indefinitely. A larger request is clamped to this.
    pub max_within_secs: u64,
    /// Max request-body bytes (the prompt JSON). Enforced as an axum
    /// `DefaultBodyLimit` layer.
    pub max_body_bytes: usize,
}

impl Default for Limits {
    fn default() -> Limits {
        Limits {
            max_within_secs: 3600,
            max_body_bytes: 1 << 20,
        }
    }
}

/// The running gateway's shared state: a routing-only client [`Harness`], a
/// client [`Granary`] for the tenancy ownership index, the tenant-token verifier,
/// the edge [`Limits`], and a readiness flag. Shared behind an `Arc` across all
/// requests; cheap, stateless (holds no durable state).
///
/// The verifier is behind an `RwLock` so it can be hot-swapped (`SIGHUP` token
/// reload) without a restart; the read is a cheap uncontended lock on the auth
/// path. The readiness flag flips to `false` on a shutdown signal so `/readyz`
/// deregisters the pod from a load balancer before the drain.
pub struct Gateway<S: HarnessSystem> {
    harness: Harness<S>,
    directory: Granary<Directory<S>>,
    tokens: RwLock<Arc<dyn TokenVerifier>>,
    limits: Limits,
    ready: AtomicBool,
}

impl<S: HarnessSystem> Gateway<S> {
    /// Build the shared state from an already-connected client harness and
    /// directory granary. Split out of [`connect`] so a test can drive the router
    /// over a single in-process system without the discovery poll.
    pub fn new(
        harness: Harness<S>,
        directory: Granary<Directory<S>>,
        tokens: Arc<dyn TokenVerifier>,
        limits: Limits,
    ) -> Arc<Gateway<S>> {
        Arc::new(Gateway {
            harness,
            directory,
            tokens: RwLock::new(tokens),
            limits,
            ready: AtomicBool::new(true),
        })
    }

    /// Verify an incoming bearer token to the principal it names, or `None`.
    pub fn principal(&self, token: &str) -> Option<PrincipalId> {
        self.tokens
            .read()
            .expect("token verifier lock is never poisoned")
            .verify(token)
    }

    /// Hot-swap the token verifier (a `SIGHUP` reload of `--auth-tokens`), so a
    /// deployment rotates tenant tokens without a restart or a dropped connection.
    pub fn reload_tokens(&self, tokens: Arc<dyn TokenVerifier>) {
        *self
            .tokens
            .write()
            .expect("token verifier lock is never poisoned") = tokens;
    }

    /// The edge limits this gateway enforces.
    pub fn limits(&self) -> Limits {
        self.limits
    }

    /// Whether the gateway is ready to serve traffic (`false` once draining).
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }

    /// Flip the readiness flag — `false` on a shutdown signal so `/readyz` fails
    /// and the load balancer stops routing new requests before the drain.
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Relaxed);
    }

    /// The routing-only client harness (no model/sandbox seams, never activates).
    pub fn harness(&self) -> &Harness<S> {
        &self.harness
    }

    /// The client handle to the tenancy ownership index (one grain per principal).
    pub fn directory(&self) -> &Granary<Directory<S>> {
        &self.directory
    }

    /// The session grain for `(principal, kind, session)`: the caller's session key
    /// scoped under its principal for tenant isolation, resolved to a routing
    /// `SessionRef`. The principal-scoping of a session name is the gateway's
    /// secret — a handler addresses a session by the tenant-facing triple and never
    /// mints a scoped id itself.
    pub fn session(&self, principal: &PrincipalId, kind: &str, session: &str) -> SessionRef<S> {
        self.harness
            .session(kind, SessionId::new(scoped_session(principal, session)))
    }

    /// Record the tenant's ownership of a session in the directory index, keyed by
    /// the principal. Best-effort and idempotent: re-recorded on every prompt, so a
    /// transient index failure self-heals next turn; the run proceeds regardless, so
    /// the failure is logged, not returned. Owns the directory entry's `GrainName`
    /// shape — `(kind, scoped session)` — so no handler re-derives it.
    pub async fn record_ownership(&self, principal: &PrincipalId, kind: &str, session: &str) {
        let scoped = scoped_session(principal, session);
        let recorded = self
            .directory
            .grain(principal.as_str())
            .ask(tenancy::Record {
                name: GrainName::new(kind.to_string(), scoped.clone()),
                meta: tenancy::Meta {
                    label: Some(session.to_string()),
                    ..tenancy::Meta::default()
                },
            })
            .await;
        if let Err(e) = recorded {
            eprintln!("[tenancy] record {kind}/{scoped} failed (will retry next turn): {e:?}");
        }
    }
}

/// Join the cluster as a client and build the [`Gateway`]: poll until both the
/// kinds' and the directory's host gateways have gossiped into this client's
/// receptionist (exactly as a node waits for its peers), then assemble the shared
/// state. `directory_shards` MUST match the nodes' `Directory` shards (the
/// granary default). Errors if discovery does not complete within `timeout`.
pub async fn connect<S: HarnessSystem>(
    system: S,
    kinds: Kinds,
    directory_shards: usize,
    tokens: Arc<dyn TokenVerifier>,
    limits: Limits,
    timeout: Duration,
) -> Result<Arc<Gateway<S>>, String> {
    let directory_type = <Directory<S> as Grain>::GRAIN_TYPE;
    let deadline = system.now() + timeout;
    loop {
        let harness = Harness::client(system.clone(), &kinds);
        let directory = system.granary_client::<Directory<S>>(directory_type, directory_shards);
        if let (Some(harness), Some(directory)) = (harness, directory) {
            return Ok(Gateway::new(harness, directory, tokens, limits));
        }
        if system.now() >= deadline {
            return Err(
                "no host gateway gossiped into the client within the discovery timeout: is the \
                 cluster up, the --secret matching, and this client admitted with --client on the \
                 nodes?"
                    .to_string(),
            );
        }
        system.sleep(Duration::from_millis(200)).await;
    }
}
