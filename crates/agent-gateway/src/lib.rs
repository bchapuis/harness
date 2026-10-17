//! The agent runtime gateway: the public multi-tenant HTTP/SSE edge, an
//! Orleans-style cluster **client** of the agent runtime cluster (design:
//! `docs/multi-tenant-edge.md`).
//!
//! A caller presents a bearer token; the gateway verifies it to a [`PrincipalId`]
//! (it **terminates** tenant auth), scopes the caller's session key under that
//! principal, and drives the session by addressing its entity **directly**, no
//! control protocol, no per-node listener, no forwarding hop.
//!
//! The gateway joins the actor transport as a non-voting, routing-only **member**
//! (`Harness::client(system, roster)` /
//! [`entities_client`](entity::EntityExt::entities_client)): a full local actor
//! system that hosts no entities but is still a member; it holds `EntityRef`s
//! discovered through the receptionist gossip, routes `ask`/`subscribe` to the
//! shard leader, and spawns the ephemeral reply mailbox the host calls back. It is
//! therefore **inside** the cluster's trust boundary (it holds the cluster secret
//! and rides the receptionist bus); untrusted callers reach it only over HTTP with
//! a bearer token.
//!
//! Stateless: the gateway holds no durable state. Run N replicas behind a load
//! balancer; each joins the cluster as its own client id.

pub mod auth;
pub mod error;
pub mod http;

use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use entity::Entity;
use entity::EntityConfig;
use entity::EntityName;
use entity::Entities;
use entity::EntityExt;
use agent::Harness;
use agent::HarnessSystem;
use agent::SessionId;
use agent::SessionRef;
use tenancy::Directory;

use crate::auth::PrincipalId;
use crate::auth::TokenVerifier;
use crate::auth::scoped_session;
use crate::error::GatewayError;

/// The operational limits the edge enforces, so a client cannot exhaust the
/// gateway. Defaults are generous; the CLI (`--max-within-secs`,
/// `--max-body-bytes`) tightens them for a deployment.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Ceiling on a prompt's `within_secs`: a client cannot park a run (and the
    /// resources behind it) indefinitely. A larger request is clamped to this.
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

/// The running gateway's shared state, held behind an `Arc` across all requests.
///
/// The verifier is behind an `RwLock` so it can be hot-swapped (`SIGHUP` token
/// reload) without a restart. The readiness flag flips to `false` on a shutdown
/// signal so `/readyz` deregisters the pod from a load balancer before the drain.
pub struct Gateway<S: HarnessSystem> {
    harness: Harness<S>,
    directory: Entities<Directory<S>>,
    tokens: RwLock<Arc<dyn TokenVerifier>>,
    limits: Limits,
    ready: AtomicBool,
}

impl<S: HarnessSystem> Gateway<S> {
    /// Build the shared state from an already-connected client harness and
    /// directory entity.
    fn new(
        harness: Harness<S>,
        directory: Entities<Directory<S>>,
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

    /// Flip the readiness flag; `false` starts the drain (see [`Gateway`]).
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Relaxed);
    }

    /// The client handle to the tenancy ownership index (one entity per principal).
    pub fn directory(&self) -> &Entities<Directory<S>> {
        &self.directory
    }

    /// The session entity for `(principal, kind, session)`: the caller's session key
    /// scoped under its principal for tenant isolation, resolved to a routing
    /// `SessionRef`. The principal-scoping of a session name is the gateway's
    /// secret: a handler addresses a session by the tenant-facing triple and never
    /// mints a scoped id itself. The kind comes off the request path, so a name
    /// this deployment does not serve is the client's `404`, not a panic.
    pub fn session(
        &self,
        principal: &PrincipalId,
        kind: &str,
        session: &str,
    ) -> Result<SessionRef<S>, GatewayError> {
        self.harness
            .session(kind, SessionId::new(scoped_session(principal, session)))
            .map_err(|e| GatewayError::unknown_kind(e.to_string()))
    }

    /// Record the tenant's ownership of a session in the directory index, keyed by
    /// the principal. Best-effort and idempotent: re-recorded on every prompt, so a
    /// transient index failure self-heals next turn; the run proceeds regardless, so
    /// the failure is logged, not returned. Owns the directory entry's `EntityName`
    /// shape: `(kind, scoped session)`, so no handler re-derives it.
    pub async fn record_ownership(&self, principal: &PrincipalId, kind: &str, session: &str) {
        let scoped = scoped_session(principal, session);
        let recorded = self
            .directory
            .entity(principal.as_str())
            .ask(tenancy::Record {
                name: EntityName::new(kind.to_string(), scoped.clone()),
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
/// receptionist, then assemble the shared state. The directory rides the entity
/// default shard count on both sides (the nodes host it with `EntityConfig`'s
/// default, and there is no flag for it either side), so a name hashes to the
/// same shard here as there. Errors if discovery does not complete within
/// `timeout`.
pub async fn connect<S: HarnessSystem>(
    system: S,
    roster: &[(String, usize)],
    tokens: Arc<dyn TokenVerifier>,
    limits: Limits,
    timeout: Duration,
) -> Result<Arc<Gateway<S>>, String> {
    let directory_type = <Directory<S> as Entity>::ENTITY_TYPE;
    let directory_shards = EntityConfig::default().shards;
    let deadline = system.now() + timeout;
    loop {
        let harness = Harness::client(system.clone(), roster);
        let directory = system.entities_client::<Directory<S>>(directory_type, directory_shards);
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
