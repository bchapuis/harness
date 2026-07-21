//! `GrainRef`, the `Granary` handle, and the system extension (spec §4.3, §5.4).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::CallError;
use actor_core::Message;
use actor_core::NodeId;
use serde::Deserialize;
use serde::Serialize;

use crate::alarm_index::AlarmIndex;
use crate::alarm_index::DueBefore;
use crate::alarm_index::index_key;
use crate::config::GranaryConfig;
use crate::error::GrainError;
use crate::gateway::Activate;
use crate::gateway::Gateway;
use crate::gateway::gateway_key;
use crate::grain::Grain;
use crate::grain::GrainHandler;
use crate::grain::GrainName;
use crate::host::Host;
use crate::host::RunTyped;
use crate::journal::Seq;
use crate::replica_store::ActorReplicaTransport;
use crate::replica_store::ReplicaStore;
use crate::replica_store::ReplicaTransport;
use crate::replica_store::replica_store_key;
use crate::shardmap::EmptyShardMap;
use crate::shardmap::ShardMapSource;
use crate::store::GrainStore;
use crate::store::MemoryGrainStore;
use crate::subscription::CloseSink;
use crate::subscription::RecordSink;
use crate::subscription::SUB_BUFFER;
use crate::subscription::Subscribe;
use crate::subscription::Subscription;
use crate::system::GranarySystem;
use crate::system::ShardId;
use crate::system::shard_for;

/// The default deadline applied to [`GrainRef::ask`] (mirrors the actor `ask`).
const DEFAULT_ASK_TIMEOUT: Duration = Duration::from_secs(5);

/// How long the client-side redirect waits between resolution attempts while a
/// shard has no reachable leader — a fresh election or a not-yet-gossiped gateway
/// registration (§5.4). Short relative to an election timeout so convergence is
/// observed promptly.
const RESOLVE_BACKOFF: Duration = Duration::from_millis(50);

/// Safety bound on the redirect loop; the real bound is the caller's deadline
/// (`within`), checked each attempt. A run started during failover is absorbed
/// rather than surfaced, so a remote call stays observably identical to a local
/// one (invariant **G13**).
const RESOLVE_ATTEMPTS: usize = 200;

/// How long one resolution attempt waits on a gateway before treating it as
/// stale and re-resolving — short, so a hint pointing at a just-crashed leader
/// fails fast instead of stalling the redirect for the full deadline.
const FORWARD_TIMEOUT: Duration = Duration::from_millis(500);

/// A node-local cache of resolved host handles, shared by every [`GrainRef`] a
/// [`Granary`] hands out (spec §5.4). A cache hit lets a call go **straight to the
/// host actor**, skipping the serial gateway on the steady-state hot path; the
/// gateway then only serializes genuine activations and the `NotLeader` refresh.
///
/// A cached handle is returned only after a cheap, local check that its node still
/// leads the grain's shard ([`HostCache::get`]). This is the pre-send guard that
/// keeps a cache hit from dispatching to a **deposed** leader: a write to a crashed
/// leader can time out, and a timeout is not safe to auto-retry (the command may
/// have committed — at-most-once, §6, §2.2). Validating leadership before sending
/// routes around a settled failover entirely, so the cache self-heals safely.
///
/// `Arc`-shared and never serialized: a `GrainRef` that crosses the wire arrives
/// cache-less and resolves through the gateway each call.
pub(crate) struct HostCache<G: Grain> {
    system: G::System,
    /// The runtime type name (spec §5.1) this cache routes for, `G::GRAIN_TYPE`
    /// by default but a caller-supplied name under [`granary_named`](GranaryExt::granary_named).
    grain_type: &'static str,
    shards: usize,
    hosts: Mutex<HashMap<GrainName, ActorRef<Host<G>>>>,
}

impl<G: Grain> HostCache<G> {
    fn new(system: G::System, grain_type: &'static str, shards: usize) -> Arc<HostCache<G>> {
        Arc::new(HostCache {
            system,
            grain_type,
            shards,
            hosts: Mutex::new(HashMap::new()),
        })
    }

    /// A cached host for `name`. On a node that **replicates** the name's shard the
    /// current leader is known locally, so a handle that no longer sits on it (the
    /// leader moved) is proactively dropped — the pre-send guard that keeps a write
    /// off a deposed leader (§5.4). A node that does **not** replicate the shard
    /// cannot know the leader (`shard_leader` is `None`), so it returns the cached
    /// handle and relies on reactive invalidation (a `NotLeader`/`DeadLetter`/
    /// `Unreachable` outcome drops it and re-resolves). The leadership read is a
    /// local lock read, off the network and off the control plane (invariant **G9**).
    fn get(&self, name: &GrainName) -> Option<ActorRef<Host<G>>> {
        let mut hosts = self.hosts.lock().expect("host cache mutex poisoned");
        let host = hosts.get(name)?.clone();
        match self
            .system
            .shard_leader(shard_for(self.grain_type, name.key(), self.shards))
        {
            // Replica node, leader moved: drop the stale handle before it is used.
            Some(leader) if host.id().node() != leader => {
                hosts.remove(name);
                None
            }
            // Leader matches, or this node cannot tell (a non-replica): use it.
            _ => Some(host),
        }
    }

    fn put(&self, name: GrainName, host: ActorRef<Host<G>>) {
        self.hosts
            .lock()
            .expect("host cache mutex poisoned")
            .insert(name, host);
    }

    fn remove(&self, name: &GrainName) {
        self.hosts
            .lock()
            .expect("host cache mutex poisoned")
            .remove(name);
    }

    fn contains(&self, name: &GrainName) -> bool {
        self.hosts
            .lock()
            .expect("host cache mutex poisoned")
            .contains_key(name)
    }
}

/// The only handle to a grain (spec §4.3): it carries the [`GrainName`] and a
/// handle to the grain type's gateway, and **never** grants access to state.
///
/// `Clone + Serialize + DeserializeOwned + Send + Sync`: it travels as the name
/// plus the gateway's id, and the gateway handle rebinds on decode (the framework
/// `ActorRef` discipline). The `G: GrainHandler<M>` bound on `ask`/`tell` proves
/// at compile time that the grain accepts `M` (invariant **G10**).
///
/// It also carries an optional, **non-serialized** [`HostCache`] (present when
/// obtained from a [`Granary`] on this node): repeated calls hit the cache and go
/// straight to the host, off the gateway (§5.4). A `GrainRef` rebuilt from the
/// wire, or a self-reference from [`GrainCtx`](crate::GrainCtx), has no cache and
/// resolves through the gateway each call — correct, just not cached.
///
/// The system handle drives the **client-side bounded redirect** (§5.4 step 4):
/// the gateway answers single-shot, and this ref follows `NotLeader(hint)` to the
/// hinted node's gateway (looked up in the receptionist), backing off until the
/// shard's leader is found or the caller's deadline expires. It is never
/// serialized; a wire-arrived ref recovers it from its decoded `gateway` handle
/// ([`ActorRef::system`]), which rebinds to the local system on decode (§4.4).
pub struct GrainRef<G: Grain> {
    /// The runtime type name (spec §5.1) this ref routes under, used to look up
    /// the leader's gateway in the receptionist (the `&'static` a [`Key`] needs).
    /// `G::GRAIN_TYPE` for the common case; a caller-supplied name when the type
    /// is hosted under [`granary_named`](GranaryExt::granary_named). A ref decoded
    /// off the wire recovers it as `G::GRAIN_TYPE` (see the `Deserialize` note).
    grain_type: &'static str,
    name: GrainName,
    gateway: ActorRef<Gateway<G>>,
    system: G::System,
    cache: Option<Arc<HostCache<G>>>,
}

// `GrainRef` travels as just its name and gateway id (the gateway rebinds on
// decode, §4.4); the system handle and host cache are reconstructed locally.
impl<G: Grain> Serialize for GrainRef<G> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        (&self.name, &self.gateway).serialize(serializer)
    }
}

// Decoding recovers the local system from the rebound gateway ref, so a ref
// embedded in a message is usable on the node that receives it (§4.4). It arrives
// cache-less and resolves through the gateway each call (correct, just not
// cached). `grain_type` recovers as `G::GRAIN_TYPE`: the receptionist `Key` needs
// a `&'static str` and the wire carries only the name's owned string, so this is
// the one path that cannot honor a runtime type name. It is correct for every
// grain hosted under its own `G::GRAIN_TYPE` (granary's native use); a type hosted
// under `granary_named` (the agentic harness's per-kind grains) must therefore
// re-mint its refs locally from its `Granary` handle rather than ship them over
// the wire — which it does (it addresses children by key through the handle).
impl<'de, G: Grain> Deserialize<'de> for GrainRef<G> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (name, gateway) = <(GrainName, ActorRef<Gateway<G>>)>::deserialize(deserializer)?;
        let system = gateway.system().clone();
        Ok(GrainRef {
            grain_type: G::GRAIN_TYPE,
            name,
            gateway,
            system,
            cache: None,
        })
    }
}

// Manual `Clone`: `G` itself need not be `Clone`.
impl<G: Grain> Clone for GrainRef<G> {
    fn clone(&self) -> Self {
        GrainRef {
            grain_type: self.grain_type,
            name: self.name.clone(),
            gateway: self.gateway.clone(),
            system: self.system.clone(),
            cache: self.cache.clone(),
        }
    }
}

impl<G: Grain> GrainRef<G> {
    pub(crate) fn new(
        grain_type: &'static str,
        name: GrainName,
        gateway: ActorRef<Gateway<G>>,
        system: G::System,
        cache: Option<Arc<HostCache<G>>>,
    ) -> GrainRef<G> {
        GrainRef {
            grain_type,
            name,
            gateway,
            system,
            cache,
        }
    }

    /// This grain's name.
    pub fn name(&self) -> &GrainName {
        &self.name
    }

    /// Send a command and await its reply, held until the command's events are
    /// durable (the output gate, §6). The `G: GrainHandler<M>` bound makes an
    /// invalid call a compile error (**G10**).
    ///
    /// `M: Clone` so the runtime can re-issue the command if the first attempt
    /// hits a stale cached host — one that hibernated (§10) or whose leader moved
    /// (§8) since it was cached. The command did not commit in that case (§6), so
    /// re-issuing is safe; the clone is of the caller's own small command value.
    pub async fn ask<M>(&self, msg: M) -> Result<M::Reply, GrainError>
    where
        G: GrainHandler<M>,
        M: Message + Clone,
    {
        self.dispatch(msg, DEFAULT_ASK_TIMEOUT).await
    }

    /// [`ask`](Self::ask) with an explicit deadline.
    pub async fn ask_timeout<M>(&self, msg: M, within: Duration) -> Result<M::Reply, GrainError>
    where
        G: GrainHandler<M>,
        M: Message + Clone,
    {
        self.dispatch(msg, within).await
    }

    /// Fire-and-forget (spec §6): returns once the host accepts the command, not
    /// after the commit, so it reports only enqueue-time failures and never
    /// `Unavailable`. At-most-once — callers make it idempotent where it matters.
    pub async fn tell<M>(&self, msg: M) -> Result<(), GrainError>
    where
        G: GrainHandler<M>,
        M: Message + Clone,
    {
        // Attempt the cached host; only a `DeadLetter` (a hibernated host that
        // never received the command, §10) is re-enqueued. An ambiguous failure is
        // surfaced, not retried — a re-enqueue could double-apply (§2.2). One
        // deadline bounds both attempts' resolutions.
        let deadline = self.system.now() + DEFAULT_ASK_TIMEOUT;
        let host = self.resolve(true, deadline).await?;
        if let Err(call) = host.tell(RunTyped(msg.clone())).await {
            if is_retriable(&call) {
                self.invalidate();
                let host = self.resolve(false, deadline).await?;
                return host.tell(RunTyped(msg)).await.map_err(GrainError::Call);
            }
            return Err(GrainError::Call(call));
        }
        Ok(())
    }

    /// Subscribe to the grain's committed records (spec §7.9): resolve the shard
    /// leader, spawn a local [`RecordSink`] to receive pushed batches, register
    /// it, and return the committed `head` with the live stream. A framework
    /// built-in, available for every grain type without a `GrainHandler` bound —
    /// the push analogue of `load`/`head`.
    ///
    /// Delivery is best-effort; the caller MUST reconcile by `Seq` (§7.9,
    /// **G16**): backfill `from`..`head` by reading the journal, then on each
    /// batch close any gap and ignore anything already seen. When the stream
    /// closes — a move, a lag-drop, or hibernation — re-subscribe and backfill
    /// from the last seq.
    pub async fn subscribe(&self, from: Seq) -> Result<Subscription<G>, GrainError> {
        // Resolve BEFORE spawning the sink: a failed resolution (an unelectable
        // shard, a redirect that runs out its deadline) must not leave an orphan
        // actor behind — the framework does not reap an actor merely because every
        // external ref to it was dropped.
        let host = self
            .resolve(false, self.system.now() + DEFAULT_ASK_TIMEOUT)
            .await?;
        let (tx, rx) = async_channel::bounded(SUB_BUFFER);
        let sink = self.system.spawn(RecordSink::<G>::new(tx));
        match host
            .ask_timeout(Subscribe::new(from, sink.clone()), DEFAULT_ASK_TIMEOUT)
            .await
        {
            Ok(subscribed) => Ok(Subscription {
                head: subscribed.head,
                records: rx,
            }),
            // The host went away between resolve and register: stop the sink we
            // just spawned (it was never registered, so no batch will ever stop it).
            Err(call) => {
                let _ = sink.tell(CloseSink).await;
                Err(GrainError::Call(call))
            }
        }
    }

    /// Resolve the name to its live host (§5.4). With `use_cache`, a cached handle
    /// is returned without touching the gateway (the steady-state fast path);
    /// otherwise this drives the **client-side bounded redirect**: the single-shot
    /// gateway answers `Ok(host)` if its node leads the shard, else
    /// `NotLeader(hint)`; this follows the hint to that node's gateway (looked up
    /// in the receptionist), backing off while the shard elects, until the leader
    /// is found or the caller's deadline (`within`) expires (§5.4 step 4). A
    /// transient miss — bootstrap not committed, an election in flight, a hint at a
    /// just-crashed leader — is waited out rather than surfaced, so a remote call
    /// stays observably identical to a local one across a failover (invariant
    /// **G13**). Driving the loop here, not in the gateway's serial handler, keeps
    /// one slow resolution from blocking another grain's activation on that node.
    ///
    /// Bounded by the **caller's** `deadline`, not a per-attempt window: the caller
    /// computes it once from its declared budget, so a dispatch that resolves,
    /// fails, and re-resolves still returns within the one budget (§5.4).
    async fn resolve(
        &self,
        use_cache: bool,
        deadline: actor_core::Instant,
    ) -> Result<ActorRef<Host<G>>, GrainError> {
        if use_cache && let Some(host) = self.cache.as_ref().and_then(|cache| cache.get(&self.name))
        {
            return Ok(host);
        }
        // Start at this ref's own gateway: local for a `Granary`-minted ref (the
        // local fast path when this node leads), the source node's for a
        // wire-arrived one. Hints and re-discovery move it toward the leader.
        let mut target = self.gateway.clone();
        for _ in 0..RESOLVE_ATTEMPTS {
            let remaining = deadline.duration_since(self.system.now());
            if remaining.is_zero() {
                break;
            }
            // Cap each attempt so a dead target fails fast and we re-resolve,
            // rather than spending the whole deadline on one unreachable gateway.
            let attempt = remaining.min(FORWARD_TIMEOUT);
            match target
                .ask_timeout(Activate::new(self.name.clone()), attempt)
                .await
            {
                Ok(Ok(host)) => {
                    if let Some(cache) = &self.cache {
                        cache.put(self.name.clone(), host.clone());
                    }
                    return Ok(host);
                }
                // Follow the leader hint to that node's gateway. If it is not (yet)
                // discoverable, keep the current target and back off — the shard is
                // still electing or the gateway has not gossiped in.
                Ok(Err(GrainError::NotLeader(hint))) => {
                    if let Some(gateway) = self.gateway_on(hint) {
                        target = gateway;
                    }
                }
                // A genuine durability outcome (quorum loss / unhandled): surface it.
                Ok(Err(other)) => return Err(other),
                // The target gateway is unreachable (its node crashed): re-discover a
                // gateway to redirect from — any but the one that just failed, else
                // a stale receptionist entry for the crashed node (rank-first) would
                // pin the loop on the dead gateway for the whole deadline.
                Err(_) => {
                    if let Some(gateway) = self.gateway_excluding(target.id().node()) {
                        target = gateway;
                    }
                }
            }
            self.system.sleep(RESOLVE_BACKOFF).await;
        }
        // Deadline or attempt bound exhausted: surface the best hint (§12).
        Err(GrainError::NotLeader(self.system.node()))
    }

    /// The gateway registered on `node`, if discovered in the receptionist (§5.3).
    fn gateway_on(&self, node: NodeId) -> Option<ActorRef<Gateway<G>>> {
        self.system
            .receptionist()
            .lookup(gateway_key::<G>(self.grain_type))
            .into_vec()
            .into_iter()
            .find(|gateway| gateway.id().node() == node)
    }

    /// A discovered gateway on any node but `failed` — used to escape a dead
    /// target (§5.4) without re-selecting the very gateway that just timed out
    /// (its registration may outlive the crash until the membership prunes it).
    /// Falls back to whatever is registered when nothing else is.
    fn gateway_excluding(&self, failed: NodeId) -> Option<ActorRef<Gateway<G>>> {
        let gateways = self
            .system
            .receptionist()
            .lookup(gateway_key::<G>(self.grain_type))
            .into_vec();
        gateways
            .iter()
            .find(|gateway| gateway.id().node() != failed)
            .or_else(|| gateways.first())
            .cloned()
    }

    /// Drop the cached host handle for this name, so the next [`resolve`] goes back
    /// through the gateway. Called when a cached host turns out to be stale.
    fn invalidate(&self) {
        if let Some(cache) = &self.cache {
            cache.remove(&self.name);
        }
    }

    /// Resolve the host and send the typed command, held until durable (§6). The
    /// first attempt prefers the cache (straight to the host, off the gateway); if
    /// that host turns out unusable, the cache entry is dropped and the command
    /// re-issued — but **only when the first attempt provably did not run** (a
    /// `NotLeader`, which never commits, §8; or a `DeadLetter` from a hibernated
    /// host, §10). An ambiguous transport failure (`Unreachable`/`Timeout`) is
    /// surfaced, never auto-retried, because the command may have committed before
    /// the reply was lost — re-issuing a non-idempotent command would double-apply
    /// (at-most-once, §2.2; reply-iff-durable, §6/G5).
    async fn dispatch<M>(&self, msg: M, within: Duration) -> Result<M::Reply, GrainError>
    where
        G: GrainHandler<M>,
        M: Message + Clone,
    {
        // ONE deadline bounds the whole call — resolution and ask, across both
        // attempts — so the retry path never restarts the caller's budget (a
        // per-step `within` could stack up to ~4× the declared timeout).
        let deadline = self.system.now() + within;

        // Attempt 1: cached host (or a fresh resolution if nothing is cached).
        let host = self.resolve(true, deadline).await?;
        let remaining = deadline.duration_since(self.system.now());
        match host.ask_timeout(RunTyped(msg.clone()), remaining).await {
            // The host's reply is itself `Result<M::Reply, GrainError>`.
            Ok(Ok(reply)) => return Ok(reply),
            // Leadership moved off the cached host (§8): refresh and retry.
            Ok(Err(GrainError::NotLeader(_))) => self.invalidate(),
            // A genuine durability outcome (quorum loss / unhandled): terminal.
            Ok(Err(other)) => return Err(other),
            // The cached host had hibernated and stopped, so the command never
            // reached a handler (§10): safe to refresh and re-issue.
            Err(call) if is_retriable(&call) => self.invalidate(),
            // Ambiguous (Unreachable/Timeout) or otherwise terminal: surface it,
            // never auto-retry an effectful command that may have committed (§2.2).
            Err(call) => return Err(GrainError::Call(call)),
        }

        // Attempt 2: a fresh gateway resolution (bypassing the cache) and re-issue,
        // within whatever budget attempt 1 left.
        let remaining = deadline.duration_since(self.system.now());
        if remaining.is_zero() {
            return Err(GrainError::Unavailable(
                "deadline exhausted before the retry".into(),
            ));
        }
        let host = self.resolve(false, deadline).await?;
        let remaining = deadline.duration_since(self.system.now());
        match host.ask_timeout(RunTyped(msg), remaining).await {
            Ok(result) => result,
            Err(call) => Err(GrainError::Call(call)),
        }
    }
}

/// Whether a transport failure proves the command **never ran**, so re-issuing it
/// against a fresh resolution cannot double-apply (at-most-once, §2.2; reply-iff-
/// durable, §6/G5). Only `DeadLetter` qualifies: the cached host had hibernated
/// and stopped, so the command dead-lettered without reaching a handler (the §10
/// eviction race). `Unreachable`/`Timeout` are **ambiguous** — the command may
/// have committed on a leader that then crashed before replying — so they are NOT
/// auto-retried for an effectful command; they surface to the caller, who makes
/// the operation idempotent where a retry matters (§2.2). A stale cached host on a
/// crashed leader is instead dropped *before* the send by the cache's leadership
/// pre-send guard ([`HostCache::get`]), so a read still self-heals without relying
/// on this ambiguous path.
fn is_retriable(call: &CallError) -> bool {
    matches!(call, CallError::DeadLetter)
}

/// A handle to a hosted grain type (spec Appendix A): address a grain by key and
/// `ask`/`tell` it. Obtained from [`GranaryExt::granary`].
pub struct Granary<G: Grain> {
    system: G::System,
    /// The runtime type name (spec §5.1) this handle addresses, `G::GRAIN_TYPE` by
    /// default; a caller-supplied name under [`granary_named`](GranaryExt::granary_named).
    grain_type: &'static str,
    gateway: ActorRef<Gateway<G>>,
    /// The number of shards the type's namespace is partitioned into (§7.1); used
    /// to resolve a name to its shard for [`Granary::leader`]/[`Granary::replicas`].
    shards: usize,
    /// The consensus-agreed shard map (§7.6), read by [`Granary::replicas`].
    shard_map: Arc<dyn ShardMapSource>,
    /// Resolved host handles shared by every [`GrainRef`] this handle hands out, so
    /// repeated calls skip the gateway (§5.4).
    cache: Arc<HostCache<G>>,
}

impl<G: Grain> Clone for Granary<G> {
    fn clone(&self) -> Self {
        Granary {
            system: self.system.clone(),
            grain_type: self.grain_type,
            gateway: self.gateway.clone(),
            shards: self.shards,
            shard_map: Arc::clone(&self.shard_map),
            cache: Arc::clone(&self.cache),
        }
    }
}

impl<G: Grain> Granary<G> {
    /// Address a grain of this type by key (spec Appendix A): a [`GrainRef`] with
    /// no activation yet — the first message activates it. The ref shares this
    /// handle's host cache, so steady-state calls skip the gateway (§5.4).
    pub fn grain(&self, key: impl Into<String>) -> GrainRef<G> {
        GrainRef::new(
            self.grain_type,
            GrainName::new(self.grain_type, key),
            self.gateway.clone(),
            self.system.clone(),
            Some(Arc::clone(&self.cache)),
        )
    }

    /// The node that currently leads the shard a grain key maps to — where that
    /// grain activates (§5.2) — or `None` during a shard election. A routing
    /// observation, not a guarantee: leadership can move immediately after.
    pub fn leader(&self, key: impl Into<String>) -> Option<NodeId> {
        let name = GrainName::new(self.grain_type, key);
        self.system
            .shard_leader(shard_for(self.grain_type, name.key(), self.shards))
    }

    /// Whether a live host handle is currently cached for `key` (spec §5.4). An
    /// optimization/observability detail — exposed for metrics and tests — not a
    /// statement about the grain's activation on its leader.
    pub fn is_cached(&self, key: impl Into<String>) -> bool {
        self.cache.contains(&GrainName::new(self.grain_type, key))
    }

    /// The nodes that replicate the shard a grain key maps to (spec §7.6) — the
    /// only nodes that hold its data and can lead it. Read live from the
    /// consensus-agreed shard map (the `Quorum` tier rebalances it as membership
    /// changes; the `Local` tier is the single node), so it reflects the latest
    /// committed allocation, not a `granary()`-time snapshot. Exposed for metrics
    /// and tests.
    pub fn replicas(&self, key: impl Into<String>) -> Vec<NodeId> {
        let name = GrainName::new(self.grain_type, key);
        let index = shard_for(self.grain_type, name.key(), self.shards).index;
        self.shard_map.replicas(index).unwrap_or_default()
    }
}

/// Host grains of a type on a system (spec Appendix A). Implemented for every
/// [`GranarySystem`], so `system.granary::<G>(config)` starts the gateway and
/// returns a [`Granary`] handle.
pub trait GranaryExt: GranarySystem {
    /// Start hosting grains of type `G` under its own `G::GRAIN_TYPE` (spec
    /// Appendix A): spawn the type's gateway and return the handle. The grain
    /// behavior is built by `G::default` on each activation (the runtime
    /// instantiates the grain). The common case — one Rust type, one grain type.
    fn granary<G>(&self, config: GranaryConfig) -> Granary<G>
    where
        G: Grain<System = Self> + Default,
    {
        self.granary_named(G::GRAIN_TYPE, config, Arc::new(G::default))
    }

    /// Address grains of type `G` as a routing-only **client** — the Orleans
    /// cluster-client pattern. Unlike [`granary`](GranaryExt::granary) /
    /// [`granary_named`](GranaryExt::granary_named) it hosts **nothing**: no
    /// gateway, replica store, or shard-map group is started. The handle routes
    /// through a *host's* gateway, discovered in the receptionist (§5.3) and
    /// seeded here; `GrainRef`'s bounded redirect re-discovers a live gateway on
    /// failover, so the seed only has to be reachable once.
    ///
    /// Returns `None` until at least one host's gateway for `grain_type` has
    /// gossiped into this client's receptionist — the caller polls, exactly as a
    /// node waits for its peers before serving. `shards` MUST match the hosts'
    /// `GranaryConfig.shards` (so a name hashes to the same shard); the client
    /// never reads the shard map on the data path, so it is left empty.
    fn granary_client<G>(&self, grain_type: &'static str, shards: usize) -> Option<Granary<G>>
    where
        G: Grain<System = Self>,
    {
        let shards = shards.max(1);
        let gateway = self
            .receptionist()
            .lookup(gateway_key::<G>(grain_type))
            .into_vec()
            .into_iter()
            .next()?;
        Some(Granary {
            system: self.clone(),
            grain_type,
            gateway,
            shards,
            shard_map: Arc::new(EmptyShardMap),
            cache: HostCache::new(self.clone(), grain_type, shards),
        })
    }

    /// Host grains of type `G` under an explicit runtime **type name** (spec
    /// §5.1), with a caller-supplied **factory** for each activation's behavior.
    ///
    /// Two extension points over [`granary`](GranaryExt::granary), both for one
    /// Rust grain that must be **many** grain types at runtime (e.g. the agentic
    /// harness, where each *kind* is its own grain type but shares one loop):
    ///
    /// - `grain_type` overrides `G::GRAIN_TYPE`, so the same `G` hosts under
    ///   several names — distinct gateways (one `gateway_key` each), distinct
    ///   shard maps, distinct consensus groups (the `Quorum` tier). It MUST be stable
    ///   cluster-wide and across runs, exactly as `G::GRAIN_TYPE` must be (§5.1);
    ///   a `&'static str` makes that lifetime explicit (deployment leaks its
    ///   bounded set of names if they are not literals).
    /// - `factory` replaces `G::default`, so the runtime can inject per-node seam
    ///   handles into each fresh activation (the grain needs no `Default`).
    ///
    /// `granary(config)` is exactly `granary_named(G::GRAIN_TYPE, config,
    /// Arc::new(G::default))`.
    fn granary_named<G>(
        &self,
        grain_type: &'static str,
        config: GranaryConfig,
        factory: Arc<dyn Fn() -> G + Send + Sync>,
    ) -> Granary<G>
    where
        G: Grain<System = Self>;

    /// Host grains of type `G` **with durable-alarm firing across hibernation and
    /// failover** (spec §16). Like [`granary_named`](GranaryExt::granary_named), but
    /// each host registers its pending [`Alarm`](crate::Alarm) deadline with the
    /// per-shard `index`, and a background driver re-activates due grains on the
    /// shards this node leads — so an alarm fires even with no caller after the
    /// grain's leader changed. The caller starts **one** shared `AlarmIndex` granary
    /// (`system.granary::<AlarmIndex<_>>(..)`) and passes its handle to every
    /// alarm-bearing type; a type without the [`Alarm`](crate::Alarm) facet gains
    /// nothing from wiring it and should use [`granary_named`](GranaryExt::granary_named).
    fn granary_named_with_alarms<G>(
        &self,
        grain_type: &'static str,
        config: GranaryConfig,
        factory: Arc<dyn Fn() -> G + Send + Sync>,
        index: Granary<AlarmIndex<Self>>,
    ) -> Granary<G>
    where
        G: Grain<System = Self>;

    /// [`granary_named_with_alarms`](GranaryExt::granary_named_with_alarms) under the
    /// type's own `G::GRAIN_TYPE`, building each activation with `G::default` — the
    /// common case, mirroring [`granary`](GranaryExt::granary).
    fn granary_with_alarms<G>(
        &self,
        config: GranaryConfig,
        index: Granary<AlarmIndex<Self>>,
    ) -> Granary<G>
    where
        G: Grain<System = Self> + Default,
    {
        self.granary_named_with_alarms(G::GRAIN_TYPE, config, Arc::new(G::default), index)
    }
}

impl<T: GranarySystem> GranaryExt for T {
    fn granary_named<G>(
        &self,
        grain_type: &'static str,
        config: GranaryConfig,
        factory: Arc<dyn Fn() -> G + Send + Sync>,
    ) -> Granary<G>
    where
        G: Grain<System = Self>,
    {
        build_granary::<Self, G>(self, grain_type, config, factory, None)
    }

    fn granary_named_with_alarms<G>(
        &self,
        grain_type: &'static str,
        config: GranaryConfig,
        factory: Arc<dyn Fn() -> G + Send + Sync>,
        index: Granary<AlarmIndex<Self>>,
    ) -> Granary<G>
    where
        G: Grain<System = Self>,
    {
        let shards = config.shards.max(1);
        let handle =
            build_granary::<Self, G>(self, grain_type, config, factory, Some(index.clone()));
        // Start this type's alarm driver (spec §16): the callerless-activation seam.
        // A background loop that, for each shard this node leads, reads the shard's
        // alarm index and re-activates every due grain — so an alarm survives its
        // grain's hibernation and a node failover, not just a resident activation.
        self.launch(Box::pin(alarm_driver_loop::<Self, G>(
            self.clone(),
            handle.clone(),
            index,
            grain_type,
            shards,
        )));
        handle
    }
}

/// How often the alarm driver sweeps the shards it leads (spec §16). The exact
/// deadline is honoured by the grain's own in-activation timer once re-activated;
/// this cadence bounds only the *re-activation* latency after a failover, so it
/// trades a little post-failover slack for a quiet steady state.
const ALARM_DRIVE_INTERVAL: Duration = Duration::from_millis(500);

/// Build the node-local hosting for a grain type (spec §7.4, Appendix A): the
/// durable store, replica store, shard map, and gateway, returning the [`Granary`]
/// handle. Shared by [`granary_named`](GranaryExt::granary_named) (no alarm index)
/// and [`granary_named_with_alarms`](GranaryExt::granary_named_with_alarms) (which
/// threads one in), so the two differ only by the `alarm_index` a host receives.
fn build_granary<S, G>(
    system: &S,
    grain_type: &'static str,
    config: GranaryConfig,
    factory: Arc<dyn Fn() -> G + Send + Sync>,
    alarm_index: Option<Granary<AlarmIndex<S>>>,
) -> Granary<G>
where
    S: GranarySystem,
    G: Grain<System = S>,
{
    let shards = config.shards.max(1);
    let replicas = config.replication_factor.max(1);
    // This node's durable grain store (§7.4): the injected factory if a deployment
    // supplied one (so records survive a restart), else a fresh ephemeral in-memory
    // store. The replica-store actor makes it reachable from a shard leader's
    // replicator (§7.2), registered under one key per type like the gateway (§5.3);
    // the transport reaches the peers' stores by `ask`.
    let store: Arc<dyn GrainStore> = match &config.grain_store {
        Some(factory) => factory(system.node()),
        None => Arc::new(MemoryGrainStore::new()),
    };
    let replica_store = system.spawn(ReplicaStore::<G>::new(Arc::clone(&store)));
    system
        .receptionist()
        .register(replica_store_key::<G>(grain_type), &replica_store);
    let transport: Arc<dyn ReplicaTransport> =
        Arc::new(ActorReplicaTransport::<G>::new(system.clone(), grain_type));
    // Build the consensus-agreed shard map (§7.6): a per-type Raft group whose
    // committed log is the allocation, so every node agrees on each shard's replica
    // set and only the replicas store it. The `Local` tier is a trivial single-node
    // map. Keyed by the runtime `grain_type`, so two type names get separate maps.
    let shard_map = system.shard_map(grain_type, shards, replicas, store, transport);
    let gateway = system.spawn(Gateway::new(
        grain_type,
        Arc::clone(&shard_map),
        shards,
        config,
        factory,
        alarm_index,
    ));
    // Register this node's gateway under the type's well-known key so other nodes
    // route activations to it (§5.3), giving one gateway entry per node.
    system
        .receptionist()
        .register(gateway_key::<G>(grain_type), &gateway);
    Granary {
        system: system.clone(),
        grain_type,
        gateway,
        shards,
        shard_map,
        cache: HostCache::new(system.clone(), grain_type, shards),
    }
}

/// The per-type alarm driver (spec §16): the callerless-activation seam that makes a
/// durable alarm fire across a grain's hibernation and a node failover, not only
/// while it is resident.
///
/// It sweeps on a fixed cadence: for every shard **this node leads**, it reads that
/// shard's alarm index for grains whose deadline has passed, and re-activates each by
/// `subscribe` (a framework built-in every grain accepts, so the driver stays generic
/// over `G`). A re-activated grain runs `on_activate`, which re-arms its own timer
/// and fires immediately for a past deadline (**G3**). The index is a hint — the
/// grain's alarm facet is the source of truth — so re-activating a grain whose alarm
/// already cleared is harmless: it simply re-registers the correct state and the
/// index entry is dropped. Only the leader activates (§5.4), so sweeping led shards
/// is exactly the set this node may act on.
async fn alarm_driver_loop<S, G>(
    system: S,
    granary: Granary<G>,
    index: Granary<AlarmIndex<S>>,
    grain_type: &'static str,
    shards: usize,
) where
    S: GranarySystem,
    G: Grain<System = S>,
{
    loop {
        system.sleep(ALARM_DRIVE_INTERVAL).await;
        let now = system.now().as_nanos();
        for shard in 0..shards {
            let id = ShardId {
                grain_type,
                index: shard as u32,
            };
            if !system.leads_shard(id) {
                continue;
            }
            let key = index_key(grain_type, shard);
            let due = match index.grain(key).ask(DueBefore { before: now }).await {
                Ok(names) => names,
                Err(_) => continue, // index shard unavailable this tick; retry next sweep
            };
            for name in due {
                // Re-activate the grain on this leader; its own timer then fires the
                // due alarm. Drop the subscription immediately — activation is the
                // only effect we want.
                let _ = granary
                    .grain(name.key().to_string())
                    .subscribe(Seq::ZERO)
                    .await;
            }
        }
    }
}
