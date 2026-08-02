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
use crate::alarm_index::Sync as AlarmSync;
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
use crate::shardmap::resolve_shard;
use crate::store::GrainStore;
use crate::store::MemoryGrainStore;
use crate::subscription::CloseSink;
use crate::subscription::RecordSink;
use crate::subscription::SUB_BUFFER;
use crate::subscription::Subscribe;
use crate::subscription::Subscription;
use crate::system::GranarySystem;
use crate::system::ShardId;

/// The default deadline applied to [`GrainRef::ask`] (mirrors the actor `ask`).
const DEFAULT_ASK_TIMEOUT: Duration = Duration::from_secs(5);

/// How long the client-side redirect waits between resolution attempts while a
/// shard has no reachable leader (§5.4). Short relative to an election timeout, so
/// convergence is observed promptly.
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

/// The cached handles, in two generations so the cache stays **bounded** without an
/// intrusive recency list (spec §5.4).
///
/// Bounding it is not housekeeping. A `GrainRef` is handed out per name, and the
/// gateway is one long-lived process fronting every tenant, so an unbounded map grows
/// with the number of *distinct names the process has ever addressed* — unbounded in
/// exactly the deployment that is scaled horizontally and expected to stay up. Worse,
/// most of what accumulates is dead weight: a grain hibernates after `idle_after`
/// (§10) and its host stops, but the handle to it lingers until some later call fails
/// against it.
///
/// A hit is served from either generation and promotes into `hot`; when `hot` fills,
/// it becomes `cold` and a fresh `hot` starts, dropping whatever `cold` held. That
/// keeps the whole structure to `2 × capacity` entries with O(1) work per operation
/// and no per-entry bookkeeping. It approximates LRU rather than implementing it —
/// which is the right trade here, because a miss is not an error: it costs one
/// gateway round-trip, the path §5.4 specifies anyway.
struct Generations<V: Clone> {
    hot: HashMap<GrainName, V>,
    cold: HashMap<GrainName, V>,
    capacity: usize,
}

/// The generation a rotation retired, handed back to the caller **to drop**.
///
/// Freeing it is `capacity` deallocations — tens of thousands at the default — and the
/// lock this map sits behind is taken on every grain dispatch, so the free must not
/// happen under it. Returning it rather than dropping it in place is what moves that
/// work outside the guard.
type Displaced<V> = Option<HashMap<GrainName, V>>;

impl<V: Clone> Generations<V> {
    fn new(capacity: usize) -> Generations<V> {
        Generations {
            hot: HashMap::new(),
            cold: HashMap::new(),
            // A zero capacity would make every insert evict and every lookup miss,
            // turning the fast path off entirely; one entry is the smallest cache
            // that is still a cache.
            capacity: capacity.max(1),
        }
    }

    fn get(&mut self, name: &GrainName) -> (Option<V>, Displaced<V>) {
        if let Some(host) = self.hot.get(name) {
            return (Some(host.clone()), None);
        }
        // A hit in the older generation is still a live handle: promote it so a name
        // in steady use is never dropped just because it was cached a while ago.
        let Some(host) = self.cold.remove(name) else {
            return (None, None);
        };
        let displaced = self.insert(name.clone(), host.clone());
        (Some(host), displaced)
    }

    fn insert(&mut self, name: GrainName, host: V) -> Displaced<V> {
        let displaced = if self.hot.len() >= self.capacity && !self.hot.contains_key(&name) {
            Some(std::mem::replace(
                &mut self.cold,
                std::mem::take(&mut self.hot),
            ))
        } else {
            None
        };
        self.hot.insert(name, host);
        displaced
    }

    fn remove(&mut self, name: &GrainName) {
        self.hot.remove(name);
        self.cold.remove(name);
    }

    fn contains(&self, name: &GrainName) -> bool {
        self.hot.contains_key(name) || self.cold.contains_key(name)
    }
}

/// A node-local cache of resolved host handles, shared by every [`GrainRef`] a
/// [`Granary`] hands out (spec §5.4). A cache hit lets a call go **straight to the
/// host actor**, skipping the serial gateway on the steady-state hot path.
///
/// A cached handle is returned only after a cheap, local check that its node still
/// leads the grain's shard ([`HostCache::get`]). This is the pre-send guard that
/// keeps a cache hit from dispatching to a **deposed** leader: a write to a crashed
/// leader can time out, and a timeout is not safe to auto-retry (the command may
/// have committed — at-most-once, §6, §2.2).
///
/// Bounded by [`Generations`], so a long-lived process that addresses many distinct
/// names does not grow one entry per name for its whole lifetime.
pub(crate) struct HostCache<G: Grain> {
    system: G::System,
    grain_type: &'static str,
    /// The founding shard count — the [`resolve_shard`] fallback while the map
    /// bootstraps (and the only resolution a routing-only client ever has).
    shards: usize,
    /// The committed partition (§5.1), the authority on name→shard once ranges
    /// have committed (a split/merge moves names between shards).
    shard_map: Arc<dyn ShardMapSource>,
    hosts: Mutex<Generations<ActorRef<Host<G>>>>,
}

impl<G: Grain> HostCache<G> {
    fn new(
        system: G::System,
        grain_type: &'static str,
        shards: usize,
        shard_map: Arc<dyn ShardMapSource>,
        capacity: usize,
    ) -> Arc<HostCache<G>> {
        Arc::new(HostCache {
            system,
            grain_type,
            shards,
            shard_map,
            hosts: Mutex::new(Generations::new(capacity)),
        })
    }

    /// A cached host for `name`. On a node that **replicates** the name's shard the
    /// current leader is known locally, so a handle that no longer sits on it is
    /// proactively dropped (§5.4). A node that does **not** replicate the shard
    /// cannot know the leader, so it returns the cached handle and relies on
    /// reactive invalidation (a `NotLeader`/`DeadLetter`/`Unreachable` outcome drops
    /// it and re-resolves). The leadership read is a local lock read, off the
    /// network and off the control plane (invariant **G9**).
    fn get(&self, name: &GrainName) -> Option<ActorRef<Host<G>>> {
        let (found, displaced) = {
            let mut hosts = self.hosts.lock().expect("host cache mutex poisoned");
            let (host, displaced) = hosts.get(name);
            let found = host.filter(|host| {
                match self.system.shard_leader(resolve_shard(
                    self.shard_map.as_ref(),
                    self.grain_type,
                    name.key(),
                    self.shards,
                )) {
                    Some(leader) if host.id().node() != leader => {
                        hosts.remove(name);
                        false
                    }
                    _ => true,
                }
            });
            (found, displaced)
        };
        drop(displaced); // outside the guard, see `Displaced`
        found
    }

    fn put(&self, name: GrainName, host: ActorRef<Host<G>>) {
        // The guard is a temporary of this statement, so it is released before
        // `displaced` is dropped at the end of the next one (see `Displaced`).
        let displaced = self
            .hosts
            .lock()
            .expect("host cache mutex poisoned")
            .insert(name, host);
        drop(displaced);
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
            .contains(name)
    }
}

/// The only handle to a grain (spec §4.3): it carries the [`GrainName`] and a
/// handle to the grain type's gateway, and **never** grants access to state.
///
/// The `G: GrainHandler<M>` bound on `ask`/`tell` proves at compile time that the
/// grain accepts `M` (invariant **G10**).
///
/// It also carries an optional, **non-serialized** [`HostCache`] (present when
/// obtained from a [`Granary`] on this node): repeated calls hit the cache and go
/// straight to the host, off the gateway (§5.4). A `GrainRef` rebuilt from the
/// wire, or a self-reference from [`GrainCtx`](crate::GrainCtx), has no cache and
/// resolves through the gateway each call — correct, just not cached.
///
/// The system handle drives the **client-side bounded redirect** (§5.4 step 4):
/// it follows `NotLeader(hint)` toward the shard's leader until the caller's
/// deadline expires. Never serialized; a wire-arrived ref recovers it from its
/// decoded `gateway` handle, which rebinds to the local system on decode (§4.4).
pub struct GrainRef<G: Grain> {
    /// The runtime type name (spec §5.1) this ref routes under, used to look up
    /// the leader's gateway in the receptionist. A ref decoded off the wire
    /// recovers it as `G::GRAIN_TYPE` (see the `Deserialize` note).
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
// embedded in a message is usable on the node that receives it (§4.4).
// `grain_type` recovers as `G::GRAIN_TYPE`: the receptionist `Key` needs a
// `&'static str` and the wire carries only the name's owned string, so this is the
// one path that cannot honor a runtime type name. A type hosted under
// `granary_named` MUST therefore re-mint its refs locally from its `Granary`
// handle rather than ship them over the wire.
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
    /// (§8). The command did not commit in that case (§6), so re-issuing is safe.
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
        // Only a `DeadLetter` is re-enqueued; an ambiguous failure is surfaced, not
        // retried, since a re-enqueue could double-apply (§2.2). Fire-and-forget
        // never reports `Unavailable` (§6), so the budget is left unguarded.
        let deadline = self.system.now() + DEFAULT_ASK_TIMEOUT;
        let retry = msg.clone();
        self.resolve_twice(
            deadline,
            false,
            async |host| match host.tell(RunTyped(msg)).await {
                Ok(()) => Attempt::Done(Ok(())),
                Err(call) if is_retriable(&call) => Attempt::Retry,
                Err(call) => Attempt::Done(Err(GrainError::Call(call))),
            },
            async |host| host.tell(RunTyped(retry)).await.map_err(GrainError::Call),
        )
        .await
    }

    /// Subscribe to the grain's committed records (spec §7.9), returning the
    /// committed `head` with the live stream. A framework built-in, available for
    /// every grain type without a `GrainHandler` bound — the push analogue of
    /// `load`/`head`.
    ///
    /// Delivery is best-effort; the caller MUST reconcile by `Seq` (§7.9,
    /// **G16**): backfill `from`..`head` by reading the journal, then on each
    /// batch close any gap and ignore anything already seen. When the stream
    /// closes — a move, a lag-drop, or hibernation — re-subscribe and backfill
    /// from the last seq.
    pub async fn subscribe(&self, from: Seq) -> Result<Subscription<G>, GrainError> {
        // Resolve BEFORE spawning the sink: a failed resolution must not leave an
        // orphan actor behind — the framework does not reap an actor merely because
        // every external ref to it was dropped.
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
    /// otherwise this drives the **client-side bounded redirect** (§5.4 step 4):
    /// the single-shot gateway answers `Ok(host)` if its node leads the shard, else
    /// `NotLeader(hint)`, which this follows to that node's gateway, backing off
    /// while the shard elects, until the leader is found or `deadline` expires. A
    /// transient miss is waited out rather than surfaced, so a remote call stays
    /// observably identical to a local one across a failover (invariant **G13**).
    /// Driving the loop here, not in the gateway's serial handler, keeps one slow
    /// resolution from blocking another grain's activation on that node.
    ///
    /// Bounded by the **caller's** `deadline`, not a per-attempt window, so a
    /// dispatch that resolves, fails, and re-resolves still returns within the one
    /// budget (§5.4).
    async fn resolve(
        &self,
        use_cache: bool,
        deadline: actor_core::Instant,
    ) -> Result<ActorRef<Host<G>>, GrainError> {
        if use_cache && let Some(host) = self.cache.as_ref().and_then(|cache| cache.get(&self.name))
        {
            return Ok(host);
        }
        // Start at this ref's own gateway: local for a `Granary`-minted ref, the
        // source node's for a wire-arrived one. Hints and re-discovery move it
        // toward the leader.
        let mut target = self.gateway.clone();
        for _ in 0..RESOLVE_ATTEMPTS {
            let remaining = deadline.duration_since(self.system.now());
            if remaining.is_zero() {
                break;
            }
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
                // Follow the leader hint. If that gateway is not (yet) discoverable,
                // keep the current target and back off — the shard is still electing
                // or the gateway has not gossiped in.
                Ok(Err(GrainError::NotLeader(hint))) => {
                    if let Some(gateway) = self.gateway_on(hint) {
                        target = gateway;
                    }
                }
                // A genuine durability outcome (quorum loss / unhandled): surface it.
                Ok(Err(other)) => return Err(other),
                // The target gateway is unreachable (its node crashed): re-discover a
                // gateway to redirect from, excluding the one that just failed.
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
    /// target (§5.4) without re-selecting the very gateway that just timed out,
    /// whose registration may outlive the crash until the membership prunes it.
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

    fn invalidate(&self) {
        if let Some(cache) = &self.cache {
            cache.remove(&self.name);
        }
    }

    /// Resolve the host and send the typed command, held until durable (§6). The
    /// first attempt prefers the cache; if that host turns out unusable, the cache
    /// entry is dropped and the command re-issued — but **only when the first
    /// attempt provably did not run** (a `NotLeader`, which never commits, §8; or a
    /// `DeadLetter` from a hibernated host, §10). An ambiguous transport failure is
    /// surfaced, never auto-retried (see [`is_retriable`]).
    async fn dispatch<M>(&self, msg: M, within: Duration) -> Result<M::Reply, GrainError>
    where
        G: GrainHandler<M>,
        M: Message + Clone,
    {
        // ONE deadline bounds the whole call — resolution and ask, across both
        // attempts — so the retry path never restarts the caller's budget (a
        // per-step `within` could stack up to ~4× the declared timeout).
        let deadline = self.system.now() + within;
        let retry = msg.clone();
        self.resolve_twice(
            deadline,
            true,
            async |host| {
                let remaining = deadline.duration_since(self.system.now());
                match host.ask_timeout(RunTyped(msg), remaining).await {
                    // The host's reply is itself `Result<M::Reply, GrainError>`.
                    Ok(Ok(reply)) => Attempt::Done(Ok(reply)),
                    // Leadership moved off the cached host (§8): refresh and retry.
                    Ok(Err(GrainError::NotLeader(_))) => Attempt::Retry,
                    // A genuine durability outcome (quorum loss / unhandled): terminal.
                    Ok(Err(other)) => Attempt::Done(Err(other)),
                    // The command provably never reached a handler (§10): re-issue.
                    Err(call) if is_retriable(&call) => Attempt::Retry,
                    // Ambiguous or terminal: never auto-retry a command that may
                    // have committed (§2.2).
                    Err(call) => Attempt::Done(Err(GrainError::Call(call))),
                }
            },
            async |host| {
                let remaining = deadline.duration_since(self.system.now());
                match host.ask_timeout(RunTyped(retry), remaining).await {
                    Ok(result) => result,
                    Err(call) => Err(GrainError::Call(call)),
                }
            },
        )
        .await
    }

    /// The two-attempt resolve-retry both [`tell`](Self::tell) and
    /// [`dispatch`](Self::dispatch) layer over the [`resolve`](Self::resolve)
    /// redirect engine. Resolve cache-preferring and run `first` under the single
    /// `deadline`; on [`Attempt::Retry`] — the command provably never ran (§8,
    /// §10) — drop the stale cache entry, resolve cache-bypassing, and run `again`,
    /// which is terminal. Never a third attempt, so an effectful command that may
    /// have committed is surfaced, never re-issued (at-most-once, §2.2).
    ///
    /// `guard_budget` returns [`Unavailable`](GrainError::Unavailable) when the
    /// deadline is spent before the retry; fire-and-forget must never report
    /// `Unavailable` (§6), so it passes `false`.
    async fn resolve_twice<T>(
        &self,
        deadline: actor_core::Instant,
        guard_budget: bool,
        first: impl AsyncFnOnce(&ActorRef<Host<G>>) -> Attempt<T>,
        again: impl AsyncFnOnce(&ActorRef<Host<G>>) -> Result<T, GrainError>,
    ) -> Result<T, GrainError> {
        let host = self.resolve(true, deadline).await?;
        if let Attempt::Done(result) = first(&host).await {
            return result;
        }
        self.invalidate();
        if guard_budget && deadline.duration_since(self.system.now()).is_zero() {
            return Err(GrainError::Unavailable(
                "deadline exhausted before the retry".into(),
            ));
        }
        let host = self.resolve(false, deadline).await?;
        again(&host).await
    }
}

/// The outcome of one attempt in [`GrainRef::resolve_twice`]: a terminal `Done`
/// result, or `Retry` — the attempt provably did not run (§2.2), so re-resolve
/// and re-issue once.
enum Attempt<T> {
    Done(Result<T, GrainError>),
    Retry,
}

/// Whether a transport failure proves the command **never ran**, so re-issuing it
/// against a fresh resolution cannot double-apply (at-most-once, §2.2; reply-iff-
/// durable, §6/G5). Only `DeadLetter` qualifies: the cached host had hibernated
/// and stopped, so the command dead-lettered without reaching a handler (the §10
/// eviction race). `Unreachable`/`Timeout` are **ambiguous** — the command may
/// have committed on a leader that then crashed before replying — so they are NOT
/// auto-retried for an effectful command; they surface to the caller, who makes
/// the operation idempotent where a retry matters (§2.2).
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
    /// The founding shard count (§7.1), the [`resolve_shard`] fallback.
    shards: usize,
    /// The consensus-agreed shard map (§7.6), read by [`Granary::replicas`].
    shard_map: Arc<dyn ShardMapSource>,
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
    /// handle's host cache (§5.4).
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
        self.system.shard_leader(resolve_shard(
            self.shard_map.as_ref(),
            self.grain_type,
            name.key(),
            self.shards,
        ))
    }

    /// Whether a live host handle is currently cached for `key` (spec §5.4). An
    /// observability detail, not a statement about the grain's activation on its
    /// leader.
    pub fn is_cached(&self, key: impl Into<String>) -> bool {
        self.cache.contains(&GrainName::new(self.grain_type, key))
    }

    /// Request a split of shard `index` at its range midpoint (spec §7.7) — the
    /// admin/test seam over [`ShardMapSource::request_split`]. Best-effort and
    /// asynchronous: a shard that is unknown, mid-migration, mid-split, or a
    /// single-point range is silently skipped, and the committed flip is
    /// observable as a [`ShardSplit`](crate::GrainEvent::ShardSplit) event and
    /// changed routing. A no-op on the `Local` tier and on a routing-only client.
    pub fn split_shard(&self, index: u32) {
        self.shard_map.request_split(index);
    }

    /// Request a merge of shard `left` with its right neighbour (spec §7.7) —
    /// the mirror of [`split_shard`](Granary::split_shard), reclaiming a
    /// leader-election group (G7) when two adjacent ranges are cold. Best-effort
    /// and asynchronous on the same terms; unknown, non-adjacent, or busy shards
    /// are skipped, and the committed flip is observable as a
    /// [`ShardMerged`](crate::GrainEvent::ShardMerged) event.
    pub fn merge_shards(&self, left: u32) {
        self.shard_map.request_merge(left);
    }

    /// Hand every shard this node leads to another of its replicas before the node
    /// departs (spec §8.3), returning how many were still led when the attempt gave
    /// up — `0` when they all moved.
    ///
    /// Call this from a graceful shutdown, after the node stops accepting new work
    /// and before the process exits. Skipping it is *safe* but expensive: each shard
    /// this node led then waits a full leader-election timeout before its replicas
    /// elect, and every grain on those shards rehydrates on the new leader. A node
    /// leading many shards therefore turns an ordinary rolling restart into that many
    /// simultaneous failovers, which is the single most common way a healthy cluster
    /// is made to look unhealthy.
    ///
    /// Best-effort: a shard whose other replicas are all lagging or unreachable keeps
    /// its leader here and fails over the slow way. A non-zero return is information
    /// for the operator, not an error to retry — the node is leaving regardless.
    ///
    /// A no-op on the `Local` tier (there are no other replicas) and on a
    /// routing-only client (it leads nothing).
    pub async fn hand_off_leadership(&self) -> usize {
        self.shard_map.hand_off_leadership().await
    }

    /// The nodes that replicate the shard a grain key maps to (spec §7.6) — the
    /// only nodes that hold its data and can lead it. Read live from the
    /// consensus-agreed shard map, so it reflects the latest committed allocation,
    /// not a `granary()`-time snapshot.
    pub fn replicas(&self, key: impl Into<String>) -> Vec<NodeId> {
        let name = GrainName::new(self.grain_type, key);
        let index = resolve_shard(
            self.shard_map.as_ref(),
            self.grain_type,
            name.key(),
            self.shards,
        )
        .index;
        self.shard_map.replicas(index).unwrap_or_default()
    }
}

/// Host grains of a type on a system (spec Appendix A). Implemented for every
/// [`GranarySystem`], so `system.granary::<G>(config)` starts the gateway and
/// returns a [`Granary`] handle.
pub trait GranaryExt: GranarySystem {
    /// Start hosting grains of type `G` under its own `G::GRAIN_TYPE` (spec
    /// Appendix A): spawn the type's gateway and return the handle. Each
    /// activation's behavior is built by `G::default`. The common case — one Rust
    /// type, one grain type.
    fn granary<G>(&self, config: GranaryConfig) -> Granary<G>
    where
        G: Grain<System = Self> + Default,
    {
        self.granary_named(G::GRAIN_TYPE, config, Arc::new(G::default))
    }

    /// Address grains of type `G` as a routing-only **client**: it hosts
    /// **nothing** — no gateway, replica store, or shard-map group is started. The
    /// handle routes through a *host's* gateway, discovered in the receptionist
    /// (§5.3) and seeded here; `GrainRef`'s bounded redirect re-discovers a live
    /// gateway on failover, so the seed only has to be reachable once.
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
        let shard_map: Arc<dyn ShardMapSource> = Arc::new(EmptyShardMap);
        Some(Granary {
            system: self.clone(),
            grain_type,
            gateway,
            shards,
            shard_map: Arc::clone(&shard_map),
            // A routing-only client has no `GranaryConfig` to read, so it takes the
            // default bound. This is the path that most needs one: the gateway is a
            // client, long-lived, and addresses every tenant's names.
            cache: HostCache::new(
                self.clone(),
                grain_type,
                shards,
                shard_map,
                GranaryConfig::default().host_cache_capacity,
            ),
        })
    }

    /// Host grains of type `G` under an explicit runtime **type name** (spec
    /// §5.1), with a caller-supplied **factory** for each activation's behavior.
    ///
    /// Two extension points over [`granary`](GranaryExt::granary), both for one
    /// Rust grain that must be **many** grain types at runtime:
    ///
    /// - `grain_type` overrides `G::GRAIN_TYPE`, so the same `G` hosts under
    ///   several names — distinct gateways, shard maps, and consensus groups. It
    ///   MUST be stable cluster-wide and across runs, exactly as `G::GRAIN_TYPE`
    ///   must be (§5.1); a `&'static str` makes that lifetime explicit.
    /// - `factory` replaces `G::default`, so the runtime can inject per-node seam
    ///   handles into each fresh activation (the grain needs no `Default`).
    fn granary_named<G>(
        &self,
        grain_type: &'static str,
        config: GranaryConfig,
        factory: Arc<dyn Fn() -> G + Send + Sync>,
    ) -> Granary<G>
    where
        G: Grain<System = Self>;

    /// Host grains of type `G` **with durable-alarm firing across hibernation and
    /// failover** (spec §7.16). Like [`granary_named`](GranaryExt::granary_named), but
    /// each host registers its pending [`Alarm`](crate::Alarm) deadline with the
    /// per-shard `index`, and a background driver re-activates due grains on the
    /// shards this node leads. The caller starts **one** shared `AlarmIndex`
    /// granary and passes its handle to every alarm-bearing type; a type without
    /// the [`Alarm`](crate::Alarm) facet gains nothing from wiring it.
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
        // Start this type's alarm driver (spec §7.16): the callerless-activation seam.
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

/// How often the alarm driver sweeps the shards it leads (spec §7.16). The exact
/// deadline is honoured by the grain's own in-activation timer once re-activated;
/// this cadence bounds only the *re-activation* latency after a failover.
const ALARM_DRIVE_INTERVAL: Duration = Duration::from_millis(500);

/// Build the node-local hosting for a grain type (spec §7.4, Appendix A): the
/// durable store, replica store, shard map, and gateway. Shared by
/// [`granary_named`](GranaryExt::granary_named) and
/// [`granary_named_with_alarms`](GranaryExt::granary_named_with_alarms), which
/// differ only by the `alarm_index` a host receives.
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
    // replicator (§7.2), registered under one key per type like the gateway (§5.3).
    let store: Arc<dyn GrainStore> = match &config.grain_store {
        // Per `(grain_type, node)`, never per node alone: the store's fence is keyed by
        // shard index and holds this type's leader-election term, which another type's
        // term would fence out of its own shards (§8.2, [`GrainStoreFactory`]).
        Some(factory) => factory(grain_type, system.node()),
        None => Arc::new(MemoryGrainStore::new()),
    };
    let io = config.blocking_io();
    let replica_store = system.spawn(ReplicaStore::<G>::new(Arc::clone(&store), Arc::clone(&io)));
    system
        .receptionist()
        .register(replica_store_key::<G>(grain_type), &replica_store);
    let transport: Arc<dyn ReplicaTransport> =
        Arc::new(ActorReplicaTransport::<G>::new(system.clone(), grain_type));
    // Build the consensus-agreed shard map (§7.6): a per-type Raft group whose
    // committed log is the allocation, so every node agrees on each shard's replica
    // set and only the replicas store it. Keyed by the runtime `grain_type`, so two
    // type names get separate maps.
    let shard_map = system.shard_map(
        grain_type,
        shards,
        replicas,
        config.shard_target_bytes,
        store,
        transport,
        Arc::clone(&io),
        crate::replicator::Deadlines {
            quorum: config.quorum_timeout,
            recover: config.recover_timeout,
        },
        config.failure_domains.clone(),
    );
    // Read before `config` moves into the gateway.
    let host_cache_capacity = config.host_cache_capacity;
    let gateway = system.spawn(Gateway::new(
        grain_type,
        Arc::clone(&shard_map),
        shards,
        config,
        factory,
        alarm_index,
    ));
    // Register this node's gateway under the type's well-known key so other nodes
    // route activations to it (§5.3).
    system
        .receptionist()
        .register(gateway_key::<G>(grain_type), &gateway);
    Granary {
        system: system.clone(),
        grain_type,
        gateway,
        shards,
        shard_map: Arc::clone(&shard_map),
        cache: HostCache::new(
            system.clone(),
            grain_type,
            shards,
            shard_map,
            host_cache_capacity,
        ),
    }
}

/// The per-type alarm driver (spec §7.16): the callerless-activation seam that makes a
/// durable alarm fire across a grain's hibernation and a node failover, not only
/// while it is resident.
///
/// It sweeps on a fixed cadence: for every shard **this node leads**, it reads that
/// shard's alarm index for grains whose deadline has passed, and re-activates each by
/// `subscribe` (a framework built-in every grain accepts, so the driver stays generic
/// over `G`). A re-activated grain runs `on_activate`, which re-arms its own timer
/// and fires immediately for a past deadline (**G3**). The index is a hint — the
/// grain's alarm facet is the source of truth — so re-activating a grain whose alarm
/// already cleared is harmless.
///
/// A split or merge (§7.7) moves a grain's key range to another shard, leaving a
/// stale entry in the old shard's index. Re-activating a due grain routes to its
/// **live** owner, so the alarm still fires and the grain re-registers into its new
/// shard's index there. The driver clears the stale entry in the old index only
/// *after* that successful re-activation, so it never orphans a wake.
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
        // Sweep the committed partition's shards — a split/merge (§7.7) changes
        // the shard set, and each shard has its own index grain. While the map is
        // still bootstrapping, fall back to the founding indices.
        let mut indices = granary.shard_map.shard_indices();
        if indices.is_empty() {
            indices = (0..shards as u32).collect();
        }
        for shard in indices {
            let id = ShardId {
                grain_type,
                index: shard,
            };
            if !system.leads_shard(id) {
                continue;
            }
            let key = index_key(grain_type, shard as usize);
            let due = match index
                .grain(key.clone())
                .ask(DueBefore { before: now })
                .await
            {
                Ok(names) => names,
                Err(_) => continue, // index shard unavailable this tick; retry next sweep
            };
            for name in due {
                // Re-activate the grain: routing sends it to its live owner, where
                // `on_activate` re-arms and fires the due alarm. The subscription
                // is dropped immediately; activation is the only effect we want.
                let reactivated = granary
                    .grain(name.key().to_string())
                    .subscribe(Seq::ZERO)
                    .await
                    .is_ok();
                // If the grain has moved off this shard (§7.7), the re-activation
                // above seeded its new shard's index, so clear the stale entry here
                // — with a max head, so the clear always wins. Only after a
                // successful re-activation, so a transient failure never orphans
                // the wake.
                let live = resolve_shard(
                    granary.shard_map.as_ref(),
                    grain_type,
                    name.key(),
                    granary.shards,
                );
                if reactivated && live.index != shard {
                    let _ = index
                        .grain(key.clone())
                        .ask(AlarmSync {
                            grain: name.clone(),
                            due: None,
                            head: u64::MAX,
                        })
                        .await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(key: &str) -> GrainName {
        GrainName::new("test.Grain", key)
    }

    /// The property the bound exists for: addressing an unbounded stream of distinct
    /// names must not grow the cache without limit. Before this, the gateway — one
    /// long-lived process fronting every tenant — kept an entry per name it had ever
    /// addressed, most of them stale handles to grains that had since hibernated.
    #[test]
    fn the_cache_stays_bounded_across_unboundedly_many_names() {
        let mut cache: Generations<u32> = Generations::new(16);
        for i in 0..10_000 {
            cache.insert(name(&format!("grain-{i}")), i);
        }
        assert!(
            cache.hot.len() + cache.cold.len() <= 32,
            "two generations of {} cap the cache at 2x, not {} entries",
            16,
            cache.hot.len() + cache.cold.len()
        );
    }

    /// A name in steady use must survive eviction, or the bound would trade a memory
    /// leak for a permanently cold hot path.
    #[test]
    fn a_name_in_steady_use_is_retained() {
        let mut cache: Generations<u32> = Generations::new(4);
        let hot = name("hot");
        cache.insert(hot.clone(), 1);
        for i in 0..100 {
            cache.insert(name(&format!("other-{i}")), i);
            // Touching it each round promotes it back into the young generation.
            assert_eq!(
                cache.get(&hot).0,
                Some(1),
                "a touched name is never dropped"
            );
        }
    }

    /// An untouched name is eventually dropped — the eviction actually happens rather
    /// than the cache silently growing.
    #[test]
    fn an_untouched_name_is_evicted() {
        let mut cache: Generations<u32> = Generations::new(2);
        cache.insert(name("cold"), 1);
        for i in 0..10 {
            cache.insert(name(&format!("other-{i}")), i);
        }
        assert_eq!(cache.get(&name("cold")).0, None);
    }

    /// Invalidation must clear both generations: a handle left in the older one would
    /// come back on the next lookup, which is exactly the deposed-leader dispatch the
    /// invalidation exists to prevent (§5.4).
    #[test]
    fn removal_clears_both_generations() {
        let mut cache: Generations<u32> = Generations::new(1);
        let stale = name("stale");
        cache.insert(stale.clone(), 1);
        cache.insert(name("push"), 2); // ages `stale` into `cold`
        cache.remove(&stale);
        assert_eq!(cache.get(&stale).0, None);
        assert!(!cache.contains(&stale));
    }
}
