//! `EntityRef`, the `Entities` handle, and the system extension (spec §3.2, §5.4).

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
use crate::config::EntityConfig;
use crate::entity::Entity;
use crate::entity::EntityHandler;
use crate::entity::EntityName;
use crate::entity::EventHistory;
use crate::error::EntityError;
use crate::gateway::Activate;
use crate::gateway::EventPage;
use crate::gateway::Gateway;
use crate::gateway::ReadEvents;
use crate::gateway::ReadReply;
use crate::gateway::gateway_key;
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
use crate::store::EntityStore;
use crate::store::MemoryEntityStore;
use crate::store::NodeStore;
use crate::subscription::CloseSink;
use crate::subscription::Follower;
use crate::subscription::RecordSink;
use crate::subscription::SUB_BUFFER;
use crate::subscription::Subscribe;
use crate::subscription::Subscription;
use crate::system::EntitySystem;
use crate::system::ShardId;

/// The default deadline applied to [`EntityRef::ask`] (mirrors the actor `ask`).
const DEFAULT_ASK_TIMEOUT: Duration = Duration::from_secs(5);

/// How long the client-side redirect waits between resolution attempts while a
/// shard has no reachable leader (§5.4). Short relative to an election timeout, so
/// convergence is observed promptly.
const RESOLVE_BACKOFF: Duration = Duration::from_millis(50);

/// Safety bound on the redirect loop; the real bound is the caller's deadline
/// (`within`), checked each attempt.
const RESOLVE_ATTEMPTS: usize = 200;

/// How long one resolution attempt waits on a gateway before treating it as
/// stale and re-resolving: short, so a hint pointing at a just-crashed leader
/// fails fast instead of stalling the redirect for the full deadline.
const FORWARD_TIMEOUT: Duration = Duration::from_millis(500);

/// Floor on the records one [`EntityRef::events`] page scans, whatever few
/// events the caller still wants: the wire `limit` bounds records *scanned*
/// (facet records among them are skipped, facets §4), so an unfloored want of one
/// or two events would cross a dense facet run one record per round trip and
/// exhaust the read's one deadline. Events scanned past the caller's `limit`
/// are dropped client-side, so the floor never overshoots the API contract.
const EVENT_SCAN_FLOOR: usize = 256;

/// The cached handles, in two generations so the cache stays **bounded** without an
/// intrusive recency list (spec §5.4).
///
/// An `EntityRef` is handed out per name, and the gateway is one long-lived process
/// fronting every tenant, so an unbounded map grows with the number of *distinct names
/// the process has ever addressed*: unbounded in exactly the deployment that is scaled
/// horizontally and expected to stay up. Most of what accumulates is dead weight: an
/// entity hibernates after `idle_after` (§8) and its host stops, but the handle to it
/// lingers until some later call fails against it.
///
/// A hit is served from either generation and promotes into `hot`; when `hot` fills,
/// it becomes `cold` and a fresh `hot` starts, dropping whatever `cold` held. That
/// keeps the whole structure to `2 × capacity` entries with O(1) work per operation
/// and no per-entry bookkeeping. It approximates LRU rather than implementing it,
/// because a miss is not an error: it costs one gateway round-trip, the path §5.4
/// specifies anyway.
struct Generations<V: Clone> {
    hot: HashMap<EntityName, V>,
    cold: HashMap<EntityName, V>,
    capacity: usize,
}

/// The generation a rotation retired, handed back to the caller **to drop**.
///
/// Freeing it is `capacity` deallocations (tens of thousands at the default), and the
/// lock this map sits behind is taken on every entity dispatch, so the free must not
/// happen under it. Returning it rather than dropping it in place is what moves that
/// work outside the guard.
type Displaced<V> = Option<HashMap<EntityName, V>>;

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

    fn get(&mut self, name: &EntityName) -> (Option<V>, Displaced<V>) {
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

    fn insert(&mut self, name: EntityName, host: V) -> Displaced<V> {
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

    fn remove(&mut self, name: &EntityName) {
        self.hot.remove(name);
        self.cold.remove(name);
    }

    fn contains(&self, name: &EntityName) -> bool {
        self.hot.contains_key(name) || self.cold.contains_key(name)
    }
}

/// A node-local cache of resolved host handles, shared by every [`EntityRef`] a
/// [`Entities`] hands out (spec §5.4). A cache hit lets a call go **straight to the
/// host actor**, skipping the serial gateway on the steady-state hot path.
///
/// A cached handle is returned only after a cheap, local check that its node still
/// leads the entity's shard ([`HostCache::get`]). This is the pre-send guard that
/// keeps a cache hit from dispatching to a **deposed** leader: a write to a crashed
/// leader can time out, and a timeout is not safe to auto-retry (the command may
/// have committed, at-most-once, §6, §1.1).
///
/// Bounded by [`Generations`], so a long-lived process that addresses many distinct
/// names does not grow one entry per name for its whole lifetime.
///
/// **One mutex, measured rather than assumed.** The map is taken on every call
/// including a hit, in a gateway that is one long-lived process fronting every tenant
/// on a many-core box (`docs/hardware-envelope.md` §3.4): the shape that invites
/// sharding it by name hash. `benches/contention.rs` sweeps concurrent lookups across
/// it, and aggregate throughput *falls* with concurrency, from 18.4 M lookups/s on one
/// thread to 8.6 M at sixteen: the signature of a lock that has become the serial
/// point rather than one that merely queues.
///
/// It is deliberately not sharded anyway, because the absolute number is the one that
/// decides. 8.6 M lookups/s is the floor under contention, and the deployment's
/// binding constraint is a 1 Gbps uplink (hw §1): a request that does one lookup and
/// then any real work cannot arrive within three orders of magnitude of that rate.
/// Sharding would buy throughput this deployment has no way to ask for, at the cost of
/// a partitioned generation policy. Revisit if a gateway is ever expected to sustain
/// millions of entity dispatches per second, or if a profile shows this lock held for
/// longer than a hash lookup.
pub(crate) struct HostCache<G: Entity> {
    system: G::System,
    entity_type: &'static str,
    /// The founding shard count: the [`resolve_shard`] fallback while the map
    /// bootstraps. `1` on a routing-only client, where no answer depends on the
    /// shard a name resolves to (see [`EntityExt::entities_client`]).
    shards: usize,
    /// The committed partition (§5.1), the authority on name→shard once ranges
    /// have committed (a split/merge moves names between shards).
    shard_map: Arc<dyn ShardMapSource>,
    hosts: Mutex<Generations<ActorRef<Host<G>>>>,
}

impl<G: Entity> HostCache<G> {
    fn new(
        system: G::System,
        entity_type: &'static str,
        shards: usize,
        shard_map: Arc<dyn ShardMapSource>,
        capacity: usize,
    ) -> Arc<HostCache<G>> {
        Arc::new(HostCache {
            system,
            entity_type,
            shards,
            shard_map,
            hosts: Mutex::new(Generations::new(capacity)),
        })
    }

    /// The shard `name` maps to: the map-first, founding-fallback resolution
    /// (§5.1) behind one method, so no caller re-assembles its inputs.
    pub(crate) fn shard_of(&self, name: &EntityName) -> ShardId {
        resolve_shard(
            self.shard_map.as_ref(),
            self.entity_type,
            name.key(),
            self.shards,
        )
    }

    /// A cached host for `name`. On a node that **replicates** the name's shard the
    /// current leader is known locally, so a handle that no longer sits on it is
    /// proactively dropped (§5.4). A node that does **not** replicate the shard
    /// cannot know the leader, so it returns the cached handle and relies on
    /// reactive invalidation (a `NotLeader`/`DeadLetter`/`Unreachable` outcome drops
    /// it and re-resolves). The leadership read is a local lock read, off the
    /// network and off the control plane (invariant **E9**).
    fn get(&self, name: &EntityName) -> Option<ActorRef<Host<G>>> {
        let (found, displaced) = {
            let mut hosts = self.hosts.lock().expect("host cache mutex poisoned");
            let (host, displaced) = hosts.get(name);
            let found = host.filter(|host| match self.system.shard_leader(self.shard_of(name)) {
                Some(leader) if host.id().node() != leader => {
                    hosts.remove(name);
                    false
                }
                _ => true,
            });
            (found, displaced)
        };
        drop(displaced); // outside the guard, see `Displaced`
        found
    }

    fn put(&self, name: EntityName, host: ActorRef<Host<G>>) {
        // The guard is a temporary of this statement, so it is released before
        // `displaced` is dropped at the end of the next one (see `Displaced`).
        let displaced = self
            .hosts
            .lock()
            .expect("host cache mutex poisoned")
            .insert(name, host);
        drop(displaced);
    }

    fn remove(&self, name: &EntityName) {
        self.hosts
            .lock()
            .expect("host cache mutex poisoned")
            .remove(name);
    }

    fn contains(&self, name: &EntityName) -> bool {
        self.hosts
            .lock()
            .expect("host cache mutex poisoned")
            .contains(name)
    }
}

/// The only handle to an entity (spec §3.2): it carries the [`EntityName`] and a
/// handle to the entity type's gateway, and **never** grants access to state.
///
/// The `G: EntityHandler<M>` bound on `ask`/`tell` proves at compile time that the
/// entity accepts `M` (invariant **E10**).
///
/// It also carries an optional, **non-serialized** [`HostCache`] (present when
/// obtained from an [`Entities`] on this node): repeated calls hit the cache and go
/// straight to the host, off the gateway (§5.4). An `EntityRef` rebuilt from the
/// wire, or a self-reference from [`EntityCtx`](crate::EntityCtx), has no cache and
/// resolves through the gateway each call: correct, just not cached.
///
/// The system handle drives the **client-side bounded redirect** (§5.4 step 4):
/// it follows `NotLeader(hint)` toward the shard's leader until the caller's
/// deadline expires. Never serialized; a wire-arrived ref recovers it from its
/// decoded `gateway` handle, which rebinds to the local system on decode (core §7).
pub struct EntityRef<G: Entity> {
    /// The runtime type name (spec §5.1) this ref routes under, used to look up
    /// the leader's gateway in the receptionist. A ref decoded off the wire
    /// recovers it as `G::ENTITY_TYPE` (see the `Deserialize` note).
    entity_type: &'static str,
    name: EntityName,
    gateway: ActorRef<Gateway<G>>,
    system: G::System,
    cache: Option<Arc<HostCache<G>>>,
}

// `EntityRef` travels as just its name and gateway id (the gateway rebinds on
// decode, core §7); the system handle and host cache are reconstructed locally.
impl<G: Entity> Serialize for EntityRef<G> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        (&self.name, &self.gateway).serialize(serializer)
    }
}

// Decoding recovers the local system from the rebound gateway ref, so a ref
// embedded in a message is usable on the node that receives it (core §7).
// `entity_type` recovers as `G::ENTITY_TYPE`: the receptionist `Key` needs a
// `&'static str` and the wire carries only the name's owned string, so this is the
// one path that cannot honor a runtime type name. A type hosted under
// `entities_named` MUST therefore re-mint its refs locally from its `Entities`
// handle rather than ship them over the wire.
impl<'de, G: Entity> Deserialize<'de> for EntityRef<G> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (name, gateway) = <(EntityName, ActorRef<Gateway<G>>)>::deserialize(deserializer)?;
        let system = gateway.system().clone();
        Ok(EntityRef {
            entity_type: G::ENTITY_TYPE,
            name,
            gateway,
            system,
            cache: None,
        })
    }
}

// Manual `Clone`: `G` itself need not be `Clone`.
impl<G: Entity> Clone for EntityRef<G> {
    fn clone(&self) -> Self {
        EntityRef {
            entity_type: self.entity_type,
            name: self.name.clone(),
            gateway: self.gateway.clone(),
            system: self.system.clone(),
            cache: self.cache.clone(),
        }
    }
}

impl<G: Entity> EntityRef<G> {
    pub(crate) fn new(
        entity_type: &'static str,
        name: EntityName,
        gateway: ActorRef<Gateway<G>>,
        system: G::System,
        cache: Option<Arc<HostCache<G>>>,
    ) -> EntityRef<G> {
        EntityRef {
            entity_type,
            name,
            gateway,
            system,
            cache,
        }
    }

    /// This entity's name.
    pub fn name(&self) -> &EntityName {
        &self.name
    }

    /// Send a command and await its reply, held until the command's events are
    /// durable (the output gate, §6). The `G: EntityHandler<M>` bound makes an
    /// invalid call a compile error (**E10**).
    ///
    /// `M: Clone` so the runtime can re-issue the command if the first attempt
    /// hits a stale cached host: one that hibernated (§8) or whose leader moved
    /// (storage §6). The command did not commit in that case (§6), so re-issuing is safe.
    pub async fn ask<M>(&self, msg: M) -> Result<M::Reply, EntityError>
    where
        G: EntityHandler<M>,
        M: Message + Clone,
    {
        self.dispatch(msg, DEFAULT_ASK_TIMEOUT).await
    }

    /// [`ask`](Self::ask) with an explicit deadline.
    pub async fn ask_timeout<M>(&self, msg: M, within: Duration) -> Result<M::Reply, EntityError>
    where
        G: EntityHandler<M>,
        M: Message + Clone,
    {
        self.dispatch(msg, within).await
    }

    /// Fire-and-forget (spec §6): returns once the host accepts the command, not
    /// after the commit, so it reports only enqueue-time failures and never
    /// `Unavailable`. At-most-once: callers make it idempotent where it matters.
    pub async fn tell<M>(&self, msg: M) -> Result<(), EntityError>
    where
        G: EntityHandler<M>,
        M: Message + Clone,
    {
        // Only a `DeadLetter` is re-enqueued; an ambiguous failure is surfaced, not
        // retried, since a re-enqueue could double-apply (§1.1). Fire-and-forget
        // never reports `Unavailable` (§6), so the budget is left unguarded.
        let deadline = self.system.now() + DEFAULT_ASK_TIMEOUT;
        let retry = msg.clone();
        self.resolve_twice(
            deadline,
            false,
            async |host| match host.tell(RunTyped(msg)).await {
                Ok(()) => Attempt::Done(Ok(())),
                Err(call) if is_retriable(&call) => Attempt::Retry,
                Err(call) => Attempt::Done(Err(EntityError::Call(call))),
            },
            async |host| host.tell(RunTyped(retry)).await.map_err(EntityError::Call),
        )
        .await
    }

    /// Subscribe to the entity's committed records (spec §7.3), returning the
    /// committed `head` with the live stream. A framework built-in, available for
    /// every entity type without an `EntityHandler` bound: the push analogue of
    /// `load`/`head`.
    ///
    /// Delivery is best-effort; the caller MUST reconcile by `Seq` (§7.3,
    /// **E16**): backfill `from`..`head` by reading the journal, then on each
    /// batch close any gap and ignore anything already seen. When the stream
    /// closes (a move, a lag-drop, or hibernation) re-subscribe and backfill
    /// from the last seq. [`follow`](Self::follow) is that reconciliation.
    pub async fn subscribe(&self, from: Seq) -> Result<Subscription<G>, EntityError> {
        // Resolve BEFORE spawning the sink: a failed resolution must not leave an
        // orphan actor behind; the framework does not reap an actor merely because
        // every external ref to it was dropped.
        let host = self
            .resolve(false, self.system.now() + DEFAULT_ASK_TIMEOUT)
            .await?;
        let (tx, rx) = async_channel::bounded(SUB_BUFFER);
        let sink = self.system.spawn(RecordSink::<G>::new(tx));
        let subscribe = Subscribe {
            from,
            sink: sink.clone(),
        };
        match host.ask_timeout(subscribe, DEFAULT_ASK_TIMEOUT).await {
            Ok(subscribed) => Ok(Subscription {
                head: subscribed.head,
                records: rx,
            }),
            // The host went away between resolve and register: stop the sink we
            // just spawned (it was never registered, so no batch will ever stop it).
            Err(call) => {
                let _ = sink.tell(CloseSink).await;
                Err(EntityError::Call(call))
            }
        }
    }

    /// Follow the entity's committed events after `from` live (spec §7.3): a
    /// [`Follower`] whose [`next`](Follower::next) yields them in exact `Seq`
    /// order, reconciling the [`subscribe`](Self::subscribe) push against
    /// [`events`](Self::events) reads so the caller need not. Caller-driven
    /// and lazy: the first `next` attaches.
    pub fn follow(&self, from: Seq) -> Follower<G> {
        Follower::new(self.clone(), self.system.clone(), from)
    }

    /// Up to `limit` of the entity's committed events after `from` (exclusive),
    /// decoded, in ascending `Seq` order, the load-path form of the
    /// subscription's event projection (§7.3), served by the shard leader's
    /// **gateway** straight from the journal,
    /// **without get-or-activating the entity** (§7): polling a hibernated
    /// entity leaves it hibernated (§8). A framework built-in like
    /// [`subscribe`](Self::subscribe), available for every entity type with no
    /// `EntityHandler` bound.
    ///
    /// The returned `Seq`s are the journal's real slots: facet records (facets §4)
    /// occupy interleaved ones, so consecutive events need not carry consecutive
    /// `Seq`s. Fewer than `limit` events come back only at the head, so an empty
    /// result means no event follows `from` **that the journal still holds**:
    /// the returned [`EventHistory`]'s `base` reports how far compaction has
    /// truncated the log (storage §9), and a `from` below it asked for events the
    /// entity's snapshot has subsumed. Page by re-asking from the last `Seq`
    /// returned. Read-your-leader like any read (§7). A failed or corrupt
    /// journal read surfaces as [`EntityError::Unavailable`]: transient; a read
    /// commits nothing, so re-asking is always safe.
    pub async fn events(
        &self,
        from: Seq,
        limit: usize,
    ) -> Result<EventHistory<G::Event>, EntityError> {
        let deadline = self.system.now() + DEFAULT_ASK_TIMEOUT;
        let codec = self.system.codec();
        let mut base = Seq::ZERO;
        let mut events = Vec::new();
        let mut cursor = from;
        // One redirect-resolved gateway serves every page; a leader move between
        // pages is re-discovered by the engine, and a read commits nothing, so
        // re-asking from the cursor is always safe.
        let mut target = self.gateway.clone();
        while events.len() < limit {
            // The wire `limit` bounds the *records scanned* server-side, so
            // scan a floored page (see `EVENT_SCAN_FLOOR`) and drop any events
            // past the caller's `limit` below; the caller re-pages from the last
            // `Seq` returned, so the drop only moves its resume point earlier.
            let want = (limit - events.len())
                .max(EVENT_SCAN_FLOOR)
                .min(u32::MAX as usize) as u32;
            let reply: ReadReply = self
                .gateway_ask(
                    &mut target,
                    || ReadEvents::new(self.name.clone(), cursor, want),
                    deadline,
                )
                .await?;
            let page: EventPage = match reply {
                ReadReply::Page(page) => page,
                // The gateway is establishing the entity's committed-head floor
                // (§7): back off and re-ask, bounded by the one deadline.
                ReadReply::Warming => {
                    if deadline.duration_since(self.system.now()).is_zero() {
                        return Err(EntityError::Unavailable(
                            "read deadline exhausted while the gateway recovered the floor".into(),
                        ));
                    }
                    self.system.sleep(RESOLVE_BACKOFF).await;
                    continue;
                }
            };
            // Pages can come from different leaders (a redirect mid-read), whose
            // stores may have compacted differently: report the highest base
            // any page carried, the honest bound on what this read may have
            // missed.
            base = base.max(page.base);
            for (seq, payload) in page.events {
                if events.len() == limit {
                    // The floored scan overshot the caller's `limit`: stop
                    // here, before advancing the cursor past the remainder.
                    return Ok(EventHistory { base, events });
                }
                let event = actor_serialization::decode::<G::Event>(&*codec, &payload)
                    .map_err(|e| EntityError::Unavailable(e.to_string()))?;
                events.push((seq, event));
            }
            cursor = page.cursor;
            if page.at_head {
                break;
            }
        }
        Ok(EventHistory { base, events })
    }

    /// Resolve the name to its live host (§5.4). With `use_cache`, a cached handle
    /// is returned without touching the gateway (the steady-state fast path);
    /// otherwise the [`gateway_ask`](Self::gateway_ask) redirect engine drives
    /// [`Activate`] to the leader's gateway and the returned handle is cached.
    async fn resolve(
        &self,
        use_cache: bool,
        deadline: actor_core::Instant,
    ) -> Result<ActorRef<Host<G>>, EntityError> {
        if use_cache && let Some(host) = self.cache.as_ref().and_then(|cache| cache.get(&self.name))
        {
            return Ok(host);
        }
        let mut target = self.gateway.clone();
        let host = self
            .gateway_ask(&mut target, || Activate::new(self.name.clone()), deadline)
            .await?;
        if let Some(cache) = &self.cache {
            cache.put(self.name.clone(), host.clone());
        }
        Ok(host)
    }

    /// The **client-side bounded redirect** (§5.4 step 4), generic over the
    /// gateway request: the single-shot gateway answers `Ok` if its node leads the
    /// shard, else `NotLeader(hint)`, which this follows to that node's gateway,
    /// backing off while the shard elects, until the leader answers or `deadline`
    /// expires. A transient miss is waited out rather than surfaced, so a remote
    /// call stays observably identical to a local one across a failover (invariant
    /// **E13**). Driving the loop here, not in the gateway's serial handler, keeps
    /// one slow resolution from blocking another entity's activation on that node.
    ///
    /// `target` starts at this ref's own gateway (local for an `Entities`-minted
    /// ref, the source node's for a wire-arrived one), and is moved toward the
    /// leader by hints and re-discovery; it is a `&mut` so a caller issuing
    /// several requests (a paging read, [`events`](Self::events)) keeps the
    /// found leader across them. Bounded by the **caller's** `deadline`, not a
    /// per-attempt window, so a dispatch that resolves, fails, and re-resolves
    /// still returns within the one budget (§5.4). Safe for the gateway's whole
    /// surface: an `Activate` commits nothing and a read commits nothing, so
    /// re-asking after a transport failure cannot double-apply (§1.1).
    async fn gateway_ask<M, T>(
        &self,
        target: &mut ActorRef<Gateway<G>>,
        make: impl Fn() -> M,
        deadline: actor_core::Instant,
    ) -> Result<T, EntityError>
    where
        M: Message<Reply = Result<T, EntityError>>,
        Gateway<G>: actor_core::Handler<M>,
    {
        let mut hint = target.id().node();
        for _ in 0..RESOLVE_ATTEMPTS {
            let remaining = deadline.duration_since(self.system.now());
            if remaining.is_zero() {
                break;
            }
            let attempt = remaining.min(FORWARD_TIMEOUT);
            match target.ask_timeout(make(), attempt).await {
                Ok(Ok(reply)) => return Ok(reply),
                // Follow the leader hint. If that gateway is not (yet) discoverable,
                // keep the current target and back off: the shard is still electing
                // or the gateway has not gossiped in.
                Ok(Err(EntityError::NotLeader(h))) => {
                    hint = h;
                    if let Some(gateway) = self.gateway_on(hint) {
                        *target = gateway;
                    }
                }
                // A genuine durability outcome (quorum loss / unhandled): surface it.
                Ok(Err(other)) => return Err(other),
                // The target gateway is unreachable (its node crashed): re-discover a
                // gateway to redirect from, excluding the one that just failed.
                Err(_) => {
                    if let Some(gateway) = self.gateway_excluding(target.id().node()) {
                        *target = gateway;
                    }
                }
            }
            self.system.sleep(RESOLVE_BACKOFF).await;
        }
        // Deadline or attempt bound exhausted: surface the last hint seen (§3.2).
        Err(EntityError::NotLeader(hint))
    }

    /// Every gateway discovered for this entity type (§5.3), one per node.
    fn gateways(&self) -> Vec<ActorRef<Gateway<G>>> {
        self.system
            .receptionist()
            .lookup(gateway_key::<G>(self.entity_type))
            .into_vec()
    }

    /// The gateway registered on `node`, if discovered in the receptionist (§5.3).
    fn gateway_on(&self, node: NodeId) -> Option<ActorRef<Gateway<G>>> {
        self.gateways()
            .into_iter()
            .find(|gateway| gateway.id().node() == node)
    }

    /// A discovered gateway on any node but `failed`: used to escape a dead
    /// target (§5.4) without re-selecting the very gateway that just timed out,
    /// whose registration may outlive the crash until the membership prunes it.
    fn gateway_excluding(&self, failed: NodeId) -> Option<ActorRef<Gateway<G>>> {
        let gateways = self.gateways();
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
    /// entry is dropped and the command re-issued, but **only when the first
    /// attempt provably did not run** (a `NotLeader`, which never commits, storage §6; or a
    /// `DeadLetter` from a hibernated host, §8). An ambiguous transport failure is
    /// surfaced, never auto-retried (see [`is_retriable`]).
    async fn dispatch<M>(&self, msg: M, within: Duration) -> Result<M::Reply, EntityError>
    where
        G: EntityHandler<M>,
        M: Message + Clone,
    {
        // ONE deadline bounds the whole call (resolution and ask, across both
        // attempts), so the retry path never restarts the caller's budget (a
        // per-step `within` could stack up to ~4× the declared timeout).
        let deadline = self.system.now() + within;
        let retry = msg.clone();
        self.resolve_twice(
            deadline,
            true,
            async |host| {
                let remaining = deadline.duration_since(self.system.now());
                match host.ask_timeout(RunTyped(msg), remaining).await {
                    // The host's reply is itself `Result<M::Reply, EntityError>`.
                    Ok(Ok(reply)) => Attempt::Done(Ok(reply)),
                    // Leadership moved off the cached host (storage §6): refresh and retry.
                    Ok(Err(EntityError::NotLeader(_))) => Attempt::Retry,
                    // A genuine durability outcome (quorum loss / unhandled): terminal.
                    Ok(Err(other)) => Attempt::Done(Err(other)),
                    // The command provably never reached a handler (§8): re-issue.
                    Err(call) if is_retriable(&call) => Attempt::Retry,
                    // Ambiguous or terminal: never auto-retry a command that may
                    // have committed (§1.1).
                    Err(call) => Attempt::Done(Err(EntityError::Call(call))),
                }
            },
            async |host| {
                let remaining = deadline.duration_since(self.system.now());
                match host.ask_timeout(RunTyped(retry), remaining).await {
                    Ok(result) => result,
                    Err(call) => Err(EntityError::Call(call)),
                }
            },
        )
        .await
    }

    /// The two-attempt resolve-retry both [`tell`](Self::tell) and
    /// [`dispatch`](Self::dispatch) layer over the [`resolve`](Self::resolve)
    /// redirect engine. Resolve cache-preferring and run `first` under the single
    /// `deadline`; on [`Attempt::Retry`]: the command provably never ran (storage §6,
    /// entity §8); drop the stale cache entry, resolve cache-bypassing, and run `again`,
    /// which is terminal. Never a third attempt, so an effectful command that may
    /// have committed is surfaced, never re-issued (at-most-once, §1.1).
    ///
    /// `guard_budget` returns [`Unavailable`](EntityError::Unavailable) when the
    /// deadline is spent before the retry; fire-and-forget must never report
    /// `Unavailable` (§6), so it passes `false`.
    async fn resolve_twice<T>(
        &self,
        deadline: actor_core::Instant,
        guard_budget: bool,
        first: impl AsyncFnOnce(&ActorRef<Host<G>>) -> Attempt<T>,
        again: impl AsyncFnOnce(&ActorRef<Host<G>>) -> Result<T, EntityError>,
    ) -> Result<T, EntityError> {
        let host = self.resolve(true, deadline).await?;
        if let Attempt::Done(result) = first(&host).await {
            return result;
        }
        self.invalidate();
        if guard_budget && deadline.duration_since(self.system.now()).is_zero() {
            return Err(EntityError::Unavailable(
                "deadline exhausted before the retry".into(),
            ));
        }
        let host = self.resolve(false, deadline).await?;
        again(&host).await
    }
}

/// The outcome of one attempt in [`EntityRef::resolve_twice`]: a terminal `Done`
/// result, or `Retry`; the attempt provably did not run (§1.1), so re-resolve
/// and re-issue once.
enum Attempt<T> {
    Done(Result<T, EntityError>),
    Retry,
}

/// Whether a transport failure proves the command **never ran**, so re-issuing it
/// against a fresh resolution cannot double-apply (at-most-once, §1.1; reply-iff-
/// durable, §6/E5). Only `DeadLetter` qualifies: the cached host had hibernated
/// and stopped, so the command dead-lettered without reaching a handler (the §8
/// eviction race). `Unreachable`/`Timeout` are **ambiguous** (the command may
/// have committed on a leader that then crashed before replying), so they are NOT
/// auto-retried for an effectful command; they surface to the caller, who makes
/// the operation idempotent where a retry matters (§1.1).
fn is_retriable(call: &CallError) -> bool {
    matches!(call, CallError::DeadLetter)
}

/// A handle to a hosted entity type (spec §5.4): address an entity by key and
/// `ask`/`tell` it. Obtained from [`EntityExt::entities`].
pub struct Entities<G: Entity> {
    gateway: ActorRef<Gateway<G>>,
    /// The shared routing state (§5.4): the system handle, the runtime type name
    /// (spec §5.1, `G::ENTITY_TYPE` unless
    /// [`entities_named`](crate::cluster::EntityNode::entities_named) overrode it), the founding
    /// shard count, and the shard map; read through the cache rather than kept
    /// as copies here, so name resolution has one set of inputs.
    cache: Arc<HostCache<G>>,
}

impl<G: Entity> Clone for Entities<G> {
    fn clone(&self) -> Self {
        Entities {
            gateway: self.gateway.clone(),
            cache: Arc::clone(&self.cache),
        }
    }
}

impl<G: Entity> Entities<G> {
    /// Address an entity of this type by key (spec §5.1): an [`EntityRef`] with
    /// no activation yet; the first message activates it. The ref shares this
    /// handle's host cache (§5.4).
    pub fn entity(&self, key: impl Into<String>) -> EntityRef<G> {
        EntityRef::new(
            self.cache.entity_type,
            EntityName::new(self.cache.entity_type, key),
            self.gateway.clone(),
            self.cache.system.clone(),
            Some(Arc::clone(&self.cache)),
        )
    }

    /// The node that currently leads the shard an entity key maps to, where that
    /// entity activates (§5.2), or `None` while the shard elects and on a node
    /// outside the shard's group (as a clustered routing-only client always is). A
    /// routing observation, not a guarantee: leadership can move immediately after.
    pub fn leader(&self, key: impl Into<String>) -> Option<NodeId> {
        let name = EntityName::new(self.cache.entity_type, key);
        self.cache.system.shard_leader(self.cache.shard_of(&name))
    }

    /// Whether a live host handle is currently cached for `key` (spec §5.4). An
    /// observability detail, not a statement about the entity's activation on its
    /// leader.
    pub fn is_cached(&self, key: impl Into<String>) -> bool {
        self.cache
            .contains(&EntityName::new(self.cache.entity_type, key))
    }

    /// Request a split of shard `index` at its range midpoint (spec storage §8): the
    /// admin/test seam over [`ShardMapSource::request_split`]. Best-effort and
    /// asynchronous: a shard that is unknown, mid-migration, mid-split, or a
    /// single-point range is silently skipped, and the committed flip is
    /// observable as a [`ShardSplit`](crate::EntityEvent::ShardSplit) event and
    /// changed routing. A no-op on the `Local` tier and on a routing-only client.
    pub fn split_shard(&self, index: u32) {
        self.cache.shard_map.request_split(index);
    }

    /// Request a merge of shard `left` with its right neighbour (spec storage §8):
    /// the mirror of [`split_shard`](Entities::split_shard), reclaiming a
    /// leader-election group (E7) when two adjacent ranges are cold. Best-effort
    /// and asynchronous on the same terms; unknown, non-adjacent, or busy shards
    /// are skipped, and the committed flip is observable as a
    /// [`ShardMerged`](crate::EntityEvent::ShardMerged) event.
    pub fn merge_shards(&self, left: u32) {
        self.cache.shard_map.request_merge(left);
    }

    /// The nodes that replicate the shard an entity key maps to (spec storage §7): the
    /// only nodes that hold its data and can lead it. Read live from the
    /// consensus-agreed shard map, so it reflects the latest committed allocation,
    /// not a `entities()`-time snapshot. Empty on a routing-only client, which holds
    /// no map.
    pub fn replicas(&self, key: impl Into<String>) -> Vec<NodeId> {
        let name = EntityName::new(self.cache.entity_type, key);
        let index = self.cache.shard_of(&name).index;
        self.cache.shard_map.replicas(index).unwrap_or_default()
    }
}

/// Host entities of a type on a system (spec §5.3). Implemented for every
/// [`EntitySystem`], so `system.entities::<G>(config)` starts the gateway and
/// returns an [`Entities`] handle.
pub trait EntityExt: EntitySystem {
    /// This node's entity hosting, carrying the capabilities every type it hosts
    /// shares (spec storage §3.2, facets §5.5, entity §3.4): the blocking-I/O pool, the metrics registry,
    /// and the alarm index.
    ///
    /// All three are properties of the *node*, not of an entity type, so a deployment
    /// sets them once here and hosts every type through the returned handle, which
    /// also hands off every hosted type's led shards at a graceful stop. Hosting
    /// straight off the system (`system.entities(..)`) is the same call with every
    /// capability defaulted (inline I/O, discarded metrics, no alarm index), which is
    /// what the `Local` tier and the deterministic simulation want.
    ///
    /// Named for what it returns rather than `node`, which on an
    /// [`ActorSystem`] is already this node's
    /// [`NodeId`].
    fn entity_node(&self) -> crate::node::EntityNode<Self> {
        crate::node::EntityNode::new(self.clone())
    }

    /// Start hosting entities of type `G` under its own `G::ENTITY_TYPE` (spec
    /// §5.3): spawn the type's gateway and return the handle. Each
    /// activation's behavior is built by `G::default`. The common case: one Rust
    /// type, one entity type. An explicit runtime type name or activation factory
    /// is the node's form, [`EntityNode::entities_named`](crate::cluster::EntityNode::entities_named).
    fn entities<G>(&self, config: EntityConfig) -> Entities<G>
    where
        G: Entity<System = Self> + Default,
    {
        self.entity_node().entities(config)
    }

    /// Address entities of type `G` as a routing-only **client**: it hosts
    /// **nothing**, no gateway, replica store, or shard-map group is started. The
    /// handle routes through a *host's* gateway, discovered in the receptionist
    /// (§5.3) and seeded here; `EntityRef`'s bounded redirect re-discovers a live
    /// gateway on failover, so the seed only has to be reachable once.
    ///
    /// Returns `None` until at least one host's gateway for `entity_type` has
    /// gossiped into this client's receptionist: the caller polls, exactly as a
    /// node waits for its peers before serving. The client needs no shard count:
    /// a node that replicates none of the type's shards can never know a shard's
    /// leader, so every name it has not cached resolves through a host's gateway,
    /// which maps it from its own committed partition (§5.4).
    fn entities_client<G>(&self, entity_type: &'static str) -> Option<Entities<G>>
    where
        G: Entity<System = Self>,
    {
        let gateway = self
            .receptionist()
            .lookup(gateway_key::<G>(entity_type))
            .into_vec()
            .into_iter()
            .next()?;
        Some(Entities {
            gateway,
            // A routing-only client has no `EntityConfig` to read, so it takes the
            // default bound, and it is the path that most needs one: a long-lived
            // gateway client addresses every tenant's names. One founding shard,
            // because no answer here depends on which shard a name resolves to: a
            // clustered client is in none of the type's groups, so `shard_leader`
            // is `None` for every shard (the `Local` tier leads every shard), and
            // the empty map lists no replicas for any.
            cache: HostCache::new(
                self.clone(),
                entity_type,
                1,
                Arc::new(EmptyShardMap),
                EntityConfig::default().host_cache_capacity,
            ),
        })
    }
}

impl<T: EntitySystem> EntityExt for T {}

/// How often the alarm driver sweeps the shards it leads (spec facets §5.5). The exact
/// deadline is honoured by the entity's own in-activation timer once re-activated;
/// this cadence bounds only the *re-activation* latency after a failover.
const ALARM_DRIVE_INTERVAL: Duration = Duration::from_millis(500);

/// Build the node-local hosting for an entity type (spec §5.3, storage §3.2): the
/// durable store, replica store, shard map, and gateway, plus the type's alarm
/// driver (facets §5.5, the callerless-activation seam) when the node has an
/// `alarm_index` and the type declares the [`Alarm`](crate::alarm::Alarm) facet.
/// The one constructor behind every [`EntityNode`](crate::node::EntityNode) hosting
/// method, which keeps the returned shard map for its leadership handoff (storage §6.3).
pub(crate) fn build_entities<S, G>(
    system: &S,
    entity_type: &'static str,
    config: EntityConfig,
    capabilities: &crate::node::NodeCapabilities,
    factory: Arc<dyn Fn() -> G + Send + Sync>,
    alarm_index: Option<Entities<AlarmIndex<S>>>,
) -> (Entities<G>, Arc<dyn ShardMapSource>)
where
    S: EntitySystem,
    G: Entity<System = S>,
{
    // Only a type that can hold a deadline gets the index: any other would pay a
    // driver sweep and an index ask per activation for nothing.
    let alarm_index = alarm_index.filter(|_| crate::alarm::declares_alarm::<G>());
    let shards = config.shards.max(1);
    let replicas = config.replication_factor.max(1);
    // This node's durable entity store (storage §3.2): the injected factory if a deployment
    // supplied one (so records survive a restart), else a fresh ephemeral in-memory
    // store. The replica-store actor makes it reachable from a shard leader's
    // replicator (storage §5), registered under one key per type like the gateway (§5.3).
    let store: Arc<dyn EntityStore> = match &config.entity_store {
        // Per `(entity_type, node)`, never per node alone: the store's fence is keyed by
        // shard index and holds this type's leader-election term, which another type's
        // term would fence out of its own shards (storage §4.2, [`EntityStoreFactory`]).
        Some(factory) => factory(entity_type, system.node()),
        None => Arc::new(MemoryEntityStore::new()),
    };
    // Every store write runs on the node's blocking pool (storage §3.2, `crate::blocking`).
    let store = NodeStore::new(store, Arc::clone(capabilities.blocking_io()));
    let replica_store = system.spawn(ReplicaStore::<S>::new(store.clone()));
    system
        .receptionist()
        .register(replica_store_key::<S>(entity_type), &replica_store);
    let transport: Arc<dyn ReplicaTransport> =
        Arc::new(ActorReplicaTransport::<S>::new(system.clone(), entity_type));
    // Build the consensus-agreed shard map (storage §7): a per-type Raft group whose
    // committed log is the allocation, so every node agrees on each shard's replica
    // set and only the replicas store it. Keyed by the runtime `entity_type`, so two
    // type names get separate maps.
    let shard_map = system.shard_map(
        entity_type,
        shards,
        replicas,
        config.shard_target_bytes,
        store,
        transport,
        crate::replicator::Deadlines {
            quorum: config.quorum_timeout,
            recover: config.recover_timeout,
        },
        config.failure_domains.clone(),
    );
    // Read before `config` moves into the gateway.
    let host_cache_capacity = config.host_cache_capacity;
    let gateway = system.spawn(Gateway::new(
        entity_type,
        Arc::clone(&shard_map),
        shards,
        config,
        capabilities.clone(),
        factory,
        alarm_index.clone(),
    ));
    // Register this node's gateway under the type's well-known key so other nodes
    // route activations to it (§5.3).
    system
        .receptionist()
        .register(gateway_key::<G>(entity_type), &gateway);
    let handle = Entities {
        gateway,
        cache: HostCache::new(
            system.clone(),
            entity_type,
            shards,
            Arc::clone(&shard_map),
            host_cache_capacity,
        ),
    };
    if let Some(index) = alarm_index {
        system.launch(Box::pin(alarm_driver_loop::<S, G>(
            system.clone(),
            handle.clone(),
            index,
        )));
    }
    (handle, shard_map)
}

/// The per-type alarm driver (spec facets §5.5): the callerless-activation seam that makes a
/// durable alarm fire across an entity's hibernation and a node failover, not only
/// while it is resident.
///
/// It sweeps on a fixed cadence: for every shard **this node leads**, it reads that
/// shard's alarm index for entities whose deadline has passed, and re-activates each by
/// `subscribe` (a framework built-in every entity accepts, so the driver stays generic
/// over `G`). A re-activated entity's host re-arms the timer from the folded
/// deadline as rehydration ends, and it fires at once for a past deadline (**E3**). The index is a hint (the
/// entity's alarm facet is the source of truth), so re-activating an entity whose alarm
/// already cleared is harmless.
///
/// A split or merge (storage §8) moves an entity's key range to another shard, leaving a
/// stale entry in the old shard's index. Re-activating a due entity routes to its
/// **live** owner, so the alarm still fires and the entity re-registers into its new
/// shard's index there. The driver clears the stale entry in the old index only
/// *after* that successful re-activation, so it never orphans a wake.
async fn alarm_driver_loop<S, G>(system: S, entities: Entities<G>, index: Entities<AlarmIndex<S>>)
where
    S: EntitySystem,
    G: Entity<System = S>,
{
    let entity_type = entities.cache.entity_type;
    loop {
        system.sleep(ALARM_DRIVE_INTERVAL).await;
        let now = system.now().as_nanos();
        // Sweep the committed partition's shards: a split/merge (storage §8) changes
        // the shard set, and each shard has its own index entity. While the map is
        // still bootstrapping, fall back to the founding indices.
        let mut indices = entities.cache.shard_map.shard_indices();
        if indices.is_empty() {
            indices = (0..entities.cache.shards as u32).collect();
        }
        for shard in indices {
            let id = ShardId {
                entity_type,
                index: shard,
            };
            if !system.leads_shard(id) {
                continue;
            }
            let key = index_key(entity_type, shard as usize);
            let due = match index
                .entity(key.clone())
                .ask(DueBefore { before: now })
                .await
            {
                Ok(names) => names,
                Err(_) => continue, // index shard unavailable this tick; retry next sweep
            };
            for name in due {
                // Re-activate the entity: routing sends it to its live owner, where
                // the host re-arms and fires the due alarm. The subscription
                // is dropped immediately; activation is the only effect we want.
                let reactivated = entities
                    .entity(name.key().to_string())
                    .subscribe(Seq::ZERO)
                    .await
                    .is_ok();
                // If the entity has moved off this shard (storage §8), the re-activation
                // above seeded its new shard's index, so clear the stale entry here,
                // with a max head, so the clear always wins. Only after a
                // successful re-activation, so a transient failure never orphans
                // the wake.
                let live = entities.cache.shard_of(&name);
                if reactivated && live.index != shard {
                    let _ = index
                        .entity(key.clone())
                        .ask(AlarmSync {
                            entity: name.clone(),
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

    fn name(key: &str) -> EntityName {
        EntityName::new("test.Entity", key)
    }

    /// The property the bound exists for: addressing an unbounded stream of distinct
    /// names must not grow the cache without limit.
    #[test]
    fn the_cache_stays_bounded_across_unboundedly_many_names() {
        let mut cache: Generations<u32> = Generations::new(16);
        for i in 0..10_000 {
            cache.insert(name(&format!("entity-{i}")), i);
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

    /// An untouched name is eventually dropped: the eviction actually happens rather
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
