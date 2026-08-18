//! The per-grain-type gateway: routing and the activation table (spec §5.3, §5.4).
//!
//! Each node runs one gateway actor per grain *type*, registered in the
//! receptionist (actor §13) under a single well-known key for that type — one
//! gateway entry per node ([`gateway_key`]). It owns the activation table mapping
//! `GrainName → Host` for shards this node leads, and getting-or-activating a name
//! is its one serial critical section: because the gateway is a serial actor, two
//! concurrent requests for a not-yet-active name are processed in order — the
//! first activates the host, the second finds it — so activation is
//! **exactly-once per node by construction**, with no lock (invariant **G6**).
//!
//! The gateway is also the **router** (§5.4), but **single-shot**: resolving a
//! name is two levels — name→shard by the committed key-range partition (the
//! shard map, §5.1; the founding [`shard_for`](crate::shard_for) while it
//! bootstraps), shard→leader from the system (`leads_shard`/`shard_leader`). When
//! this node leads the shard it activates the host locally and returns the handle;
//! otherwise it returns `NotLeader(hint)` *immediately* (§5.4 step 4). The bounded
//! redirect — following the hint, waiting out an election — is the **caller's**
//! job, driven by [`GrainRef`](crate::GrainRef) and bounded by the caller's own
//! deadline, so a slow resolution never blocks another grain's activation on this
//! node.
//!
//! Beside routing, the gateway serves the **non-activating read** ([`ReadEvents`],
//! §7.5): one bounded page of a grain's committed events straight from the
//! shard's journal, so observation never wakes a hibernated grain (§10).

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Terminated;
use actor_core::receptionist::Key;
use serde::Deserialize;
use serde::Serialize;

use crate::alarm_index::AlarmIndex;
use crate::config::GranaryConfig;
use crate::error::GrainError;
use crate::grain::Grain;
use crate::grain::GrainName;
use crate::grain::events_in_page;
use crate::grainref::Granary;
use crate::host::CommittedCell;
use crate::host::HEAD_UNPUBLISHED;
use crate::host::Host;
use crate::journal::Seq;
use crate::journal::Term;
use crate::shardmap::ShardMapSource;
use crate::shardmap::resolve_shard;
use crate::system::GranarySystem;
use crate::system::ShardId;

/// The receptionist key the gateway for a grain type registers under (spec
/// §5.3): one well-known key per type, one entry per node. Routing looks the
/// leader node's gateway up here.
///
/// `grain_type` is the runtime type name (spec §5.1), `G::GRAIN_TYPE` by default
/// but a caller-supplied name when one Rust grain is hosted under several type
/// names ([`granary_named`](crate::GranaryExt::granary_named)) — two such names
/// register distinct keys even though both are `Key<Gateway<G>>`.
pub(crate) fn gateway_key<G: Grain>(grain_type: &'static str) -> Key<Gateway<G>> {
    Key::new(grain_type)
}

/// Get-or-activate the host for a name and return a handle to it (spec §5.4). The
/// reply is the live `Host` activation — on this node when it leads the name's
/// shard, otherwise the activation on the leader node. The caller then sends the
/// command straight to it, keeping the serial gateway off the steady-state hot
/// path.
///
/// Registered for network dispatch (see [`Gateway::register`]) so a caller on
/// another node can drive a remote activation; the returned `ActorRef<Host<G>>`
/// rebinds on the caller's node (the framework decodes replies under the local
/// system, actor §4.4).
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub(crate) struct Activate<G: Grain> {
    pub(crate) name: GrainName,
    #[serde(skip)]
    _marker: PhantomData<fn() -> G>,
}

impl<G: Grain> Activate<G> {
    pub(crate) fn new(name: GrainName) -> Activate<G> {
        Activate {
            name,
            _marker: PhantomData,
        }
    }
}

impl<G: Grain> Message for Activate<G> {
    type Reply = Result<ActorRef<Host<G>>, GrainError>;
    const MANIFEST: Manifest = Manifest::new("granary.Activate");
}

/// Server-side bound on one [`ReadEvents`] page (spec §7.5): the most journal
/// **records** one ask scans. It caps both the reply and the time the serial
/// gateway spends serving a read, so no caller can hold activations behind one
/// huge scan; a caller wanting more pages by re-asking from the returned cursor.
pub(crate) const READ_PAGE: usize = 1024;

/// Read one page of a grain's committed events straight from the shard's
/// journal (spec §7.5) — the **non-activating read**: the gateway serves it from
/// `GrainJournal::load` without get-or-activating the grain, so polling a
/// hibernated grain leaves it hibernated (§10). Like [`Activate`], it is
/// answered only on the shard's leader (read-your-leader, §7.5); elsewhere it
/// returns `NotLeader(hint)` for the caller's bounded redirect (§5.4).
///
/// `limit` bounds the **records scanned** (clamped to [`READ_PAGE`]), which
/// upper-bounds the events returned — facet records (§7.12) among the scanned
/// page are skipped, not surfaced. The events come back as their undecoded
/// payload bytes at the journal's real slots; the caller decodes with its own
/// codec ([`GrainRef::events`](crate::GrainRef::events)), so the serial gateway
/// pays one bounded local read and no decode.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub(crate) struct ReadEvents<G: Grain> {
    pub(crate) name: GrainName,
    pub(crate) from: Seq,
    pub(crate) limit: u32,
    #[serde(skip)]
    _marker: PhantomData<fn() -> G>,
}

impl<G: Grain> ReadEvents<G> {
    pub(crate) fn new(name: GrainName, from: Seq, limit: u32) -> ReadEvents<G> {
        ReadEvents {
            name,
            from,
            limit,
            _marker: PhantomData,
        }
    }
}

impl<G: Grain> Message for ReadEvents<G> {
    type Reply = Result<ReadReply, GrainError>;
    const MANIFEST: Manifest = Manifest::new("granary.ReadEvents");
}

/// A [`ReadEvents`] reply.
#[derive(Serialize, Deserialize)]
pub(crate) enum ReadReply {
    /// One served page.
    Page(EventPage),
    /// The gateway is still establishing the grain's committed-head floor — a
    /// spawned per-leadership `head` recovery for a hibernated name, or a
    /// resident activation mid-rehydration (§7.5). Transient by construction:
    /// the caller retries after a short backoff, bounded by its own deadline.
    Warming,
}

/// One [`ReadEvents`] page. `cursor` is the last **record** scanned — event or
/// not — so the next page begins after the facet records this one skipped, and
/// a page of only facet records still makes progress; `at_head` reports whether
/// the scan reached the end of what this leader may serve (the journal's head,
/// or its committed-head floor), the explicit form of the "short page only at
/// head" contract a filtered page cannot carry on its own. `base` is the
/// store's compaction base when the page was served (§9): slots at
/// `Seq <= base` are subsumed by the grain's snapshot and no longer readable,
/// so a caller whose ask began below it is looking at truncated history, not
/// at the facet slots the projection legally skips (§7.12). Every page carries
/// it because compaction is not bounded by any reader's cursor — a snapshot at
/// the head can outrun a slow pager mid-read.
#[derive(Serialize, Deserialize)]
pub(crate) struct EventPage {
    pub(crate) events: Vec<(Seq, Vec<u8>)>,
    pub(crate) cursor: Seq,
    pub(crate) at_head: bool,
    pub(crate) base: Seq,
}

/// The gateway's note-to-self completing a spawned floor recovery (§7.5): a
/// **local-only** tell from the task [`Gateway::warm`] launched — never accepted
/// off the wire (absent from [`Gateway::register`]), like any internal
/// self-drive. `head` is `None` when the recovery failed or leadership lapsed
/// while it ran; the term, shard, and epoch are re-checked on the serial actor
/// either way.
#[derive(Serialize, Deserialize)]
pub(crate) struct Recovered {
    name: GrainName,
    shard: u32,
    term: Term,
    head: Option<Seq>,
    /// The `warming` token this recovery was spawned under: stale if an
    /// activation of **this name** was created since (its appends move the
    /// head this recovery measured — even if that activation has already
    /// hibernated again). Per-name, so unrelated activations never invalidate
    /// it.
    epoch: u64,
}

impl Message for Recovered {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("granary.GatewayRecovered");
}

/// The node-local gateway for grain type `G` (spec §5.3).
pub(crate) struct Gateway<G: Grain> {
    /// Names this node currently hosts, each with the shard index it activated
    /// under and the activation's published committed head ([`CommittedCell`]) —
    /// the resident floor of the non-activating read (§7.5). The serial actor is
    /// the only writer, so no lock guards it (**G6**). The stored index guards
    /// against a stale entry after a split/merge (§7.7).
    table: HashMap<GrainName, (u32, ActorRef<Host<G>>, CommittedCell)>,
    /// Committed-head floors for **non-resident** names (§7.5), each stamped with
    /// the shard index and shard term its `head` recovery ran under: valid while
    /// that exact leadership persists — leadership regained is a higher term
    /// (§8), and a split/merge changes the shard index (§7.7, indexes are never
    /// reused), so neither can alias — and no activation has started since
    /// ([`get_or_activate`](Gateway::get_or_activate) removes the entry).
    /// Bounded by `host_cache_capacity`: on overflow one arbitrary entry is
    /// evicted, which only costs the next read of that name a fresh recovery.
    floors: HashMap<GrainName, (u32, Term, Seq)>,
    /// Names whose floor recovery is in flight, so concurrent reads spawn one
    /// recovery, not one each — each carrying the token its recovery was
    /// spawned with. Cleared when its [`Recovered`] lands. **Per-name**
    /// invalidation: [`get_or_activate`](Gateway::get_or_activate) for the name
    /// rotates the token, so a [`Recovered`] carrying the old one is discarded
    /// — closing the window where an activation is created — and appends, and
    /// even hibernates — while that name's floor recovery is still in flight,
    /// without discarding recoveries of unrelated names (an activation of B
    /// cannot move A's head, so it must not starve A's read).
    warming: HashMap<GrainName, u64>,
    /// Allocator for the per-name `warming` tokens: bumped once per spawn and
    /// once per invalidation, never compared globally.
    warm_epoch: u64,
    /// The runtime type name (spec §5.1), as passed to [`gateway_key`]. Used to
    /// map a name to its shard.
    grain_type: &'static str,
    /// The shard map (§7.6): which nodes replicate each shard and this node's local
    /// store for the shards it replicates, read live as the consensus allocation
    /// commits.
    shard_map: Arc<dyn ShardMapSource>,
    /// How many shards the type's namespace is partitioned into (§7.1).
    shards: usize,
    config: GranaryConfig,
    /// What this **node** provides to every type it hosts (§7.4, §13), as opposed to
    /// the per-type `config` above. Carried here so each activation gets the node's
    /// handles rather than rebuilding a default pair per host.
    capabilities: crate::node::NodeCapabilities,
    /// How a fresh activation's behavior value is built: the runtime instantiates
    /// the grain, the user supplies no value per `GrainName`.
    factory: Arc<dyn Fn() -> G + Send + Sync>,
    /// The per-shard alarm index handed to each host it activates (spec §7.16), or
    /// `None` when alarm-index wiring is off. Set by `granary_with_alarms`.
    alarm_index: Option<Granary<AlarmIndex<G::System>>>,
}

impl<G: Grain> Gateway<G> {
    #[allow(clippy::too_many_arguments)] // one call site, `build_granary`
    pub(crate) fn new(
        grain_type: &'static str,
        shard_map: Arc<dyn ShardMapSource>,
        shards: usize,
        config: GranaryConfig,
        capabilities: crate::node::NodeCapabilities,
        factory: Arc<dyn Fn() -> G + Send + Sync>,
        alarm_index: Option<Granary<AlarmIndex<G::System>>>,
    ) -> Gateway<G> {
        Gateway {
            table: HashMap::new(),
            floors: HashMap::new(),
            warming: HashMap::new(),
            warm_epoch: 0,
            grain_type,
            shard_map,
            shards,
            config,
            capabilities,
            factory,
            alarm_index,
        }
    }

    /// Get-or-activate the host for `name` on **this** node (its shard's leader),
    /// returning a live handle. Exactly-once per node by the serial actor
    /// (**G6**, see module docs). Called only when this node replicates and leads
    /// the shard, so the shard's `journal` is present.
    fn get_or_activate(
        &mut self,
        name: GrainName,
        shard: ShardId,
        ctx: &Ctx<Gateway<G>>,
    ) -> ActorRef<Host<G>> {
        // Reuse the cached host only if it is still alive AND still activated
        // under the shard the name currently resolves to. The liveness check
        // closes the eviction race (§10): a host that hibernated before its
        // `Terminated` pruned the table must not be handed back. The shard check
        // closes the split/merge race (§7.7): a host activated over the old
        // shard's journal must not serve the name once the partition moved it.
        if let Some((activated_shard, host, _)) = self.table.get(&name) {
            if *activated_shard == shard.index
                && ctx.system().resolve_local::<Host<G>>(host.id()).is_some()
            {
                return host.clone();
            }
            self.table.remove(&name);
        }
        // The activation may append, so a non-resident read floor recorded for
        // the name is stale from here on (§7.5): the resident cell replaces it.
        // Rotating the name's warming token likewise retires any floor recovery
        // still in flight for it — its measured head predates whatever this
        // activation commits. Per-name, so recoveries of other names survive.
        self.floors.remove(&name);
        if let Some(token) = self.warming.get_mut(&name) {
            self.warm_epoch += 1;
            *token = self.warm_epoch;
        }

        // Otherwise spawn a restartable host that rehydrates from its shard's
        // journal (§9).
        let journal = self
            .shard_map
            .journal(shard.index)
            .expect("a leader replicates the shard, so its journal is present");
        let config = self.config.clone();
        let capabilities = self.capabilities.clone();
        let factory = Arc::clone(&self.factory);
        let gateway = ctx.this();
        let activated = name.clone();
        let grain_type = self.grain_type;
        let alarm_index = self.alarm_index.clone();
        let shard_index = shard.index;
        // The activation's published committed head (§7.5): unpublished until its
        // rehydration recovers the grain's head.
        let committed: CommittedCell = Arc::new(AtomicU64::new(HEAD_UNPUBLISHED));
        let cell = Arc::clone(&committed);
        let host = ctx.spawn_with(move || {
            Host::new(
                grain_type,
                (factory)(),
                activated.clone(),
                shard_index,
                journal.clone(),
                config.clone(),
                &capabilities,
                gateway.clone(),
                alarm_index.clone(),
                Arc::clone(&cell),
            )
        });
        // Prune the table when the host stops — idle hibernation or fault (§10).
        ctx.watch(&host);
        self.table
            .insert(name, (shard.index, host.clone(), committed));
        host
    }

    /// The shard a name resolves to right now (spec §5.1): the committed shard
    /// map's key-range lookup, with the founding-partition fallback while the
    /// map bootstraps ([`resolve_shard`]).
    fn shard_of(&self, key: &str) -> ShardId {
        resolve_shard(self.shard_map.as_ref(), self.grain_type, key, self.shards)
    }
}

impl<G: Grain> Actor for Gateway<G> {
    type System = G::System;

    /// Accept [`Activate`] and [`ReadEvents`] over the network (spec §5.4,
    /// §7.5). This is the gateway's whole network surface — a typed command
    /// then travels straight to the host (`RunTyped`, registered on [`Host`]),
    /// while the non-activating read is answered here, touching no host.
    fn register(registry: &mut HandlerRegistry<Gateway<G>>) {
        registry.accept::<Activate<G>>();
        registry.accept::<ReadEvents<G>>();
    }
}

impl<G: Grain> Handler<Activate<G>> for Gateway<G> {
    /// Single-shot (§5.4): if this node replicates and leads the name's shard,
    /// get-or-activate the host and return it; otherwise return `NotLeader(hint)`
    /// at once, leaving the redirect loop to the caller.
    async fn handle(
        &mut self,
        msg: Activate<G>,
        ctx: &Ctx<Gateway<G>>,
    ) -> Result<ActorRef<Host<G>>, GrainError> {
        let shard = self.shard_of(msg.name.key());
        if self.shard_map.journal(shard.index).is_some() && ctx.system().leads_shard(shard) {
            Ok(self.get_or_activate(msg.name, shard, ctx))
        } else {
            Err(GrainError::NotLeader(self.redirect_hint(shard, ctx)))
        }
    }
}

impl<G: Grain> Handler<ReadEvents<G>> for Gateway<G> {
    /// Serve one page of the grain's committed events from the shard's journal
    /// (spec §7.5), **without** get-or-activating the grain — a poll against a
    /// hibernated name leaves it hibernated (§10). Single-shot like
    /// [`Activate`]: a non-leader answers `NotLeader(hint)` at once.
    ///
    /// The local store alone cannot tell a committed record from a tentative
    /// one: a leader writes its own replica before the quorum resolves (§7.2),
    /// and a fresh leader's store may hold undecided records — or miss committed
    /// ones — until a `head` recovery read-repairs it (§8). So every page is
    /// bounded by the grain's **committed-head floor**: the resident
    /// activation's published head, or, for a hibernated name, a floor recovered
    /// once per leadership by [`warm`](Gateway::warm) — which also backfills the
    /// local store, making the read below complete. Records above the floor are
    /// simply not served yet; no observer sees an unacknowledged write (**G5**).
    ///
    /// The await below never leaves this node: `load` is a local, fence-free
    /// read of the leader's own store (§7.3), resolved from memory on both
    /// tiers, and the clamp bounds it to one page — so the serial gateway is
    /// held for a bounded local copy, not for I/O. The recovery, which *is*
    /// I/O, runs on a spawned task with the caller retrying ([`ReadReply::Warming`]).
    async fn handle(
        &mut self,
        msg: ReadEvents<G>,
        ctx: &Ctx<Gateway<G>>,
    ) -> Result<ReadReply, GrainError> {
        let shard = self.shard_of(msg.name.key());
        let journal = match self.shard_map.journal(shard.index) {
            Some(journal) if ctx.system().leads_shard(shard) => journal,
            _ => return Err(GrainError::NotLeader(self.redirect_hint(shard, ctx))),
        };
        let floor = match self.table.get(&msg.name) {
            // Resident (even if just stopped — the cell only ever holds
            // committed heads): the activation's published head is the floor.
            Some((activated_shard, _, committed)) if *activated_shard == shard.index => {
                let published = committed.load(Ordering::Acquire);
                if published == HEAD_UNPUBLISHED {
                    // Mid-rehydration: the head is being recovered; the caller
                    // retries, exactly as the input gate would have made it wait.
                    return Ok(ReadReply::Warming);
                }
                Seq::new(published)
            }
            // Hibernated (or never activated here): a recovered floor, valid
            // while the leadership it ran under persists (§8 — regaining is a
            // higher term) and no activation started since (`get_or_activate`
            // removes it).
            _ => {
                let Some(term) = journal.term() else {
                    return Err(GrainError::NotLeader(self.redirect_hint(shard, ctx)));
                };
                match self.floors.get(&msg.name) {
                    Some((under_shard, under_term, head))
                        if *under_shard == shard.index && *under_term == term =>
                    {
                        *head
                    }
                    _ => {
                        self.warm(msg.name.clone(), shard.index, term, journal, ctx);
                        return Ok(ReadReply::Warming);
                    }
                }
            }
        };
        // At least one record per page, or an empty non-head page could send
        // the caller's paging loop nowhere forever.
        let limit = (msg.limit as usize).clamp(1, READ_PAGE);
        let page = journal
            .load(&msg.name, msg.from, limit)
            .await
            .map_err(|e| GrainError::Unavailable(e.to_string()))?;
        let base = page.base;
        let loaded = page.records.len();
        // Serve only the committed prefix: slots above the floor are an
        // in-flight append's tentative writes (or undecided leftovers) and are
        // not served until a commit or recovery raises the floor.
        let records: Vec<(Seq, Vec<u8>)> = page
            .records
            .into_iter()
            .take_while(|(seq, _)| *seq <= floor)
            .collect();
        let at_head = records.len() < loaded || loaded < limit;
        let cursor = records.last().map(|(seq, _)| *seq).unwrap_or(msg.from);
        // The projection consumes the page in place — the serial gateway pays
        // one bounded local copy from the store, never a second allocation.
        let events =
            events_in_page(records).map_err(|e| GrainError::Unavailable(e.to_string()))?;
        Ok(ReadReply::Page(EventPage {
            events,
            cursor,
            at_head,
            base,
        }))
    }
}

impl<G: Grain> Gateway<G> {
    /// Launch the once-per-leadership floor recovery for a non-resident name
    /// (§7.5): `journal.head` — on the `Quorum` tier the §8 quorum read-repair,
    /// which both decides the committed head and backfills this leader's store;
    /// on the `Local` tier a local read. Runs on a spawned task so the serial
    /// gateway never awaits it; the result comes back as a local [`Recovered`]
    /// tell. Deduplicated by `warming`, so a poll storm spawns one recovery.
    fn warm(
        &mut self,
        name: GrainName,
        shard: u32,
        term: Term,
        journal: Arc<dyn crate::journal::DynGrainJournal>,
        ctx: &Ctx<Gateway<G>>,
    ) {
        if self.warming.contains_key(&name) {
            return;
        }
        self.warm_epoch += 1;
        let epoch = self.warm_epoch;
        self.warming.insert(name.clone(), epoch);
        let gateway = ctx.this();
        ctx.system().launch(Box::pin(async move {
            let head = journal.head(&name).await.ok();
            // Leadership must have held for the whole recovery: a term still
            // equal afterwards proves no other node could have appended while
            // it ran (§8). The serial handler re-checks on arrival, closing the
            // gap between this check and delivery.
            let head = head.filter(|_| journal.term() == Some(term));
            let _ = gateway
                .tell(Recovered {
                    name,
                    shard,
                    term,
                    head,
                    epoch,
                })
                .await;
        }));
    }
}

impl<G: Grain> Handler<Recovered> for Gateway<G> {
    /// Land a spawned floor recovery (§7.5). The floor is recorded only if it is
    /// still trustworthy on arrival: no activation of **this name** was spawned
    /// since it started (the rotated warming token — an activation's appends
    /// move the head, even if it already hibernated again; the check is
    /// per-name so activations of unrelated names cannot starve this read), no
    /// host activated under this shard is resident (its cell is the floor
    /// then — a **stale-shard** entry left by a split/merge must not block the
    /// recovered floor, or reads of the moved name would warm forever, §7.7),
    /// the name still resolves to the shard it was recovered under (§7.7), and
    /// that shard's term is unchanged (no other leadership could have
    /// appended, §8).
    async fn handle(&mut self, msg: Recovered, _ctx: &Ctx<Gateway<G>>) {
        let token = self.warming.remove(&msg.name);
        let Some(head) = msg.head else { return };
        if token != Some(msg.epoch)
            || self
                .table
                .get(&msg.name)
                .is_some_and(|(activated_shard, _, _)| *activated_shard == msg.shard)
        {
            return;
        }
        let shard = self.shard_of(msg.name.key());
        let current = self.shard_map.journal(shard.index).and_then(|j| j.term());
        if shard.index != msg.shard || current != Some(msg.term) {
            return;
        }
        // Bounded by the same knob as the host cache (§5.4). Overflow evicts
        // one arbitrary entry, not the map: a full map means many hibernated
        // names are being polled, and clearing would send every poller into a
        // simultaneous fresh quorum recovery; one eviction costs one name one
        // recovery.
        if self.floors.len() >= self.config.host_cache_capacity
            && let Some(evict) = self.floors.keys().next().cloned()
        {
            self.floors.remove(&evict);
        }
        self.floors.insert(msg.name, (msg.shard, msg.term, head));
    }
}

impl<G: Grain> Gateway<G> {
    /// The node a non-leading gateway redirects the caller to (§5.4). The believed
    /// leader when known **and still plausible against the committed map**;
    /// otherwise a replica from the consensus-agreed shard map — so the caller
    /// reaches a node that can serve or name the real leader. A replica
    /// mid-election (or a not-yet-committed map) hints itself, so the caller backs
    /// off and retries here until the shard settles.
    ///
    /// The plausibility check matters after a rebalance (§7.7): a node evicted
    /// from the shard keeps its last local Raft view — often naming *itself*
    /// leader — since the reconfigured group no longer heartbeats it. Trusting
    /// that stale view would pin every caller in a redirect loop back to this
    /// gateway for their whole deadline.
    fn redirect_hint(&self, shard: ShardId, ctx: &Ctx<Gateway<G>>) -> NodeId {
        let replicas = self.shard_map.replicas(shard.index).unwrap_or_default();
        if let Some(leader) = ctx.system().shard_leader(shard)
            && (replicas.is_empty() || replicas.contains(&leader))
        {
            return leader;
        }
        let me = ctx.system().node();
        match replicas.iter().find(|&&n| n != me) {
            // A non-replica: send the caller to a replica it can route through.
            Some(&replica) if !replicas.contains(&me) => replica,
            // A replica mid-election, or no map yet: retry us once the shard settles.
            _ => me,
        }
    }
}

impl<G: Grain> Handler<Terminated> for Gateway<G> {
    async fn handle(&mut self, signal: Terminated, _ctx: &Ctx<Gateway<G>>) {
        // The next message for that name re-activates it (§10).
        self.table.retain(|_, (_, host, _)| *host.id() != signal.id);
    }
}
