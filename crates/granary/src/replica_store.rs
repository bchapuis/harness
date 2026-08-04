//! The per-node replica-store actor and the [`ReplicaTransport`] seam (spec §7.2, §8).
//!
//! Durability in the `Quorum` tier is a **per-grain quorum append** (§7.2): the
//! shard leader's [`QuorumReplicator`](crate::replicator::QuorumReplicator) fans a
//! grain's records out to the shard's replicas and reports them durable once a
//! quorum has stored them. This module is the replicas' side of that protocol and
//! the leader's way of reaching them, both built on actor messaging so granary adds
//! no transport (spec §2.2): a per-node [`ReplicaStore`] actor, registered in the
//! receptionist under one key per grain type, owns this node's [`GrainStore`], and
//! [`ActorReplicaTransport`] reaches a replica's store by an ordinary `ask` to that
//! node's actor (local on the leader's own replica, §5.2).

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::Duration;

use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::BoxFuture;
use actor_core::CallError;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::receptionist::Key;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

use crate::blobs::BlobId;
use crate::blocking::BlockingIo;
use crate::blocking::on_store;
use crate::grain::Grain;
use crate::grain::GrainName;
use crate::journal::Seq;
use crate::journal::Term;
use serde_bytes::ByteBuf;

use crate::store::BlobAck;
use crate::store::GrainStore;
use crate::store::ReadOutcome;
use crate::store::StoreAck;
use crate::store::WriteKind;
use crate::system::GranarySystem;

/// Per-grain-type interned key strings for the replica store. The receptionist keys
/// purely by string, so the replica store MUST register under a string distinct from
/// the gateway's (which is the bare `grain_type`, §5.3) — otherwise a `lookup` would
/// mix the two actor types. We derive `granary.replica/<grain_type>` and intern it
/// (one bounded leak per distinct type, as a runtime type name already permits, §A).
static REPLICA_KEY_IDS: LazyLock<Mutex<HashMap<&'static str, &'static str>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn replica_store_key_id(grain_type: &'static str) -> &'static str {
    let mut ids = REPLICA_KEY_IDS.lock().expect("replica key cache poisoned");
    if let Some(id) = ids.get(grain_type) {
        return id;
    }
    let id: &'static str = Box::leak(format!("granary.replica/{grain_type}").into_boxed_str());
    ids.insert(grain_type, id);
    id
}

/// The receptionist key the replica store for a grain type registers under: one
/// well-known key per type (distinct from the gateway's), one entry per node — the
/// replicator looks a replica node's store up here, the way routing looks a gateway
/// up (spec §5.3).
pub(crate) fn replica_store_key<G: Grain>(grain_type: &'static str) -> Key<ReplicaStore<G>> {
    Key::new(replica_store_key_id(grain_type))
}

/// Quorum-append a grain's records to one replica, fenced by the shard `term`
/// (spec §7.2, §8). The reply is the replica's [`StoreAck`].
#[derive(Serialize, Deserialize)]
pub(crate) struct StoreRecord {
    pub(crate) shard: u32,
    pub(crate) grain: GrainName,
    pub(crate) after: Seq,
    pub(crate) term: Term,
    /// `ByteBuf` rather than `Vec<u8>` so the codec is told these are bytes: it
    /// derefs to `Vec<u8>` and encodes identically (see `wire_bytes.rs`), but it
    /// reaches `serialize_bytes`/`deserialize_bytes` instead of serde's default
    /// element-at-a-time sequence path.
    pub(crate) records: Vec<ByteBuf>,
    /// A recovery write-back (read-repair, §8) versus a normal append.
    pub(crate) kind: WriteKind,
}

impl Message for StoreRecord {
    type Reply = StoreAck;
    const MANIFEST: Manifest = Manifest::new("granary.StoreRecord");
}

/// Fenced recovery read of one replica's view of a grain (spec §8): promise not to
/// accept a lower shard term, then return every occupied slot with its term and the
/// latest snapshot. The reply is a [`ReadOutcome`] (`Prepared` or `Fenced`).
#[derive(Serialize, Deserialize)]
pub(crate) struct ReadGrain {
    pub(crate) shard: u32,
    pub(crate) grain: GrainName,
    pub(crate) term: Term,
}

impl Message for ReadGrain {
    type Reply = ReadOutcome;
    const MANIFEST: Manifest = Manifest::new("granary.ReadGrain");
}

/// Quorum-store a grain snapshot to one replica, fenced by the shard `term`
/// (spec §9). The reply is the replica's [`StoreAck`]. A
/// [`WriteKind::Transfer`] skips the fence — the split/merge driver landing a
/// moved grain's snapshot under the destination shard's keys (§7.7).
#[derive(Serialize, Deserialize)]
pub(crate) struct StoreSnapshot {
    pub(crate) shard: u32,
    pub(crate) grain: GrainName,
    pub(crate) at: Seq,
    pub(crate) term: Term,
    #[serde(with = "serde_bytes")]
    pub(crate) state: Vec<u8>,
    pub(crate) kind: WriteKind,
}

impl Message for StoreSnapshot {
    type Reply = StoreAck;
    const MANIFEST: Manifest = Manifest::new("granary.StoreSnapshot");
}

/// Tighten one replica's per-shard **append bound** (spec §7.7): durably refuse
/// every future append whose grain hash is `>= from`, at any term. The split
/// (or merge) driver fans this to the shard's replicas and proceeds only once a
/// majority has acked — from then on no append to the moved range can assemble
/// a write quorum, the store half of G15. Idempotent and monotone (the bound
/// only tightens). The ask resolving is the durable acknowledgement.
#[derive(Serialize, Deserialize)]
pub(crate) struct SealRange {
    pub(crate) shard: u32,
    pub(crate) from: u64,
}

impl Message for SealRange {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("granary.SealRange");
}

/// Store one immutable, content-addressed blob on a replica (durable-workspace
/// design). No `after`, no `term`, no `repair`: a content hash names exactly one byte
/// sequence, so there is nothing to fence or order and no `Fenced`/`Stale` outcome to
/// report. The reply is a [`BlobAck`]: the one thing a replica can still refuse is
/// storing at all, and a quorum that could not distinguish that from success would
/// count a failed replica as a durable copy (**G18**).
#[derive(Serialize, Deserialize)]
pub(crate) struct StoreBlob {
    pub(crate) shard: u32,
    pub(crate) grain: GrainName,
    pub(crate) id: BlobId,
    /// The mebibyte this whole exercise is about: one encode on the leader and one
    /// decode on every peer, per block, on the create path.
    #[serde(with = "serde_bytes")]
    pub(crate) bytes: Vec<u8>,
}

impl Message for StoreBlob {
    type Reply = BlobAck;
    const MANIFEST: Manifest = Manifest::new("granary.StoreBlob");
}

/// Fetch one blob's bytes from a replica, or `None` if it does not hold it. The
/// caller verifies the bytes against the id (B1), so a misdelivery is detectable.
#[derive(Serialize, Deserialize)]
pub(crate) struct FetchBlob {
    pub(crate) shard: u32,
    pub(crate) grain: GrainName,
    pub(crate) id: BlobId,
}

impl Message for FetchBlob {
    /// `ByteBuf` for the same reason `StoreBlob::bytes` uses it — this is the read
    /// half of the same mebibyte, and a restore fetches every block of an image, so
    /// the decode is paid per block there too. The transport's own signature keeps
    /// `Vec<u8>`: unwrapping the newtype is a move, not a copy.
    type Reply = Option<ByteBuf>;
    const MANIFEST: Manifest = Manifest::new("granary.FetchBlob");
}

/// Whether a replica holds one blob.
#[derive(Serialize, Deserialize)]
pub(crate) struct HasBlob {
    pub(crate) shard: u32,
    pub(crate) grain: GrainName,
    pub(crate) id: BlobId,
}

impl Message for HasBlob {
    type Reply = bool;
    const MANIFEST: Manifest = Manifest::new("granary.HasBlob");
}

/// Reclaim a grain's blobs on a replica (durable-workspace design): `retain = None`
/// drops the whole area (destroy), `retain = Some(ids)` keeps only those (the
/// mark-from-roots sweep). Idempotent; the reply is `()` (the ask resolving is the
/// acknowledgement).
#[derive(Serialize, Deserialize)]
pub(crate) struct SweepBlobs {
    pub(crate) shard: u32,
    pub(crate) grain: GrainName,
    pub(crate) retain: Option<Vec<BlobId>>,
}

impl Message for SweepBlobs {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("granary.SweepBlobs");
}

/// Enumerate every grain a replica holds anything for under one shard — the
/// migration driver's grain discovery (replica-set migration, §7.7). Read from a
/// quorum of the shard's replicas so no committed grain is missed.
#[derive(Serialize, Deserialize)]
pub(crate) struct ListGrains {
    pub(crate) shard: u32,
}

impl Message for ListGrains {
    type Reply = Vec<GrainName>;
    const MANIFEST: Manifest = Manifest::new("granary.ListGrains");
}

/// Enumerate one grain's blob ids on a replica — the migration driver's source
/// list when copying a grain's blob area to a new replica (§7.7).
#[derive(Serialize, Deserialize)]
pub(crate) struct ListBlobs {
    pub(crate) shard: u32,
    pub(crate) grain: GrainName,
}

impl Message for ListBlobs {
    type Reply = Vec<BlobId>;
    const MANIFEST: Manifest = Manifest::new("granary.ListBlobs");
}

/// The node-local replica store for grain type `G` (spec §7.2): a thin actor over
/// this node's [`GrainStore`], reachable across the cluster so the shard leader's
/// replicator can quorum-append to it and read it back for recovery. One per node
/// per grain type (like the gateway), registered under [`replica_store_key`].
pub(crate) struct ReplicaStore<G: Grain> {
    store: Arc<dyn GrainStore>,
    /// Where this replica's blocking store calls run (§7.4). Every durable write in
    /// the cluster that is not the leader's own copy arrives here, so this is the
    /// hottest fsync path there is: run inline it would block the async worker that
    /// is also driving this node's Raft heartbeats and every other shard's quorum
    /// wait (see [`crate::blocking`]).
    io: Arc<dyn BlockingIo>,
    _marker: PhantomData<fn() -> G>,
}

impl<G: Grain> ReplicaStore<G> {
    pub(crate) fn new(store: Arc<dyn GrainStore>, io: Arc<dyn BlockingIo>) -> ReplicaStore<G> {
        ReplicaStore {
            store,
            io,
            _marker: PhantomData,
        }
    }
}

impl<G: Grain> Actor for ReplicaStore<G> {
    type System = G::System;

    fn register(registry: &mut HandlerRegistry<ReplicaStore<G>>) {
        registry.accept::<StoreRecord>();
        registry.accept::<ReadGrain>();
        registry.accept::<StoreSnapshot>();
        registry.accept::<SealRange>();
        registry.accept::<StoreBlob>();
        registry.accept::<FetchBlob>();
        registry.accept::<HasBlob>();
        registry.accept::<SweepBlobs>();
        registry.accept::<ListGrains>();
        registry.accept::<ListBlobs>();
    }
}

// Every handler's reply IS the peer's acknowledgement, so each returns only what the
// store has already made durable: a reply released early would let the leader count
// this replica toward a quorum for something this node has not stored (**G14**). The
// store settles the write before it returns, so the durability is the offloaded call's
// own — there is nothing further to await here.
impl<G: Grain> Handler<StoreRecord> for ReplicaStore<G> {
    async fn handle(&mut self, msg: StoreRecord, _ctx: &Ctx<ReplicaStore<G>>) -> StoreAck {
        on_store(&self.io, &self.store, move |store| {
            store.store_record(
                msg.shard,
                &msg.grain,
                msg.after,
                msg.term,
                // Rebuilding the outer `Vec` of pointers, not the records: a
                // `ByteBuf` is a newtype over the same allocation, so each
                // `into_vec` moves rather than copies.
                msg.records.into_iter().map(ByteBuf::into_vec).collect(),
                msg.kind,
            )
        })
        .await
    }
}

impl<G: Grain> Handler<ReadGrain> for ReplicaStore<G> {
    async fn handle(&mut self, msg: ReadGrain, _ctx: &Ctx<ReplicaStore<G>>) -> ReadOutcome {
        on_store(&self.io, &self.store, move |store| {
            store.prepare(msg.shard, &msg.grain, msg.term)
        })
        .await
    }
}

impl<G: Grain> Handler<StoreSnapshot> for ReplicaStore<G> {
    async fn handle(&mut self, msg: StoreSnapshot, _ctx: &Ctx<ReplicaStore<G>>) -> StoreAck {
        on_store(&self.io, &self.store, move |store| {
            store.store_snapshot(msg.shard, &msg.grain, msg.at, msg.term, msg.state, msg.kind)
        })
        .await
    }
}

impl<G: Grain> Handler<SealRange> for ReplicaStore<G> {
    async fn handle(&mut self, msg: SealRange, _ctx: &Ctx<ReplicaStore<G>>) {
        // The bound must be durable before this ask resolves, or a majority could
        // report itself sealed while a restart would forget the promise (**G15**).
        on_store(&self.io, &self.store, move |store| {
            store.seal_range(msg.shard, msg.from)
        })
        .await;
    }
}

impl<G: Grain> Handler<StoreBlob> for ReplicaStore<G> {
    async fn handle(&mut self, msg: StoreBlob, _ctx: &Ctx<ReplicaStore<G>>) -> BlobAck {
        on_store(&self.io, &self.store, move |store| {
            store.put_blob(msg.shard, &msg.grain, msg.id, msg.bytes)
        })
        .await
    }
}

impl<G: Grain> Handler<FetchBlob> for ReplicaStore<G> {
    async fn handle(&mut self, msg: FetchBlob, _ctx: &Ctx<ReplicaStore<G>>) -> Option<ByteBuf> {
        self.store
            .get_blob(msg.shard, &msg.grain, msg.id)
            .map(ByteBuf::from)
    }
}

impl<G: Grain> Handler<HasBlob> for ReplicaStore<G> {
    async fn handle(&mut self, msg: HasBlob, _ctx: &Ctx<ReplicaStore<G>>) -> bool {
        self.store.has_blob(msg.shard, &msg.grain, msg.id)
    }
}

impl<G: Grain> Handler<SweepBlobs> for ReplicaStore<G> {
    async fn handle(&mut self, msg: SweepBlobs, _ctx: &Ctx<ReplicaStore<G>>) {
        // Offloaded like the writes: a sweep unlinks one file per reclaimed blob, so
        // a grain that has churned a large disk image makes this thousands of
        // synchronous unlinks against the same device the durability path needs.
        on_store(&self.io, &self.store, move |store| match msg.retain {
            None => store.delete_blobs(msg.shard, &msg.grain),
            Some(ids) => store.retain_blobs(msg.shard, &msg.grain, &ids.into_iter().collect()),
        })
        .await;
    }
}

impl<G: Grain> Handler<ListGrains> for ReplicaStore<G> {
    async fn handle(&mut self, msg: ListGrains, _ctx: &Ctx<ReplicaStore<G>>) -> Vec<GrainName> {
        self.store.grains(msg.shard)
    }
}

impl<G: Grain> Handler<ListBlobs> for ReplicaStore<G> {
    async fn handle(&mut self, msg: ListBlobs, _ctx: &Ctx<ReplicaStore<G>>) -> Vec<BlobId> {
        self.store.blob_ids(msg.shard, &msg.grain)
    }
}

/// How the replicator reaches a shard's replica stores (spec §7.2). Object-safe, so
/// the journal stays generic over just the consensus type `R` and never names `G`:
/// the one G-aware piece (the typed [`ReplicaStore`] ref and its receptionist key)
/// lives behind this seam, built in `granary_named` where `G` is known.
pub trait ReplicaTransport: Send + Sync + 'static {
    #[allow(clippy::too_many_arguments)]
    fn store_record(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        after: Seq,
        term: Term,
        records: Vec<Vec<u8>>,
        kind: WriteKind,
        within: Duration,
    ) -> BoxFuture<'static, Result<StoreAck, CallError>>;

    fn read_grain(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        term: Term,
        within: Duration,
    ) -> BoxFuture<'static, Result<ReadOutcome, CallError>>;

    #[allow(clippy::too_many_arguments)]
    fn store_snapshot(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        at: Seq,
        term: Term,
        state: Vec<u8>,
        kind: WriteKind,
        within: Duration,
    ) -> BoxFuture<'static, Result<StoreAck, CallError>>;

    /// Tighten a replica's per-shard append bound (split/merge seal, §7.7): the
    /// ask resolving means the bound is durable there.
    fn seal_range(
        &self,
        node: NodeId,
        shard: u32,
        from: u64,
        within: Duration,
    ) -> BoxFuture<'static, Result<(), CallError>>;

    /// Store one immutable blob on a replica (durable-workspace design): unfenced,
    /// unordered — the immutable subset of [`store_record`](ReplicaTransport::store_record).
    /// The [`BlobAck`] distinguishes a stored copy from a replica that could not
    /// store it, so only real copies count toward the quorum (**G18**).
    fn store_blob(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        id: BlobId,
        bytes: Vec<u8>,
        within: Duration,
    ) -> BoxFuture<'static, Result<BlobAck, CallError>>;

    /// Fetch one blob's bytes from a replica, or `None` if it lacks it.
    fn fetch_blob(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        id: BlobId,
        within: Duration,
    ) -> BoxFuture<'static, Result<Option<Vec<u8>>, CallError>>;

    /// Whether a replica holds one blob.
    fn has_blob(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        id: BlobId,
        within: Duration,
    ) -> BoxFuture<'static, Result<bool, CallError>>;

    /// Reclaim a grain's blobs on a replica: `retain = None` drops the area,
    /// `retain = Some(ids)` keeps only those (the mark-from-roots sweep).
    fn sweep_blobs(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        retain: Option<Vec<BlobId>>,
        within: Duration,
    ) -> BoxFuture<'static, Result<(), CallError>>;

    /// Enumerate every grain a replica holds under one shard (migration, §7.7).
    fn list_grains(
        &self,
        node: NodeId,
        shard: u32,
        within: Duration,
    ) -> BoxFuture<'static, Result<Vec<GrainName>, CallError>>;

    /// Enumerate one grain's blob ids on a replica (migration, §7.7).
    fn list_blobs(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        within: Duration,
    ) -> BoxFuture<'static, Result<Vec<BlobId>, CallError>>;

    /// Launch a detached background task (spec §7.2). The replicator uses it to drain
    /// the straggler peer asks of an append that already committed on a quorum, so the
    /// commit returns at quorum latency while every issued ask still runs to
    /// completion — its `AskIssued`/`AskOutcome` bracket closes, preserving
    /// no-silent-loss (§14). Backed by [`GranarySystem::launch`](crate::GranarySystem).
    fn launch(&self, task: BoxFuture<'static, ()>);
}

/// The actor-messaging [`ReplicaTransport`] (spec §2.2: no new transport): it
/// resolves a node's [`ReplicaStore`] in the receptionist and `ask`s it. A store on
/// this node resolves to the local actor, so the leader's append to its own replica
/// is a local call with no serialization (§5.2). Resolution is a local receptionist
/// read each call, never stale across a peer restart (a restarted node re-registers a
/// fresh ref).
pub(crate) struct ActorReplicaTransport<G: Grain> {
    system: G::System,
    grain_type: &'static str,
}

impl<G: Grain> ActorReplicaTransport<G> {
    pub(crate) fn new(system: G::System, grain_type: &'static str) -> ActorReplicaTransport<G> {
        ActorReplicaTransport { system, grain_type }
    }

    /// The replica store registered on `node`, if discovered (spec §5.3).
    fn resolve(&self, node: NodeId) -> Option<ActorRef<ReplicaStore<G>>> {
        self.system
            .receptionist()
            .lookup(replica_store_key::<G>(self.grain_type))
            .into_vec()
            .into_iter()
            .find(|store| store.id().node() == node)
    }

    /// Resolve `node`'s replica store and `ask` it, or `Unreachable` if it is not
    /// discovered — the one shape every [`ReplicaTransport`] method shares (§7.2).
    fn ask<M>(
        &self,
        node: NodeId,
        msg: M,
        within: Duration,
    ) -> BoxFuture<'static, Result<M::Reply, CallError>>
    where
        M: Message,
        ReplicaStore<G>: Handler<M>,
    {
        let store = self.resolve(node);
        Box::pin(async move {
            store
                .ok_or(CallError::Unreachable)?
                .ask_timeout(msg, within)
                .await
        })
    }
}

impl<G: Grain> ReplicaTransport for ActorReplicaTransport<G> {
    #[allow(clippy::too_many_arguments)]
    fn store_record(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        after: Seq,
        term: Term,
        records: Vec<Vec<u8>>,
        kind: WriteKind,
        within: Duration,
    ) -> BoxFuture<'static, Result<StoreAck, CallError>> {
        self.ask(
            node,
            StoreRecord {
                shard,
                grain,
                after,
                term,
                // As in the handler: a move per record, no bytes copied. The
                // transport's own signature stays `Vec<Vec<u8>>` so the wire type
                // does not leak into every caller.
                records: records.into_iter().map(ByteBuf::from).collect(),
                kind,
            },
            within,
        )
    }

    fn read_grain(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        term: Term,
        within: Duration,
    ) -> BoxFuture<'static, Result<ReadOutcome, CallError>> {
        self.ask(node, ReadGrain { shard, grain, term }, within)
    }

    #[allow(clippy::too_many_arguments)]
    fn store_snapshot(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        at: Seq,
        term: Term,
        state: Vec<u8>,
        kind: WriteKind,
        within: Duration,
    ) -> BoxFuture<'static, Result<StoreAck, CallError>> {
        self.ask(
            node,
            StoreSnapshot {
                shard,
                grain,
                at,
                term,
                state,
                kind,
            },
            within,
        )
    }

    fn seal_range(
        &self,
        node: NodeId,
        shard: u32,
        from: u64,
        within: Duration,
    ) -> BoxFuture<'static, Result<(), CallError>> {
        self.ask(node, SealRange { shard, from }, within)
    }

    fn store_blob(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        id: BlobId,
        bytes: Vec<u8>,
        within: Duration,
    ) -> BoxFuture<'static, Result<BlobAck, CallError>> {
        self.ask(
            node,
            StoreBlob {
                shard,
                grain,
                id,
                bytes,
            },
            within,
        )
    }

    fn fetch_blob(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        id: BlobId,
        within: Duration,
    ) -> BoxFuture<'static, Result<Option<Vec<u8>>, CallError>> {
        let reply = self.ask(node, FetchBlob { shard, grain, id }, within);
        Box::pin(async move {
            reply
                .await
                .map(|found: Option<ByteBuf>| found.map(ByteBuf::into_vec))
        })
    }

    fn has_blob(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        id: BlobId,
        within: Duration,
    ) -> BoxFuture<'static, Result<bool, CallError>> {
        self.ask(node, HasBlob { shard, grain, id }, within)
    }

    fn sweep_blobs(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        retain: Option<Vec<BlobId>>,
        within: Duration,
    ) -> BoxFuture<'static, Result<(), CallError>> {
        self.ask(
            node,
            SweepBlobs {
                shard,
                grain,
                retain,
            },
            within,
        )
    }

    fn list_grains(
        &self,
        node: NodeId,
        shard: u32,
        within: Duration,
    ) -> BoxFuture<'static, Result<Vec<GrainName>, CallError>> {
        self.ask(node, ListGrains { shard }, within)
    }

    fn list_blobs(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        within: Duration,
    ) -> BoxFuture<'static, Result<Vec<BlobId>, CallError>> {
        self.ask(node, ListBlobs { shard, grain }, within)
    }

    fn launch(&self, task: BoxFuture<'static, ()>) {
        self.system.launch(task);
    }
}
