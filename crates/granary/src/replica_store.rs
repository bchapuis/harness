//! The per-node replica-store actor and the [`ReplicaTransport`] seam (spec §7.2, §8).
//!
//! Durability in the `Quorum` tier is a **per-grain quorum append** (§7.2): the
//! shard leader's [`QuorumReplicator`](crate::replicator::QuorumReplicator) fans a
//! grain's records out to the shard's replicas and reports them durable once a
//! quorum has stored them. This module is the replicas' side of that protocol and
//! the leader's way of reaching them — both built on **actor messaging**, so
//! granary adds no transport (spec §2.2): a per-node [`ReplicaStore`] actor,
//! registered in the receptionist under one key per grain type, owns this node's
//! [`GrainStore`], and [`ActorReplicaTransport`] reaches a replica's store by an
//! ordinary `ask` to that node's actor (local on the leader's own replica, §5.2).

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
use crate::grain::Grain;
use crate::grain::GrainName;
use crate::journal::Seq;
use crate::store::GrainStore;
use crate::store::ReadOutcome;
use crate::store::StoreAck;
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
    pub(crate) term: u64,
    pub(crate) records: Vec<Vec<u8>>,
    /// A recovery write-back (read-repair, §8) versus a normal append: a normal
    /// append onto a stale head is rejected with `Stale`; a write-back is not.
    pub(crate) repair: bool,
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
    pub(crate) term: u64,
}

impl Message for ReadGrain {
    type Reply = ReadOutcome;
    const MANIFEST: Manifest = Manifest::new("granary.ReadGrain");
}

/// Quorum-store a grain snapshot to one replica, fenced by the shard `term`
/// (spec §9). The reply is the replica's [`StoreAck`].
#[derive(Serialize, Deserialize)]
pub(crate) struct StoreSnapshot {
    pub(crate) shard: u32,
    pub(crate) grain: GrainName,
    pub(crate) at: Seq,
    pub(crate) term: u64,
    pub(crate) state: Vec<u8>,
}

impl Message for StoreSnapshot {
    type Reply = StoreAck;
    const MANIFEST: Manifest = Manifest::new("granary.StoreSnapshot");
}

/// Store one immutable, content-addressed blob on a replica (durable-workspace
/// design). No `after`, no `term`, no `repair`: nothing to fence or order, and so
/// no ack variants — an immutable blob has no `Fenced`/`Stale` outcome to report (a
/// content hash names exactly one byte sequence), so the reply is just `()`: the ask
/// resolving is the durable acknowledgement (the blob path is the record path's
/// immutable subset).
#[derive(Serialize, Deserialize)]
pub(crate) struct StoreBlob {
    pub(crate) shard: u32,
    pub(crate) grain: GrainName,
    pub(crate) id: BlobId,
    pub(crate) bytes: Vec<u8>,
}

impl Message for StoreBlob {
    type Reply = ();
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
    type Reply = Option<Vec<u8>>;
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

/// The node-local replica store for grain type `G` (spec §7.2): a thin actor over
/// this node's [`GrainStore`], reachable across the cluster so the shard leader's
/// replicator can quorum-append to it and read it back for recovery. One per node
/// per grain type (like the gateway), registered under [`replica_store_key`].
pub(crate) struct ReplicaStore<G: Grain> {
    store: Arc<dyn GrainStore>,
    _marker: PhantomData<fn() -> G>,
}

impl<G: Grain> ReplicaStore<G> {
    pub(crate) fn new(store: Arc<dyn GrainStore>) -> ReplicaStore<G> {
        ReplicaStore {
            store,
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
        registry.accept::<StoreBlob>();
        registry.accept::<FetchBlob>();
        registry.accept::<HasBlob>();
        registry.accept::<SweepBlobs>();
    }
}

impl<G: Grain> Handler<StoreRecord> for ReplicaStore<G> {
    async fn handle(&mut self, msg: StoreRecord, _ctx: &Ctx<ReplicaStore<G>>) -> StoreAck {
        self.store.store_record(
            msg.shard,
            &msg.grain,
            msg.after,
            msg.term,
            msg.records,
            msg.repair,
        )
    }
}

impl<G: Grain> Handler<ReadGrain> for ReplicaStore<G> {
    async fn handle(&mut self, msg: ReadGrain, _ctx: &Ctx<ReplicaStore<G>>) -> ReadOutcome {
        self.store.prepare(msg.shard, &msg.grain, msg.term)
    }
}

impl<G: Grain> Handler<StoreSnapshot> for ReplicaStore<G> {
    async fn handle(&mut self, msg: StoreSnapshot, _ctx: &Ctx<ReplicaStore<G>>) -> StoreAck {
        self.store
            .store_snapshot(msg.shard, &msg.grain, msg.at, msg.term, msg.state)
    }
}

impl<G: Grain> Handler<StoreBlob> for ReplicaStore<G> {
    async fn handle(&mut self, msg: StoreBlob, _ctx: &Ctx<ReplicaStore<G>>) {
        self.store
            .put_blob(msg.shard, &msg.grain, msg.id, msg.bytes);
    }
}

impl<G: Grain> Handler<FetchBlob> for ReplicaStore<G> {
    async fn handle(&mut self, msg: FetchBlob, _ctx: &Ctx<ReplicaStore<G>>) -> Option<Vec<u8>> {
        self.store.get_blob(msg.shard, &msg.grain, msg.id)
    }
}

impl<G: Grain> Handler<HasBlob> for ReplicaStore<G> {
    async fn handle(&mut self, msg: HasBlob, _ctx: &Ctx<ReplicaStore<G>>) -> bool {
        self.store.has_blob(msg.shard, &msg.grain, msg.id)
    }
}

impl<G: Grain> Handler<SweepBlobs> for ReplicaStore<G> {
    async fn handle(&mut self, msg: SweepBlobs, _ctx: &Ctx<ReplicaStore<G>>) {
        match msg.retain {
            None => self.store.delete_blobs(msg.shard, &msg.grain),
            Some(ids) => self
                .store
                .retain_blobs(msg.shard, &msg.grain, &ids.into_iter().collect()),
        }
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
        term: u64,
        records: Vec<Vec<u8>>,
        repair: bool,
        within: Duration,
    ) -> BoxFuture<'static, Result<StoreAck, CallError>>;

    fn read_grain(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        term: u64,
        within: Duration,
    ) -> BoxFuture<'static, Result<ReadOutcome, CallError>>;

    #[allow(clippy::too_many_arguments)]
    fn store_snapshot(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        at: Seq,
        term: u64,
        state: Vec<u8>,
        within: Duration,
    ) -> BoxFuture<'static, Result<StoreAck, CallError>>;

    /// Store one immutable blob on a replica (durable-workspace design): unfenced,
    /// unordered — the immutable subset of [`store_record`](ReplicaTransport::store_record).
    fn store_blob(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        id: BlobId,
        bytes: Vec<u8>,
        within: Duration,
    ) -> BoxFuture<'static, Result<(), CallError>>;

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
/// read each call — cheap, and never stale across a peer restart (a restarted node
/// re-registers a fresh ref).
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
}

impl<G: Grain> ReplicaTransport for ActorReplicaTransport<G> {
    #[allow(clippy::too_many_arguments)]
    fn store_record(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        after: Seq,
        term: u64,
        records: Vec<Vec<u8>>,
        repair: bool,
        within: Duration,
    ) -> BoxFuture<'static, Result<StoreAck, CallError>> {
        let store = self.resolve(node);
        Box::pin(async move {
            let store = store.ok_or(CallError::Unreachable)?;
            store
                .ask_timeout(
                    StoreRecord {
                        shard,
                        grain,
                        after,
                        term,
                        records,
                        repair,
                    },
                    within,
                )
                .await
        })
    }

    fn read_grain(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        term: u64,
        within: Duration,
    ) -> BoxFuture<'static, Result<ReadOutcome, CallError>> {
        let store = self.resolve(node);
        Box::pin(async move {
            let store = store.ok_or(CallError::Unreachable)?;
            store
                .ask_timeout(ReadGrain { shard, grain, term }, within)
                .await
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn store_snapshot(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        at: Seq,
        term: u64,
        state: Vec<u8>,
        within: Duration,
    ) -> BoxFuture<'static, Result<StoreAck, CallError>> {
        let store = self.resolve(node);
        Box::pin(async move {
            let store = store.ok_or(CallError::Unreachable)?;
            store
                .ask_timeout(
                    StoreSnapshot {
                        shard,
                        grain,
                        at,
                        term,
                        state,
                    },
                    within,
                )
                .await
        })
    }

    fn store_blob(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        id: BlobId,
        bytes: Vec<u8>,
        within: Duration,
    ) -> BoxFuture<'static, Result<(), CallError>> {
        let store = self.resolve(node);
        Box::pin(async move {
            let store = store.ok_or(CallError::Unreachable)?;
            store
                .ask_timeout(
                    StoreBlob {
                        shard,
                        grain,
                        id,
                        bytes,
                    },
                    within,
                )
                .await
        })
    }

    fn fetch_blob(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        id: BlobId,
        within: Duration,
    ) -> BoxFuture<'static, Result<Option<Vec<u8>>, CallError>> {
        let store = self.resolve(node);
        Box::pin(async move {
            let store = store.ok_or(CallError::Unreachable)?;
            store
                .ask_timeout(FetchBlob { shard, grain, id }, within)
                .await
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
        let store = self.resolve(node);
        Box::pin(async move {
            let store = store.ok_or(CallError::Unreachable)?;
            store
                .ask_timeout(HasBlob { shard, grain, id }, within)
                .await
        })
    }

    fn sweep_blobs(
        &self,
        node: NodeId,
        shard: u32,
        grain: GrainName,
        retain: Option<Vec<BlobId>>,
        within: Duration,
    ) -> BoxFuture<'static, Result<(), CallError>> {
        let store = self.resolve(node);
        Box::pin(async move {
            let store = store.ok_or(CallError::Unreachable)?;
            store
                .ask_timeout(
                    SweepBlobs {
                        shard,
                        grain,
                        retain,
                    },
                    within,
                )
                .await
        })
    }

    fn launch(&self, task: BoxFuture<'static, ()>) {
        self.system.launch(task);
    }
}
