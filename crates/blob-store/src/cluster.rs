//! The `Clustered` tier: a replicate-by-hash, content-addressed store (spec §5.2,
//! §5.3).
//!
//! The fault-tolerant tier. Each blob is replicated to **R** owner nodes, and a
//! `put` is durable once **W ≤ R** of them have stored it (spec §5.2). It has no
//! leader and no quorum-intersection requirement: with immutable content, `W` and
//! `R` are independent durability and availability knobs, not a correctness
//! constraint (spec §4, contrast entity §8). A fan-out of immutable bytes, a
//! verified rank-order read, and a monotonic delete flag, with no write-time
//! read-repair (**B4**).
//!
//! A node running this tier owns its on-disk [`LocalBlobStore`], a [`BlobReplica`]
//! actor over it (so peers can reach it), and the node's [`TombstoneSet`] (shared
//! with the replica). It reaches a peer's replica through the receptionist and an
//! ordinary `ask` (spec §6): no transport of its own, so the deterministic
//! simulator drives the real code through the framework's seams (spec §8). Owner
//! selection is the pure [`placement`] function of the serving set (**B5**), so a
//! writer and a reader agree on where a blob lives with no directory lookup.
//!
//! This module is the data and delete paths; the reconcile loop that restores the
//! `R` margin after a node departs (spec §7, **B6**) lives in `reconcile`.

use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use actor_core::ActorRef;
use actor_core::CallError;
use actor_core::NodeId;
use futures::StreamExt;
use futures::stream::FuturesUnordered;

use crate::blob::BlobConfig;
use crate::blob::BlobError;
use crate::blob::BlobId;
use crate::blob::BlobStore;
use crate::blob::Namespace;
use crate::blob::slice;
use crate::blob::verify;
use crate::event::BlobEvent;
use crate::local::LocalBlobStore;
use crate::placement;
use crate::replica::BlobReplica;
use crate::replica::DeleteAck;
use crate::replica::DeleteNamespace;
use crate::replica::FetchBlob;
use crate::replica::HasBlob;
use crate::replica::StoreAck;
use crate::replica::StoreBlob;
use crate::replica::SyncTombstones;
use crate::replica::blob_replica_key;
use crate::system::BlobSystem;
use crate::tombstone::Tombstone;
use crate::tombstone::TombstoneSet;

/// How long a `put`/`delete` waits for its durability target before returning
/// [`BlobError::Unavailable`], and how long a `get`/`has` waits on each owner.
/// Modeled on entity's quorum/recover timeouts.
const PUT_TIMEOUT: Duration = Duration::from_secs(2);
const READ_TIMEOUT: Duration = Duration::from_secs(2);
const DELETE_TIMEOUT: Duration = Duration::from_secs(2);

/// The `Clustered` tier (spec §5.2). Clone is cheap (an `Arc<Inner>`).
pub struct ClusteredBlobStore<S: BlobSystem> {
    inner: Arc<Inner<S>>,
}

impl<S: BlobSystem> Clone for ClusteredBlobStore<S> {
    fn clone(&self) -> Self {
        ClusteredBlobStore {
            inner: self.inner.clone(),
        }
    }
}

/// The shared state of a [`ClusteredBlobStore`] node. Crate-visible so the
/// reconcile loop can hold a `Weak` to it.
pub(crate) struct Inner<S: BlobSystem> {
    pub(crate) system: S,
    pub(crate) config: BlobConfig,
    /// This node's own on-disk store: where it keeps the blobs it owns and the
    /// durable tombstones it has learned (spec §5.1, §5.3).
    pub(crate) local: LocalBlobStore,
    /// This node's cluster-wide awareness set, shared with its [`BlobReplica`] so
    /// the durable store and the in-memory set move together (spec §5.3).
    pub(crate) tombstones: TombstoneSet,
    pub(crate) self_node: NodeId,
}

/// How the tier reaches a node's [`BlobReplica`] (spec §6, no new transport): it
/// resolves the replica in the receptionist and `ask`s it. A replica on this node
/// resolves to the local actor, so an owner's store to itself is a local call with
/// no serialization (spec §5.2). Resolution is a local receptionist read on every
/// call, so it is never stale across a peer restart. Each method resolves before
/// returning its future, so the future owns everything it needs and can be drained
/// on the spawner after the caller has moved on.
impl<S: BlobSystem> Inner<S> {
    /// The replica registered on `node`; `Unreachable` if none is discovered.
    fn replica_on(&self, node: NodeId) -> Result<ActorRef<BlobReplica<S>>, CallError> {
        self.system
            .receptionist()
            .lookup(blob_replica_key::<S>())
            .into_vec()
            .into_iter()
            .find(|replica| replica.id().node() == node)
            .ok_or(CallError::Unreachable)
    }

    /// `msg` to `node`'s replica, resolved before the future is returned so the
    /// future owns everything it needs.
    fn ask<M>(
        &self,
        node: NodeId,
        msg: M,
        within: Duration,
    ) -> impl Future<Output = Result<M::Reply, CallError>> + Send + 'static
    where
        M: actor_core::Message,
        BlobReplica<S>: actor_core::Handler<M>,
    {
        let replica = self.replica_on(node);
        async move { replica?.ask_timeout(msg, within).await }
    }

    /// `StoreBlob` to `node`'s replica (spec §5.2). A `Stored` ack counts toward
    /// the `W` durability target; a `Deleted` ack surfaces the tombstone.
    pub(crate) fn store_blob(
        &self,
        node: NodeId,
        ns: Namespace,
        id: BlobId,
        bytes: Vec<u8>,
        within: Duration,
    ) -> impl Future<Output = Result<StoreAck, CallError>> + Send + 'static {
        self.ask(node, StoreBlob { ns, id, bytes }, within)
    }

    /// `FetchBlob` from `node`'s replica (spec §5.2): the whole raw bytes, or
    /// `None`. The caller verifies (B1).
    pub(crate) fn fetch_blob(
        &self,
        node: NodeId,
        ns: Namespace,
        id: BlobId,
        range: Option<Range<u64>>,
        within: Duration,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, CallError>> + Send + 'static {
        let fetched = self.ask(node, FetchBlob { ns, id, range }, within);
        async move {
            fetched
                .await
                .map(|found| found.map(serde_bytes::ByteBuf::into_vec))
        }
    }

    /// `HasBlob` on `node`'s replica (spec §7): gates a reconcile copy.
    pub(crate) fn has_blob(
        &self,
        node: NodeId,
        ns: Namespace,
        id: BlobId,
        within: Duration,
    ) -> impl Future<Output = Result<bool, CallError>> + Send + 'static {
        self.ask(node, HasBlob { ns, id }, within)
    }

    /// `DeleteNamespace` on `node`'s replica (spec §5.3): record the tombstone and
    /// sweep. Fanned out to every serving node, not only a blob's owners.
    pub(crate) fn delete_namespace(
        &self,
        node: NodeId,
        ns: Namespace,
        deleted_at: u64,
        within: Duration,
    ) -> impl Future<Output = Result<DeleteAck, CallError>> + Send + 'static {
        self.ask(node, DeleteNamespace { ns, deleted_at }, within)
    }

    /// `SyncTombstones` on `node`'s replica (spec §5.3, **B7**): pull its awareness
    /// set so a rejoining or lagging node learns the deletes it missed.
    pub(crate) fn sync_tombstones(
        &self,
        node: NodeId,
        within: Duration,
    ) -> impl Future<Output = Result<Vec<Tombstone>, CallError>> + Send + 'static {
        self.ask(node, SyncTombstones, within)
    }
}

impl<S: BlobSystem> ClusteredBlobStore<S> {
    /// Bring the `Clustered` tier up on `system` with this node's on-disk `local`
    /// store: spawn and register the node's [`BlobReplica`] so peers can reach it,
    /// and share the tombstone awareness set with it.
    ///
    /// `local` is opened by the caller (one store per node), so the deterministic
    /// simulator can give each node its own directory (spec §8).
    pub fn start(system: S, config: BlobConfig, local: LocalBlobStore) -> ClusteredBlobStore<S> {
        let tombstones = TombstoneSet::new();
        let replica = system.spawn(BlobReplica::<S>::new(local.clone(), tombstones.clone()));
        system
            .receptionist()
            .register(blob_replica_key::<S>(), &replica);
        let self_node = system.node();
        let inner = Arc::new(Inner {
            system,
            config,
            local,
            tombstones,
            self_node,
        });
        // Drive rebalancing and the tombstone anti-entropy pull in the background,
        // holding a `Weak` so the loop exits once the last store handle is dropped
        // (spec §7).
        inner
            .system
            .launch(Box::pin(crate::reconcile::reconcile_loop(Arc::downgrade(
                &inner,
            ))));
        ClusteredBlobStore { inner }
    }

    /// The node's tombstone awareness set (shared with its replica): consulted
    /// before a reconcile copy (spec §7).
    pub fn tombstones(&self) -> &TombstoneSet {
        &self.inner.tombstones
    }

    /// This node's on-disk store, which the reconcile loop enumerates to re-push
    /// under-replicated blobs to their current owners (spec §7, **B6**).
    pub fn local(&self) -> &LocalBlobStore {
        &self.inner.local
    }

    fn r(&self) -> usize {
        self.inner.config.replication_factor
    }

    fn w(&self) -> usize {
        self.inner.config.write_quorum
    }

    async fn run_put(&self, ns: &Namespace, bytes: Vec<u8>) -> Result<BlobId, BlobError> {
        // Enforce the size bound (spec §2): the whole-blob verify and whole-blob
        // fetch (B1) assume a bounded unit. The failure is non-retryable, but the
        // error model has no dedicated variant, so it is surfaced as `Unavailable`
        // with an explicit reason.
        if bytes.len() > self.inner.config.max_blob_bytes {
            return Err(BlobError::Unavailable(format!(
                "blob of {} bytes exceeds max_blob_bytes {}",
                bytes.len(),
                self.inner.config.max_blob_bytes
            )));
        }
        let id = BlobId::of(&bytes);
        // Refuse early if this node already knows the namespace is gone (spec §5.3).
        if self.inner.tombstones.contains(ns) {
            return Err(BlobError::Deleted(ns.clone()));
        }
        let members = self.inner.system.serving_members();
        let owners = placement::owners(&members, ns, &id, self.r());
        // B3 / §5.2: a `put` acks only once at least W copies are stored, so the
        // blob then survives the loss of any R − W owners. With fewer than W
        // candidate owners the target is unreachable, so refuse now rather than ack
        // at min(W, owners) copies, which would weaken the contract silently.
        let need = self.w();
        if owners.len() < need {
            return Err(BlobError::Unavailable(format!(
                "{} serving owners, below write quorum W={need}",
                owners.len()
            )));
        }

        // Fan a StoreBlob out to every owner; the local owner resolves to the
        // in-process replica (no serialization, spec §5.2). Acknowledge at `need`
        // and drain the rest in the background, off the latency path (entity §7.2).
        let mut acks = FuturesUnordered::new();
        for owner in owners {
            acks.push(
                self.inner
                    .store_blob(owner, ns.clone(), id, bytes.clone(), PUT_TIMEOUT),
            );
        }

        let mut stored = 0usize;
        while let Some(result) = acks.next().await {
            match result {
                Ok(StoreAck::Stored) => {
                    stored += 1;
                    if stored >= need {
                        self.inner
                            .system
                            .emit_blob_event(BlobEvent::PutAcked { ns: ns.clone(), id });
                        self.drain(acks);
                        return Ok(id);
                    }
                }
                // An owner that holds the tombstone refuses the store (spec §5.2):
                // the namespace is gone, so the put cannot succeed.
                Ok(StoreAck::Deleted) => return Err(BlobError::Deleted(ns.clone())),
                Err(_) => {} // unreachable/timeout owner; keep counting the rest
            }
        }
        Err(BlobError::Unavailable(format!(
            "stored {stored} of {need} required copies"
        )))
    }

    async fn run_get(
        &self,
        ns: &Namespace,
        id: &BlobId,
        range: Option<std::ops::Range<u64>>,
    ) -> Result<Vec<u8>, BlobError> {
        // A node that knows the namespace is tombstoned short-circuits without
        // asking anyone (spec §5.2).
        if self.inner.tombstones.contains(ns) {
            return Err(BlobError::Deleted(ns.clone()));
        }
        let members = self.inner.system.serving_members();

        // Ask the R owners in rank order, then widen to the remaining serving
        // members: during a membership transition a blob may still sit on a node
        // that was an owner under the previous view (placement is routing, not a
        // lease, spec §5.2), and widening lets a read find it.
        let mut candidates = placement::owners(&members, ns, id, self.r());
        for member in &members {
            if !candidates.contains(member) {
                candidates.push(*member);
            }
        }

        let mut saw_non_verifying = false;
        for node in candidates {
            // v1 fetches the whole blob (range reserved for §10) and verifies it
            // against `id` after transfer (B1) before slicing.
            match self
                .inner
                .fetch_blob(node, ns.clone(), *id, None, READ_TIMEOUT)
                .await
            {
                Ok(Some(bytes)) => match verify(id, &bytes) {
                    Ok(()) => {
                        self.inner.system.emit_blob_event(BlobEvent::GetVerified {
                            ns: ns.clone(),
                            id: *id,
                        });
                        return Ok(slice(bytes, range));
                    }
                    Err(_) => saw_non_verifying = true, // try the next owner
                },
                Ok(None) => {} // absent (or tombstoned) here; keep trying
                Err(_) => {}   // unreachable owner; keep trying
            }
        }

        if saw_non_verifying {
            // Some owner answered with bytes that did not verify; the data is
            // corrupt, not merely unreachable (spec §5.2).
            self.inner.system.emit_blob_event(BlobEvent::GetCorrupt {
                ns: ns.clone(),
                id: *id,
            });
            Err(BlobError::Corrupt(*id))
        } else {
            Err(BlobError::Unavailable(format!(
                "no owner yielded blob {id}"
            )))
        }
    }

    async fn run_has(&self, ns: &Namespace, id: &BlobId) -> Result<bool, BlobError> {
        if self.inner.tombstones.contains(ns) {
            return Ok(false);
        }
        let members = self.inner.system.serving_members();
        let owners = placement::owners(&members, ns, id, self.r());
        // §3 / B3: "durably present" means the durability target is met, at
        // least W confirmed copies, the same floor `put` refuses to ack below.
        // With fewer than W candidate owners the answer is `false`, not a quorum
        // of whatever is left, which would report a one-copy blob as durable.
        let need = self.w();

        let mut present = 0usize;
        for owner in owners {
            if let Ok(true) = self
                .inner
                .has_blob(owner, ns.clone(), *id, READ_TIMEOUT)
                .await
            {
                present += 1;
                if present >= need {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    async fn run_delete(&self, ns: &Namespace) -> Result<(), BlobError> {
        // Stamp the delete from the virtual clock so the event stream is
        // seed-reproducible (spec §8); the stamp is informational (spec §5.3).
        let deleted_at = self.inner.system.now().as_nanos();
        let members = self.inner.system.serving_members();

        // The tombstone must reach every serving node, not only a blob's owners,
        // because a namespace's blobs scatter cluster-wide (spec §5.2, §5.3). It is
        // durably anchored once W of the namespace's R tombstone owners have
        // recorded it.
        let anchor_owners = placement::tombstone_owners(&members, ns, self.r());
        let need_anchor = self.w().min(anchor_owners.len().max(1));

        // Record locally up front so this node refuses stores and short-circuits
        // gets immediately; the fan-out makes it durable and cluster-wide.
        self.inner.tombstones.insert(ns, deleted_at);

        let mut acks = FuturesUnordered::new();
        for node in &members {
            let node = *node;
            let is_anchor = anchor_owners.contains(&node);
            let fut = self
                .inner
                .delete_namespace(node, ns.clone(), deleted_at, DELETE_TIMEOUT);
            acks.push(async move { (is_anchor, fut.await) });
        }

        // Decided at W anchor acks, reported after the drain, so the rest of the
        // cluster sweeps on the delete's latency path (spec §5.3). The tombstone
        // itself is never reclaimed (spec §10).
        let mut anchored = 0usize;
        let mut result = Err(BlobError::Unavailable(format!(
            "anchored 0 of {need_anchor} tombstone owners"
        )));
        while let Some((is_anchor, ack)) = acks.next().await {
            if ack.is_ok() && is_anchor {
                anchored += 1;
                if anchored >= need_anchor && result.is_err() {
                    result = Ok(());
                }
            }
        }
        result
    }

    /// Drain the remaining store acks in the background, so the commit returns at
    /// `W` latency while every issued ask still runs to completion (spec §5.2).
    fn drain<F>(&self, mut pending: FuturesUnordered<F>)
    where
        F: Future<Output = Result<StoreAck, CallError>> + Send + 'static,
    {
        if pending.is_empty() {
            return;
        }
        self.inner.system.launch(Box::pin(
            async move { while pending.next().await.is_some() {} },
        ));
    }
}

impl<S: BlobSystem> BlobStore for ClusteredBlobStore<S> {
    fn put(
        &self,
        ns: &Namespace,
        bytes: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<BlobId, BlobError>> + Send {
        let this = self.clone();
        let ns = ns.clone();
        async move { this.run_put(&ns, bytes).await }
    }

    fn get(
        &self,
        ns: &Namespace,
        id: &BlobId,
        range: Option<std::ops::Range<u64>>,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, BlobError>> + Send {
        let this = self.clone();
        let ns = ns.clone();
        let id = *id;
        async move { this.run_get(&ns, &id, range).await }
    }

    fn has(
        &self,
        ns: &Namespace,
        id: &BlobId,
    ) -> impl std::future::Future<Output = Result<bool, BlobError>> + Send {
        let this = self.clone();
        let ns = ns.clone();
        let id = *id;
        async move { this.run_has(&ns, &id).await }
    }

    fn delete_namespace(
        &self,
        ns: &Namespace,
    ) -> impl std::future::Future<Output = Result<(), BlobError>> + Send {
        let this = self.clone();
        let ns = ns.clone();
        async move { this.run_delete(&ns).await }
    }
}
