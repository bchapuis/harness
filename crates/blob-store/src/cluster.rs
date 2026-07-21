//! The `Clustered` tier: a replicate-by-hash, content-addressed store (spec §5.2,
//! §5.3).
//!
//! The fault-tolerant tier. Each blob is replicated to **R** owner nodes, and a
//! `put` is durable once **W ≤ R** of them have stored it (spec §5.2). It has no
//! leader and no quorum-intersection requirement: with immutable content, `W` and
//! `R` are independent durability and availability knobs, not a correctness
//! constraint (spec §4, contrast granary §8). It is the grain Quorum replicator
//! (granary §7.2) with its hard half removed — a fan-out of immutable bytes, a
//! verified rank-order read, and a monotonic delete flag, with no election, no
//! term, and no write-time read-repair (**B4**).
//!
//! A node running this tier owns its on-disk [`LocalBlobStore`], a [`BlobReplica`]
//! actor over it (so peers can reach it), the node's [`TombstoneSet`] (shared with
//! the replica), and an [`AnchorTracker`] for the namespaces it anchors. Owner
//! selection is the pure [`placement`] function of the serving set (**B5**), so a
//! writer and a reader agree on where a blob lives with no directory lookup.
//!
//! The push-based reconcile loop that restores the `R` margin after a node departs
//! (spec §7, **B6**) is a separate concern (`reconcile`); this module is the data
//! and delete paths.

use std::sync::Arc;
use std::time::Duration;

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
use crate::replica::ActorBlobTransport;
use crate::replica::BlobReplica;
use crate::replica::BlobTransport;
use crate::replica::StoreAck;
use crate::replica::StoreAckFuture;
use crate::replica::blob_replica_key;
use crate::system::BlobSystem;
use crate::tombstone::AnchorTracker;
use crate::tombstone::TombstoneSet;

/// How long a `put`/`delete` waits for its durability target before returning
/// [`BlobError::Unavailable`], and how long a `get`/`has` waits on each owner.
/// Modeled on granary's quorum/recover timeouts.
const PUT_TIMEOUT: Duration = Duration::from_secs(2);
const READ_TIMEOUT: Duration = Duration::from_secs(2);
const DELETE_TIMEOUT: Duration = Duration::from_secs(2);

/// The `Clustered` tier (spec §5.2). Clone is cheap (an `Arc<Inner>`), so it
/// satisfies the [`BlobStore`] bound and the background reconcile loop can hold a
/// handle.
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
/// reconcile loop ([`crate::reconcile`]) can hold a `Weak` to it and read the
/// node's capabilities each pass.
pub(crate) struct Inner<S: BlobSystem> {
    pub(crate) system: S,
    pub(crate) config: BlobConfig,
    /// This node's own on-disk store: where it keeps the blobs it owns and the
    /// durable tombstones it has learned (spec §5.1, §5.3).
    pub(crate) local: LocalBlobStore,
    /// This node's cluster-wide awareness set, shared with its [`BlobReplica`] so
    /// the durable store and the in-memory set move together (spec §5.3).
    pub(crate) tombstones: TombstoneSet,
    /// Sweep tracking for namespaces this node anchors (spec §5.3, B7). Reclamation
    /// is driven by the reconcile loop (spec §7).
    pub(crate) anchors: AnchorTracker,
    /// How the tier reaches peers' replicas — a seam, for deterministic simulation
    /// (spec §6, §8).
    pub(crate) transport: Arc<dyn BlobTransport>,
    pub(crate) self_node: NodeId,
}

impl<S: BlobSystem> ClusteredBlobStore<S> {
    /// Bring the `Clustered` tier up on `system` with this node's on-disk `local`
    /// store: spawn and register the node's [`BlobReplica`] so peers can reach it,
    /// and wire the transport, tombstone awareness, and anchor tracker. The
    /// reconcile loop is started separately (spec §7).
    ///
    /// `local` is opened by the caller (one store per node), so the tier stays
    /// agnostic to where bytes live on disk — the deviation from Appendix A's
    /// path-less sketch that lets the deterministic simulator give each node its
    /// own directory (spec §8).
    pub fn start(system: S, config: BlobConfig, local: LocalBlobStore) -> ClusteredBlobStore<S> {
        let tombstones = TombstoneSet::new();
        let replica = system.spawn(BlobReplica::<S>::new(local.clone(), tombstones.clone()));
        system
            .receptionist()
            .register(blob_replica_key::<S>(), &replica);
        let self_node = system.node();
        let transport: Arc<dyn BlobTransport> = Arc::new(ActorBlobTransport::new(system.clone()));
        let inner = Arc::new(Inner {
            system,
            config,
            local,
            tombstones,
            anchors: AnchorTracker::new(),
            transport,
            self_node,
        });
        // Drive rebalancing and tombstone reclamation in the background, holding a
        // `Weak` so the loop exits once the last store handle is dropped (the
        // granary `reconcile_loop` pattern, spec §7).
        inner
            .system
            .launch(Box::pin(crate::reconcile::reconcile_loop(Arc::downgrade(
                &inner,
            ))));
        ClusteredBlobStore { inner }
    }

    /// The node's tombstone awareness set (shared with its replica). Exposed for
    /// the reconcile loop, which consults it before copying a blob (spec §7).
    pub fn tombstones(&self) -> &TombstoneSet {
        &self.inner.tombstones
    }

    /// The node's anchor tracker. Exposed for the reconcile loop's reclamation pass
    /// (spec §5.3, §7).
    pub fn anchors(&self) -> &AnchorTracker {
        &self.inner.anchors
    }

    /// This node's on-disk store. Exposed for the reconcile loop, which enumerates
    /// the blobs this node holds and re-pushes the under-replicated ones to their
    /// current owners (spec §7, **B6**).
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
        // Enforce the size bound (spec §2: "an implementation SHOULD bound a blob's
        // size and a consumer SHOULD chunk beyond that bound"). `max_blob_bytes` is
        // the tier's one size lever; refuse past it rather than store an unbounded
        // blob — the whole-blob verify and whole-blob fetch (B1) assume a bounded
        // unit. The error is non-retryable, but the v1 error model has no dedicated
        // variant, so it is surfaced as `Unavailable` with an explicit reason.
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
        if owners.is_empty() {
            return Err(BlobError::Unavailable("no serving members".to_string()));
        }
        let need = self.w().min(owners.len());

        // Fan a StoreBlob out to every owner; the local owner resolves to the
        // in-process replica (no serialization, spec §5.2). Acknowledge at `need`
        // and drain the rest in the background, off the latency path (granary §7.2).
        let mut acks = FuturesUnordered::new();
        for owner in owners {
            acks.push(self.inner.transport.store_blob(
                owner,
                ns.clone(),
                id,
                bytes.clone(),
                PUT_TIMEOUT,
            ));
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
                Err(_) => {} // unreachable/timeout owner — keep counting the rest
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
                .transport
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
                Ok(None) => {} // absent (or tombstoned) here — keep trying
                Err(_) => {}   // unreachable owner — keep trying
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
        let need = self.w().min(owners.len().max(1));

        let mut present = 0usize;
        for owner in owners {
            if let Ok(true) = self
                .inner
                .transport
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
            let fut =
                self.inner
                    .transport
                    .delete_namespace(node, ns.clone(), deleted_at, DELETE_TIMEOUT);
            acks.push(async move { (node, is_anchor, fut.await) });
        }

        let mut anchored = 0usize;
        let mut swept: Vec<NodeId> = Vec::new();
        let mut result = Err(BlobError::Unavailable(format!(
            "anchored 0 of {need_anchor} tombstone owners"
        )));
        while let Some((node, is_anchor, ack)) = acks.next().await {
            if ack.is_ok() {
                swept.push(node);
                if is_anchor {
                    anchored += 1;
                    if anchored >= need_anchor && result.is_err() {
                        result = Ok(());
                        // Keep draining so the rest of the cluster sweeps too; do not
                        // return yet — the loop finishes the fan-out below.
                    }
                }
            }
        }

        // If this node anchors the namespace, begin tracking sweep completion so the
        // tombstone is retained until every member-at-anchor has swept or reached a
        // terminal state (spec §5.3, B7). Reclamation runs in the reconcile loop.
        if anchor_owners.contains(&self.inner.self_node) {
            self.inner.anchors.anchor(ns, members.iter().copied());
            for node in swept {
                self.inner.anchors.record_sweep(ns, node);
            }
        }
        result
    }

    /// Drain the remaining store acks in the background, so the commit returns at
    /// `W` latency while every issued ask still runs to completion (spec §5.2).
    fn drain(&self, mut pending: FuturesUnordered<StoreAckFuture>) {
        if pending.is_empty() {
            return;
        }
        self.inner.transport.launch(Box::pin(
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
