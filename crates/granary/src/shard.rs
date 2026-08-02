//! The clustered `Quorum` journal (spec §7, §7.4).
//!
//! [`QuorumGrainJournal`] is the [`GrainJournal`] seam over a shard, composing the
//! two parts the substrate now rests on (§7.3):
//!
//! - the shard's **leader-election group** ([`LeaderElection`]) supplies placement —
//!   who may write, under which term — holding no grain data (§7.1, §8);
//! - a per-grain **[`QuorumReplicator`]** supplies durability — it quorum-appends a
//!   grain's records to the shard's replicas, fenced by the shard term, and recovers
//!   a grain's head from a quorum on activation by read-repair (§7.2, §8, **G14**).
//!
//! A grain write is an independent per-grain quorum append (§7.2), not an entry in
//! a shared shard log, so a write-heavy grain never queues behind its shard-mates.

use std::sync::Arc;

use actor_cluster::GroupId;
use actor_cluster::RaftConsensus;

use crate::blobs::BlobId;
use crate::election::LeaderElection;
use crate::grain::GrainName;
use crate::journal::AppendOutcome;
use crate::journal::GrainJournal;
use crate::journal::GrainJournalError;
use crate::journal::Seq;
use crate::replica_store::ReplicaTransport;
use crate::replicator::QuorumReplicator;
use crate::replicator::ShardControl;
use crate::store::GrainStore;

/// A [`GrainJournal`] over a shard's leader-election group and per-grain
/// [`QuorumReplicator`] (spec §7.4). Cloning shares one replicator handle.
pub struct QuorumGrainJournal<R: RaftConsensus> {
    replicator: Arc<QuorumReplicator<R>>,
}

impl<R: RaftConsensus> Clone for QuorumGrainJournal<R> {
    fn clone(&self) -> Self {
        QuorumGrainJournal {
            replicator: Arc::clone(&self.replicator),
        }
    }
}

impl<R: RaftConsensus> QuorumGrainJournal<R> {
    /// Build the journal for one shard. `group` is the shard's leader-election group
    /// (already created by [`shardmap`](crate::shardmap)); `control` is the shard's
    /// **live** control state — replica sets, owned range, split freeze — shared
    /// with the shard map's apply loop so a committed reallocation or split
    /// reaches in-flight journals (§7.7); `local` is this node's [`GrainStore`];
    /// `transport` reaches the peer replicas' stores (spec §7.2, §8); `io` is where
    /// this node's own store writes block (§7.4); `deadlines` are the quorum and
    /// recovery timeouts the deployment set (§11).
    #[allow(clippy::too_many_arguments)] // one call site, from the shard map
    pub(crate) fn new(
        consensus: R,
        group: GroupId,
        shard: u32,
        control: Arc<std::sync::Mutex<ShardControl>>,
        local: Arc<dyn GrainStore>,
        transport: Arc<dyn ReplicaTransport>,
        io: Arc<dyn crate::BlockingIo>,
        deadlines: crate::replicator::Deadlines,
    ) -> QuorumGrainJournal<R> {
        let self_node = consensus.node();
        let election = LeaderElection::new(consensus, group);
        let replicator = QuorumReplicator::new(
            election, local, transport, control, shard, self_node, io, deadlines,
        );
        QuorumGrainJournal {
            replicator: Arc::new(replicator),
        }
    }

    /// The shard's replicator — the migration driver's handle (§7.7).
    pub(crate) fn replicator(&self) -> Arc<QuorumReplicator<R>> {
        Arc::clone(&self.replicator)
    }
}

impl<R: RaftConsensus> GrainJournal for QuorumGrainJournal<R> {
    async fn append(&self, grain: &GrainName, after: Seq, events: Vec<Vec<u8>>) -> AppendOutcome {
        self.replicator.append(grain, after, events).await
    }

    async fn load(
        &self,
        grain: &GrainName,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<(Seq, Vec<u8>)>, GrainJournalError> {
        self.replicator.load(grain, from, limit).await
    }

    async fn head(&self, grain: &GrainName) -> Result<Seq, GrainJournalError> {
        // On the `Quorum` tier `head` *is* the rehydration barrier (§8, §9): it
        // recovers the grain's head from a write quorum by read-repair, so a fresh
        // leader never folds onto a stale head.
        self.replicator.recover(grain).await
    }

    async fn save_snapshot(&self, grain: &GrainName, at: Seq, state: Vec<u8>) -> AppendOutcome {
        self.replicator.save_snapshot(grain, at, state).await
    }

    async fn load_snapshot(
        &self,
        grain: &GrainName,
    ) -> Result<Option<(Seq, Vec<u8>)>, GrainJournalError> {
        self.replicator.load_snapshot(grain).await
    }

    async fn put_blob(
        &self,
        grain: &GrainName,
        id: BlobId,
        bytes: Vec<u8>,
    ) -> Result<(), GrainJournalError> {
        self.replicator.put_blob(grain, id, bytes).await
    }

    async fn get_blob(
        &self,
        grain: &GrainName,
        id: BlobId,
    ) -> Result<Option<Vec<u8>>, GrainJournalError> {
        self.replicator.get_blob(grain, id).await
    }

    async fn has_blob(&self, grain: &GrainName, id: BlobId) -> Result<bool, GrainJournalError> {
        self.replicator.has_blob(grain, id).await
    }

    async fn retain_blobs(&self, grain: &GrainName, retain: Vec<BlobId>) {
        self.replicator.retain_blobs(grain, retain).await
    }

    async fn delete_blobs(&self, grain: &GrainName) {
        self.replicator.delete_blobs(grain).await
    }
}
