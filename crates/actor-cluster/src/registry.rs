//! The external-registry seam of the registry-based control plane (spec §6.4.2).
//!
//! In registry-based mode the authoritative member set lives in an **external
//! registry** (a platform API, a database, a coordination service) that the
//! cluster reads but does not operate. [`RegistryClient`] is the trait the sync
//! loop speaks (spec §2.17): production implements it against the real
//! platform; the simulator supplies an in-memory registry with seeded latency,
//! staleness, and unavailability (spec §2.12.2, §10).

use actor_core::BoxFuture;
use actor_core::NodeId;

/// The desired state of a registered member (spec §6.4.2). Absence from the
/// registry is the third state: not a member; admission *is* the entry, and
/// removing it finalizes `down`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RegistryState {
    /// A full member, in service.
    Up,
    /// Cordoned for maintenance: the reversible `draining` state (spec §6.1).
    Draining,
}

/// One registry entry: `node`'s desired state, stamped with the **revision** of
/// the mutation that last changed it (a `modRevision`). The revision is the
/// authority stamp the membership merge orders decisions by (spec §6.2).
#[derive(Clone, Copy, Debug)]
pub struct RegistryEntry {
    pub node: NodeId,
    pub state: RegistryState,
    pub revision: u64,
}

/// One consistent read of the registry: the full desired member set as of
/// `revision`, the registry's global monotonic revision at read time (spec
/// §6.4.2 item 1). The global revision also stamps *removals* (a member absent
/// from a snapshot is `down` at that revision), and orders snapshots, so a
/// stale read is detected and skipped.
#[derive(Clone, Debug)]
pub struct RegistrySnapshot {
    pub revision: u64,
    pub entries: Vec<RegistryEntry>,
}

/// A registry operation failed (spec §6.4.2 item 6). Unavailability pauses
/// membership *changes* only: the data plane keeps running on the last-synced
/// view, and a node must not treat its own inability to reach the registry as
/// evidence about peer liveness.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RegistryError {
    Unavailable,
}

/// The registry client (spec §2.17): the seam between the cluster and
/// the external registry. The sync loop polls [`fetch`](RegistryClient::fetch);
/// the mutation methods are the operator/platform side (who calls them is
/// deployment policy, spec §6.4.2 item 2), each returning the new revision.
///
/// Dyn-compatible (`BoxFuture` returns) so a [`ClusterConfig`] can carry
/// `Arc<dyn RegistryClient>`.
///
/// [`ClusterConfig`]: crate::ClusterConfig
pub trait RegistryClient: Send + Sync + 'static {
    /// Read the full desired member set.
    fn fetch(&self) -> BoxFuture<'static, Result<RegistrySnapshot, RegistryError>>;

    /// Register `node` as a member (`Up`). Idempotent; admission is the entry
    /// itself (spec §6.4.2 item 2).
    fn register(&self, node: NodeId) -> BoxFuture<'static, Result<u64, RegistryError>>;

    /// Set a registered member's desired state: the `drain`/`resume` cordon
    /// (spec §6.4.2 item 5).
    fn set_state(
        &self,
        node: NodeId,
        state: RegistryState,
    ) -> BoxFuture<'static, Result<u64, RegistryError>>;

    /// Remove `node` from the registry: the removal's revision finalizes
    /// `down`/`removed` and runs the node-down cascade (spec §6.4.2 items 3–4).
    fn deregister(&self, node: NodeId) -> BoxFuture<'static, Result<u64, RegistryError>>;
}
