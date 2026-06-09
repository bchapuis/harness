//! The external-registry seam of the registry-based control plane (spec §9.4.2).
//!
//! In registry-based mode the authoritative member set lives in an **external
//! registry** — a platform API, a database, a coordination service — that the
//! cluster reads but does not operate. [`RegistryClient`] is the trait the sync
//! loop speaks, like [`Transport`](crate::Transport) (spec §9.4.2 item 7):
//! production implements it against the real platform; the simulator supplies an
//! in-memory registry with seeded latency, staleness, and unavailability (spec
//! §18.2, §18.3). [`InMemoryRegistry`] is the plain, fault-free implementation —
//! the production bootstrap and the substrate a faulted simulation wrapper
//! delegates to.

use std::collections::BTreeMap;
use std::sync::Mutex;

use actor_core::BoxFuture;
use actor_core::NodeId;

/// The desired state of a registered member (spec §9.4.2). Absence from the
/// registry is the third state: not a member — admission *is* the entry, and
/// removing it finalizes `down`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RegistryState {
    /// A full member, in service.
    Up,
    /// Cordoned for maintenance — the reversible `draining` state (spec §9.1).
    Draining,
}

/// One registry entry: `node`'s desired state, stamped with the **revision** of
/// the mutation that last changed it (a `modRevision`). The revision is the
/// authority stamp the membership merge orders decisions by (spec §9.2).
#[derive(Clone, Copy, Debug)]
pub struct RegistryEntry {
    pub node: NodeId,
    pub state: RegistryState,
    pub revision: u64,
}

/// One consistent read of the registry: the full desired member set as of
/// `revision`, the registry's global monotonic revision at read time (spec
/// §9.4.2 item 1). The global revision also stamps *removals* — a member absent
/// from a snapshot is `down` at that revision — and orders snapshots, so a
/// stale read is detected and skipped.
#[derive(Clone, Debug)]
pub struct RegistrySnapshot {
    pub revision: u64,
    pub entries: Vec<RegistryEntry>,
}

/// A registry operation failed (spec §9.4.2 item 6). Unavailability pauses
/// membership *changes* only: the data plane keeps running on the last-synced
/// view, and a node must not treat its own inability to reach the registry as
/// evidence about peer liveness.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RegistryError {
    Unavailable,
}

/// The registry client (spec §9.4.2 item 7) — the seam between the cluster and
/// the external registry. The sync loop polls [`fetch`](RegistryClient::fetch);
/// the mutation methods are the operator/platform side (who calls them is
/// deployment policy, spec §9.4.2 item 2), each returning the new revision.
///
/// Dyn-compatible (`BoxFuture` returns) so a [`ClusterConfig`] can carry
/// `Arc<dyn RegistryClient>`.
///
/// [`ClusterConfig`]: crate::ClusterConfig
pub trait RegistryClient: Send + Sync + 'static {
    /// Read the full desired member set.
    fn fetch(&self) -> BoxFuture<'static, Result<RegistrySnapshot, RegistryError>>;

    /// Register `node` as a member (`Up`). Idempotent; admission is the entry
    /// itself (spec §9.4.2 item 2).
    fn register(&self, node: NodeId) -> BoxFuture<'static, Result<u64, RegistryError>>;

    /// Set a registered member's desired state — the `drain`/`resume` cordon
    /// (spec §9.4.2 item 5).
    fn set_state(
        &self,
        node: NodeId,
        state: RegistryState,
    ) -> BoxFuture<'static, Result<u64, RegistryError>>;

    /// Remove `node` from the registry — the removal's revision finalizes
    /// `down`/`removed` and runs the node-down cascade (spec §9.4.2 items 3–4).
    fn deregister(&self, node: NodeId) -> BoxFuture<'static, Result<u64, RegistryError>>;
}

struct RegistryInner {
    /// The global monotonic revision, bumped by every mutation.
    revision: u64,
    /// Each member's desired state and the revision that last changed it.
    entries: BTreeMap<NodeId, (RegistryState, u64)>,
}

/// A plain in-memory [`RegistryClient`]: always available, no latency, no
/// staleness. The reference implementation and the substrate the simulator's
/// faulted registry delegates to (spec §18.2).
pub struct InMemoryRegistry {
    inner: Mutex<RegistryInner>,
}

impl InMemoryRegistry {
    pub fn new() -> InMemoryRegistry {
        InMemoryRegistry {
            inner: Mutex::new(RegistryInner {
                revision: 0,
                entries: BTreeMap::new(),
            }),
        }
    }

    /// The current global revision (for tests/inspection).
    pub fn revision(&self) -> u64 {
        self.inner.lock().expect("registry mutex poisoned").revision
    }

    /// The current snapshot, synchronously (what [`fetch`](RegistryClient::fetch)
    /// returns asynchronously).
    pub fn snapshot(&self) -> RegistrySnapshot {
        let inner = self.inner.lock().expect("registry mutex poisoned");
        RegistrySnapshot {
            revision: inner.revision,
            entries: inner
                .entries
                .iter()
                .map(|(node, (state, revision))| RegistryEntry {
                    node: *node,
                    state: *state,
                    revision: *revision,
                })
                .collect(),
        }
    }

    /// Synchronous [`register`](RegistryClient::register).
    pub fn register_sync(&self, node: NodeId) -> u64 {
        let mut inner = self.inner.lock().expect("registry mutex poisoned");
        inner.revision += 1;
        let revision = inner.revision;
        // Re-registering keeps the entry but restamps it `Up` at the new
        // revision; the membership merge's terminal stickiness (invariant #15)
        // is what stops this resurrecting a node already declared `down`.
        inner.entries.insert(node, (RegistryState::Up, revision));
        revision
    }

    /// Synchronous [`set_state`](RegistryClient::set_state). A no-op revision
    /// bump if `node` is not registered.
    pub fn set_state_sync(&self, node: NodeId, state: RegistryState) -> u64 {
        let mut inner = self.inner.lock().expect("registry mutex poisoned");
        inner.revision += 1;
        let revision = inner.revision;
        if let Some(entry) = inner.entries.get_mut(&node) {
            *entry = (state, revision);
        }
        revision
    }

    /// Synchronous [`deregister`](RegistryClient::deregister).
    pub fn deregister_sync(&self, node: NodeId) -> u64 {
        let mut inner = self.inner.lock().expect("registry mutex poisoned");
        inner.revision += 1;
        let revision = inner.revision;
        inner.entries.remove(&node);
        revision
    }
}

impl Default for InMemoryRegistry {
    fn default() -> Self {
        InMemoryRegistry::new()
    }
}

impl RegistryClient for InMemoryRegistry {
    fn fetch(&self) -> BoxFuture<'static, Result<RegistrySnapshot, RegistryError>> {
        let snapshot = self.snapshot();
        Box::pin(async move { Ok(snapshot) })
    }

    fn register(&self, node: NodeId) -> BoxFuture<'static, Result<u64, RegistryError>> {
        let revision = self.register_sync(node);
        Box::pin(async move { Ok(revision) })
    }

    fn set_state(
        &self,
        node: NodeId,
        state: RegistryState,
    ) -> BoxFuture<'static, Result<u64, RegistryError>> {
        let revision = self.set_state_sync(node, state);
        Box::pin(async move { Ok(revision) })
    }

    fn deregister(&self, node: NodeId) -> BoxFuture<'static, Result<u64, RegistryError>> {
        let revision = self.deregister_sync(node);
        Box::pin(async move { Ok(revision) })
    }
}
