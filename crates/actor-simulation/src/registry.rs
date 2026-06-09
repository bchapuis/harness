//! The simulated external registry for registry-based mode (spec §9.4.2,
//! §18.2, §18.3).
//!
//! [`SimRegistry`] implements the [`RegistryClient`] seam the registry-based
//! control plane syncs against, with **seeded faults**: latency on every fetch,
//! stale reads served from the snapshot history, and operator-controlled
//! unavailability — the registry-mode rows of the spec's virtualized-trait and
//! fault-injection tables. The same handle is the *operator's* side of the
//! registry: tests register, drain, resume, and deregister members through it,
//! and toggle availability.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::RegistryClient;
use actor_cluster::RegistryEntry;
use actor_cluster::RegistryError;
use actor_cluster::RegistrySnapshot;
use actor_cluster::RegistryState;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::NodeId;

use crate::FaultStats;
use crate::SimClock;
use crate::SimEntropy;
use crate::Simulation;

/// How many past snapshots a stale read may be served from.
const HISTORY_DEPTH: usize = 32;

/// Seeded registry faults (spec §18.3: a stalled, lagging, or unavailable
/// registry sync). All draws come from the run's single [`SimEntropy`], so a
/// faulted run stays reproducible from its seed.
#[derive(Clone, Copy, Debug)]
pub struct RegistryFaultPolicy {
    /// Each fetch sleeps a seeded duration in `[0, max_latency]` before
    /// returning (zero disables).
    pub max_latency: Duration,
    /// A fetch returns a **stale** snapshot — an older state from the history —
    /// with probability `stale_num / stale_den` (a zero numerator disables).
    pub stale_num: u64,
    pub stale_den: u64,
    /// How many revisions behind a stale read may lag, at most.
    pub max_staleness: usize,
}

impl Default for RegistryFaultPolicy {
    fn default() -> Self {
        RegistryFaultPolicy {
            max_latency: Duration::ZERO,
            stale_num: 0,
            stale_den: 1,
            max_staleness: 4,
        }
    }
}

struct SimRegistryState {
    /// The global monotonic revision, bumped by every mutation.
    revision: u64,
    /// Each member's desired state and the revision that last changed it.
    entries: BTreeMap<NodeId, (RegistryState, u64)>,
    /// Recent snapshots, oldest first — what a stale read is served from.
    history: Vec<RegistrySnapshot>,
    /// While `false`, every client call fails `Unavailable` (spec §9.4.2 item 6).
    available: bool,
    /// Outage windows opened so far (coverage telemetry, spec §18.3).
    outages: u64,
    /// Stale snapshots served so far (coverage telemetry, spec §18.3).
    stale_served: u64,
}

impl SimRegistryState {
    fn snapshot(&self) -> RegistrySnapshot {
        RegistrySnapshot {
            revision: self.revision,
            entries: self
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

    /// Record the current state in the bounded history (after a mutation).
    fn record(&mut self) {
        let snapshot = self.snapshot();
        self.history.push(snapshot);
        if self.history.len() > HISTORY_DEPTH {
            self.history.remove(0);
        }
    }
}

/// The simulated external registry (spec §9.4.2, §18.2). Cloning shares the
/// same registry; [`client`](SimRegistry::client) yields the
/// [`RegistryClient`] handle a [`SimNetwork`](crate::SimNetwork) node syncs
/// against, while the mutation methods are the operator/platform side a test
/// drives directly.
#[derive(Clone)]
pub struct SimRegistry {
    state: Arc<Mutex<SimRegistryState>>,
    clock: SimClock,
    entropy: SimEntropy,
    faults: RegistryFaultPolicy,
}

impl SimRegistry {
    /// An empty, always-available, fault-free registry on `sim`'s runtime seam.
    pub fn new(sim: &Simulation) -> SimRegistry {
        let mut state = SimRegistryState {
            revision: 0,
            entries: BTreeMap::new(),
            history: Vec::new(),
            available: true,
            outages: 0,
            stale_served: 0,
        };
        state.record(); // the empty state at revision 0, so a stale read can lag to it
        SimRegistry {
            state: Arc::new(Mutex::new(state)),
            clock: sim.clock(),
            entropy: sim.entropy(),
            faults: RegistryFaultPolicy::default(),
        }
    }

    /// Enable seeded registry faults (spec §18.3).
    pub fn with_faults(mut self, faults: RegistryFaultPolicy) -> SimRegistry {
        self.faults = faults;
        self
    }

    /// The [`RegistryClient`] handle for a node's sync loop.
    pub fn client(&self) -> Arc<dyn RegistryClient> {
        Arc::new(self.clone())
    }

    /// The current global revision.
    pub fn revision(&self) -> u64 {
        self.state.lock().expect("registry mutex poisoned").revision
    }

    /// Toggle availability (spec §9.4.2 item 6): while unavailable, every
    /// client call fails `Unavailable`. Mutations through this operator handle
    /// still apply — the registry exists; only the cluster's path to it is out.
    pub fn set_available(&self, available: bool) {
        let mut state = self.state.lock().expect("registry mutex poisoned");
        if state.available && !available {
            state.outages += 1;
        }
        state.available = available;
    }

    /// The registry faults this instance has exercised so far (spec §18.3), as
    /// the registry rows of [`FaultStats`] — summed with the network's by the
    /// cluster driver so a swarm can assert registry faults actually fired.
    pub fn fault_stats(&self) -> FaultStats {
        let state = self.state.lock().expect("registry mutex poisoned");
        FaultStats {
            registry_outages: state.outages,
            registry_stale: state.stale_served,
            ..FaultStats::default()
        }
    }

    fn mutate(&self, f: impl FnOnce(&mut SimRegistryState)) -> u64 {
        let mut state = self.state.lock().expect("registry mutex poisoned");
        state.revision += 1;
        f(&mut state);
        state.record();
        state.revision
    }

    /// Register `node` as a member (`Up`) — the operator/platform admission
    /// (spec §9.4.2 item 2). Returns the mutation's revision.
    pub fn register(&self, node: NodeId) -> u64 {
        self.mutate(|state| {
            let revision = state.revision;
            state.entries.insert(node, (RegistryState::Up, revision));
        })
    }

    /// Cordon `node` for maintenance (spec §9.4.2 item 5). A no-op revision
    /// bump if `node` is not registered.
    pub fn drain(&self, node: NodeId) -> u64 {
        self.set_state(node, RegistryState::Draining)
    }

    /// Return a drained `node` to service (spec §9.4.2 item 5).
    pub fn resume(&self, node: NodeId) -> u64 {
        self.set_state(node, RegistryState::Up)
    }

    fn set_state(&self, node: NodeId, new: RegistryState) -> u64 {
        self.mutate(|state| {
            let revision = state.revision;
            if let Some(entry) = state.entries.get_mut(&node) {
                *entry = (new, revision);
            }
        })
    }

    /// Remove `node` from the registry — the removal finalizes `down` at this
    /// revision (spec §9.4.2 items 3–4).
    pub fn deregister(&self, node: NodeId) -> u64 {
        self.mutate(|state| {
            state.entries.remove(&node);
        })
    }
}

impl RegistryClient for SimRegistry {
    fn fetch(&self) -> BoxFuture<'static, Result<RegistrySnapshot, RegistryError>> {
        let registry = self.clone();
        Box::pin(async move {
            // Seeded latency: the read takes virtual time, so a sync can lag a
            // mutation (spec §18.3).
            if !registry.faults.max_latency.is_zero() {
                let span = registry.faults.max_latency.as_nanos() as u64 + 1;
                let delay = Duration::from_nanos(registry.entropy.next_u64() % span);
                registry.clock.sleep(delay).await;
            }
            let state = registry.state.lock().expect("registry mutex poisoned");
            if !state.available {
                return Err(RegistryError::Unavailable);
            }
            // Seeded staleness: serve an older snapshot from the history; the
            // sync loop's monotonic-revision guard must absorb it.
            let lag_by = state.history.len().saturating_sub(1);
            if lag_by > 0
                && registry
                    .entropy
                    .buggify(registry.faults.stale_num, registry.faults.stale_den)
            {
                let back = 1 + registry.entropy.next_u64() as usize
                    % registry.faults.max_staleness.max(1).min(lag_by);
                let snapshot = state.history[state.history.len() - 1 - back].clone();
                drop(state);
                registry
                    .state
                    .lock()
                    .expect("registry mutex poisoned")
                    .stale_served += 1;
                return Ok(snapshot);
            }
            Ok(state.snapshot())
        })
    }

    fn register(&self, node: NodeId) -> BoxFuture<'static, Result<u64, RegistryError>> {
        let registry = self.clone();
        Box::pin(async move {
            if !registry.available() {
                return Err(RegistryError::Unavailable);
            }
            Ok(registry.register(node))
        })
    }

    fn set_state(
        &self,
        node: NodeId,
        state: RegistryState,
    ) -> BoxFuture<'static, Result<u64, RegistryError>> {
        let registry = self.clone();
        Box::pin(async move {
            if !registry.available() {
                return Err(RegistryError::Unavailable);
            }
            Ok(registry.set_state(node, state))
        })
    }

    fn deregister(&self, node: NodeId) -> BoxFuture<'static, Result<u64, RegistryError>> {
        let registry = self.clone();
        Box::pin(async move {
            if !registry.available() {
                return Err(RegistryError::Unavailable);
            }
            Ok(registry.deregister(node))
        })
    }
}

impl SimRegistry {
    fn available(&self) -> bool {
        self.state
            .lock()
            .expect("registry mutex poisoned")
            .available
    }
}
