//! Shared simulation invariants over grain events (granary spec §7, spec §18.5).
//!
//! These are safety predicates every grain-backed swarm wants: commits advance,
//! and a grain is live at most once per node. They live here rather than in each
//! test binary because they are claims about *granary's* contract, not about any
//! one suite — and because six independent copies of "commits are monotonic" can
//! drift apart, so that one of them quietly stops checking what its name says.
//!
//! Each is constructed with the label it reports under, so a suite still names
//! its own violations: `machine-commit-monotonic` and `disk-grain-commit-monotonic`
//! are the same predicate observed from different workloads.
//!
//! Behind the `testing` feature: this is test support, not part of the durable
//! object API, and it should not ship in a production build of the crate.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use actor_core::Event;
use actor_core::NodeId;
use actor_simulation::Invariant;

use crate::GrainEvent;
use crate::GrainName;

/// **Commit monotonicity** (invariants **G3**/**G5**): a grain's committed
/// sequence strictly increases.
///
/// A commit at a sequence at or below the current head means two writers both
/// believed themselves authoritative — a minority "leader" that committed, or a
/// replayed entry accepted twice. Either is a split of the commit log.
pub struct CommitMonotonic {
    label: &'static str,
    noun: &'static str,
    last: BTreeMap<GrainName, u64>,
}

impl CommitMonotonic {
    /// Observe under `label`, describing the subject as `noun` in violations —
    /// "grain" for a plain grain suite, "machine" where the grain is a machine.
    pub fn new(label: &'static str, noun: &'static str) -> CommitMonotonic {
        CommitMonotonic {
            label,
            noun,
            last: BTreeMap::new(),
        }
    }
}

impl Invariant for CommitMonotonic {
    fn name(&self) -> &'static str {
        self.label
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        if let Some(GrainEvent::Committed { name, seq, .. }) = event.as_app::<GrainEvent>() {
            let prev = self.last.get(name).copied().unwrap_or(0);
            if *seq <= prev {
                return Err(format!(
                    "{} {name} committed seq {seq} not after previous head {prev} (G3/G5)",
                    self.noun,
                ));
            }
            self.last.insert(name.clone(), *seq);
        }
        Ok(())
    }
}

/// **Exactly-once activation per node** (invariant **G6**): on any one node, a
/// grain is never live twice at once.
///
/// Keyed by `(node, name)`, so an activation that migrates to another leader on
/// failover is not mistaken for a second one. Crash-sound: a node's live set is
/// cleared when the stream reports that node `NodeDown` — its activations are
/// gone with it — so a re-activation after the node rejoins and re-leads is not
/// a false positive.
///
/// **Not** [`actor_simulation::SingletonAtMostOnePerNode`], despite the similar
/// name: that one is the *cluster-utilities* singleton (U2), keyed off
/// `Event::SingletonStarted`. This is the grain analogue, keyed off
/// `GrainEvent::Activated`. Different layer, different event, different
/// catalogue — and the reason to reach for the shared type rather than write a
/// third one.
pub struct ActivationSingletonPerNode {
    label: &'static str,
    noun: &'static str,
    live: BTreeSet<(NodeId, GrainName)>,
}

impl ActivationSingletonPerNode {
    /// Observe under `label`, describing the subject as `noun` in violations.
    pub fn new(label: &'static str, noun: &'static str) -> ActivationSingletonPerNode {
        ActivationSingletonPerNode {
            label,
            noun,
            live: BTreeSet::new(),
        }
    }
}

impl Invariant for ActivationSingletonPerNode {
    fn name(&self) -> &'static str {
        self.label
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        // A node declared down loses its activations; drop them so a later
        // re-activation on the recovered node is sound (G6 is per live node).
        if let Event::NodeDown { node, .. } = event {
            self.live.retain(|(n, _)| n != node);
            return Ok(());
        }
        match event.as_app::<GrainEvent>() {
            Some(GrainEvent::Activated { node, name }) => {
                let fresh = self.live.insert((*node, name.clone()));
                if !fresh {
                    return Err(format!(
                        "{} {name} activated while already live on {node} (G6)",
                        self.noun,
                    ));
                }
            }
            Some(GrainEvent::Passivated { node, name }) => {
                self.live.remove(&(*node, name.clone()));
            }
            _ => {}
        }
        Ok(())
    }
}
