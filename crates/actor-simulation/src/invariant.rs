//! Continuously-checked invariants over the event stream (spec §18.5).
//!
//! Each [`Invariant`] observes the [`Event`] stream live and reports a violation
//! string; the [`Checker`](crate::Checker) collects them. Seven ship as
//! continuous checkers; the rest are verified by example tests.
//!
//! [`catalogue`](crate::catalogue) records which of the 22 §18.5 invariants is
//! verified how, kept consistent with [`default_invariants`] by the
//! `conformance_catalogue` test (spec §17, §18.5, §18.6).

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use actor_core::ActorId;
use actor_core::Event;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Terminated;

use crate::CheckerCoverage;

/// A property checked continuously during a run and at final quiescence
/// (spec §18.5). Observation must be side-effect-free apart from the invariant's
/// own bookkeeping, and must never panic: a violation is a returned `Err`, so
/// the run is reported, not unwound through the executor. The cluster-utilities
/// invariants (U1, U2, … see
/// [`utilities_catalogue`](crate::utilities_catalogue)) use the same mechanism.
pub trait Invariant: Send {
    /// A stable name for reporting.
    fn name(&self) -> &'static str;

    /// Observe one event; return `Err(detail)` on violation.
    fn observe(&mut self, event: &Event) -> Result<(), String>;

    /// Forget everything known about `node`'s process, because that process has
    /// **ended** — a [`NodeRestarted`](crate::NodeRestarted), which the
    /// [`Checker`](crate::Checker) dispatches here before passing the event on.
    ///
    /// Process death leaves brackets open (a dispatch without its end, an `ask`
    /// never answered, an identity never resigned), and the successor reuses the
    /// identities, since a fresh process numbers paths and incarnations from
    /// zero. Neither is a violation. An invariant that accumulates per-node state
    /// overrides this to drop it; the default does nothing, which is right for a
    /// claim a restart does not reset, such as [`OneLeaderPerTerm`].
    fn forget_node(&mut self, _node: NodeId) {}

    /// Final check once the run is quiescent; return `Err(detail)` on violation.
    fn at_quiescence(&mut self) -> Result<(), String> {
        Ok(())
    }
}

/// The default invariants every workload checks unless it overrides them.
pub fn default_invariants() -> Vec<Box<dyn Invariant>> {
    vec![
        Box::new(NoSilentLoss::default()),
        Box::new(SerialExecution::default()),
        Box::new(LifecycleExactlyOnce::default()),
        Box::new(DownIsTerminal::default()),
        Box::new(SignalInBand::default()),
        Box::new(OneLeaderPerTerm::default()),
        Box::new(SingletonAtMostOnePerNode::default()),
    ]
}

/// Which catalogued invariant each checker in [`default_invariants`] enforces.
///
/// Declared here, beside the checkers, because this is the claim an edit to an
/// `observe` body falsifies: narrow what [`OneLeaderPerTerm`] watches and the
/// row saying it carries #22 is what stopped being true. The catalogue states
/// the same pairing from the other side, and the `conformance_catalogue` test
/// holds the two equal **per pair**, in both directions.
///
/// Comparing the pairs rather than the two sets of checker *names* is the
/// point. A name set cannot see one entry dropping its `Verify::Checker` while
/// a sibling entry still names the same checker — which is exactly how a
/// "Verified by" column drifts: quietly, one row at a time, with the totals
/// still matching.
pub fn checker_coverage() -> &'static [CheckerCoverage] {
    CHECKER_COVERAGE
}

const CHECKER_COVERAGE: &[CheckerCoverage] = &[
    CheckerCoverage::core("no-silent-loss", 1),
    CheckerCoverage::core("serial-execution", 4),
    CheckerCoverage::core("lifecycle-exactly-once", 6),
    CheckerCoverage::core("signal-in-band", 13),
    CheckerCoverage::core("down-is-terminal", 15),
    CheckerCoverage::core("one-leader-per-term", 22),
    CheckerCoverage::utilities("singleton-at-most-one-per-node", 2),
];

/// **No silent loss** (spec §18.5 #1): every issued `ask` reaches an outcome,
/// and none remains pending at quiescence.
///
/// Counted per **calling** node, so a restart can forget the calls that died
/// with its process: nothing is left to receive their answers. An ask a *live*
/// caller issued *to* the dead node must still resolve, with `Unreachable`
/// (invariant #2).
#[derive(Default)]
pub struct NoSilentLoss {
    outstanding: BTreeMap<NodeId, i64>,
}

impl Invariant for NoSilentLoss {
    fn name(&self) -> &'static str {
        "no-silent-loss"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        match event {
            Event::AskIssued { caller, .. } => {
                *self.outstanding.entry(*caller).or_default() += 1;
            }
            Event::AskOutcome { caller, .. } => {
                let count = self.outstanding.entry(*caller).or_default();
                *count -= 1;
                if *count < 0 {
                    return Err(format!(
                        "ask outcome with no matching issued ask (caller {caller})"
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn forget_node(&mut self, node: NodeId) {
        self.outstanding.remove(&node);
    }

    fn at_quiescence(&mut self) -> Result<(), String> {
        let pending: i64 = self.outstanding.values().sum();
        if pending != 0 {
            return Err(format!("{pending} ask(s) still pending at quiescence"));
        }
        Ok(())
    }
}

/// **Serial, non-reentrant execution** (spec §18.5 #4): an actor never has two
/// dispatches in flight at once.
#[derive(Default)]
pub struct SerialExecution {
    busy: BTreeMap<ActorId, bool>,
}

impl Invariant for SerialExecution {
    fn name(&self) -> &'static str {
        "serial-execution"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        match event {
            Event::DispatchStart { actor, .. } => {
                let slot = self.busy.entry(actor.clone()).or_insert(false);
                if *slot {
                    return Err(format!("reentrant dispatch on {actor}"));
                }
                *slot = true;
            }
            Event::DispatchEnd { actor, .. } => {
                let slot = self.busy.entry(actor.clone()).or_insert(false);
                if !*slot {
                    return Err(format!("dispatch end without start on {actor}"));
                }
                *slot = false;
            }
            _ => {}
        }
        Ok(())
    }

    fn forget_node(&mut self, node: NodeId) {
        // A process killed mid-dispatch leaves `DispatchStart` unmatched; that is
        // death, not reentrancy, and the successor reuses the actor ids.
        self.busy.retain(|actor, _| actor.node() != node);
    }

    fn at_quiescence(&mut self) -> Result<(), String> {
        for (actor, busy) in &self.busy {
            if *busy {
                return Err(format!("dispatch on {actor} never completed"));
            }
        }
        Ok(())
    }
}

/// **Lifecycle order and exactly-once** (spec §18.5 #6): per actor,
/// `AssignId` → `ActorReady` → `ResignId`, with assign/ready/resign each at most
/// once and never out of order.
///
/// An [`ActorId`] names one actor only within a process: a restarted node
/// assigns paths and incarnations from zero again (spec §11.2). So a
/// [`NodeRestarted`](crate::NodeRestarted) forgets that node's actors, and the
/// successor's `/user/0#0` is judged on its own rather than as a second
/// assignment of its predecessor's.
#[derive(Default)]
pub struct LifecycleExactlyOnce {
    actors: BTreeMap<ActorId, Lifecycle>,
}

#[derive(Default)]
struct Lifecycle {
    assigned: u32,
    readied: u32,
    resigned: u32,
}

impl Invariant for LifecycleExactlyOnce {
    fn name(&self) -> &'static str {
        "lifecycle-exactly-once"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        match event {
            Event::AssignId { id } => {
                let life = self.actors.entry(id.clone()).or_default();
                life.assigned += 1;
                if life.assigned > 1 {
                    return Err(format!("{id} assigned more than once"));
                }
            }
            Event::ActorReady { id } => {
                let life = self.actors.entry(id.clone()).or_default();
                if life.assigned == 0 {
                    return Err(format!("{id} ready before assign"));
                }
                life.readied += 1;
                if life.readied > 1 {
                    return Err(format!("{id} readied more than once"));
                }
            }
            Event::ResignId { id } => {
                let life = self.actors.entry(id.clone()).or_default();
                if life.assigned == 0 {
                    return Err(format!("{id} resigned before assign"));
                }
                life.resigned += 1;
                if life.resigned > 1 {
                    return Err(format!("{id} resigned more than once"));
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn forget_node(&mut self, node: NodeId) {
        self.actors.retain(|id, _| id.node() != node);
    }
}

/// **`down` is terminal** (spec §18.5 #15): once an observer declares a node
/// `down`, that observer never sees its reachability change again. Tracked per
/// `(observer, subject)`: without gossip each node decides `down` independently,
/// so node A downing C does not bind node B's view of C.
#[derive(Default)]
pub struct DownIsTerminal {
    down: BTreeSet<(NodeId, NodeId)>,
}

impl Invariant for DownIsTerminal {
    fn name(&self) -> &'static str {
        "down-is-terminal"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        match event {
            Event::NodeDown { observer, node } => {
                self.down.insert((*observer, *node));
            }
            Event::Reachable { observer, node }
            | Event::Suspected { observer, node }
            | Event::Unreachable { observer, node }
                if self.down.contains(&(*observer, *node)) =>
            {
                return Err(format!(
                    "{observer} changed its view of {node} after declaring it down"
                ));
            }
            _ => {}
        }
        Ok(())
    }

    fn forget_node(&mut self, node: NodeId) {
        // Only the *observer* side: a fresh process remembers downing nobody,
        // but another node's `down` verdict about this one stays terminal
        // however often the subject restarts.
        self.down.retain(|(observer, _)| *observer != node);
    }
}

/// **Signal ordering / in-band delivery** (spec §18.5 #13, §12): a `Terminated`
/// is delivered through the watcher's mailbox like any other message — never out
/// of band, straight into a running handler.
///
/// Signals enter through [`enqueue_signal`](actor_core::Mailbox), so "in band"
/// is checkable as a prefix property: a `Terminated` is never *dispatched* on an
/// actor more times than it was *enqueued* there. Holding at every prefix, it is
/// sound for both quiescence-driven and time-bounded runs. The serial,
/// non-reentrant half of #13 is covered by [`SerialExecution`] (#4).
#[derive(Default)]
pub struct SignalInBand {
    enqueued: BTreeMap<ActorId, u64>,
    dispatched: BTreeMap<ActorId, u64>,
}

impl SignalInBand {
    fn is_terminated(manifest: &str) -> bool {
        manifest == <Terminated as Message>::MANIFEST.as_str()
    }
}

impl Invariant for SignalInBand {
    fn name(&self) -> &'static str {
        "signal-in-band"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        match event {
            Event::Enqueue { actor, manifest } if Self::is_terminated(manifest) => {
                *self.enqueued.entry(actor.clone()).or_default() += 1;
            }
            Event::DispatchStart { actor, manifest } if Self::is_terminated(manifest) => {
                let dispatched = self.dispatched.entry(actor.clone()).or_default();
                *dispatched += 1;
                let enqueued = self.enqueued.get(actor).copied().unwrap_or(0);
                if *dispatched > enqueued {
                    return Err(format!(
                        "{actor} dispatched a Terminated signal never enqueued on its \
                         mailbox — delivered out of band (spec §12)"
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn forget_node(&mut self, node: NodeId) {
        self.enqueued.retain(|actor, _| actor.node() != node);
        self.dispatched.retain(|actor, _| actor.node() != node);
    }
}

/// **Quorum-gated control plane — election safety** (spec §18.5 #22, §9.4.3):
/// at most one node ever announces leadership for a given term. The quorum-gating
/// and minority-cannot-evict halves of #22 are scenario properties, verified by
/// `conformance_leader.rs`. Vacuously green outside leader-based mode (no
/// `LeaderElected` is ever emitted), so it is safe in [`default_invariants`].
///
/// Terms are **per Raft group**, so election safety is keyed by `(group, term)`:
/// two groups legitimately reaching term `N` is not a double election. The
/// membership control plane is group `0`.
#[derive(Default)]
pub struct OneLeaderPerTerm {
    leaders: BTreeMap<(u64, u64), NodeId>,
}

impl Invariant for OneLeaderPerTerm {
    fn name(&self) -> &'static str {
        "one-leader-per-term"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        if let Event::LeaderElected { node, term, group } = event {
            if let Some(winner) = self.leaders.get(&(*group, *term)) {
                if winner != node {
                    return Err(format!(
                        "two leaders elected for term {term} in group {group}: \
                         {winner} and {node} (election safety, invariant #22)"
                    ));
                }
            } else {
                self.leaders.insert((*group, *term), *node);
            }
        }
        Ok(())
    }
}

/// **Singleton activation discipline — the per-node half** (utilities spec §4,
/// invariant U2): a node never has two live activations of one singleton name
/// at once — every `SingletonStarted` for a `(name, node)` must follow the
/// `SingletonStopped` of its predecessor. Overlap *across* nodes during
/// divergence is legal (cross-node "exactly one" holds only at view convergence)
/// and not flagged here; `conformance_singleton.rs` and the singleton swarm
/// workload verify the other halves. Vacuously green for workloads that host no
/// singleton, so it is safe in [`default_invariants`]. Not restart-safe: a
/// `SimNetwork::restart` of a hosting node abandons its manager without a
/// `SingletonStopped`, so singleton workloads use crash/partition nemeses.
#[derive(Default)]
pub struct SingletonAtMostOnePerNode {
    live: BTreeMap<(&'static str, NodeId), ActorId>,
}

impl Invariant for SingletonAtMostOnePerNode {
    fn name(&self) -> &'static str {
        "singleton-at-most-one-per-node"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        match event {
            Event::SingletonStarted { name, actor } => {
                let slot = (*name, actor.node());
                if let Some(live) = self.live.get(&slot) {
                    return Err(format!(
                        "node {} activated singleton {name:?} as {actor} while {live} \
                         is still live (per-node at-most-one, invariant U2)",
                        actor.node()
                    ));
                }
                self.live.insert(slot, actor.clone());
            }
            Event::SingletonStopped { name, actor } => {
                let slot = (*name, actor.node());
                if self.live.get(&slot) == Some(actor) {
                    self.live.remove(&slot);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn forget_node(&mut self, node: NodeId) {
        self.live.retain(|(_, host), _| *host != node);
    }
}

// Two invariants are *not* continuous checkers, and must not be made ones.
//
// #11 (death-watch exactly-once): a watcher may `watch` the same target more
// than once, and watching an already-terminated actor yields a fresh
// `Terminated` each time (spec §12, #12). The stream carries no per-`watch`
// identity, so "exactly one per watch" is not expressible over it.
//
// #5 (bounded, non-dropping mailbox): the "bounded" half is structural and the
// "blocks or returns `MailboxFull`" half is a per-call API contract; neither is
// emergent on the event stream, and "depth 0 at quiescence" is unsound for the
// time-bounded cluster runs (`run_for`) that stop mid-flight.
//
// Both are verified by targeted tests (`actor.rs`, `conformance_messaging.rs`).

#[cfg(test)]
mod tests {
    use super::*;
    use actor_core::Path;

    fn id(n: u64) -> ActorId {
        ActorId::new(NodeId::new(0), Path::new(format!("/user/{n}")), 0)
    }

    fn issued(n: u64) -> Event {
        Event::AskIssued {
            actor: id(n),
            caller: NodeId::new(0),
            manifest: "m",
        }
    }
    fn outcome(n: u64) -> Event {
        Event::AskOutcome {
            actor: id(n),
            caller: NodeId::new(0),
            manifest: "m",
            failed: false,
        }
    }
    fn start(n: u64) -> Event {
        Event::DispatchStart {
            actor: id(n),
            manifest: "m",
        }
    }
    fn end(n: u64) -> Event {
        Event::DispatchEnd {
            actor: id(n),
            manifest: "m",
        }
    }

    #[test]
    fn no_silent_loss_flags_pending_ask() {
        let mut inv = NoSilentLoss::default();
        assert!(inv.observe(&issued(0)).is_ok());
        // never reaches an outcome
        assert!(inv.at_quiescence().is_err());
    }

    #[test]
    fn no_silent_loss_accepts_balanced_asks() {
        let mut inv = NoSilentLoss::default();
        inv.observe(&issued(0)).unwrap();
        inv.observe(&outcome(0)).unwrap();
        assert!(inv.at_quiescence().is_ok());
    }

    #[test]
    fn serial_execution_flags_reentrancy() {
        let mut inv = SerialExecution::default();
        inv.observe(&start(0)).unwrap();
        assert!(inv.observe(&start(0)).is_err());
    }

    #[test]
    fn serial_execution_accepts_sequential_dispatch() {
        let mut inv = SerialExecution::default();
        inv.observe(&start(0)).unwrap();
        inv.observe(&end(0)).unwrap();
        inv.observe(&start(0)).unwrap();
        inv.observe(&end(0)).unwrap();
        assert!(inv.at_quiescence().is_ok());
    }

    #[test]
    fn lifecycle_flags_double_assign() {
        let mut inv = LifecycleExactlyOnce::default();
        inv.observe(&Event::AssignId { id: id(0) }).unwrap();
        assert!(inv.observe(&Event::AssignId { id: id(0) }).is_err());
    }

    #[test]
    fn lifecycle_flags_ready_before_assign() {
        let mut inv = LifecycleExactlyOnce::default();
        assert!(inv.observe(&Event::ActorReady { id: id(0) }).is_err());
    }

    #[test]
    fn signal_in_band_flags_out_of_band_dispatch() {
        let mut inv = SignalInBand::default();
        let term = <Terminated as Message>::MANIFEST.as_str();
        let enqueue = |n| Event::Enqueue {
            actor: id(n),
            manifest: term,
        };
        let dispatch = |n| Event::DispatchStart {
            actor: id(n),
            manifest: term,
        };
        // Enqueue then dispatch is in band — fine.
        inv.observe(&enqueue(0)).unwrap();
        assert!(inv.observe(&dispatch(0)).is_ok());
        // A second dispatch with no matching enqueue is out of band — flagged.
        assert!(inv.observe(&dispatch(0)).is_err());
    }

    #[test]
    fn signal_in_band_ignores_ordinary_messages() {
        let mut inv = SignalInBand::default();
        // A non-Terminated manifest is not a signal: dispatching it without a
        // tracked enqueue must not be mistaken for an out-of-band delivery.
        assert!(
            inv.observe(&Event::DispatchStart {
                actor: id(0),
                manifest: "app.Greet",
            })
            .is_ok()
        );
    }

    #[test]
    fn one_leader_per_term_flags_a_double_election() {
        let mut inv = OneLeaderPerTerm::default();
        let a = NodeId::new(1);
        let b = NodeId::new(2);
        let g = 0; // the control group
        inv.observe(&Event::LeaderElected {
            node: a,
            term: 3,
            group: g,
        })
        .unwrap();
        // The same winner re-announcing a term is tolerated; a different one is
        // an election-safety violation.
        assert!(
            inv.observe(&Event::LeaderElected {
                node: a,
                term: 3,
                group: g,
            })
            .is_ok()
        );
        assert!(
            inv.observe(&Event::LeaderElected {
                node: b,
                term: 3,
                group: g,
            })
            .is_err()
        );
        // A later term may elect someone else.
        assert!(
            inv.observe(&Event::LeaderElected {
                node: b,
                term: 4,
                group: g,
            })
            .is_ok()
        );
    }

    #[test]
    fn one_leader_per_term_is_keyed_per_group() {
        // Two groups legitimately reaching the same term number with different
        // leaders is not a double election — terms are per group.
        let mut inv = OneLeaderPerTerm::default();
        let a = NodeId::new(1);
        let b = NodeId::new(2);
        inv.observe(&Event::LeaderElected {
            node: a,
            term: 1,
            group: 1,
        })
        .unwrap();
        assert!(
            inv.observe(&Event::LeaderElected {
                node: b,
                term: 1,
                group: 2,
            })
            .is_ok()
        );
        // But a second leader for the *same* (group, term) is still a violation.
        assert!(
            inv.observe(&Event::LeaderElected {
                node: b,
                term: 1,
                group: 1,
            })
            .is_err()
        );
    }

    #[test]
    fn down_is_terminal_flags_resurrection() {
        let mut inv = DownIsTerminal::default();
        let observer = NodeId::new(1);
        let node = NodeId::new(2);
        inv.observe(&Event::NodeDown { observer, node }).unwrap();
        assert!(inv.observe(&Event::Reachable { observer, node }).is_err());
        // A different observer's view of `node` is independent.
        let other = NodeId::new(3);
        assert!(
            inv.observe(&Event::Reachable {
                observer: other,
                node,
            })
            .is_ok()
        );
    }

    fn activation(node: u64, incarnation: u64) -> ActorId {
        ActorId::new(NodeId::new(node), Path::new("/user/0"), incarnation)
    }

    #[test]
    fn singleton_flags_overlapping_activations_on_one_node() {
        let mut inv = SingletonAtMostOnePerNode::default();
        let first = activation(1, 0);
        inv.observe(&Event::SingletonStarted {
            name: "s",
            actor: first.clone(),
        })
        .unwrap();
        // A second activation on the same node before the first stops.
        assert!(
            inv.observe(&Event::SingletonStarted {
                name: "s",
                actor: activation(1, 1),
            })
            .is_err()
        );
        // A concurrent activation on another node is legal (divergence, U2).
        assert!(
            inv.observe(&Event::SingletonStarted {
                name: "s",
                actor: activation(2, 0),
            })
            .is_ok()
        );
        // Another singleton name on the same node is independent.
        assert!(
            inv.observe(&Event::SingletonStarted {
                name: "t",
                actor: activation(1, 1),
            })
            .is_ok()
        );
        // Stopped-then-started on the same node is the legal hand-back cycle.
        inv.observe(&Event::SingletonStopped {
            name: "s",
            actor: first,
        })
        .unwrap();
        assert!(
            inv.observe(&Event::SingletonStarted {
                name: "s",
                actor: activation(1, 2),
            })
            .is_ok()
        );
    }
}
