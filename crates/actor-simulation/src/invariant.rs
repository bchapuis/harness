//! Continuously-checked invariants over the event stream (spec §18.5).
//!
//! Correctness is expressed as a small set of invariants checked on **every**
//! run, not as bespoke example tests. Each
//! [`Invariant`] observes the [`Event`] stream live and reports a violation
//! string; the [`Checker`](crate::Checker) collects them. Seven ship as
//! continuous checkers; the rest are verified by example tests.
//!
//! [`catalogue`](crate::catalogue) is the single source of truth linking each of
//! the 22 §18.5 invariants to *how* it is verified (a continuous [`Checker`], a
//! conformance test, a compile-fail case, a differential test, or a compile-time
//! bound). The `conformance_catalogue` integration test asserts this catalogue
//! stays consistent with [`default_invariants`] — so a checker added in code but
//! not recorded here (or vice versa) fails the build (spec §17, §18.5, §18.6).

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use actor_core::ActorId;
use actor_core::Event;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Terminated;

/// A property checked continuously during a run and at final quiescence
/// (spec §18.5). Observation must be side-effect-free apart from the invariant's
/// own bookkeeping, and must never panic (a violation is a returned `Err`, so
/// the run is reported, not unwound through the executor). Alongside the core
/// catalogue, the cluster-utilities invariants (U1, U2, … — see
/// [`utilities_catalogue`](crate::utilities_catalogue)) check through the same
/// mechanism.
pub trait Invariant: Send {
    /// A stable name for reporting.
    fn name(&self) -> &'static str;

    /// Observe one event; return `Err(detail)` on violation.
    fn observe(&mut self, event: &Event) -> Result<(), String>;

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

/// **No silent loss** (spec §18.5 #1): every issued `ask` reaches an outcome,
/// and none remains pending at quiescence.
#[derive(Default)]
pub struct NoSilentLoss {
    outstanding: i64,
}

impl Invariant for NoSilentLoss {
    fn name(&self) -> &'static str {
        "no-silent-loss"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        match event {
            Event::AskIssued { .. } => self.outstanding += 1,
            Event::AskOutcome { .. } => {
                self.outstanding -= 1;
                if self.outstanding < 0 {
                    return Err("ask outcome with no matching issued ask".into());
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn at_quiescence(&mut self) -> Result<(), String> {
        if self.outstanding != 0 {
            return Err(format!(
                "{} ask(s) still pending at quiescence",
                self.outstanding
            ));
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
}

/// **`down` is terminal** (spec §18.5 #15): once an observer declares a node
/// `down`, that observer never sees its reachability change again. Tracked per
/// `(observer, subject)`, because without gossip each node decides `down`
/// independently — node A downing C does not bind node B's view of C.
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
}

/// **Signal ordering / in-band delivery** (spec §18.5 #13, §12): a `Terminated`
/// is delivered through the watcher's mailbox like any other message — never out
/// of band, straight into a running handler.
///
/// A `Terminated` flows through [`enqueue_signal`](actor_core::Mailbox), which
/// emits an `Enqueue` for the `Terminated` manifest and then lets the serial
/// message loop dispatch it (a `DispatchStart`). So "in band" is checkable as a
/// prefix property: a `Terminated` is never *dispatched* on an actor more times
/// than it was *enqueued* there. An out-of-band delivery — invoking the handler
/// directly, bypassing the queue — would `DispatchStart` a signal that was never
/// enqueued, and is caught here. The serial, non-reentrant half of #13 is already
/// covered by [`SerialExecution`] (#4), which treats the signal like any message.
///
/// This is a *per-event* invariant (it holds at every prefix), so it is sound for
/// both quiescence-driven single-node runs and time-bounded cluster runs.
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
}

/// **Quorum-gated control plane — election safety** (spec §18.5 #22, §9.4.3):
/// at most one node ever announces leadership for a given term. This is the
/// half of #22 expressible as a safety property over the event stream
/// (`LeaderElected` carries the term); the quorum-gating and
/// minority-cannot-evict halves are scenario properties, verified by the
/// targeted tests in `conformance_leader.rs`. Vacuously green outside
/// leader-based mode (no `LeaderElected` is ever emitted), so it is safe in
/// [`default_invariants`].
///
/// Terms are **per Raft group** (the engine runs O(groups) independent groups,
/// each with its own term sequence), so election safety is keyed by
/// `(group, term)`: two groups legitimately reaching term `N` is not a double
/// election. The membership control plane is group `0`.
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
/// `SingletonStopped` of its predecessor. The cross-node "exactly one" is only
/// guaranteed at view convergence, so overlap *across* nodes during divergence
/// is legal and deliberately not flagged here; the converged-exactly-one and
/// re-activation halves are scenario properties, verified by
/// `conformance_singleton.rs` and the singleton swarm workload. Vacuously green
/// for workloads that host no singleton, so it is safe in
/// [`default_invariants`]. (Not restart-safe: a `SimNetwork::restart` of a
/// hosting node abandons its manager without a `SingletonStopped`, so singleton
/// workloads use crash/partition nemeses, not restart.)
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
}

// Note: death-watch exactly-once (#11) is intentionally *not* a continuous
// checker. The tempting form — "at most one `TerminatedDelivered` per
// (target, watcher)" — is unsound: a watcher may legitimately `watch` the same
// target more than once, and watching an already-terminated actor yields a fresh
// `Terminated` each time (spec §12, invariant #12). The receptionist does
// exactly this when anti-entropy re-delivers a stale registration for an actor
// that has since died. With no per-`watch` identity on the event stream, the
// "exactly one *per watch*" property is not expressible as a safety invariant
// over the existing events, so #11 stays verified by targeted tests.
//
// Likewise, bounded-non-dropping mailbox (#5) is *not* a continuous checker. Its
// "bounded" half is structural — the mailbox is a fixed-capacity channel that
// cannot exceed its bound — and its "blocks or returns `MailboxFull`, never drops
// silently" half is a per-call API contract (`tell` awaits, `try_tell` returns
// `MailboxFull`). Neither is an emergent property of the event stream: a depth
// check would need per-actor capacity in the stream, and "depth 0 at quiescence"
// is unsound for the time-bounded cluster runs (`run_for`) that stop mid-flight.
// So #5 stays verified by targeted tests (`actor.rs`, `conformance_messaging.rs`).

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
            manifest: "m",
        }
    }
    fn outcome(n: u64) -> Event {
        Event::AskOutcome {
            actor: id(n),
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
