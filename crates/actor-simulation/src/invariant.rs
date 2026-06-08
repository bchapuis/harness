//! Continuously-checked invariants over the event stream (spec §18.5).
//!
//! Correctness is expressed as a small set of invariants checked on **every**
//! run, not as bespoke example tests. Each
//! [`Invariant`] observes the [`Event`] stream live and reports a violation
//! string; the [`Checker`](crate::Checker) collects them. Four ship as
//! continuous checkers; the rest are verified by example tests.
//!
//! [`catalogue`] is the single source of truth linking each of the 21 §18.5
//! invariants to *how* it is verified (a continuous [`Checker`], a conformance
//! test, a compile-fail case, a differential test, or a compile-time bound). The
//! `conformance_catalogue` integration test asserts this catalogue stays
//! consistent with [`default_invariants`] — so a checker added in code but not
//! recorded here (or vice versa) fails the build (spec §17, §18.5, §18.6).

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
/// the run is reported, not unwound through the executor).
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
    ]
}

/// How a §18.5 invariant is verified — the machine-readable form of the §17
/// conformance table's "Verified by" column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verify {
    /// A continuous [`Invariant`] in [`default_invariants`], named by its
    /// [`Invariant::name`]. Cross-checked against the live checker set.
    Checker(&'static str),
    /// One or more example/conformance tests (a human-readable file pointer;
    /// not machine-verified to exist).
    SimTest(&'static str),
    /// A `trybuild` compile-fail case asserting invalid code is rejected (#20).
    CompileFail(&'static str),
    /// A local-vs-remote differential test (#21).
    Differential(&'static str),
    /// Enforced at compile time by a trait bound or exhaustive enum — no runtime
    /// test is possible or needed.
    CompileTime(&'static str),
}

/// One row of the §18.5 invariant catalogue: the invariant number, the spec
/// sections that define it, a one-line property, and how it is verified.
#[derive(Clone, Copy, Debug)]
pub struct CatalogueEntry {
    pub invariant: u8,
    pub spec: &'static str,
    pub property: &'static str,
    pub verify: &'static [Verify],
}

/// The §18.5 invariant catalogue (#1–#21): the single source of truth linking
/// each invariant to the code that verifies it (spec §17, §18.5). Kept
/// consistent with [`default_invariants`] by the `conformance_catalogue` test.
pub fn catalogue() -> &'static [CatalogueEntry] {
    CATALOGUE
}

const CATALOGUE: &[CatalogueEntry] = &[
    CatalogueEntry {
        invariant: 1,
        spec: "§7.2, §14",
        property: "No silent loss: every ask reaches exactly one outcome; none pending at quiescence",
        verify: &[
            Verify::Checker("no-silent-loss"),
            Verify::SimTest("swarm.rs, conformance_messaging.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 2,
        spec: "§7.2, §10",
        property: "An ask to a downed node completes with Unreachable, never hangs",
        verify: &[Verify::SimTest("failure.rs, conformance_faults.rs")],
    },
    CatalogueEntry {
        invariant: 3,
        spec: "§6",
        property: "Per-pair FIFO: messages from one sender to one recipient observed in send order",
        verify: &[Verify::SimTest("actor.rs, cluster.rs, conformance_faults.rs")],
    },
    CatalogueEntry {
        invariant: 4,
        spec: "§6",
        property: "Serial, non-reentrant execution: an actor never dispatches two messages at once",
        verify: &[
            Verify::Checker("serial-execution"),
            Verify::SimTest("actor.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 5,
        spec: "§6",
        property: "Bounded, non-dropping mailbox: a full mailbox blocks or returns MailboxFull",
        verify: &[Verify::SimTest("actor.rs, conformance_messaging.rs")],
    },
    CatalogueEntry {
        invariant: 6,
        spec: "§4.2",
        property: "Lifecycle order and exactly-once: assign_id → actor_ready → resign_id",
        verify: &[
            Verify::Checker("lifecycle-exactly-once"),
            Verify::SimTest("conformance_lifecycle.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 7,
        spec: "§4.3",
        property: "resolve classifies locality with no network round-trip",
        verify: &[Verify::SimTest("conformance_lifecycle.rs")],
    },
    CatalogueEntry {
        invariant: 8,
        spec: "§4.4, §5, §15",
        property: "Manifest dispatch and allowlist: unregistered (type, manifest) → Unhandled",
        verify: &[Verify::SimTest("conformance_serialization.rs, wire.rs")],
    },
    CatalogueEntry {
        invariant: 9,
        spec: "§4.3, §4.4",
        property: "Local sends skip serialization, with a result identical to the remote path",
        verify: &[Verify::SimTest("cluster.rs")],
    },
    CatalogueEntry {
        invariant: 10,
        spec: "§4.4",
        property: "An ActorRef in a message/reply is rebound to the receiving system on decode",
        verify: &[Verify::SimTest("conformance_messaging.rs")],
    },
    CatalogueEntry {
        invariant: 11,
        spec: "§12",
        property: "Death-watch exactly-once, including NodeDown",
        verify: &[Verify::SimTest("conformance_deathwatch.rs, watch.rs")],
    },
    CatalogueEntry {
        invariant: 12,
        spec: "§12",
        property: "Watching an already-terminated actor yields Terminated immediately",
        verify: &[Verify::SimTest("watch.rs")],
    },
    CatalogueEntry {
        invariant: 13,
        spec: "§12",
        property: "Signal ordering: Terminated delivered through the mailbox in serial order",
        verify: &[
            Verify::Checker("signal-in-band"),
            Verify::SimTest("conformance_deathwatch.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 14,
        spec: "§9.2",
        property: "Membership convergence once faults cease and partitions heal",
        verify: &[Verify::SimTest("gossip.rs")],
    },
    CatalogueEntry {
        invariant: 15,
        spec: "§9.1",
        property: "down is terminal: a node observed down never reappears up at the same incarnation",
        verify: &[
            Verify::Checker("down-is-terminal"),
            Verify::SimTest("failure.rs, conformance_join.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 16,
        spec: "§9.2",
        property: "Partition tolerance: under the default policy a partition alone never downs a member",
        verify: &[Verify::SimTest("failure.rs, conformance_membership.rs")],
    },
    CatalogueEntry {
        invariant: 17,
        spec: "§10",
        property: "SWIM refutation: a suspected node refutes via a higher incarnation",
        verify: &[Verify::SimTest("gossip.rs, conformance_membership.rs")],
    },
    CatalogueEntry {
        invariant: 18,
        spec: "§11",
        property: "Supervision containment: a panic never crashes the node; default Stop; restarts back off",
        verify: &[Verify::SimTest("supervision.rs, escalation.rs")],
    },
    CatalogueEntry {
        invariant: 19,
        spec: "§13",
        property: "Receptionist consistency: pruned on node down; subscribe delivers snapshot then changes",
        verify: &[Verify::SimTest("receptionist.rs, conformance_receptionist.rs")],
    },
    CatalogueEntry {
        invariant: 20,
        spec: "§3.3",
        property: "Type-safety: an ask/tell of a message the actor has no Handler for does not compile",
        verify: &[Verify::CompileFail("actor-core/tests/compile_fail")],
    },
    CatalogueEntry {
        invariant: 21,
        spec: "§3.3",
        property: "Location transparency: local vs remote target produce identical replies and ordering",
        verify: &[Verify::Differential("cluster.rs")],
    },
];

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
}
