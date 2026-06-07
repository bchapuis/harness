//! Continuously-checked invariants over the event stream (spec §18.5).
//!
//! In the FoundationDB style, correctness is expressed as a small set of
//! invariants checked on **every** run, not as bespoke example tests. Each
//! [`Invariant`] observes the [`Event`] stream live and reports a violation
//! string; the [`Checker`](crate::Checker) collects them. The single-node slice
//! ships three; the catalogue grows with each subsystem (spec §18.5 #1–#21).

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use actor_core::ActorId;
use actor_core::Event;
use actor_core::NodeId;

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
