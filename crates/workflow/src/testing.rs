//! The continuous D-checkers over the workflow's event stream (workflow spec §9).
//!
//! One checker, `workflow-cycle`, carrying four of the seven invariants: it is the
//! grammar of a claim's life. A key is launched (or resolved as orphaned), and
//! then exactly one of two things happens to it — it settles, or it is discarded
//! as a straggler — and no activation hibernates or announces quiescence with a
//! claim still open.
//!
//! Test support behind the `testing` feature, not part of the workflow's API, and
//! mountable by any consumer's swarm as entity and the harness both ship theirs.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use actor_core::Event;
use actor_core::NodeId;
use actor_simulation::Invariant;
use actor_simulation::default_invariants;
use entity::EntityEvent;
use entity::EntityName;

use crate::WorkflowEvent;

/// An entity's open claims, per node hosting an activation of it (entity §13).
/// Under failover a deposed leader and its successor are two activations of one
/// entity, and their claims must not be confused.
type Claims = BTreeMap<NodeId, BTreeSet<String>>;

/// **The workflow cycle's grammar** (W2, W3, W5, W7).
///
/// - **W3, launch-once per claim.** A `Launched` for a key already open on this
///   activation is a second launch of an effect in flight, which no tick may
///   produce. A `Settled` closes an open claim; one with no claim open here must
///   at least name a key some activation launched — an outcome of a previous
///   activation's launch reaching the one that now holds the entity (§3.3) — or
///   the workflow settled something nothing ever sent.
/// - **W5, straggler rejection.** A `Discarded` names a key whose outcome was
///   dropped without a record; it must correspond to a launch on the stream, or
///   the workflow discarded something it never sent.
/// - **W7, quiescence.** A `Quiescent` must not be announced while a claim is
///   open, and on the single-node tier the entity's `Deactivated` (entity §13) must
///   not arrive while one is: an activation with an effect in flight has not
///   settled its work, and hibernating under it would strand the outcome.
/// - **W2, launch coverage**, in the shape a stream can carry: the `Quiescent`
///   half of the rule above.
///
/// What the stream cannot show — that a *backed-off* key is eventually relaunched
/// — is a scenario assertion rather than a grammar rule, because the liveness is
/// conditional on the activation surviving (§9 W2). The checker does hold the
/// half that is checkable: a `Settled { committed: false }` closes its claim, so
/// a stranded key shows up as a `Deactivated` with nothing open and the work still
/// in the fold, which the scenario suites assert against the journal.
///
/// **Two tiers, one rule that differs.** A forced step-down — a leadership move,
/// quorum loss — is involuntary, ignores `can_hibernate`, and legitimately
/// abandons its launches (§6); on the stream it is a `Deactivated` with claims open,
/// exactly the shape of a W7 violation, and entity's event carries no reason that
/// would tell the two apart. So the checker is built for the tier it watches:
/// [`strict`](WorkflowCycle::strict) for the single-node suites, where nothing is
/// ever deposed and every `Deactivated` is an idle hibernation, and
/// [`under_failover`](WorkflowCycle::under_failover) for the clustered sweeps, where
/// the `Deactivated` half of W7 is left to the scenario suites and the `Quiescent`
/// half still holds.
pub struct WorkflowCycle {
    /// Whether a `Deactivated` with claims open is a violation (single-node tier)
    /// or a forced step-down abandoning its launches (clustered tier, §6).
    hibernation_is_idle: bool,
    /// Keys currently claimed, per entity and per activation of it: launched and
    /// not yet terminated.
    open: BTreeMap<EntityName, Claims>,
    /// Every key ever launched per entity, across activations, so a `Discarded`
    /// or an unclaimed `Settled` can be held to having had a launch behind it.
    seen: BTreeMap<EntityName, BTreeSet<String>>,
}

impl Default for WorkflowCycle {
    fn default() -> Self {
        WorkflowCycle::strict()
    }
}

impl WorkflowCycle {
    /// The single-node tier's checker: every `Deactivated` is an idle hibernation,
    /// so one with a claim open is a W7 violation.
    pub fn strict() -> WorkflowCycle {
        WorkflowCycle {
            hibernation_is_idle: true,
            open: BTreeMap::new(),
            seen: BTreeMap::new(),
        }
    }

    /// The clustered tier's checker: a `Deactivated` with claims open is read as a
    /// forced step-down abandoning its launches (§6), and only the `Quiescent`
    /// half of W7 is held.
    pub fn under_failover() -> WorkflowCycle {
        WorkflowCycle {
            hibernation_is_idle: false,
            ..WorkflowCycle::strict()
        }
    }

    /// The claims open on `entity`'s activation at `node`, for reading.
    fn open(&self, node: NodeId, entity: &EntityName) -> Option<&BTreeSet<String>> {
        self.open.get(entity).and_then(|claims| claims.get(&node))
    }

    /// Close `key`'s claim on `entity`'s activation at `node`, if one is open.
    fn close(&mut self, node: NodeId, entity: &EntityName, key: &str) -> bool {
        self.open
            .get_mut(entity)
            .and_then(|claims| claims.get_mut(&node))
            .is_some_and(|open| open.remove(key))
    }

    /// Forget every claim of `entity`'s activation at `node`: it deactivated, or a
    /// successor activated there.
    fn clear(&mut self, node: NodeId, entity: &EntityName) {
        if let Some(claims) = self.open.get_mut(entity) {
            claims.remove(&node);
        }
    }

    fn launched_somewhere(&self, entity: &EntityName, key: &str) -> bool {
        self.seen.get(entity).is_some_and(|seen| seen.contains(key))
    }

    /// Open a claim: a launch, or an orphaned resolution standing in for one.
    fn claim(
        &mut self,
        node: NodeId,
        entity: &EntityName,
        key: &str,
        how: &str,
    ) -> Result<(), String> {
        let seen = self.seen.entry(entity.clone()).or_default();
        if !seen.contains(key) {
            seen.insert(key.to_string());
        }
        let open = self
            .open
            .entry(entity.clone())
            .or_default()
            .entry(node)
            .or_default();
        if !open.insert(key.to_string()) {
            return Err(format!(
                "{entity} on {node} {how} {key} twice with the first still in flight — a \
                 later tick relaunched a claimed effect (W3)"
            ));
        }
        Ok(())
    }
}

impl Invariant for WorkflowCycle {
    fn name(&self) -> &'static str {
        "workflow-cycle"
    }

    fn forget_node(&mut self, node: NodeId) {
        // Process death: its activations' launches are abandoned, and their
        // outcomes become stragglers at whichever activation next holds each
        // entity (§6). `seen` is kept, because those stragglers must still be
        // recognized as having had a launch behind them (W5).
        for claims in self.open.values_mut() {
            claims.remove(&node);
        }
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        // The activation lifecycle is the entity's own (entity §13); the workflow
        // mints no duplicate of it (§8).
        if let Some(entity) = event.as_app::<EntityEvent>() {
            match entity {
                EntityEvent::Deactivated { node, name } => {
                    if self.hibernation_is_idle
                        && let Some(open) = self.open(*node, name)
                        && !open.is_empty()
                    {
                        return Err(format!(
                            "{name} on {node} deactivated with {} claim(s) still open ({}) — \
                             an activation with an effect in flight has not settled its \
                             work (W7)",
                            open.len(),
                            open.iter().cloned().collect::<Vec<_>>().join(", "),
                        ));
                    }
                    // Idle or forced, nothing is owed at deactivation: the next
                    // activation starts from an empty table (§6).
                    self.clear(*node, name);
                }
                EntityEvent::Activated { node, name } => {
                    self.clear(*node, name);
                }
                _ => {}
            }
            return Ok(());
        }

        let Some(event) = event.as_app::<WorkflowEvent>() else {
            return Ok(());
        };
        match event {
            WorkflowEvent::Launched { entity, key, node } => {
                self.claim(*node, entity, key, "launched")?;
            }
            WorkflowEvent::Interrupted { entity, key, node } => {
                self.claim(*node, entity, key, "resolved orphaned")?;
            }
            WorkflowEvent::Settled {
                entity, key, node, ..
            } => {
                if !self.close(*node, entity, key) && !self.launched_somewhere(entity, key) {
                    return Err(format!(
                        "{entity} on {node} settled {key}, which no activation launched — an \
                         outcome with no claim behind it (W3)"
                    ));
                }
            }
            WorkflowEvent::Discarded { entity, key, node } => {
                // A straggler may be an *earlier* activation's launch reaching a
                // later one, so its claim need not be open here — but the workflow
                // must never discard a key nothing ever launched.
                self.close(*node, entity, key);
                if !self.launched_somewhere(entity, key) {
                    return Err(format!(
                        "{entity} on {node} discarded {key}, which no launch on this stream \
                         produced (W5)"
                    ));
                }
            }
            WorkflowEvent::Quiescent { entity, node } => {
                if let Some(open) = self.open(*node, entity)
                    && !open.is_empty()
                {
                    return Err(format!(
                        "{entity} on {node} announced quiescence with {} claim(s) open — the \
                         desired set is not at rest (W7)",
                        open.len()
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Which D-invariant each tick checker enforces, as `(checker name, D number)`
/// — the pairing §9's "Verified by" column asserts, stated from the checker's
/// side, and held equal to the catalogue's per pair by `conformance_catalogue`.
pub fn checker_coverage() -> &'static [(&'static str, u8)] {
    &[
        ("workflow-cycle", 2),
        ("workflow-cycle", 3),
        ("workflow-cycle", 5),
        ("workflow-cycle", 7),
    ]
}

/// The core checkers plus the workflow's own, for a single-node run.
pub fn workflow_invariants() -> Vec<Box<dyn Invariant>> {
    let mut v = default_invariants();
    v.push(Box::new(WorkflowCycle::strict()));
    v
}

/// The core checkers plus the workflow's own, for a clustered sweep where a forced
/// step-down may abandon launches (§6).
pub fn workflow_invariants_under_failover() -> Vec<Box<dyn Invariant>> {
    let mut v = default_invariants();
    v.push(Box::new(WorkflowCycle::under_failover()));
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    const N1: NodeId = NodeId::new(1);
    const N2: NodeId = NodeId::new(2);

    fn name() -> EntityName {
        EntityName::new("test.Workflow", "g")
    }

    fn launched_on_node(node: NodeId, key: &str) -> Event {
        WorkflowEvent::Launched {
            entity: name(),
            key: key.into(),
            node,
        }
        .into()
    }

    fn launched(key: &str) -> Event {
        launched_on_node(N1, key)
    }

    fn interrupted(key: &str) -> Event {
        WorkflowEvent::Interrupted {
            entity: name(),
            key: key.into(),
            node: N1,
        }
        .into()
    }

    fn settled_on_node(node: NodeId, key: &str, committed: bool) -> Event {
        WorkflowEvent::Settled {
            entity: name(),
            key: key.into(),
            node,
            ok: true,
            committed,
        }
        .into()
    }

    fn settled(key: &str, committed: bool) -> Event {
        settled_on_node(N1, key, committed)
    }

    fn discarded(key: &str) -> Event {
        WorkflowEvent::Discarded {
            entity: name(),
            key: key.into(),
            node: N1,
        }
        .into()
    }

    fn quiescent() -> Event {
        WorkflowEvent::Quiescent {
            entity: name(),
            node: N1,
        }
        .into()
    }

    fn deactivated_on(node: NodeId) -> Event {
        Event::app(EntityEvent::Deactivated { node, name: name() })
    }

    fn activated_on(node: NodeId) -> Event {
        Event::app(EntityEvent::Activated { node, name: name() })
    }

    #[test]
    fn a_launch_and_its_settle_are_a_complete_claim() {
        let mut c = WorkflowCycle::strict();
        assert!(c.observe(&launched("k")).is_ok());
        assert!(c.observe(&settled("k", true)).is_ok());
        assert!(c.observe(&deactivated_on(N1)).is_ok(), "nothing left open");
    }

    #[test]
    fn a_settle_that_committed_nothing_still_closes_its_claim() {
        // The reason `Settled` carries `committed` and fires on every settle
        // (§8): an event that fired only on a commit would leave this claim open
        // forever and W7 could never be closed.
        let mut c = WorkflowCycle::strict();
        c.observe(&launched("k")).unwrap();
        assert!(c.observe(&settled("k", false)).is_ok());
        assert!(c.observe(&deactivated_on(N1)).is_ok());
    }

    #[test]
    fn relaunching_a_claimed_key_is_rejected() {
        let mut c = WorkflowCycle::strict();
        c.observe(&launched("k")).unwrap();
        let err = c.observe(&launched("k")).expect_err("W3");
        assert!(err.contains("twice"), "{err}");
    }

    #[test]
    fn relaunching_after_a_settle_is_the_ordinary_retry() {
        let mut c = WorkflowCycle::strict();
        c.observe(&launched("k")).unwrap();
        c.observe(&settled("k", false)).unwrap();
        assert!(
            c.observe(&launched("k")).is_ok(),
            "a backed-off relaunch is one launch per claim, not a second per claim"
        );
    }

    #[test]
    fn deactivating_with_a_claim_open_is_rejected_on_the_single_node_tier() {
        let mut c = WorkflowCycle::strict();
        c.observe(&launched("k")).unwrap();
        let err = c.observe(&deactivated_on(N1)).expect_err("W7");
        assert!(err.contains("still open"), "{err}");
    }

    #[test]
    fn deactivating_with_a_claim_open_is_a_step_down_under_failover() {
        // §6: a forced step-down abandons its launches, and entity's `Deactivated`
        // does not say which kind it was. The clustered checker reads it as the
        // legal kind and starts the next activation from an empty table.
        let mut c = WorkflowCycle::under_failover();
        c.observe(&launched("k")).unwrap();
        assert!(c.observe(&deactivated_on(N1)).is_ok());
        assert!(
            c.observe(&activated_on(N1)).is_ok() && c.observe(&launched("k")).is_ok(),
            "the successor relaunches the abandoned key as a fresh claim (§3.3)"
        );
    }

    #[test]
    fn announcing_quiescence_with_a_claim_open_is_rejected_on_both_tiers() {
        for mut c in [WorkflowCycle::strict(), WorkflowCycle::under_failover()] {
            c.observe(&launched("k")).unwrap();
            let err = c.observe(&quiescent()).expect_err("W7");
            assert!(err.contains("not at rest"), "{err}");
        }
    }

    #[test]
    fn an_orphan_resolution_opens_the_claim_its_settle_closes() {
        // W6 on the stream: the synthesized `Interrupted` outcome is a claim like
        // any other, resolved exactly once.
        let mut c = WorkflowCycle::strict();
        c.observe(&interrupted("k")).unwrap();
        let err = c.observe(&interrupted("k")).expect_err("resolved twice");
        assert!(err.contains("twice"), "{err}");
        assert!(c.observe(&settled("k", true)).is_ok());
        assert!(c.observe(&deactivated_on(N1)).is_ok());
    }

    #[test]
    fn a_straggler_of_a_previous_activation_is_accepted_but_an_invented_one_is_not() {
        let mut c = WorkflowCycle::strict();
        c.observe(&launched("k")).unwrap();
        c.observe(&settled("k", true)).unwrap();
        assert!(
            c.observe(&discarded("k")).is_ok(),
            "a duplicate outcome after the first settled is the straggler case"
        );
        let err = c.observe(&discarded("never")).expect_err("W5");
        assert!(err.contains("no launch"), "{err}");
    }

    #[test]
    fn a_settle_with_no_launch_behind_it_anywhere_is_rejected() {
        let mut c = WorkflowCycle::strict();
        let err = c.observe(&settled("k", true)).expect_err("W3");
        assert!(err.contains("no activation launched"), "{err}");
    }

    #[test]
    fn an_outcome_of_a_previous_activations_launch_may_settle_at_the_successor() {
        // §3.3: across activations an effect may run more than once, and the
        // outcome of the deposed leader's launch may reach the node that now
        // holds the entity. It closes no claim there, but it is not an invention.
        let mut c = WorkflowCycle::under_failover();
        c.observe(&launched_on_node(N1, "k")).unwrap();
        c.observe(&deactivated_on(N1)).unwrap();
        c.observe(&activated_on(N2)).unwrap();
        assert!(c.observe(&settled_on_node(N2, "k", true)).is_ok());
    }

    #[test]
    fn claims_are_per_activation_not_per_entity() {
        // Two activations of one entity on two nodes: a launch on each is not a
        // double launch, and one's settle does not close the other's claim.
        let mut c = WorkflowCycle::under_failover();
        c.observe(&launched_on_node(N1, "k")).unwrap();
        assert!(c.observe(&launched_on_node(N2, "k")).is_ok());
        c.observe(&settled_on_node(N2, "k", true)).unwrap();
        let err = c
            .observe(&quiescent())
            .expect_err("N1's claim is still open");
        assert!(err.contains("not at rest"), "{err}");
    }

    #[test]
    fn a_process_death_abandons_its_claims_and_keeps_what_it_launched() {
        let mut c = WorkflowCycle::under_failover();
        c.observe(&launched_on_node(N1, "k")).unwrap();
        c.forget_node(N1);
        assert!(
            c.observe(&quiescent()).is_ok(),
            "nothing open on N1 any more"
        );
        assert!(
            c.observe(&discarded("k")).is_ok(),
            "the dead process's launch still counts as the launch behind a straggler (W5)"
        );
    }
}
