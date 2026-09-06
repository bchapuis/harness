//! The continuous D-checkers over the drive's event stream (drive spec §9).
//!
//! One checker, `drive-cycle`, carrying four of the seven invariants: it is the
//! grammar of a claim's life. A key is launched (or resolved as dangling), and
//! then exactly one of two things happens to it — it settles, or it is discarded
//! as a straggler — and no activation hibernates or announces quiescence with a
//! claim still open.
//!
//! Test support behind the `testing` feature, not part of the drive's API, and
//! mountable by any consumer's swarm as granary and the harness both ship theirs.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use actor_core::Event;
use actor_core::NodeId;
use actor_simulation::Invariant;
use actor_simulation::default_invariants;
use granary::GrainEvent;
use granary::GrainName;

use crate::DriveEvent;

/// A grain's open claims, per node hosting an activation of it (granary §13).
/// Under failover a deposed leader and its successor are two activations of one
/// grain, and their claims must not be confused.
type Claims = BTreeMap<NodeId, BTreeSet<String>>;

/// **The drive cycle's grammar** (D2, D3, D5, D7).
///
/// - **D3, launch-once per claim.** A `Launched` for a key already open on this
///   activation is a second launch of an effect in flight, which no re-drive may
///   produce. A `Settled` closes an open claim; one with no claim open here must
///   at least name a key some activation launched — an outcome of a previous
///   activation's launch reaching the one that now holds the grain (§3.3) — or
///   the drive settled something nothing ever sent.
/// - **D5, straggler rejection.** A `Discarded` names a key whose outcome was
///   dropped without a record; it must correspond to a launch on the stream, or
///   the drive discarded something it never sent.
/// - **D7, quiescence.** A `Quiescent` must not be announced while a claim is
///   open, and on the single-node tier the grain's `Passivated` (granary §13) must
///   not arrive while one is: an activation with an effect in flight has not
///   settled its work, and hibernating under it would strand the outcome.
/// - **D2, launch coverage**, in the shape a stream can carry: the `Quiescent`
///   half of the rule above.
///
/// What the stream cannot show — that a *backed-off* key is eventually relaunched
/// — is a scenario assertion rather than a grammar rule, because the liveness is
/// conditional on the activation surviving (§9 D2). The checker does hold the
/// half that is checkable: a `Settled { committed: false }` closes its claim, so
/// a stranded key shows up as a `Passivated` with nothing open and the work still
/// in the fold, which the scenario suites assert against the journal.
///
/// **Two tiers, one rule that differs.** A forced step-down — a leadership move,
/// quorum loss — is involuntary, ignores `can_passivate`, and legitimately
/// abandons its launches (§6); on the stream it is a `Passivated` with claims open,
/// exactly the shape of a D7 violation, and granary's event carries no reason that
/// would tell the two apart. So the checker is built for the tier it watches:
/// [`strict`](DriveCycle::strict) for the single-node suites, where nothing is
/// ever deposed and every `Passivated` is an idle hibernation, and
/// [`under_failover`](DriveCycle::under_failover) for the clustered sweeps, where
/// the `Passivated` half of D7 is left to the scenario suites and the `Quiescent`
/// half still holds.
pub struct DriveCycle {
    /// Whether a `Passivated` with claims open is a violation (single-node tier)
    /// or a forced step-down abandoning its launches (clustered tier, §6).
    passivation_is_idle: bool,
    /// Keys currently claimed, per grain and per activation of it: launched and
    /// not yet terminated.
    open: BTreeMap<GrainName, Claims>,
    /// Every key ever launched per grain, across activations, so a `Discarded`
    /// or an unclaimed `Settled` can be held to having had a launch behind it.
    seen: BTreeMap<GrainName, BTreeSet<String>>,
}

impl Default for DriveCycle {
    fn default() -> Self {
        DriveCycle::strict()
    }
}

impl DriveCycle {
    /// The single-node tier's checker: every `Passivated` is an idle hibernation,
    /// so one with a claim open is a D7 violation.
    pub fn strict() -> DriveCycle {
        DriveCycle {
            passivation_is_idle: true,
            open: BTreeMap::new(),
            seen: BTreeMap::new(),
        }
    }

    /// The clustered tier's checker: a `Passivated` with claims open is read as a
    /// forced step-down abandoning its launches (§6), and only the `Quiescent`
    /// half of D7 is held.
    pub fn under_failover() -> DriveCycle {
        DriveCycle {
            passivation_is_idle: false,
            ..DriveCycle::strict()
        }
    }

    /// The claims open on `grain`'s activation at `node`, for reading.
    fn open(&self, node: NodeId, grain: &GrainName) -> Option<&BTreeSet<String>> {
        self.open.get(grain).and_then(|claims| claims.get(&node))
    }

    /// Close `key`'s claim on `grain`'s activation at `node`, if one is open.
    fn close(&mut self, node: NodeId, grain: &GrainName, key: &str) -> bool {
        self.open
            .get_mut(grain)
            .and_then(|claims| claims.get_mut(&node))
            .is_some_and(|open| open.remove(key))
    }

    /// Forget every claim of `grain`'s activation at `node`: it deactivated, or a
    /// successor activated there.
    fn clear(&mut self, node: NodeId, grain: &GrainName) {
        if let Some(claims) = self.open.get_mut(grain) {
            claims.remove(&node);
        }
    }

    fn launched_somewhere(&self, grain: &GrainName, key: &str) -> bool {
        self.seen.get(grain).is_some_and(|seen| seen.contains(key))
    }

    /// Open a claim: a launch, or a dangling resolution standing in for one.
    fn claim(
        &mut self,
        node: NodeId,
        grain: &GrainName,
        key: &str,
        how: &str,
    ) -> Result<(), String> {
        let seen = self.seen.entry(grain.clone()).or_default();
        if !seen.contains(key) {
            seen.insert(key.to_string());
        }
        let open = self
            .open
            .entry(grain.clone())
            .or_default()
            .entry(node)
            .or_default();
        if !open.insert(key.to_string()) {
            return Err(format!(
                "{grain} on {node} {how} {key} twice with the first still in flight — a \
                 re-drive relaunched a claimed effect (D3)"
            ));
        }
        Ok(())
    }
}

impl Invariant for DriveCycle {
    fn name(&self) -> &'static str {
        "drive-cycle"
    }

    fn forget_node(&mut self, node: NodeId) {
        // Process death: its activations' launches are abandoned, and their
        // outcomes become stragglers at whichever activation next holds each
        // grain (§6). `seen` is kept, because those stragglers must still be
        // recognized as having had a launch behind them (D5).
        for claims in self.open.values_mut() {
            claims.remove(&node);
        }
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        // The activation lifecycle is the grain's own (granary §13); the drive
        // mints no duplicate of it (§8).
        if let Some(grain) = event.as_app::<GrainEvent>() {
            match grain {
                GrainEvent::Passivated { node, name } => {
                    if self.passivation_is_idle
                        && let Some(open) = self.open(*node, name)
                        && !open.is_empty()
                    {
                        return Err(format!(
                            "{name} on {node} passivated with {} claim(s) still open ({}) — \
                             an activation with an effect in flight has not settled its \
                             work (D7)",
                            open.len(),
                            open.iter().cloned().collect::<Vec<_>>().join(", "),
                        ));
                    }
                    // Idle or forced, nothing is owed at deactivation: the next
                    // activation starts from an empty table (§6).
                    self.clear(*node, name);
                }
                GrainEvent::Activated { node, name } => {
                    self.clear(*node, name);
                }
                _ => {}
            }
            return Ok(());
        }

        let Some(event) = event.as_app::<DriveEvent>() else {
            return Ok(());
        };
        match event {
            DriveEvent::Launched { grain, key, node } => {
                self.claim(*node, grain, key, "launched")?;
            }
            DriveEvent::Interrupted { grain, key, node } => {
                self.claim(*node, grain, key, "resolved dangling")?;
            }
            DriveEvent::Settled {
                grain, key, node, ..
            } => {
                if !self.close(*node, grain, key) && !self.launched_somewhere(grain, key) {
                    return Err(format!(
                        "{grain} on {node} settled {key}, which no activation launched — an \
                         outcome with no claim behind it (D3)"
                    ));
                }
            }
            DriveEvent::Discarded { grain, key, node } => {
                // A straggler may be an *earlier* activation's launch reaching a
                // later one, so its claim need not be open here — but the drive
                // must never discard a key nothing ever launched.
                self.close(*node, grain, key);
                if !self.launched_somewhere(grain, key) {
                    return Err(format!(
                        "{grain} on {node} discarded {key}, which no launch on this stream \
                         produced (D5)"
                    ));
                }
            }
            DriveEvent::Quiescent { grain, node } => {
                if let Some(open) = self.open(*node, grain)
                    && !open.is_empty()
                {
                    return Err(format!(
                        "{grain} on {node} announced quiescence with {} claim(s) open — the \
                         desired set is not at rest (D7)",
                        open.len()
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Which D-invariant each drive checker enforces, as `(checker name, D number)`
/// — the pairing §9's "Verified by" column asserts, stated from the checker's
/// side, and held equal to the catalogue's per pair by `conformance_catalogue`.
pub fn checker_coverage() -> &'static [(&'static str, u8)] {
    &[
        ("drive-cycle", 2),
        ("drive-cycle", 3),
        ("drive-cycle", 5),
        ("drive-cycle", 7),
    ]
}

/// The core checkers plus the drive's own, for a single-node run.
pub fn drive_invariants() -> Vec<Box<dyn Invariant>> {
    let mut v = default_invariants();
    v.push(Box::new(DriveCycle::strict()));
    v
}

/// The core checkers plus the drive's own, for a clustered sweep where a forced
/// step-down may abandon launches (§6).
pub fn drive_invariants_under_failover() -> Vec<Box<dyn Invariant>> {
    let mut v = default_invariants();
    v.push(Box::new(DriveCycle::under_failover()));
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    const N1: NodeId = NodeId::new(1);
    const N2: NodeId = NodeId::new(2);

    fn name() -> GrainName {
        GrainName::new("test.Driven", "g")
    }

    fn launched_on_node(node: NodeId, key: &str) -> Event {
        DriveEvent::Launched {
            grain: name(),
            key: key.into(),
            node,
        }
        .into()
    }

    fn launched(key: &str) -> Event {
        launched_on_node(N1, key)
    }

    fn interrupted(key: &str) -> Event {
        DriveEvent::Interrupted {
            grain: name(),
            key: key.into(),
            node: N1,
        }
        .into()
    }

    fn settled_on_node(node: NodeId, key: &str, committed: bool) -> Event {
        DriveEvent::Settled {
            grain: name(),
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
        DriveEvent::Discarded {
            grain: name(),
            key: key.into(),
            node: N1,
        }
        .into()
    }

    fn quiescent() -> Event {
        DriveEvent::Quiescent {
            grain: name(),
            node: N1,
        }
        .into()
    }

    fn passivated_on(node: NodeId) -> Event {
        Event::app(GrainEvent::Passivated { node, name: name() })
    }

    fn activated_on(node: NodeId) -> Event {
        Event::app(GrainEvent::Activated { node, name: name() })
    }

    #[test]
    fn a_launch_and_its_settle_are_a_complete_claim() {
        let mut c = DriveCycle::strict();
        assert!(c.observe(&launched("k")).is_ok());
        assert!(c.observe(&settled("k", true)).is_ok());
        assert!(c.observe(&passivated_on(N1)).is_ok(), "nothing left open");
    }

    #[test]
    fn a_settle_that_committed_nothing_still_closes_its_claim() {
        // The reason `Settled` carries `committed` and fires on every settle
        // (§8): an event that fired only on a commit would leave this claim open
        // forever and D7 could never be closed.
        let mut c = DriveCycle::strict();
        c.observe(&launched("k")).unwrap();
        assert!(c.observe(&settled("k", false)).is_ok());
        assert!(c.observe(&passivated_on(N1)).is_ok());
    }

    #[test]
    fn relaunching_a_claimed_key_is_rejected() {
        let mut c = DriveCycle::strict();
        c.observe(&launched("k")).unwrap();
        let err = c.observe(&launched("k")).expect_err("D3");
        assert!(err.contains("twice"), "{err}");
    }

    #[test]
    fn relaunching_after_a_settle_is_the_ordinary_retry() {
        let mut c = DriveCycle::strict();
        c.observe(&launched("k")).unwrap();
        c.observe(&settled("k", false)).unwrap();
        assert!(
            c.observe(&launched("k")).is_ok(),
            "a backed-off relaunch is one launch per claim, not a second per claim"
        );
    }

    #[test]
    fn passivating_with_a_claim_open_is_rejected_on_the_single_node_tier() {
        let mut c = DriveCycle::strict();
        c.observe(&launched("k")).unwrap();
        let err = c.observe(&passivated_on(N1)).expect_err("D7");
        assert!(err.contains("still open"), "{err}");
    }

    #[test]
    fn passivating_with_a_claim_open_is_a_step_down_under_failover() {
        // §6: a forced step-down abandons its launches, and granary's `Passivated`
        // does not say which kind it was. The clustered checker reads it as the
        // legal kind and starts the next activation from an empty table.
        let mut c = DriveCycle::under_failover();
        c.observe(&launched("k")).unwrap();
        assert!(c.observe(&passivated_on(N1)).is_ok());
        assert!(
            c.observe(&activated_on(N1)).is_ok() && c.observe(&launched("k")).is_ok(),
            "the successor relaunches the abandoned key as a fresh claim (§3.3)"
        );
    }

    #[test]
    fn announcing_quiescence_with_a_claim_open_is_rejected_on_both_tiers() {
        for mut c in [DriveCycle::strict(), DriveCycle::under_failover()] {
            c.observe(&launched("k")).unwrap();
            let err = c.observe(&quiescent()).expect_err("D7");
            assert!(err.contains("not at rest"), "{err}");
        }
    }

    #[test]
    fn a_dangling_resolution_opens_the_claim_its_settle_closes() {
        // D6 on the stream: the synthesized `Interrupted` outcome is a claim like
        // any other, resolved exactly once.
        let mut c = DriveCycle::strict();
        c.observe(&interrupted("k")).unwrap();
        let err = c.observe(&interrupted("k")).expect_err("resolved twice");
        assert!(err.contains("twice"), "{err}");
        assert!(c.observe(&settled("k", true)).is_ok());
        assert!(c.observe(&passivated_on(N1)).is_ok());
    }

    #[test]
    fn a_straggler_of_a_previous_activation_is_accepted_but_an_invented_one_is_not() {
        let mut c = DriveCycle::strict();
        c.observe(&launched("k")).unwrap();
        c.observe(&settled("k", true)).unwrap();
        assert!(
            c.observe(&discarded("k")).is_ok(),
            "a duplicate outcome after the first settled is the straggler case"
        );
        let err = c.observe(&discarded("never")).expect_err("D5");
        assert!(err.contains("no launch"), "{err}");
    }

    #[test]
    fn a_settle_with_no_launch_behind_it_anywhere_is_rejected() {
        let mut c = DriveCycle::strict();
        let err = c.observe(&settled("k", true)).expect_err("D3");
        assert!(err.contains("no activation launched"), "{err}");
    }

    #[test]
    fn an_outcome_of_a_previous_activations_launch_may_settle_at_the_successor() {
        // §3.3: across activations an effect may run more than once, and the
        // outcome of the deposed leader's launch may reach the node that now
        // holds the grain. It closes no claim there, but it is not an invention.
        let mut c = DriveCycle::under_failover();
        c.observe(&launched_on_node(N1, "k")).unwrap();
        c.observe(&passivated_on(N1)).unwrap();
        c.observe(&activated_on(N2)).unwrap();
        assert!(c.observe(&settled_on_node(N2, "k", true)).is_ok());
    }

    #[test]
    fn claims_are_per_activation_not_per_grain() {
        // Two activations of one grain on two nodes: a launch on each is not a
        // double launch, and one's settle does not close the other's claim.
        let mut c = DriveCycle::under_failover();
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
        let mut c = DriveCycle::under_failover();
        c.observe(&launched_on_node(N1, "k")).unwrap();
        c.forget_node(N1);
        assert!(
            c.observe(&quiescent()).is_ok(),
            "nothing open on N1 any more"
        );
        assert!(
            c.observe(&discarded("k")).is_ok(),
            "the dead process's launch still counts as the launch behind a straggler (D5)"
        );
    }
}
