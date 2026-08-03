//! Coverage accounting for the faults the transport cannot see.
//!
//! `run_cluster_swarm_coverage` proves a sweep injected loss, duplication,
//! delay, and blocking, because those are the wire's and `FaultStats` counts
//! them. The two faults that matter most to a hibernating sweep are not the
//! wire's: whether an activation actually passivated and came back, and whether
//! the nemesis actually killed a process. A sweep that configures a short
//! `idle_after` but whose grains all stay resident, or one whose vocabulary
//! includes a restart it never draws, is a green run that proves nothing.
//!
//! [`Exercised`] is an [`Invariant`] that never fails — it only counts, off the
//! same event stream the real checkers read. A workload holds one, hands clones
//! to each run through `invariants()`, and the test asserts on the totals after
//! the sweep.
//!
//! A total asserted on a narrowable sweep must hold at **any** width, because
//! those sweeps narrow locally (`sweep_seeds` to 8, `slow_seeds` to 1) and a
//! coverage claim that only came true on a wide run would be a claim about the
//! run rather than the workload. Most hibernating workloads earn that by
//! *driving* these paths rather than sampling them: they idle past `idle_after`
//! on a fixed cadence rather than a seeded coin, and snapshot often enough that a
//! grain has a checkpoint to return from before it first passivates. What stays
//! seeded is everything the nemesis and the wire do around that.
//!
//! A workload that cannot drive them has to say so, and one cannot: the disk
//! swarm's grains must import a multi-megabyte image before they can commit
//! anything, and under a nemesis that kills processes there are seeds where the
//! cluster never gives them a window — seeds on which nothing passivates at all.
//! Its totals are therefore an aggregate over the seed range rather than a
//! per-seed property, so they are asserted on a sweep sized by `coverage_seeds`,
//! which never narrows. See
//! `disk_swarm.rs::disk_hibernation_actually_passivates_and_restores_from_a_snapshot`,
//! which records how that was found out. Before assuming a new workload drives
//! these paths, check it at width 1 across the declared range rather than at the
//! one seed a local run happens to draw.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use actor_core::Event;
use actor_simulation::Invariant;
use actor_simulation::NodeRestarted;
use granary::GrainEvent;

/// What a sweep actually exercised, accumulated across its seeds.
#[derive(Clone, Default)]
pub struct Exercised {
    passivated: Arc<AtomicUsize>,
    from_snapshot: Arc<AtomicUsize>,
    restarted: Arc<AtomicUsize>,
}

impl Exercised {
    /// Activations that hibernated (`GrainEvent::Passivated`).
    pub fn passivated(&self) -> usize {
        self.passivated.load(Ordering::Relaxed)
    }

    /// Activations that came back from a snapshot rather than replaying from an
    /// empty base (`GrainEvent::Rehydrated { from_snapshot: true, .. }`).
    pub fn snapshot_restores(&self) -> usize {
        self.from_snapshot.load(Ordering::Relaxed)
    }

    /// Node processes the nemesis killed and replaced.
    pub fn restarted(&self) -> usize {
        self.restarted.load(Ordering::Relaxed)
    }

    /// Assert this sweep really hibernated — the shape every hibernating sweep
    /// wants, so the four of them state it once.
    ///
    /// Deliberately *not* including [`restarted`](Self::restarted). Whether a
    /// grain passivates and returns is something the workload drives, so it holds
    /// however narrow the run; whether the nemesis ever *draws* a restart is a
    /// property of the seed range, which at `slow_seeds`' single local seed is a
    /// coin it loses about two runs in five. That claim belongs on a sweep sized
    /// by `coverage_seeds`, which never narrows — see
    /// `grain_swarm.rs::the_nemesis_actually_restarts_a_node`, where it is stated
    /// once for the nemesis rather than four times for four facets.
    pub fn assert_hibernated(&self) {
        assert!(
            self.passivated() > 0,
            "no activation ever hibernated: this is the resident sweep with a \
             shorter idle_after, not a hibernating one",
        );
        assert!(
            self.snapshot_restores() > 0,
            "no activation ever came back from a snapshot: every rehydration \
             replayed from an empty base, so the snapshot restore path is untested",
        );
    }
}

impl Invariant for Exercised {
    fn name(&self) -> &'static str {
        "hibernation-exercised"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        if let Some(grain) = event.as_app::<GrainEvent>() {
            match grain {
                GrainEvent::Passivated { .. } => {
                    self.passivated.fetch_add(1, Ordering::Relaxed);
                }
                GrainEvent::Rehydrated { from_snapshot, .. } if *from_snapshot => {
                    self.from_snapshot.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
        }
        if event.as_app::<NodeRestarted>().is_some() {
            self.restarted.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}
