//! Raft control-plane safety under a **rolling-partition election storm** (spec
//! §9.4.3, invariants #14, #15, #22), under deterministic simulation (§18).
//!
//! `conformance_leader.rs` pins the leader-based control plane through *scripted,
//! single-shot* faults: one election, one failover, one minority partition. That
//! proves each mechanism in isolation. This file is the adversarial complement —
//! a Jepsen-style nemesis that repeatedly isolates **whichever node is currently
//! leader**, forcing term after term of re-election while committed membership
//! entries flow on the majority side, then heals and proves the deep Raft
//! properties held *throughout*:
//!
//! - **State Machine Safety / log matching (#22, "applied in log order").** No two
//!   voters ever apply a different `(status, stamp)` for any member — the stamp is
//!   the commit index (the authority stamp, §9.2), so disagreement would mean the
//!   committed logs forked. Asserted at convergence after the storm.
//! - **Election safety (#22).** Across every term the storm induces, at most one
//!   leader is ever elected per term (checked over the §16 `LeaderElected` stream).
//! - **Convergence (#14).** Once the partitions heal, every `up` member converges
//!   on one membership view — voters by log replication, the non-voter by gossip.
//! - **`down` is terminal / no spurious down (#15, #16).** Under the conservative
//!   policy a partition alone downs nobody; every node survives the storm.
//!
//! The nemesis uses `SimNetwork::partition()`/`heal()` (heal-able, symmetric — the
//! sim has no asymmetric-partition or clock-skew primitive). The downing policy is
//! `Conservative` deliberately: leadership must churn via Raft's *own* election
//! timeout while every member stays alive, so any final disagreement is a true log
//! fork, never an expected eviction.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::MemberStatus;
use actor_cluster::RaftConfig;
use actor_cluster::SwimConfig;
use actor_core::Event;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_simulation::Recorder;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);
const D: NodeId = NodeId::new(4);
const E: NodeId = NodeId::new(5);
const F: NodeId = NodeId::new(6); // a non-voter member: the drain/resume payload

const VOTERS: [NodeId; 5] = [A, B, C, D, E];

fn swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(300),
        indirect_count: 2,
    }
}

fn raft() -> RaftConfig {
    let mut config = RaftConfig::new(VOTERS.to_vec());
    config.election_timeout = Duration::from_millis(500);
    config.heartbeat_interval = Duration::from_millis(100);
    config
}

fn drive<T: Send + 'static>(
    sim: &Simulation,
    settle: Duration,
    future: impl Future<Output = T> + Send + 'static,
) -> T {
    let cell: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let out = Arc::clone(&cell);
    sim.spawner().launch(Box::pin(async move {
        *out.lock().unwrap() = Some(future.await);
    }));
    sim.run_for(settle);
    cell.lock()
        .unwrap()
        .take()
        .expect("future did not complete")
}

/// Bring up the 5-voter cluster plus the non-voter member F, with an event
/// recorder. Returns `(net, systems, F-system, recorder)`; `systems` is indexed to
/// align with `VOTERS`.
fn storm_cluster(sim: &Simulation) -> (SimNetwork, Vec<SimCluster>, SimCluster, Recorder) {
    let recorder = Recorder::new();
    let net = SimNetwork::new(sim)
        .with_leader(swim(), raft(), DowningPolicy::Conservative)
        .with_events(Arc::new(recorder.clone()));
    let systems: Vec<SimCluster> = VOTERS.iter().map(|&n| net.join(n)).collect();
    sim.run_for(Duration::from_secs(3)); // elect the first leader
    let f = net.join_seeded(F, &[A]); // a non-voter member, admitted through the log
    sim.run_for(Duration::from_secs(3)); // gossip F in and commit its admission
    (net, systems, f, recorder)
}

/// The leader the up majority currently agrees on, read from a node that is not
/// presently isolated. After a full heal+reconverge every node agrees, so any
/// reachable voter is a valid vantage point.
fn agreed_leader(systems: &[SimCluster]) -> Option<NodeId> {
    let mut votes: BTreeMap<NodeId, usize> = BTreeMap::new();
    for s in systems {
        if let Some(l) = s.leader() {
            *votes.entry(l).or_default() += 1;
        }
    }
    // The leader a quorum (3 of 5) names.
    votes.into_iter().find(|&(_, n)| n >= 3).map(|(l, _)| l)
}

#[test]
fn rolling_partitions_never_fork_the_committed_membership_log() {
    // The crown jewel for the control plane. Repeatedly isolate the current leader
    // into a minority, commit a membership change (drain/resume F) on the majority
    // side, then heal — round after round. The storm forces many terms and many
    // leaders. At the end, every voter must agree on the exact `(status, stamp)` of
    // every member: identical commit indices mean identical committed logs. A stale
    // leader that committed on a minority side, or a lost/duplicated entry, would
    // surface as two voters disagreeing on a member's stamp here.
    for seed in 0..8 {
        let sim = Simulation::new(seed);
        let (net, systems, f, recorder) = storm_cluster(&sim);

        let mut committed_proposals = 0usize;
        const ROUNDS: usize = 6;
        for round in 0..ROUNDS {
            let leader = match agreed_leader(&systems) {
                Some(l) => l,
                None => {
                    // Mid-flux: let it settle and retry this round.
                    sim.run_for(Duration::from_secs(2));
                    continue;
                }
            };
            let majority: Vec<NodeId> = VOTERS.iter().copied().filter(|&n| n != leader).collect();

            // Isolate the leader; the majority's Raft election timeout fires and a
            // new leader is elected at a higher term.
            net.partition(&[leader], &majority);
            sim.run_for(Duration::from_secs(3));

            // Commit a membership change from a majority node (it forwards to the
            // new leader). Alternate drain/resume so F's committed history grows.
            let proposer = systems
                .iter()
                .find(|s| s.node() != leader)
                .expect("a majority node exists")
                .clone();
            let ok = drive(&sim, Duration::from_secs(6), async move {
                if round % 2 == 0 {
                    proposer.drain(F).await
                } else {
                    proposer.resume(F).await
                }
            });
            if ok {
                committed_proposals += 1;
            }

            // Heal: the isolated old leader learns the higher term and steps down.
            net.heal();
            sim.run_for(Duration::from_secs(4));
        }

        // Full reconvergence after the storm.
        net.heal();
        sim.run_for(Duration::from_secs(8));

        // (#22 / State Machine Safety) Every voter agrees on every *other* member's
        // (status, stamp). The stamp is the commit index, so agreement here is
        // log-matching: the committed logs never diverged. A node tracks its own
        // status via `self_status()`, not the peer map (`status(self)` is `None`),
        // so the self-entry is checked separately below — comparing it here would
        // flag a measurement artifact, not a fork.
        let members = [A, B, C, D, E, F];
        for &m in &members {
            let views: Vec<(NodeId, Option<MemberStatus>, Option<u64>)> = systems
                .iter()
                .filter(|s| s.node() != m)
                .map(|s| (s.node(), s.membership().status(m), s.membership().stamp(m)))
                .collect();
            let first = (views[0].1, views[0].2);
            assert!(
                views.iter().all(|v| (v.1, v.2) == first),
                "seed {seed}: voters disagree on member {m:?} — committed log forked \
                 (#22). views = {views:?}",
            );
            // The member's own self-view must match its peers' committed view of it
            // (#14 convergence) — a node never disagrees with the cluster about its
            // own committed status.
            if let Some(self_sys) = systems.iter().find(|s| s.node() == m) {
                assert_eq!(
                    Some(self_sys.membership().self_status()),
                    first.0,
                    "seed {seed}: {m:?}'s self-view diverges from the committed cluster view (#14)",
                );
            }
        }

        // (#15 / #16) No node was ever downed — a partition alone evicts nobody
        // under the conservative policy, and there were no crashes.
        for &m in &members {
            assert!(
                !systems[0].membership().is_down(m),
                "seed {seed}: member {m:?} was downed by a mere partition (#16)",
            );
        }

        // Teeth: the storm must have genuinely churned leadership AND committed
        // entries through that churn — otherwise the agreement check above compared
        // a static, never-contested log and proved nothing. Confirm from the §16
        // stream that several terms elected leaders and that leadership actually
        // moved between distinct nodes (a real, repeated split-brain).
        let mut distinct_leaders: BTreeSet<NodeId> = BTreeSet::new();
        let mut elected_terms: BTreeSet<u64> = BTreeSet::new();
        for event in recorder.events() {
            if let Event::LeaderElected { node, term, .. } = event {
                distinct_leaders.insert(node);
                elected_terms.insert(term);
            }
        }
        assert!(
            committed_proposals > 0,
            "seed {seed}: no membership change committed — the storm proved nothing",
        );
        assert!(
            distinct_leaders.len() >= 2 && elected_terms.len() >= 3,
            "seed {seed}: weak storm (leaders={distinct_leaders:?}, terms={}) — the log was \
             never genuinely contested, so non-divergence is not meaningfully tested",
            elected_terms.len(),
        );

        // (#14) The non-voter F converged on the same view by gossip, matching the
        // voters' log-replicated truth.
        let f_status_per_voter = systems[0].membership().status(F);
        assert_eq!(
            Some(f.membership().self_status()),
            f_status_per_voter,
            "seed {seed}: the non-voter F did not converge on the committed view (#14)",
        );
    }
}

#[test]
fn one_leader_per_term_survives_an_election_storm() {
    // Election safety (#22) is a continuous safety property; here it must hold
    // across an election STORM, not a single failover. The rolling partition forces
    // a new term every round; over the whole §16 `LeaderElected` stream, no term in
    // any group may ever name two different leaders. Teeth: assert the term actually
    // advanced well past the start, so this is not a vacuous single-election pass.
    for seed in 0..8 {
        let sim = Simulation::new(seed);
        let (net, systems, _f, recorder) = storm_cluster(&sim);

        const ROUNDS: usize = 8;
        for _ in 0..ROUNDS {
            let Some(leader) = agreed_leader(&systems) else {
                sim.run_for(Duration::from_secs(2));
                continue;
            };
            let majority: Vec<NodeId> = VOTERS.iter().copied().filter(|&n| n != leader).collect();
            net.partition(&[leader], &majority);
            sim.run_for(Duration::from_secs(3)); // a fresh election on the majority
            net.heal();
            sim.run_for(Duration::from_secs(3)); // old leader steps down, reconverge
        }

        // One leader per (group, term) over the entire run.
        let mut leaders: BTreeMap<(u64, u64), NodeId> = BTreeMap::new();
        let mut terms_seen: BTreeSet<u64> = BTreeSet::new();
        let mut max_term = 0u64;
        for event in recorder.events() {
            if let Event::LeaderElected { node, term, group } = event {
                terms_seen.insert(term);
                max_term = max_term.max(term);
                if let Some(prev) = leaders.insert((group, term), node) {
                    assert_eq!(
                        prev, node,
                        "seed {seed}: two leaders in group {group} term {term}: \
                         {prev:?} and {node:?} (election safety #22)",
                    );
                }
            }
        }

        // Teeth: the storm really churned leadership through many terms.
        assert!(
            max_term >= ROUNDS as u64,
            "seed {seed}: only reached term {max_term} after {ROUNDS} partitions — \
             the storm did not actually force re-elections",
        );
        assert!(
            terms_seen.len() >= ROUNDS,
            "seed {seed}: only {} distinct terms saw an election; expected the storm to \
             force at least {ROUNDS}",
            terms_seen.len(),
        );
    }
}

#[test]
fn the_cluster_keeps_committing_and_reconverges_after_the_storm() {
    // Liveness (#14): a leader-based control plane that churns leadership under a
    // partition storm must, once healed, return to committing membership changes
    // and present a single converged view from every node. Run the storm, heal,
    // then prove a fresh proposal commits and every voter sees its effect at the
    // same stamp — the control plane is neither wedged nor split.
    let sim = Simulation::new(3);
    let (net, systems, _f, _recorder) = storm_cluster(&sim);

    for round in 0..5 {
        let Some(leader) = agreed_leader(&systems) else {
            sim.run_for(Duration::from_secs(2));
            continue;
        };
        let majority: Vec<NodeId> = VOTERS.iter().copied().filter(|&n| n != leader).collect();
        net.partition(&[leader], &majority);
        sim.run_for(Duration::from_secs(3));
        let proposer = systems.iter().find(|s| s.node() != leader).unwrap().clone();
        let _ = drive(&sim, Duration::from_secs(6), async move {
            if round % 2 == 0 {
                proposer.drain(F).await
            } else {
                proposer.resume(F).await
            }
        });
        net.heal();
        sim.run_for(Duration::from_secs(4));
    }

    net.heal();
    sim.run_for(Duration::from_secs(8));

    // A fresh proposal after the storm commits through the (re-stabilized) leader.
    let leader = agreed_leader(&systems).expect("the cluster reconverged on a leader");
    let proposer = systems.iter().find(|s| s.node() == leader).unwrap().clone();
    let committed = drive(&sim, Duration::from_secs(8), async move {
        proposer.drain(F).await
    });
    assert!(
        committed,
        "the control plane commits again after the storm (#14 liveness)"
    );
    sim.run_for(Duration::from_secs(3));

    // Every voter sees F draining at one and the same committed stamp.
    let stamp = systems[0].membership().stamp(F);
    for s in &systems {
        assert_eq!(
            s.membership().status(F),
            Some(MemberStatus::Draining),
            "voter {:?} sees the commit",
            s.node()
        );
        assert_eq!(
            s.membership().stamp(F),
            stamp,
            "voter {:?} holds it at the one log index",
            s.node()
        );
    }
}
