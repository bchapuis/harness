//! The Tier-2 Raft-backed journal under deterministic simulation (granary §14).
//!
//! Drives a 3-node leader-mode cluster and exercises [`RaftJournal`] directly
//! (not yet through a running `Granary`): an append is durable on a quorum and
//! visible on every replica (§7.2); a follower's append is fenced with
//! `NotLeader` (§8, G8); committed state survives leader failover (G14); and
//! quorum loss surfaces as `Unavailable` (§11, G11).

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::GroupId;
use actor_cluster::RaftConfig;
use actor_cluster::RaftLog;
use actor_cluster::SwimConfig;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use granary::AppendOutcome;
use granary::GrainName;
use granary::Journal;
use granary::RaftJournal;
use granary::Seq;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);

/// The grain type's journal group (an application Raft group, distinct from the
/// membership control group).
const SHARD: GroupId = GroupId(1);

fn swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(300),
        indirect_count: 2,
    }
}

fn raft() -> RaftConfig {
    let mut config = RaftConfig::new(vec![A, B, C]);
    config.election_timeout = Duration::from_millis(500);
    config.heartbeat_interval = Duration::from_millis(100);
    config
}

fn leader_net(sim: &Simulation) -> SimNetwork {
    SimNetwork::new(sim).with_leader(swim(), raft(), DowningPolicy::Conservative)
}

/// Drive an async call to completion under the perpetually-running cluster loops
/// (copied from the actor-cluster conformance harness).
fn drive<T: Send + 'static>(
    sim: &Simulation,
    settle: Duration,
    future: impl std::future::Future<Output = T> + Send + 'static,
) -> T {
    let cell: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let out = Arc::clone(&cell);
    sim.spawner().launch(Box::pin(async move {
        *out.lock().unwrap() = Some(future.await);
    }));
    sim.run_for(settle);
    cell.lock().unwrap().take().expect("future did not complete")
}

/// Bring up a 3-node leader cluster, create the shard group on every node, and
/// build a [`RaftJournal`] per node (subscribing before the group is driven).
fn cluster(sim: &Simulation) -> (SimNetwork, Vec<SimCluster>, Vec<RaftJournal<SimCluster>>) {
    let net = leader_net(sim);
    let systems = vec![net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2)); // elect the control-plane leader
    let journals: Vec<RaftJournal<SimCluster>> = systems
        .iter()
        .map(|system| {
            system.create_group(SHARD, vec![A, B, C], vec![]);
            RaftJournal::new(system.clone(), SHARD)
        })
        .collect();
    sim.run_for(Duration::from_secs(2)); // elect the shard group's leader
    (net, systems, journals)
}

/// The index of the node that leads the shard group, if any.
fn shard_leader(systems: &[SimCluster]) -> Option<usize> {
    systems.iter().position(|s| s.group_is_leader(SHARD))
}

#[test]
fn an_append_is_quorum_durable_and_visible_on_every_replica() {
    let sim = Simulation::new(1);
    let (_net, systems, journals) = cluster(&sim);
    let leader = shard_leader(&systems).expect("the shard group elected a leader");
    let grain = GrainName::new("test.Acct", "1");

    let outcome = {
        let journal = journals[leader].clone();
        let grain = grain.clone();
        drive(&sim, Duration::from_secs(3), async move {
            journal.append(&grain, Seq::ZERO, vec![b"e1".to_vec()]).await
        })
    };
    assert_eq!(outcome, AppendOutcome::Committed(Seq::new(1)));

    // The committed event is replicated, so every node's projection has it.
    for journal in &journals {
        let head = {
            let journal = journal.clone();
            let grain = grain.clone();
            drive(&sim, Duration::from_secs(1), async move { journal.head(&grain).await })
        }
        .expect("head read");
        assert_eq!(head, Seq::new(1), "the write reached this replica");

        let loaded = {
            let journal = journal.clone();
            let grain = grain.clone();
            drive(&sim, Duration::from_secs(1), async move {
                journal.load(&grain, Seq::ZERO, 10).await
            })
        }
        .expect("load read");
        assert_eq!(loaded, vec![(Seq::new(1), b"e1".to_vec())]);
    }
}

#[test]
fn a_follower_append_is_fenced_with_not_leader() {
    let sim = Simulation::new(2);
    let (_net, systems, journals) = cluster(&sim);
    let leader = shard_leader(&systems).expect("the shard group elected a leader");
    let follower = (leader + 1) % systems.len();
    let leader_node = systems[leader].node();
    let grain = GrainName::new("test.Acct", "1");

    let outcome = {
        let journal = journals[follower].clone();
        drive(&sim, Duration::from_secs(1), async move {
            journal.append(&grain, Seq::ZERO, vec![b"e1".to_vec()]).await
        })
    };
    assert_eq!(
        outcome,
        AppendOutcome::NotLeader(leader_node),
        "the single-writer fence redirects a follower to the leader (§8)",
    );
}

#[test]
fn committed_state_survives_leader_failover() {
    let sim = Simulation::new(3);
    let (net, systems, journals) = cluster(&sim);
    let leader = shard_leader(&systems).expect("the shard group elected a leader");
    let leader_node = systems[leader].node();
    let grain = GrainName::new("test.Acct", "1");

    // Commit one event under the original leader.
    let first = {
        let journal = journals[leader].clone();
        let grain = grain.clone();
        drive(&sim, Duration::from_secs(3), async move {
            journal.append(&grain, Seq::ZERO, vec![b"before".to_vec()]).await
        })
    };
    assert_eq!(first, AppendOutcome::Committed(Seq::new(1)));

    // Crash the leader; the surviving quorum re-elects.
    net.crash(leader_node);
    sim.run_for(Duration::from_secs(5));

    let survivors: Vec<usize> = (0..systems.len()).filter(|&i| i != leader).collect();
    let new_leader = survivors
        .iter()
        .copied()
        .find(|&i| systems[i].group_is_leader(SHARD))
        .expect("a survivor took over the shard group");

    // A new append commits through the new leader, and its head reflects BOTH
    // the pre-crash and post-crash events — committed state survived (G14).
    let second = {
        let journal = journals[new_leader].clone();
        let grain = grain.clone();
        drive(&sim, Duration::from_secs(3), async move {
            journal.append(&grain, Seq::new(1), vec![b"after".to_vec()]).await
        })
    };
    assert_eq!(second, AppendOutcome::Committed(Seq::new(2)));

    let loaded = {
        let journal = journals[new_leader].clone();
        let grain = grain.clone();
        drive(&sim, Duration::from_secs(1), async move {
            journal.load(&grain, Seq::ZERO, 10).await
        })
    }
    .expect("load read");
    assert_eq!(
        loaded,
        vec![
            (Seq::new(1), b"before".to_vec()),
            (Seq::new(2), b"after".to_vec()),
        ],
        "no acknowledged write was lost across failover",
    );
}

#[test]
fn quorum_loss_pauses_writes_with_unavailable() {
    let sim = Simulation::new(4);
    let (net, systems, journals) = cluster(&sim);
    let leader = shard_leader(&systems).expect("the shard group elected a leader");

    // Crash the two followers, leaving the leader without a quorum.
    for &i in &[(leader + 1) % systems.len(), (leader + 2) % systems.len()] {
        net.crash(systems[i].node());
    }
    sim.run_for(Duration::from_secs(1));

    // The leader still believes it leads, so it accepts the append and proposes,
    // but the entry can never commit — the wait times out as `Unavailable` (§11).
    let grain = GrainName::new("test.Acct", "1");
    let outcome = {
        let journal = journals[leader].clone();
        drive(&sim, Duration::from_secs(12), async move {
            journal.append(&grain, Seq::ZERO, vec![b"e1".to_vec()]).await
        })
    };
    assert!(
        matches!(outcome, AppendOutcome::Unavailable(_)),
        "quorum loss must pause writes as Unavailable, got {outcome:?}",
    );
}

#[test]
fn a_full_cluster_cold_restart_rehydrates_before_serving() {
    // The whole cluster goes down and comes back — no survivor keeps a leader or an
    // advanced commit index, so every group must re-elect from scratch before its
    // restored log is re-committed and replayed. That opens the rehydration race the
    // single-node restart above never can: a grain can activate and read its head in
    // the window after a node reloads but before the new leader's term-opening Noop
    // commits the restored prefix. The barrier ([`Journal::catch_up`]) must hold the
    // read until the projection reflects that prefix; before the fix it returned as
    // soon as the not-yet-driven commit stream looked empty, and the head raced ahead
    // to an empty projection (surfacing downstream as the host's `stale head`).
    let sim = Simulation::new(7);
    let (net, systems, journals) = cluster(&sim);
    let leader = shard_leader(&systems).expect("the shard group elected a leader");
    let grain = GrainName::new("test.Acct", "1");

    // Commit fewer than COMPACT_EVERY (64) events, so the shard log is NOT compacted:
    // rehydration must replay the restored log entry-by-entry — the path a cold
    // restart of an uncompacted shard stresses (and the one the demo hit).
    for i in 1..=5u64 {
        let outcome = {
            let journal = journals[leader].clone();
            let grain = grain.clone();
            drive(&sim, Duration::from_secs(3), async move {
                journal.append(&grain, Seq::ZERO, vec![b"e".to_vec()]).await
            })
        };
        assert_eq!(outcome, AppendOutcome::Committed(Seq::new(i)), "pre-restart append committed");
    }

    // Cold-restart EVERY node: each reloads only its own persisted log (no peer to
    // replicate from), and fresh journals subscribe before the restarted groups are
    // driven (subscribe-before-drive).
    let mut systems = systems;
    for (idx, node) in [A, B, C].into_iter().enumerate() {
        let system = net.restart(node);
        system.create_group(SHARD, vec![A, B, C], vec![]);
        systems[idx] = system;
    }
    let journals: Vec<RaftJournal<SimCluster>> =
        systems.iter().map(|s| RaftJournal::new(s.clone(), SHARD)).collect();

    // Rehydrate THEN read the head in one shot, with no prior settle: `catch_up`
    // itself must do the waiting. The drive advances virtual time so the cluster
    // re-elects and replays its log *while* `catch_up` is blocked on the barrier.
    let head = {
        let journal = journals[0].clone();
        let grain = grain.clone();
        drive(&sim, Duration::from_secs(8), async move {
            journal.catch_up().await;
            journal.head(&grain).await
        })
    }
    .expect("head read");
    assert_eq!(
        head,
        Seq::new(5),
        "catch_up holds the read until the cold-restarted projection reflects every committed event",
    );

    // The re-elected leader commits the next event from the rebuilt head — landing at
    // base+1, exactly the contiguous advance whose absence is the host's stale-head
    // step-down.
    let leader = shard_leader(&systems).expect("a node re-led the shard group after the cold restart");
    let outcome = {
        let journal = journals[leader].clone();
        let grain = grain.clone();
        drive(&sim, Duration::from_secs(5), async move {
            journal.append(&grain, Seq::new(5), vec![b"after".to_vec()]).await
        })
    };
    assert_eq!(
        outcome,
        AppendOutcome::Committed(Seq::new(6)),
        "a cold-restarted, re-elected leader commits the next event without a head gap",
    );
}

#[test]
fn a_full_cluster_cold_restart_rehydrates_a_compacted_shard() {
    // Like the cold restart above, but the shard log is COMPACTED first (more than
    // COMPACT_EVERY events), so each node reloads a state-machine *snapshot* plus a
    // short tail rather than a full log. With no survivor, a node cannot catch up
    // from a leader's InstallSnapshot — it must rebuild its projection from the
    // snapshot it reloaded itself. The engine re-delivers that reloaded snapshot to
    // a fresh subscriber as the first observation on its stream; without it, the
    // projection would see only the post-snapshot tail and silently drop the whole
    // compacted prefix.
    let sim = Simulation::new(8);
    let (net, systems, journals) = cluster(&sim);
    let leader = shard_leader(&systems).expect("the shard group elected a leader");
    let grain = GrainName::new("test.Acct", "1");

    // Commit past COMPACT_EVERY (64) so every replica compacts its shard log to a
    // snapshot at 64 and retains only the 65..=70 tail.
    for i in 1..=70u64 {
        let outcome = {
            let journal = journals[leader].clone();
            let grain = grain.clone();
            drive(&sim, Duration::from_secs(3), async move {
                journal.append(&grain, Seq::ZERO, vec![b"e".to_vec()]).await
            })
        };
        assert_eq!(outcome, AppendOutcome::Committed(Seq::new(i)), "pre-restart append committed");
    }
    sim.run_for(Duration::from_secs(2)); // let every replica apply and compact

    // Cold-restart EVERY node: each reloads its own snapshot + tail (no peer to
    // install a snapshot from), and fresh journals subscribe before the groups run.
    let mut systems = systems;
    for (idx, node) in [A, B, C].into_iter().enumerate() {
        let system = net.restart(node);
        system.create_group(SHARD, vec![A, B, C], vec![]);
        systems[idx] = system;
    }
    let journals: Vec<RaftJournal<SimCluster>> =
        systems.iter().map(|s| RaftJournal::new(s.clone(), SHARD)).collect();

    // Rehydrate THEN read the head with no prior settle: the head must reflect the
    // full history — the 64 events folded from the reloaded snapshot plus the 6 tail
    // events — not just the tail.
    let head = {
        let journal = journals[0].clone();
        let grain = grain.clone();
        drive(&sim, Duration::from_secs(8), async move {
            journal.catch_up().await;
            journal.head(&grain).await
        })
    }
    .expect("head read");
    assert_eq!(
        head,
        Seq::new(70),
        "the cold-restarted projection rebuilds from the reloaded snapshot, not just the post-snapshot tail",
    );

    // Every event is durably present, including ones from inside the compacted
    // prefix — proof the snapshot's contents survived, not merely its head count.
    let loaded = {
        let journal = journals[0].clone();
        let grain = grain.clone();
        drive(&sim, Duration::from_secs(1), async move {
            journal.load(&grain, Seq::ZERO, 100).await
        })
    }
    .expect("load read");
    assert_eq!(loaded.len(), 70, "all 70 committed events survived the cold restart");

    // And the re-elected leader keeps committing from the rebuilt head.
    let leader = shard_leader(&systems).expect("a node re-led the shard group after the cold restart");
    let outcome = {
        let journal = journals[leader].clone();
        let grain = grain.clone();
        drive(&sim, Duration::from_secs(5), async move {
            journal.append(&grain, Seq::new(70), vec![b"after".to_vec()]).await
        })
    };
    assert_eq!(
        outcome,
        AppendOutcome::Committed(Seq::new(71)),
        "a cold-restarted, re-elected leader commits the next event atop the rehydrated snapshot",
    );
}

#[test]
fn a_restarted_leader_keeps_committing_after_reusing_its_node_id() {
    // A node that crashes and re-starts reuses its stable `NodeId` but builds a
    // fresh journal (§7.2). After it catches up and is re-elected, it must keep
    // committing — its writes are durable and visible, none swallowed. (The
    // per-incarnation `ProposalId` epoch that keeps the dedup id spaces disjoint is
    // unit-tested directly in `shard.rs`; here we exercise the whole restart +
    // re-election + continued-write path end to end.)
    let sim = Simulation::new(0);
    let (net, systems, journals) = cluster(&sim);
    let leader = shard_leader(&systems).expect("the shard group elected a leader");
    let leader_node = systems[leader].node();
    let grain = GrainName::new("test.Acct", "1");

    // Commit past COMPACT_EVERY (64) so the shard log compacts: the applied set is
    // then carried in a state-machine snapshot that the restarted node installs,
    // rather than replayed entry-by-entry. All ids here are the old incarnation's.
    for _ in 0..70 {
        let outcome = {
            let journal = journals[leader].clone();
            let grain = grain.clone();
            drive(&sim, Duration::from_secs(3), async move {
                journal.append(&grain, Seq::ZERO, vec![b"e".to_vec()]).await
            })
        };
        assert!(matches!(outcome, AppendOutcome::Committed(_)), "pre-restart append committed");
    }

    // Restart the leader with the same NodeId and a fresh journal (a new epoch),
    // then let it catch up via snapshot install and re-win the election.
    let restarted_system = net.restart(leader_node);
    restarted_system.create_group(SHARD, vec![A, B, C], vec![]);
    let restarted = RaftJournal::new(restarted_system.clone(), SHARD);
    sim.run_for(Duration::from_secs(6));
    assert!(
        restarted_system.group_is_leader(SHARD),
        "the restarted node re-leads its shard, so its journal is the proposer",
    );

    // The head the restarted leader rebuilt from the old incarnation's committed
    // log.
    let base = {
        let journal = restarted.clone();
        let grain = grain.clone();
        drive(&sim, Duration::from_secs(1), async move { journal.head(&grain).await })
    }
    .expect("head read");
    assert!(base.value() > 0, "the restarted leader rebuilt the old committed events");

    // The restarted leader's first post-restart append (a fresh journal, so nonce 0
    // again) commits and advances the head — not swallowed, not stalled.
    let outcome = {
        let journal = restarted.clone();
        let grain = grain.clone();
        drive(&sim, Duration::from_secs(5), async move {
            journal.append(&grain, base, vec![b"after-restart".to_vec()]).await
        })
    };
    assert_eq!(
        outcome,
        AppendOutcome::Committed(Seq::new(base.value() + 1)),
        "a restarted, re-elected leader keeps committing",
    );

    // And it is durably visible at the new head.
    let loaded = {
        let journal = restarted.clone();
        let grain = grain.clone();
        drive(&sim, Duration::from_secs(1), async move {
            journal.load(&grain, base, 10).await
        })
    }
    .expect("load read");
    assert_eq!(loaded, vec![(Seq::new(base.value() + 1), b"after-restart".to_vec())]);
}
