//! Cluster conformance (harness spec §7; invariants H3, H6, H7): the Agent
//! grain hosted on a real 3-node `ClusterSystem`, sessions placed by granary's
//! shard map and driven from several nodes. A session activates on its shard's
//! leader (§7.2); a re-submitted `TurnId` never starts a second run (H7); the
//! grain's single-writer fence (G1) keeps each transcript one total order — the
//! harness builds no fence of its own (§6.2). Failover after a leader crash is
//! the grain's rehydration (§7.5), exercised here by crashing a node and
//! re-submitting until the run completes on the new leader.
//!
//! This is the converged-cluster check. Pushing granary's Tier-2 consensus
//! through a *continuous* partition/crash nemesis is granary's own V&V remit
//! (its swarm harness), not the harness's.

mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::RaftConfig;
use actor_cluster::SwimConfig;
use actor_core::Event;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::Simulation;
use granary::GranaryConfig;
use harness::Budget;
use harness::Harness;
use harness::Kind;
use harness::Kinds;
use harness::RecordBody;
use harness::SessionId;
use harness::Tier;
use harness::Turn;
use harness::TurnId;
use serde_json::json;

use support::CollectingSink;
use support::ScriptedModel;
use support::ScriptedSandboxes;
use support::check_events;
use support::final_message;
use support::tool_call;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);
const SESSIONS: usize = 4;

fn raft() -> RaftConfig {
    let mut config = RaftConfig::new(vec![A, B, C]);
    config.election_timeout = Duration::from_millis(500);
    config.heartbeat_interval = Duration::from_millis(100);
    config
}

fn kinds() -> Kinds {
    Kinds::new().register(
        "worker",
        Kind::new("worker")
            .sandboxed("shell", "run", &json!({ "type": "object" }), Tier::Workspace)
            .budget(Budget::new(10_000, 10))
            // Two shards over the 3-node cluster, replicated, no hibernation
            // during the test.
            .grain(GranaryConfig {
                shards: 2,
                replication_factor: 3,
                idle_after: Duration::from_secs(60),
                ..GranaryConfig::default()
            }),
    )
}

fn model() -> ScriptedModel {
    ScriptedModel::steps(vec![Ok(tool_call("c1", "shell", json!({}))), Ok(final_message("done"))])
}

/// Drive an async call to completion under the perpetually-running cluster loops.
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
    cell.lock().unwrap().take().expect("future did not complete in the settle window")
}

/// Bring up a 3-node leader cluster, host the worker kind on every node, and
/// return the systems and a harness per node.
fn cluster(sim: &Simulation, sink: Arc<dyn actor_core::EventSink>) -> (SimNetwork, Vec<Harness<SimCluster>>) {
    let net = SimNetwork::new(sim)
        .with_leader(SwimConfig::default(), raft(), DowningPolicy::Conservative)
        .with_events(sink);
    let systems = [net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2)); // elect the control-plane leader
    let harnesses: Vec<Harness<SimCluster>> = systems
        .iter()
        .map(|s| Harness::new(s.clone(), kinds(), Arc::new(model()), Arc::new(ScriptedSandboxes::echo())))
        .collect();
    sim.run_for(Duration::from_secs(3)); // elect each shard group's leader
    (net, harnesses)
}

#[test]
fn sessions_run_once_across_a_converged_cluster() {
    let sim = Simulation::new(7);
    let sink = CollectingSink::default();
    let (_net, harnesses) = cluster(&sim, Arc::new(sink.clone()));

    // Drive each session from a different node: the shard map, not the entry
    // point, decides who hosts it (§7.2). Re-submit the same TurnId until the
    // recorded outcome comes back (caller-driven resumption, §7.5).
    for i in 0..SESSIONS {
        let harness = harnesses[i % harnesses.len()].clone();
        let session = harness.session("worker", SessionId::new(format!("s-{i}")));
        let completed = drive(&sim, Duration::from_secs(30), async move {
            loop {
                if let Ok(Ok(c)) = session.prompt(Turn::new(TurnId::new("t-1"), "go")).await {
                    return c.text().to_string();
                }
            }
        });
        assert_eq!(completed, "done");
    }

    // Each session ran exactly once — one submitted turn, one terminal outcome —
    // on the grain's fence (H7, H3, G1).
    for i in 0..SESSIONS {
        let harness = harnesses[i % harnesses.len()].clone();
        let session = harness.session("worker", SessionId::new(format!("s-{i}")));
        let records: Vec<RecordBody> = drive(&sim, Duration::from_secs(10), async move {
            loop {
                if let Ok(page) = session.tail(granary::Seq::new(0), 1_000_000).await {
                    return page.into_iter().map(|(_, r)| r.body).collect();
                }
            }
        });
        let submitted = records.iter().filter(|b| matches!(b, RecordBody::TurnSubmitted { .. })).count();
        let ended = records.iter().filter(|b| matches!(b, RecordBody::RunEnded { .. })).count();
        assert_eq!((submitted, ended), (1, 1), "session s-{i} ran exactly once");
    }

    assert_invariants(&sink.events());
}

#[test]
fn a_run_resumes_on_a_new_leader_after_a_crash() {
    let sim = Simulation::new(11);
    let sink = CollectingSink::default();
    let (net, harnesses) = cluster(&sim, Arc::new(sink.clone()));

    // Run a session to completion, then crash the node that led its shard. A
    // re-submission of the same TurnId reactivates the session on the new leader
    // (rehydrate + fold, §7.5) and returns the recorded outcome (H7) — the run
    // is not re-executed.
    let first = drive(&sim, Duration::from_secs(30), {
        let session = harnesses[0].session("worker", SessionId::new("s-crash"));
        async move {
            loop {
                if let Ok(Ok(c)) = session.prompt(Turn::new(TurnId::new("t-1"), "go")).await {
                    return c.text().to_string();
                }
            }
        }
    });
    assert_eq!(first, "done");

    // Crash one node and let the shard groups re-elect.
    net.crash(B);
    sim.run_for(Duration::from_secs(3));

    // A surviving node re-contacts the session: it returns the recorded outcome,
    // never a second run.
    let again = drive(&sim, Duration::from_secs(30), {
        let session = harnesses[2].session("worker", SessionId::new("s-crash"));
        async move {
            loop {
                if let Ok(Ok(c)) = session.prompt(Turn::new(TurnId::new("t-1"), "go")).await {
                    return c.text().to_string();
                }
            }
        }
    });
    assert_eq!(again, "done");

    // Still exactly one run on the journal (H7, H3) despite the failover.
    let records: Vec<RecordBody> = drive(&sim, Duration::from_secs(10), {
        let session = harnesses[2].session("worker", SessionId::new("s-crash"));
        async move {
            loop {
                if let Ok(page) = session.tail(granary::Seq::new(0), 1_000_000).await {
                    return page.into_iter().map(|(_, r)| r.body).collect();
                }
            }
        }
    });
    let submitted = records.iter().filter(|b| matches!(b, RecordBody::TurnSubmitted { .. })).count();
    let ended = records.iter().filter(|b| matches!(b, RecordBody::RunEnded { .. })).count();
    assert_eq!((submitted, ended), (1, 1), "one run survives the crash");

    assert_invariants(&sink.events());
}

fn assert_invariants(events: &[Event]) {
    let violations = check_events(events);
    assert!(violations.is_empty(), "checkers: {violations:?}");
}

/// Regression (the standalone-harness `NotLeader` livelock): a kind carries a
/// `GranaryConfig`, so each kind becomes a Tier-2 grain type that needs the
/// system's Raft engine to elect a shard leader. Building the harness on a
/// cluster left in the default `Static` membership mode — no `.with_leader(...)`,
/// hence no engine — must panic at construction (granary's guard), not hand back
/// a harness whose every turn would loop on `NotLeader`. This is the deployment
/// layer inheriting the guard `tests/requires_consensus.rs` checks in granary.
#[test]
#[should_panic(expected = "leader-based consensus")]
fn building_a_harness_without_consensus_panics() {
    let sim = Simulation::new(1);
    // No `.with_leader(...)`: the cluster has no Raft engine.
    let system = SimNetwork::new(&sim).join(A);
    let _ = Harness::new(system, kinds(), Arc::new(model()), Arc::new(ScriptedSandboxes::echo()));
}
