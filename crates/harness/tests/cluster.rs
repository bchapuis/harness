//! Cluster conformance (harness spec §7, §12.2 topology faults; invariants
//! H2, H3, H6): sessions placed by rendezvous over a real `ClusterSystem`,
//! driven from several nodes while the nemesis partitions, crashes, and
//! heals. Ownership moves under partition; callers re-submit their `TurnId`s
//! until the runs complete (caller-driven resumption, §7.5); the shared
//! journal's fence keeps every transcript a single order through the
//! divergence (§6.2).
//!
//! A "crashed" node here loses its transport, not its tasks: it models the
//! partitioned-alive node — exactly the stale owner the fence exists for.

mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::SwimConfig;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterWorkload;
use actor_simulation::Invariant;
use actor_simulation::SimCluster;
use actor_simulation::run_cluster_swarm_coverage;
use harness::Budget;
use harness::Harness;
use harness::HarnessConfig;
use harness::InMemoryJournal;
use harness::Kind;
use harness::Kinds;
use harness::RecordBody;
use harness::SessionId;
use harness::Tier;
use harness::Turn;
use harness::TurnId;
use serde_json::json;

use support::ScriptedModel;
use support::ScriptedSandboxes;
use support::final_message;
use support::harness_invariants;
use support::tool_call;

const SESSIONS: usize = 4;

struct ClusterHarness {
    harnesses: Mutex<Vec<Harness<SimCluster>>>,
    journal: Mutex<Option<InMemoryJournal>>,
    clock: Mutex<Option<actor_simulation::SimClock>>,
}

impl ClusterHarness {
    fn kinds() -> Kinds {
        Kinds::new().register(
            "worker",
            Kind::new("worker")
                .sandboxed("shell", "run", &json!({"type": "object"}), Tier::Workspace)
                .budget(Budget::new(10_000, 10)),
        )
    }

    fn model() -> ScriptedModel {
        ScriptedModel::steps(vec![
            Ok(tool_call("c1", "shell", json!({}))),
            Ok(final_message("done")),
        ])
    }
}

impl ClusterWorkload for ClusterHarness {
    fn name(&self) -> &'static str {
        "harness-cluster"
    }

    fn node_count(&self) -> usize {
        3
    }

    fn swim(&self) -> SwimConfig {
        SwimConfig::default()
    }

    fn setup(&self, ctx: &ClusterCtx) {
        // One logical journal shared by every node (§6.1); one harness per
        // node, same kinds and seams everywhere (§7.1).
        let journal = InMemoryJournal::new();
        *self.journal.lock().expect("journal") = Some(journal.clone());
        *self.clock.lock().expect("clock") = Some(ctx.nodes()[0].clock().clone());
        let config = HarnessConfig {
            idle_timeout: Duration::from_secs(5),
            tick_interval: Duration::from_secs(1),
            submit_deadline: Duration::from_secs(10),
            ..HarnessConfig::default()
        };
        let mut harnesses = self.harnesses.lock().expect("harnesses");
        harnesses.clear();
        for node in ctx.nodes() {
            harnesses.push(Harness::with_config(
                node.clone(),
                Self::kinds(),
                Arc::new(journal.clone()),
                Arc::new(Self::model()),
                Arc::new(ScriptedSandboxes::echo()),
                config.clone(),
            ));
        }
    }

    fn drive(&self, _ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        let harnesses = self.harnesses.lock().expect("harnesses").clone();
        let journal = self
            .journal
            .lock()
            .expect("journal")
            .clone()
            .expect("setup");
        let clock = self.clock.lock().expect("clock").clone().expect("setup");
        Box::pin(async move {
            // Let the membership converge before traffic.
            clock.sleep(Duration::from_secs(3)).await;
            let mut waiting = Vec::new();
            for i in 0..SESSIONS {
                // Drive each session from a different node: placement, not
                // the entry point, decides who hosts it (§7.2).
                let harness = harnesses[i % harnesses.len()].clone();
                let clock = clock.clone();
                waiting.push(async move {
                    let session = harness.session("worker", SessionId::new(format!("s-{i}")));
                    let turn = Turn::new(TurnId::new("t-1"), "go");
                    // Caller-driven resumption (§7.5): re-submit the same
                    // TurnId through partitions, crashes, and ownership
                    // moves until the run's recorded outcome comes back.
                    loop {
                        match session.prompt(turn.clone()).await {
                            Ok(outcome) => {
                                let completion = outcome.expect("the scripted run succeeds");
                                assert_eq!(completion.text(), "done");
                                return;
                            }
                            Err(_) => clock.sleep(Duration::from_millis(700)).await,
                        }
                    }
                });
            }
            futures::future::join_all(waiting).await;
            // Audit at quiescence: one total order per session, exactly one
            // submitted turn and one terminal outcome — under any
            // divergence the nemesis produced (H2, H7).
            for i in 0..SESSIONS {
                let records = journal.records(&SessionId::new(format!("s-{i}")));
                let submitted = records
                    .iter()
                    .filter(|r| matches!(r.body, RecordBody::TurnSubmitted { .. }))
                    .count();
                let ended = records
                    .iter()
                    .filter(|r| matches!(r.body, RecordBody::RunEnded { .. }))
                    .count();
                assert_eq!((submitted, ended), (1, 1), "session s-{i} ran exactly once");
            }
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        harness_invariants()
    }
}

#[test]
fn sessions_survive_partitions_crashes_and_ownership_moves() {
    let workload = ClusterHarness {
        harnesses: Mutex::new(Vec::new()),
        journal: Mutex::new(None),
        clock: Mutex::new(None),
    };
    let faults =
        run_cluster_swarm_coverage(&workload, 1..=10).expect("every seed upholds the invariants");
    // Coverage accounting (§11): the sweep provably exercised transport
    // faults while agent traffic flowed — loss or duplication or delay, and
    // partition/crash blocking — not just the happy path.
    assert!(
        faults.dropped + faults.duplicated + faults.delayed > 0,
        "no transport fault fired across the sweep: {faults:?}"
    );
    assert!(
        faults.blocked > 0,
        "no partition/crash blocked a frame across the sweep: {faults:?}"
    );
}
