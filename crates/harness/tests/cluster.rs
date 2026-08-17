//! Cluster conformance (harness spec §7; invariants H3, H6, H7): the Agent
//! grain hosted on a real 3-node `ClusterSystem`, sessions placed by granary's
//! shard map and driven from several nodes. A session activates on its shard's
//! leader (§7.2); a re-submitted `TurnId` never starts a second run (H7); the
//! grain's single-writer fence (G1) keeps each transcript one total order — the
//! harness builds no fence of its own (§6.2). Failover after a leader crash is
//! the grain's rehydration (§7.5), exercised here by crashing a node and
//! re-submitting until the run completes on the new leader.
//!
//! This is the converged-cluster check. Pushing granary's `Quorum`-tier consensus
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
use actor_simulation::SimNetwork;
use actor_simulation::SimNode;
use actor_simulation::Simulation;
use granary::GrainEvent;
use granary::GranaryConfig;
use harness::Budget;
use harness::Harness;
use harness::Kind;
use harness::Kinds;
use harness::RecordBody;
use harness::RunError;
use harness::SessionId;
use harness::Tier;
use harness::Turn;
use harness::TurnId;
use serde_json::json;

use support::CollectingSink;
use support::ScriptedModel;
use support::ScriptedSandboxes;
use support::SlowModel;
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
            .sandboxed(
                "shell",
                "run",
                &json!({ "type": "object" }),
                Tier::Workspace,
            )
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
    ScriptedModel::steps(vec![
        Ok(tool_call("c1", "shell", json!({}))),
        Ok(final_message("done")),
    ])
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
    cell.lock()
        .unwrap()
        .take()
        .expect("future did not complete in the settle window")
}

/// Bring up a 3-node leader cluster, host the worker kind on every node, and
/// return the systems and a harness per node.
fn cluster(
    sim: &Simulation,
    sink: Arc<dyn actor_core::EventSink>,
) -> (SimNetwork, Vec<Harness<SimNode>>) {
    let net = SimNetwork::new(sim)
        .with_leader(SwimConfig::default(), raft(), DowningPolicy::Conservative)
        .with_events(sink);
    let systems = [net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2)); // elect the control-plane leader
    let harnesses: Vec<Harness<SimNode>> = systems
        .iter()
        .map(|s| {
            Harness::cluster(
                s.clone(),
                &kinds(),
                Arc::new(model()),
                Arc::new(ScriptedSandboxes::echo()),
            )
        })
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
                if let Ok(page) = session.tail(granary::Seq::new(0), harness::TAIL_PAGE).await {
                    return page.into_iter().map(|(_, r)| r.body).collect();
                }
            }
        });
        let submitted = records
            .iter()
            .filter(|b| matches!(b, RecordBody::TurnSubmitted { .. }))
            .count();
        let ended = records
            .iter()
            .filter(|b| matches!(b, RecordBody::RunEnded { .. }))
            .count();
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
                if let Ok(page) = session.tail(granary::Seq::new(0), harness::TAIL_PAGE).await {
                    return page.into_iter().map(|(_, r)| r.body).collect();
                }
            }
        }
    });
    let submitted = records
        .iter()
        .filter(|b| matches!(b, RecordBody::TurnSubmitted { .. }))
        .count();
    let ended = records
        .iter()
        .filter(|b| matches!(b, RecordBody::RunEnded { .. }))
        .count();
    assert_eq!((submitted, ended), (1, 1), "one run survives the crash");

    assert_invariants(&sink.events());
}

// -- crash-window cancel propagation (§9.2, H5) ------------------------------

fn delegation_kinds() -> Kinds {
    let grain = || GranaryConfig {
        shards: 2,
        replication_factor: 3,
        idle_after: Duration::from_secs(600),
        ..GranaryConfig::default()
    };
    Kinds::new()
        .register(
            "parent",
            Kind::new("parent agent")
                .delegates_to(&["child"])
                .budget(Budget::new(100_000, 10))
                .grain(grain()),
        )
        .register(
            "child",
            Kind::new("child agent")
                .budget(Budget::new(50_000, 10))
                .grain(grain()),
        )
}

/// The parent delegates immediately; every model call takes an hour of logical
/// time, so inside the test window the child's run can only end by cancel.
fn delegation_model(sim: &Simulation) -> Arc<dyn harness::Model> {
    let script = ScriptedModel::new(|req| {
        if req.system_prompt == "child agent" {
            Ok(final_message("child-answer"))
        } else {
            let step = req
                .transcript
                .iter()
                .filter(|e| matches!(e, harness::Entry::Assistant { .. }))
                .count();
            if step == 0 {
                Ok(tool_call(
                    "d1",
                    "delegate",
                    json!({ "kind": "child", "prompt": "sub-task" }),
                ))
            } else {
                Ok(final_message("parent-answer"))
            }
        }
    });
    Arc::new(SlowModel {
        inner: Arc::new(script),
        clock: sim.clock(),
        delay: Duration::from_secs(3_600),
    })
}

/// The node of `key`'s most recent grain activation (grain §13).
fn last_activation(sink: &CollectingSink, key: &str) -> Option<NodeId> {
    sink.events()
        .iter()
        .filter_map(|e| e.as_app::<GrainEvent>())
        .filter_map(|e| match e {
            GrainEvent::Activated { node, name } if name.key() == key => Some(*node),
            _ => None,
        })
        .last()
}

/// Poll a session's journal until `ready` holds over its record bodies;
/// panics ("did not complete in the settle window") if it never does.
fn await_journal(
    sim: &Simulation,
    harness: &Harness<SimNode>,
    kind: &str,
    session: &SessionId,
    settle: Duration,
    ready: impl Fn(&[RecordBody]) -> bool + Send + Sync + 'static,
) -> Vec<RecordBody> {
    let session = harness.session(kind, session.clone());
    drive(sim, settle, async move {
        loop {
            if let Ok(page) = session.tail(granary::Seq::new(0), harness::TAIL_PAGE).await {
                let bodies: Vec<RecordBody> = page.into_iter().map(|(_, r)| r.body).collect();
                if ready(&bodies) {
                    return bodies;
                }
            }
        }
    })
}

/// The R1 crash window (§9.2, H5): `RunEnded { Cancelled }` commits, and the
/// leader crashes before the propagating send reaches the recorded child. The
/// owed propagation is a fold fact (`cancels_owed`), so the next contact —
/// a re-sent `Cancel`, the §7.5 resume trigger — re-derives it on the new
/// leader and drives it to the child, which ends `Cancelled`; the delivery
/// then journals `CancelDelivered`, retiring the debt.
#[test]
fn an_owed_cancel_survives_a_leader_crash_and_propagates_on_resume() {
    let sim = Simulation::new(23);
    let sink = CollectingSink::default();
    let net = SimNetwork::new(&sim)
        .with_leader(SwimConfig::default(), raft(), DowningPolicy::Conservative)
        .with_events(Arc::new(sink.clone()));
    let systems = [net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2)); // elect the control-plane leader
    let model = delegation_model(&sim);
    let harnesses: Vec<Harness<SimNode>> = systems
        .iter()
        .map(|s| {
            Harness::cluster(
                s.clone(),
                &delegation_kinds(),
                Arc::clone(&model),
                Arc::new(ScriptedSandboxes::echo()),
            )
        })
        .collect();
    sim.run_for(Duration::from_secs(3)); // elect each shard group's leader

    // Start the parent, fire-and-forget: its hour-long first model call
    // journals the delegation at 3600s, and the child's own hour-long call is
    // then in flight.
    {
        let session = harnesses[0].session("parent", SessionId::new("root-p"));
        sim.spawner().launch(Box::pin(async move {
            let _ = session
                .prompt_within(
                    Turn::new(TurnId::new("t-1"), "go"),
                    Duration::from_secs(50_000),
                )
                .await;
        }));
    }
    sim.run_for(Duration::from_secs(3_700));

    // The journaled delegation names the child (§8.1).
    let parent_bodies = await_journal(
        &sim,
        &harnesses[0],
        "parent",
        &SessionId::new("root-p"),
        Duration::from_secs(10),
        |bodies| {
            bodies
                .iter()
                .any(|b| matches!(b, RecordBody::ChildRun { .. }))
        },
    );
    let (child_kind, child_session) = parent_bodies
        .iter()
        .find_map(|b| match b {
            RecordBody::ChildRun {
                child_kind,
                child_session,
                ..
            } => Some((child_kind.clone(), child_session.clone())),
            _ => None,
        })
        .expect("journaled delegation");

    // Hold the propagation window open: isolate the child's leader so the
    // propagating send cannot land, cancel the parent (its `RunEnded {
    // Cancelled }` commits on the majority side — the ack is the output
    // gate's release), and crash the parent's leader before the child shard
    // could re-elect a reachable leader (the ack arrives well inside the
    // 500ms election timeout). The crash therefore destroys the send after
    // the terminal record committed — exactly the R1 window.
    let leader = last_activation(&sink, "root-p").expect("parent activated");
    let child_leader = last_activation(&sink, child_session.as_str()).expect("child activated");
    assert_ne!(
        leader, child_leader,
        "this seed must place the parent's and the child's shard leaders apart"
    );
    let spare = *[A, B, C]
        .iter()
        .find(|n| **n != leader && **n != child_leader)
        .expect("a third node");
    let surviving = harnesses[[A, B, C].iter().position(|n| *n == spare).unwrap()].clone();
    net.partition(&[leader, spare], &[child_leader]);
    let acked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let acked = Arc::clone(&acked);
        let session = surviving.session("parent", SessionId::new("root-p"));
        sim.spawner().launch(Box::pin(async move {
            loop {
                if session.cancel(&TurnId::new("t-1")).await.is_ok() {
                    acked.store(true, std::sync::atomic::Ordering::SeqCst);
                    return;
                }
            }
        }));
    }
    for _ in 0..100 {
        if acked.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }
        sim.run_for(Duration::from_millis(100));
    }
    assert!(
        acked.load(std::sync::atomic::Ordering::SeqCst),
        "the cancel was acked"
    );
    net.crash(leader);
    net.heal();

    // Let the shard groups re-elect, then check the window is real: the
    // child's run must still be live — the crash destroyed the send, and with
    // it the old design's only copy of the propagation.
    sim.run_for(Duration::from_secs(3));
    let child_before = await_journal(
        &sim,
        &surviving,
        child_kind.as_str(),
        &child_session,
        Duration::from_secs(10),
        |_| true,
    );
    assert!(
        !child_before
            .iter()
            .any(|b| matches!(b, RecordBody::RunEnded { .. })),
        "crash-window: the child is still live before the parent is re-contacted"
    );

    // Re-contact the cancelled session (a re-sent Cancel, §7.5): the new
    // leader's fold still owes the propagation, and drives it to the child.
    drive(&sim, Duration::from_secs(30), {
        let session = surviving.session("parent", SessionId::new("root-p"));
        async move {
            loop {
                if session.cancel(&TurnId::new("t-1")).await.is_ok() {
                    return;
                }
            }
        }
    });

    // The child's run ends Cancelled (H5)…
    await_journal(
        &sim,
        &surviving,
        child_kind.as_str(),
        &child_session,
        Duration::from_secs(120),
        |bodies| {
            bodies.iter().any(|b| {
                matches!(
                    b,
                    RecordBody::RunEnded {
                        outcome: Err(RunError::Cancelled),
                        ..
                    }
                )
            })
        },
    );
    // …and the parent journals the `CancelDelivered` that retires the debt.
    await_journal(
        &sim,
        &surviving,
        "parent",
        &SessionId::new("root-p"),
        Duration::from_secs(60),
        |bodies| {
            bodies
                .iter()
                .any(|b| matches!(b, RecordBody::CancelDelivered { .. }))
        },
    );

    assert_invariants(&sink.events());
}

// -- crash-window queued turn (§7.3, §7.5) -----------------------------------

/// The R2 crash window: a turn acked *behind* a live run is fold state
/// (`TurnSubmitted` enqueues, §7.3), so a leader crash between the ack and the
/// run start loses nothing. The sharpest form is asserted here: after the
/// crash, only the *first* turn's caller ever re-contacts the session, yet the
/// queued second turn still runs to completion — any contact drives the
/// dispatcher, which starts the queue head once no run is live (§7.5).
#[test]
fn an_acked_queued_turn_survives_a_leader_crash_and_runs_on_resume() {
    let sim = Simulation::new(31);
    let sink = CollectingSink::default();
    let net = SimNetwork::new(&sim)
        .with_leader(SwimConfig::default(), raft(), DowningPolicy::Conservative)
        .with_events(Arc::new(sink.clone()));
    let systems = [net.join(A), net.join(B), net.join(C)];
    sim.run_for(Duration::from_secs(2)); // elect the control-plane leader
    // Every model call is a 600s final message: long enough to hold the queue
    // open across the crash, short enough for the settle windows below.
    let model: Arc<dyn harness::Model> = Arc::new(SlowModel {
        inner: Arc::new(ScriptedModel::new(|_| Ok(final_message("done")))),
        clock: sim.clock(),
        delay: Duration::from_secs(600),
    });
    let harnesses: Vec<Harness<SimNode>> = systems
        .iter()
        .map(|s| {
            Harness::cluster(
                s.clone(),
                &kinds(),
                Arc::clone(&model),
                Arc::new(ScriptedSandboxes::echo()),
            )
        })
        .collect();
    sim.run_for(Duration::from_secs(3)); // elect each shard group's leader

    // t-1 starts its 600s model call; t-2 is then accepted behind it. Both
    // submitters fire and forget — neither returns after the crash.
    for turn in ["t-1", "t-2"] {
        let session = harnesses[0].session("worker", SessionId::new("s-queued"));
        sim.spawner().launch(Box::pin(async move {
            let _ = session
                .prompt_within(
                    Turn::new(TurnId::new(turn), "go"),
                    Duration::from_secs(30_000),
                )
                .await;
        }));
        sim.run_for(Duration::from_secs(2));
    }
    // The ack committed: both turns are journaled, one started, none ended.
    let before = await_journal(
        &sim,
        &harnesses[0],
        "worker",
        &SessionId::new("s-queued"),
        Duration::from_secs(10),
        |bodies| {
            bodies
                .iter()
                .filter(|b| matches!(b, RecordBody::TurnSubmitted { .. }))
                .count()
                == 2
        },
    );
    assert_eq!(
        before
            .iter()
            .filter(|b| matches!(b, RecordBody::TurnStarted { .. }))
            .count(),
        1,
        "t-2 is accepted but not started while t-1's run is live"
    );

    // Crash the session's leader mid-run, with t-2 still queued.
    let leader = last_activation(&sink, "s-queued").expect("session activated");
    net.crash(leader);
    sim.run_for(Duration::from_secs(3)); // re-elect

    let spare = *[A, B, C].iter().find(|n| **n != leader).expect("survivor");
    let surviving = harnesses[[A, B, C].iter().position(|n| *n == spare).unwrap()].clone();

    // Only t-1's caller re-contacts (§7.5): the attach resumes the run on the
    // new leader; its re-issued 600s call completes it.
    let first = drive(&sim, Duration::from_secs(700), {
        let session = surviving.session("worker", SessionId::new("s-queued"));
        async move {
            loop {
                if let Ok(Ok(c)) = session
                    .prompt_within(
                        Turn::new(TurnId::new("t-1"), "go"),
                        Duration::from_secs(650),
                    )
                    .await
                {
                    return c.text().to_string();
                }
            }
        }
    });
    assert_eq!(first, "done");

    // Nobody ever re-contacts t-2, yet it runs: the dispatcher starts the
    // fold's queue head once t-1's terminal record commits.
    let bodies = await_journal(
        &sim,
        &surviving,
        "worker",
        &SessionId::new("s-queued"),
        Duration::from_secs(700),
        |bodies| {
            bodies
                .iter()
                .filter(|b| matches!(b, RecordBody::RunEnded { .. }))
                .count()
                == 2
        },
    );
    let submitted = bodies
        .iter()
        .filter(|b| matches!(b, RecordBody::TurnSubmitted { .. }))
        .count();
    let started: Vec<&TurnId> = bodies
        .iter()
        .filter_map(|b| match b {
            RecordBody::TurnStarted { turn } => Some(turn),
            _ => None,
        })
        .collect();
    assert_eq!(
        submitted, 2,
        "the re-contact deduped, never re-journaled (H7)"
    );
    assert_eq!(started.len(), 2, "each turn started exactly once");
    assert!(
        started.iter().any(|t| t.as_str() == "t-2"),
        "the acked queued turn started without its caller returning"
    );

    assert_invariants(&sink.events());
}

fn assert_invariants(events: &[Event]) {
    let violations = check_events(events);
    assert!(violations.is_empty(), "checkers: {violations:?}");
}

/// Regression (the standalone-harness `NotLeader` livelock): a kind carries a
/// `GranaryConfig`, so each kind becomes a `Quorum`-tier grain type that needs the
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
    let _ = Harness::cluster(
        system,
        &kinds(),
        Arc::new(model()),
        Arc::new(ScriptedSandboxes::echo()),
    );
}
