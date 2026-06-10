//! Conformance: cluster singleton (utilities spec §4, invariant U2).
//!
//! The per-node half of U2 (activations on one node never overlap) is checked
//! continuously by the `singleton-at-most-one-per-node` checker on every swarm
//! run; these scenario tests pin the rest of §4 — exactly-one on a converged
//! cluster, handoff on drain, re-activation after an anchor crash or an
//! instance death, dual activation across a partition being *resolved* on heal,
//! and the proxy's fail-fast behavior during gaps.

mod support;

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_core::ActorId;
use actor_core::CallError;
use actor_core::Event;
use actor_core::NodeId;
use actor_core::Spawner;
use actor_simulation::Recorder;
use actor_simulation::SimCluster;
use actor_simulation::SimNetwork;
use actor_simulation::SimRegistry;
use actor_simulation::Simulation;
use support::Greet;
use support::Greeter;
use support::Stop;

const NAME: &str = "greeter-singleton";

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);

fn swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(300),
        indirect_count: 2,
    }
}

/// Host the singleton on `node`: same name, factory, and stop message on every
/// hosting node, per utilities spec §4 item 1.
fn host(node: &SimCluster) -> actor_cluster::SingletonProxy<Greeter<SimCluster>> {
    node.singleton(NAME, || Greeter::new("Hello"), Stop)
}

/// Run `future` to completion by advancing virtual time (cluster background
/// loops never quiesce, so `block_on` does not apply).
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

/// The `(name, actor)` pairs of `SingletonStarted` events, in emission order.
fn started(recorder: &Recorder) -> Vec<ActorId> {
    recorder
        .events()
        .iter()
        .filter_map(|e| match e {
            Event::SingletonStarted { name, actor } if *name == NAME => Some(actor.clone()),
            _ => None,
        })
        .collect()
}

/// The activations started but not yet observed stopped — the live instances.
fn open_activations(recorder: &Recorder) -> Vec<ActorId> {
    let mut open = Vec::new();
    for event in recorder.events().iter() {
        match event {
            Event::SingletonStarted { name, actor } if *name == NAME => {
                open.push(actor.clone());
            }
            Event::SingletonStopped { name, actor } if *name == NAME => {
                open.retain(|a| a != actor);
            }
            _ => {}
        }
    }
    open
}

#[test]
fn a_converged_cluster_activates_exactly_one_instance() {
    // §4 items 1 and 3: one anchor, one activation; every proxy resolves it and
    // can call it, wherever it lives.
    let sim = Simulation::new(51);
    let recorder = Recorder::new();
    let net = SimNetwork::new(&sim)
        .with_gossip(swim(), DowningPolicy::Conservative)
        .with_events(Arc::new(recorder.clone()));
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    let proxies = [host(&a), host(&b), host(&c)];
    sim.run_for(Duration::from_secs(2));

    let activations = started(&recorder);
    assert_eq!(activations.len(), 1, "exactly one activation cluster-wide");
    let instance = &activations[0];
    assert_eq!(
        Some(instance.node()),
        a.place(NAME.as_bytes()),
        "the activation lives on the placement anchor"
    );
    for proxy in &proxies {
        assert_eq!(
            proxy.resolve().map(|r| r.id().clone()),
            Some(instance.clone())
        );
    }
    let replies = drive(&sim, Duration::from_secs(2), async move {
        let mut replies = Vec::new();
        for proxy in proxies {
            replies.push(
                proxy
                    .ask(Greet {
                        name: "world".into(),
                    })
                    .await,
            );
        }
        replies
    });
    for reply in replies {
        assert_eq!(reply, Ok("Hello, world!".to_string()));
    }
}

#[test]
fn an_anchor_crash_re_activates_on_a_new_anchor() {
    // §4 item 1 (anchor follows the serving set) and the re-activation half of
    // U2: when the anchor is confirmed unreachable, a survivor's view stops
    // naming it and the new anchor activates.
    let sim = Simulation::new(53);
    let recorder = Recorder::new();
    let net = SimNetwork::new(&sim)
        .with_gossip(swim(), DowningPolicy::Timeout(Duration::from_millis(300)))
        .with_events(Arc::new(recorder.clone()));
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    let _proxies = [host(&a), host(&b), host(&c)];
    sim.run_for(Duration::from_secs(2));

    let first = started(&recorder);
    assert_eq!(first.len(), 1);
    let anchor = first[0].node();

    net.crash(anchor);
    sim.run_for(Duration::from_secs(3)); // suspect → unreachable → new anchor

    let activations = started(&recorder);
    assert_eq!(activations.len(), 2, "a successor activation appeared");
    assert_ne!(
        activations[1].node(),
        anchor,
        "the successor lives on a different node"
    );
    // A survivor's proxy follows the move (the crashed node's registration is
    // routed around once it is downed).
    let survivor = if a.node() == anchor { &b } else { &a };
    let resolved = survivor
        .singleton_proxy::<Greeter<SimCluster>>(NAME)
        .resolve()
        .expect("survivor resolves the successor");
    assert_eq!(resolved.id(), &activations[1]);
}

#[test]
fn draining_the_anchor_hands_off_gracefully() {
    // §4 item 2: the cordoned anchor leaves the serving set, its manager
    // delivers the stop message, the instance terminates (a paired
    // SingletonStopped), and the new anchor activates.
    let sim = Simulation::new(59);
    let recorder = Recorder::new();
    let registry = SimRegistry::new(&sim);
    for node in [A, B, C] {
        registry.register(node);
    }
    let net = SimNetwork::new(&sim)
        .with_registry(swim(), registry.client(), Duration::from_millis(200))
        .with_events(Arc::new(recorder.clone()));
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    let _proxies = [host(&a), host(&b), host(&c)];
    sim.run_for(Duration::from_secs(2));

    let first = started(&recorder);
    assert_eq!(first.len(), 1);
    let anchor = first[0].node();

    registry.drain(anchor);
    sim.run_for(Duration::from_secs(3)); // sync → handoff → successor

    let open = open_activations(&recorder);
    assert_eq!(open.len(), 1, "the drained instance stopped, one successor");
    assert_ne!(open[0].node(), anchor, "the successor avoids the cordon");
    assert!(
        !started(&recorder).is_empty() && started(&recorder).len() == 2,
        "exactly one successor activation"
    );
}

#[test]
fn a_partition_may_dual_activate_but_heals_to_exactly_one() {
    // §4 item 3, honesty: divergence MAY run two instances; convergence MUST
    // stop the surplus. Conservative downing, so the partition alone downs
    // nobody and both sides survive to be reconciled.
    let sim = Simulation::new(61);
    let recorder = Recorder::new();
    let net = SimNetwork::new(&sim)
        .with_gossip(swim(), DowningPolicy::Conservative)
        .with_events(Arc::new(recorder.clone()));
    let a = net.join(A);
    let b = net.join(B);
    let c = net.join(C);
    let _proxies = [host(&a), host(&b), host(&c)];
    sim.run_for(Duration::from_secs(2));

    let first = started(&recorder);
    assert_eq!(first.len(), 1);
    let anchor = first[0].node();
    let others: Vec<NodeId> = [A, B, C].into_iter().filter(|n| *n != anchor).collect();

    net.partition(&[anchor], &others);
    sim.run_for(Duration::from_secs(3)); // the cut-off side elects its own anchor

    assert_eq!(
        started(&recorder).len(),
        2,
        "the majority side activated its own instance during divergence"
    );
    assert_eq!(
        open_activations(&recorder).len(),
        2,
        "legal dual activation"
    );

    net.heal();
    sim.run_for(Duration::from_secs(3)); // views converge; the surplus stops

    let open = open_activations(&recorder);
    assert_eq!(open.len(), 1, "convergence stopped the surplus instance");
    // Every stop paired with a prior start of the same activation.
    let all_started = started(&recorder);
    for event in recorder.events().iter() {
        if let Event::SingletonStopped { name, actor } = event {
            if *name == NAME {
                assert!(all_started.contains(actor), "stop without a start");
            }
        }
    }
}

#[test]
fn an_instance_death_re_activates_while_the_anchor_is_unchanged() {
    // §4 item 4, liveness: the instance stops by its own hand; the manager
    // observes the termination and re-activates on the next tick.
    let sim = Simulation::new(67);
    let recorder = Recorder::new();
    let net = SimNetwork::new(&sim).with_events(Arc::new(recorder.clone()));
    let node = net.join(A);
    let proxy = host(&node);
    sim.run_for(Duration::from_secs(3)); // static mode ticks at the default cadence

    let first = started(&recorder);
    assert_eq!(first.len(), 1);

    // The instance stops itself (the same path supervision-Stop takes).
    let outcome = drive(&sim, Duration::from_secs(1), {
        let instance = proxy.resolve().expect("active instance");
        async move { instance.tell(Stop).await }
    });
    assert_eq!(outcome, Ok(()));
    sim.run_for(Duration::from_secs(3)); // manager notices, re-activates

    let activations = started(&recorder);
    assert_eq!(activations.len(), 2, "re-activated after the death");
    assert_eq!(activations[0].node(), activations[1].node());
    assert_ne!(
        activations[0], activations[1],
        "a fresh incarnation, not the dead one"
    );
    assert_eq!(open_activations(&recorder).len(), 1);
}

#[test]
fn a_proxy_with_no_instance_fails_fast() {
    // §4 item 5: a gap is an immediate DeadLetter, never a buffer.
    let (sim, net) = support::cluster(71, None);
    let node = net.join(A);
    // A client-only proxy; nobody hosts, so the listing stays empty.
    let proxy = node.singleton_proxy::<Greeter<SimCluster>>(NAME);
    assert!(!proxy.is_active());
    let (ask, tell) = drive(&sim, Duration::from_secs(1), async move {
        (
            proxy
                .ask(Greet {
                    name: "world".into(),
                })
                .await,
            proxy.tell(Stop).await,
        )
    });
    assert_eq!(ask.map(|_| ()), Err(CallError::DeadLetter));
    assert_eq!(tell, Err(CallError::DeadLetter));
}
