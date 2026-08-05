//! Conformance: a cluster running two wire revisions at once (compatibility spec
//! §3.1, §4; actor spec §7.1).
//!
//! The negotiation itself is pinned in `conformance_compatibility.rs`, node by
//! node and without faults. This is the claim that matters operationally: while a
//! seeded nemesis rolls the cluster forward release by release — and back again —
//! under partitions, crashes, freezes and loss, **no node is ever sent a form of a
//! message its build cannot read.**
//!
//! The revision-varying behavior lives here rather than in the tree, deliberately.
//! No boundary in the workspace has a second revision yet, so a sweep over the
//! real protocol would have nothing to vary and would prove only that shuffling
//! windows does not break a cluster. A synthetic `Form` on this workload's own
//! message is enough to make the property falsifiable: the sender picks it from
//! `Transport::peer_version`, and a receiver whose window does not accept what
//! arrived records a violation. Sending the newest form unconditionally — the bug
//! the gate exists to prevent — fails this test.
//!
//! What holds it together is that a [`Rollout`] is a *legal* upgrade path,
//! checked when it is built: adjacent releases share a revision and each accepts
//! what its neighbour writes (**V4**, **V5**). That is what makes a rollback
//! landing on an in-flight frame safe, and it is why the property can be asserted
//! without a quiesce between steps.

mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::SwimConfig;
use actor_cluster::Transport;
use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Key;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_simulation::ClusterCtx;
use actor_simulation::ClusterModeSpec;
use actor_simulation::ClusterWorkload;
use actor_simulation::Rehost;
use actor_simulation::Rollout;
use actor_simulation::SimNetwork;
use actor_simulation::SimNode;
use actor_simulation::coverage_seeds;
use actor_simulation::run_cluster_swarm;
use actor_simulation::run_cluster_swarm_coverage;
use actor_simulation::sweep_seeds;
use compat::Version;
use compat::Window;
use serde::Deserialize;
use serde::Serialize;

const GREETERS: Key<VersionedGreeter> = Key::new("mixed.greeter");

/// The three releases of a wire bump, in order (**V4**: read-new first, write-new
/// later). `Rollout::new` rejects a sequence that is not a legal path, so this
/// list is checked rather than asserted about.
fn rollout() -> Rollout {
    Rollout::new(vec![
        Window::at("actor.wire", 1),        // ships v1
        Window::new("actor.wire", 1, 2, 1), // reads v2, still writes v1
        Window::new("actor.wire", 1, 2, 2), // writes v2
    ])
}

/// The form a message travels in. Stands in for a real wire change — a payload
/// laid out differently, a field added ahead of the body, a compression tag — and
/// like all of those it is unreadable to a build that predates it.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
enum Form {
    V1,
    V2,
}

impl Form {
    /// The revision a reader needs to accept to make sense of this form.
    fn needs(self) -> Version {
        match self {
            Form::V1 => Version(1),
            Form::V2 => Version(2),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Greet {
    form: Form,
}

impl Message for Greet {
    /// Whether the receiver could read the form it was sent. The violation is
    /// recorded on the receiving side too, because a reply that never arrives
    /// (partition, crash, timeout) must not be how the property is judged.
    type Reply = bool;
    const MANIFEST: Manifest = Manifest::new("mixed.Greet");
}

/// A greeter that reads only what its node's build accepts.
struct VersionedGreeter {
    net: SimNetwork,
    node: NodeId,
    violations: Arc<AtomicU64>,
}

impl Actor for VersionedGreeter {
    type System = SimNode;
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Greet>();
    }
}

impl Handler<Greet> for VersionedGreeter {
    async fn handle(&mut self, msg: Greet, _ctx: &Ctx<Self>) -> bool {
        // The window is read now, not captured at spawn: an upgrade moves it
        // under a running node, and this actor is respawned on a restarted one.
        let accepted = self.net.wire_window(self.node).accepted();
        if !accepted.holds(msg.form.needs()) {
            self.violations.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        true
    }
}

/// Every node greets every peer, in whatever form that peer's association allows,
/// while the nemesis rolls releases forward and back underneath.
struct MixedVersionGreetings {
    nodes: usize,
    rounds: u64,
    /// Messages that reached a node whose build could not read them. The property.
    violations: Arc<AtomicU64>,
    /// Messages sent in the newer form. A vacuity guard: with this at zero the run
    /// never exercised a mixed cluster at all and the property above is free.
    newer_sent: Arc<AtomicU64>,
    /// This run's network, published by [`setup`](ClusterWorkload::setup).
    ///
    /// A `rehost` closure is built from `&self` and has only a [`SimNode`] to work
    /// with, but a respawned greeter needs to read its node's window — so the one
    /// thing that knows windows has to reach it some other way. Replaced on every
    /// seed, since each run builds its own network.
    net: Arc<Mutex<Option<SimNetwork>>>,
}

impl MixedVersionGreetings {
    fn new(nodes: usize, rounds: u64) -> MixedVersionGreetings {
        MixedVersionGreetings {
            nodes,
            rounds,
            violations: Arc::new(AtomicU64::new(0)),
            newer_sent: Arc::new(AtomicU64::new(0)),
            net: Arc::new(Mutex::new(None)),
        }
    }
}

impl ClusterWorkload for MixedVersionGreetings {
    fn name(&self) -> &'static str {
        "mixed-version-greetings"
    }

    fn node_count(&self) -> usize {
        self.nodes
    }

    fn swim(&self) -> SwimConfig {
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            rtt: Duration::from_millis(50),
            suspect_timeout: Duration::from_millis(200),
            indirect_count: 2,
        }
    }

    fn mode(&self) -> ClusterModeSpec {
        ClusterModeSpec::Gossip {
            swim: self.swim(),
            downing: DowningPolicy::Timeout(Duration::from_millis(300)),
        }
    }

    fn rollout(&self) -> Option<Rollout> {
        Some(rollout())
    }

    fn rehost(&self) -> Option<Rehost> {
        // An upgrade restarts the node, so the fresh process needs its greeter
        // back — otherwise the rollout would shrink the cluster instead of
        // upgrading it, and the property would hold because nothing is sent.
        let violations = Arc::clone(&self.violations);
        let net = Arc::clone(&self.net);
        Some(Arc::new(move |system: &SimNode| {
            let net = net.lock().expect("net mutex poisoned").clone();
            let net = net.expect("setup publishes the network before the nemesis runs");
            spawn_greeter(system, net, Arc::clone(&violations));
        }))
    }

    fn setup(&self, ctx: &ClusterCtx) {
        *self.net.lock().expect("net mutex poisoned") = Some(ctx.net().clone());
        for node in ctx.nodes() {
            spawn_greeter(node, ctx.net().clone(), Arc::clone(&self.violations));
        }
    }

    fn drive(&self, ctx: &ClusterCtx) -> BoxFuture<'static, ()> {
        // Every ask is issued from the first node, and only from there: a workload
        // that consents to restarts keeps no long-lived handle on any other node
        // (docs/simulation-testing.md), and issuing calls *from* a system the
        // nemesis has since replaced puts the no-silent-loss tally off by the
        // asks the dead process had outstanding.
        //
        // The sender still moves through the rollout. An upgrade restarts a node
        // only where it may, and never the first one, so node 1's window moves
        // under its running process — it varies what it writes exactly as its
        // peers vary what they can read.
        let caller = ctx.nodes()[0].clone();
        let net = ctx.net().clone();
        let rounds = self.rounds;
        let violations = Arc::clone(&self.violations);
        let newer_sent = Arc::clone(&self.newer_sent);
        Box::pin(async move {
            let clock = caller.clock().clone();
            for _ in 0..rounds {
                // Yield so receptionist replication, the detector, and the
                // nemesis all make progress between rounds.
                clock.sleep(Duration::from_millis(200)).await;
                for peer in caller.receptionist().lookup(GREETERS).iter() {
                    let to = peer.id().node();
                    if to == caller.node() {
                        continue;
                    }
                    // The gate. `None` — no association yet, or none any more —
                    // means write the oldest revision this build accepts, never
                    // the newest.
                    let form = match net.transport(caller.node()).peer_version(to) {
                        Some(v) if v >= Version(2) => {
                            newer_sent.fetch_add(1, Ordering::Relaxed);
                            Form::V2
                        }
                        _ => Form::V1,
                    };
                    // Every outcome is acceptable: a partitioned or crashed peer
                    // fails the call, and a failed call carries no claim. What may
                    // not happen is the message *arriving* unreadable.
                    let _ = peer
                        .ask_timeout(Greet { form }, Duration::from_millis(500))
                        .await;
                }
            }
            assert_eq!(
                violations.load(Ordering::Relaxed),
                0,
                "a node was sent a form its build cannot read — the send-side gate \
                 either was not consulted or was consulted too early",
            );
        })
    }
}

fn spawn_greeter(system: &SimNode, net: SimNetwork, violations: Arc<AtomicU64>) {
    let greeter = system.spawn(VersionedGreeter {
        net,
        node: system.node(),
        violations,
    });
    system.receptionist().register(GREETERS, &greeter);
}

#[test]
fn a_rolling_upgrade_never_sends_a_peer_a_form_it_cannot_read() {
    let workload = MixedVersionGreetings::new(3, 10);
    if let Err(failure) = run_cluster_swarm(&workload, sweep_seeds(0..48)) {
        panic!("{failure}");
    }
}

#[test]
fn the_rollout_sweep_actually_mixes_revisions() {
    // #8: the property above is vacuous unless the sweep really upgraded nodes
    // and really sent the newer form to the ones that had moved. A rollout that
    // silently stopped firing would leave every seed green and every claim empty.
    let workload = MixedVersionGreetings::new(3, 10);
    let stats = match run_cluster_swarm_coverage(&workload, coverage_seeds(0..32)) {
        Ok(stats) => stats,
        Err(failure) => panic!("{failure}"),
    };
    assert!(
        stats.upgrades > 0,
        "the sweep never moved a node between releases, so nothing ran mixed: {stats:?}",
    );
    assert!(
        workload.newer_sent.load(Ordering::Relaxed) > 0,
        "the sweep upgraded nodes but never sent the newer form, so the gate was \
         never asked for anything but its floor",
    );
    assert!(
        stats.blocked > 0,
        "the sweep never blocked a frame, so the rollout never overlapped a \
         partition or crash: {stats:?}",
    );
}

/// A rollout whose stages are not a legal upgrade path is refused when it is
/// built, not discovered as a workload failure halfway through a sweep.
mod illegal_rollouts {
    use super::*;

    #[test]
    #[should_panic(expected = "share no revision")]
    fn stages_that_cannot_associate_are_refused() {
        // v1-only and v7..=v9: two releases that could never form an association
        // (**V2**), so no sequence of restarts gets from one to the other.
        Rollout::new(vec![
            Window::at("actor.wire", 1),
            Window::new("actor.wire", 7, 9, 7),
        ]);
    }

    #[test]
    #[should_panic(expected = "cannot read what the older release wrote")]
    fn a_stage_that_drops_what_its_predecessor_writes_is_refused() {
        // Writes v1, then a release that no longer reads v1: **V4** in reverse —
        // the upgrade cannot read what is already on the wire.
        Rollout::new(vec![
            Window::new("actor.wire", 1, 2, 1),
            Window::new("actor.wire", 2, 3, 2),
        ]);
    }

    #[test]
    #[should_panic(expected = "cannot read what the newer release wrote")]
    fn a_stage_that_cannot_read_its_successor_is_refused() {
        // The rollback case (**V5**): the older release must still accept what the
        // newer one wrote, or rolling back cannot read its own cluster's traffic.
        Rollout::new(vec![
            Window::at("actor.wire", 1),
            Window::new("actor.wire", 1, 2, 2),
        ]);
    }
}
