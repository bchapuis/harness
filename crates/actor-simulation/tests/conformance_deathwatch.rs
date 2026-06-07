//! Conformance: death watch (spec §12) — the gaps beyond the existing watch
//! tests: multiple watchers, `unwatch`, and signal ordering through the mailbox.

mod support;

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::Terminated;
use actor_simulation::SimSystem;
use serde::Deserialize;
use serde::Serialize;

use support::Greeter;
use support::Stop;

/// `-1` marks a `Terminated`; positive values mark `Note`s, so a test can assert
/// the order in which a watcher observed them.
type Log = Arc<Mutex<Vec<i64>>>;

#[derive(Serialize, Deserialize)]
struct Note(i64);
impl Message for Note {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("conf.Note");
}

#[derive(Serialize, Deserialize)]
struct DoUnwatch;
impl Message for DoUnwatch {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("conf.DoUnwatch");
}

struct Watcher<S: ActorSystem> {
    target: ActorRef<Greeter<S>>,
    log: Log,
    _system: PhantomData<fn() -> S>,
}

impl<S: ActorSystem> Watcher<S> {
    fn new(target: ActorRef<Greeter<S>>, log: Log) -> Self {
        Watcher {
            target,
            log,
            _system: PhantomData,
        }
    }
}

impl<S: ActorSystem> Actor for Watcher<S> {
    type System = S;
    async fn started(&mut self, ctx: &Ctx<Self>) -> Result<(), actor_core::BoxError> {
        ctx.watch(&self.target);
        Ok(())
    }
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Note>();
        r.accept::<DoUnwatch>();
    }
}

impl<S: ActorSystem> Handler<Note> for Watcher<S> {
    async fn handle(&mut self, msg: Note, _ctx: &Ctx<Self>) {
        self.log.lock().unwrap().push(msg.0);
    }
}

impl<S: ActorSystem> Handler<DoUnwatch> for Watcher<S> {
    async fn handle(&mut self, _msg: DoUnwatch, ctx: &Ctx<Self>) {
        ctx.unwatch(&self.target);
    }
}

impl<S: ActorSystem> Handler<Terminated> for Watcher<S> {
    async fn handle(&mut self, _signal: Terminated, _ctx: &Ctx<Self>) {
        self.log.lock().unwrap().push(-1);
    }
}

#[test]
fn two_watchers_each_receive_exactly_one_terminated() {
    let (sim, system) = support::local(1);
    let log_a: Log = Arc::new(Mutex::new(Vec::new()));
    let log_b: Log = Arc::new(Mutex::new(Vec::new()));
    let (a, b) = (Arc::clone(&log_a), Arc::clone(&log_b));

    sim.block_on(async move {
        let clock = system.clock().clone();
        let target = system.spawn(Greeter::<SimSystem>::new("Hi"));
        system.spawn(Watcher::new(target.clone(), a));
        system.spawn(Watcher::new(target.clone(), b));
        clock.sleep(Duration::from_millis(1)).await; // let both watches register
        target.tell(Stop).await.unwrap();
        clock.sleep(Duration::from_millis(1)).await;
    });

    assert_eq!(*log_a.lock().unwrap(), vec![-1]);
    assert_eq!(*log_b.lock().unwrap(), vec![-1]);
}

#[test]
fn unwatch_stops_delivery_of_terminated() {
    let (sim, system) = support::local(2);
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&log);

    sim.block_on(async move {
        let clock = system.clock().clone();
        let target = system.spawn(Greeter::<SimSystem>::new("Hi"));
        let watcher = system.spawn(Watcher::new(target.clone(), observed));
        clock.sleep(Duration::from_millis(1)).await;
        watcher.tell(DoUnwatch).await.unwrap();
        clock.sleep(Duration::from_millis(1)).await;
        target.tell(Stop).await.unwrap();
        clock.sleep(Duration::from_millis(1)).await;
    });

    assert!(
        log.lock().unwrap().is_empty(),
        "an unwatched target must not deliver Terminated",
    );
}

#[test]
fn terminated_arrives_in_mailbox_order_after_earlier_messages() {
    let (sim, system) = support::local(3);
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&log);

    sim.block_on(async move {
        let clock = system.clock().clone();
        let target = system.spawn(Greeter::<SimSystem>::new("Hi"));
        let watcher = system.spawn(Watcher::new(target.clone(), observed));
        clock.sleep(Duration::from_millis(1)).await;
        // Messages sent before the target stops are observed before Terminated
        // (spec §12 #13 — signals ride the mailbox in serial order).
        watcher.tell(Note(1)).await.unwrap();
        watcher.tell(Note(2)).await.unwrap();
        target.tell(Stop).await.unwrap();
        clock.sleep(Duration::from_millis(1)).await;
    });

    assert_eq!(*log.lock().unwrap(), vec![1, 2, -1]);
}

#[test]
fn remote_graceful_stop_notifies_a_remote_watcher() {
    // A watcher on node A watches a greeter on node B. When B's greeter stops
    // gracefully, B sends a `Terminated` frame to A and the watcher is notified —
    // not only on node-down (spec §12).
    let (sim, net) = support::cluster(8, None);
    let node_a = net.join(actor_core::NodeId::new(1));
    let node_b = net.join(actor_core::NodeId::new(2));
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&log);

    sim.block_on(async move {
        let clock = node_a.clock().clone();
        let greeter = node_b.spawn(Greeter::<actor_simulation::SimCluster>::new("Hi"));
        let remote = node_a.resolve::<Greeter<actor_simulation::SimCluster>>(greeter.id().clone());
        node_a.spawn(Watcher::new(remote, observed));
        clock.sleep(Duration::from_millis(5)).await; // let the Watch reach B
        greeter.tell(Stop).await.unwrap(); // graceful stop on B
        clock.sleep(Duration::from_millis(5)).await; // let Terminated reach A
    });

    assert_eq!(
        *log.lock().unwrap(),
        vec![-1],
        "the remote watcher receives exactly one Terminated on a graceful stop",
    );
}
