//! Invariants of the **production** transport under real concurrency (spec §3.3,
//! §2.13, §6, §2.5). The deterministic simulator proves these about the protocol;
//! these tests run the same properties over the real `TcpTransport` + tokio seam
//! on loopback sockets, where races, hangs, and leaks the simulator can't see
//! would surface. They use a multi-thread runtime and generous timeouts; they are
//! not seed-reproducible, so they poll until converged rather than assuming
//! timing.
//!
//! The transport's *decisions* (the address book's rules, dial-race
//! arbitration, ABA-safe eviction) live socket-free in `src/association.rs`,
//! where unit tests spell each race out deterministically. What remains here is
//! what only real sockets can show: the I/O ordering, teardown, and liveness
//! around those decisions.

mod support;

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_cluster::Reachability;
use actor_cluster::SwimConfig;
use actor_core::Actor;
use actor_core::ActorId;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::CallError;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::Path;
use actor_core::Terminated;
use actor_runtime::TcpCluster;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use support::Greet;
use support::Greeter;

// --- §4 / no silent loss: every ask terminates under concurrency -----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_asks_all_terminate() {
    let (sys_a, sys_b) = support::two_nodes().await;
    let greeter = sys_b.spawn(Greeter::<TcpCluster>::new("Hi"));
    let remote = sys_a.resolve::<Greeter<TcpCluster>>(greeter.id().clone());

    // Many asks race to establish the single A→B connection (double-dial path)
    // and share its bounded queue; every one must terminate with its reply.
    let mut handles = Vec::new();
    for i in 0..64u32 {
        let r = remote.clone();
        handles.push(tokio::spawn(async move {
            r.ask(Greet {
                name: i.to_string(),
            })
            .await
        }));
    }
    for (i, h) in handles.into_iter().enumerate() {
        let outcome = tokio::time::timeout(Duration::from_secs(10), h)
            .await
            .expect("ask must not hang")
            .expect("task panicked");
        assert_eq!(outcome, Ok(format!("Hi, {i}!")));
    }
}

// --- §3.3 / per-pair FIFO: ordered tells arrive in order over the wire ----------

#[derive(Serialize, Deserialize)]
struct Seq(u64);
impl Message for Seq {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("rt.Seq");
}

struct Order {
    log: Arc<Mutex<Vec<u64>>>,
}
impl Actor for Order {
    type System = TcpCluster;
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Seq>();
    }
}
impl Handler<Seq> for Order {
    async fn handle(&mut self, msg: Seq, _ctx: &Ctx<Self>) {
        self.log.lock().unwrap().push(msg.0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tells_preserve_per_pair_order() {
    let (sys_a, sys_b) = support::two_nodes().await;
    let log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let order = sys_b.spawn(Order {
        log: Arc::clone(&log),
    });
    let remote = sys_a.resolve::<Order>(order.id().clone());

    for i in 0..100u64 {
        remote.tell(Seq(i)).await.unwrap();
    }
    // Drain: wait until all 100 have been observed.
    for _ in 0..200 {
        if log.lock().unwrap().len() == 100 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(*log.lock().unwrap(), (0..100).collect::<Vec<_>>());
}

// --- §2.5 / watch-exactly-once across the wire on a graceful stop --------------

#[derive(Serialize, Deserialize)]
struct StopNow;
impl Message for StopNow {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("rt.StopNow");
}

struct Stoppable;
impl Actor for Stoppable {
    type System = TcpCluster;
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<StopNow>();
    }
}
impl Handler<StopNow> for Stoppable {
    async fn handle(&mut self, _msg: StopNow, ctx: &Ctx<Self>) {
        ctx.stop();
    }
}

struct CountWatcher {
    target: ActorRef<Stoppable>,
    count: Arc<AtomicUsize>,
    _p: PhantomData<()>,
}
impl Actor for CountWatcher {
    type System = TcpCluster;
    async fn started(&mut self, ctx: &Ctx<Self>) -> Result<(), actor_core::BoxError> {
        ctx.watch(&self.target);
        Ok(())
    }
    fn register(_r: &mut HandlerRegistry<Self>) {}
}
impl Handler<Terminated> for CountWatcher {
    async fn handle(&mut self, _signal: Terminated, _ctx: &Ctx<Self>) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_watch_fires_exactly_once_on_graceful_stop() {
    let (sys_a, sys_b) = support::two_nodes().await;
    let target = sys_b.spawn(Stoppable);
    let remote = sys_a.resolve::<Stoppable>(target.id().clone());

    let count = Arc::new(AtomicUsize::new(0));
    sys_a.spawn(CountWatcher {
        target: remote.clone(),
        count: Arc::clone(&count),
        _p: PhantomData,
    });
    tokio::time::sleep(Duration::from_millis(200)).await; // let the Watch reach B

    // Stop it gracefully; B must send exactly one Terminated to A.
    remote.tell(StopNow).await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert_eq!(count.load(Ordering::SeqCst), 1);
}

// --- §2.13 / §7: the detector keeps running over a dead peer (connect no-hang) --

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn swim_marks_an_unreachable_peer_without_stalling() {
    // A single live node that knows one peer at a refused address. SWIM must
    // probe it, fail, and mark it unreachable, proving the failure detector
    // runs over the real transport and a dead dial never wedges it (spec §2.13,
    // §7). Conservative downing keeps it unreachable, not down (#16).
    let (la, addr_a) = support::bind_local().await;
    // A port nothing listens on: bind then drop, so dials are refused.
    let (dead, dead_addr) = support::bind_local().await;
    drop(dead);

    let node_a = NodeId::new(1);
    let ghost = NodeId::new(2);
    let peers: BTreeMap<NodeId, SocketAddr> =
        BTreeMap::from([(node_a, addr_a), (ghost, dead_addr)]);
    let swim = SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(100),
        suspect_timeout: Duration::from_millis(400),
        ..SwimConfig::default()
    };
    let sys_a = support::start_node_swim(support::tcp_config(node_a, peers), la, swim);
    sys_a.add_member(ghost);

    let mut reached = false;
    for _ in 0..100 {
        if sys_a.membership().reachability(ghost) == Some(Reachability::Unreachable) {
            reached = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(reached, "SWIM should mark the dead peer unreachable");
    assert!(
        !sys_a.membership().is_down(ghost),
        "conservative downing must not down it across a (perceived) partition",
    );
}

// --- §6.3 graceful shutdown: a stopped node frees its port and is seen down ---

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_node_releases_its_port_and_is_seen_unreachable() {
    let (la, addr_a) = support::bind_local().await;
    let (lb, addr_b) = support::bind_local().await;
    let node_a = NodeId::new(1);
    let node_b = NodeId::new(2);
    let peers: BTreeMap<NodeId, SocketAddr> = BTreeMap::from([(node_a, addr_a), (node_b, addr_b)]);
    let swim = SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(100),
        suspect_timeout: Duration::from_millis(400),
        ..SwimConfig::default()
    };

    let sys_a = support::start_node_swim(support::tcp_config(node_a, peers.clone()), la, swim);
    let sys_b = support::start_node_swim(support::tcp_config(node_b, peers), lb, swim);
    sys_a.add_member(node_b);
    sys_b.add_member(node_a);
    tokio::time::sleep(Duration::from_millis(300)).await; // let them see each other

    // Stop B gracefully.
    sys_b.shutdown();

    // A's detector keeps probing and, finding B gone, marks it unreachable,
    // not down (conservative, #16).
    let mut seen_unreachable = false;
    for _ in 0..100 {
        if sys_a.membership().reachability(node_b) == Some(Reachability::Unreachable) {
            seen_unreachable = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        seen_unreachable,
        "A should observe the stopped node unreachable"
    );
    assert!(
        !sys_a.membership().is_down(node_b),
        "conservative downing must not down it"
    );

    // B released its listener: the port can be bound again.
    assert!(
        TcpListener::bind(addr_b).await.is_ok(),
        "a stopped node must free its listener port",
    );
}

// --- §2.13/§9 regression: a silent peer is dropped at the handshake timeout -----

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_silent_peer_is_dropped_at_the_handshake_timeout() {
    // A peer that connects but never sends its Hello (slowloris) must be cut off
    // at the configured handshake timeout, not tie up the accept task forever.
    let (la, addr_a) = support::bind_local().await;
    let node_a = NodeId::new(1);
    let peers: BTreeMap<NodeId, SocketAddr> = BTreeMap::from([(node_a, addr_a)]);
    let mut cfg = support::tcp_config(node_a, peers);
    cfg.handshake_timeout = Duration::from_millis(300);
    let _sys = support::start_node(cfg, la);

    // Connect a raw socket and stay silent. The node must close it; the read
    // then completes (EOF or reset) well inside the 2s bound, and would block
    // past it if the timeout were not honoured.
    let mut raw = TcpStream::connect(addr_a).await.unwrap();
    let mut buf = [0u8; 1];
    let res = tokio::time::timeout(Duration::from_secs(2), raw.read(&mut buf)).await;
    assert!(
        res.is_ok(),
        "node must drop a silent peer at the handshake timeout, but the connection stayed open",
    );
}

// --- §2.13 #3: a malformed frame tears down the association, not the node -------

/// Write one length-delimited record the way `wire.rs` frames every message: a
/// `u32` big-endian length, then the bytes. Spelled out here because the test's
/// whole point is to put bytes on the wire the transport's own writer would
/// never produce.
async fn write_record(sock: &mut TcpStream, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).unwrap();
    sock.write_all(&len.to_be_bytes()).await.unwrap();
    sock.write_all(bytes).await.unwrap();
    sock.flush().await.unwrap();
}

/// Read one length-delimited record (the node's own `Hello`).
async fn read_record(sock: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    sock.read_exact(&mut len).await?;
    let mut buf = vec![0u8; u32::from_be_bytes(len) as usize];
    sock.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Dial `addr` as node `node` and complete the §2.14 handshake by hand: the JSON
/// `Hello` preamble (announcing the wire version, identity, codec, and cluster
/// secret), then the node's own `Hello` back. What returns is an established
/// association, over which the caller can write whatever it likes.
async fn handshake_raw(addr: SocketAddr, node: u64) -> TcpStream {
    let mut sock = TcpStream::connect(addr).await.unwrap();
    let hello = serde_json::json!({
        "version": actor_cluster::WIRE.version(),
        "node": node,
        // Dialed back only by endpoint gossip, and refused there: this end
        // speaks, it is never spoken to.
        "advertised": "127.0.0.1:1",
        "codec_name": "json",
        "cluster_secret": support::SECRET,
    });
    write_record(&mut sock, &serde_json::to_vec(&hello).unwrap()).await;
    read_record(&mut sock)
        .await
        .expect("the node answers a valid Hello with its own");
    sock
}

/// The association is gone: the node dropped its end, so a read completes at
/// once with EOF or a reset instead of blocking on a live connection.
async fn assert_torn_down(sock: &mut TcpStream, what: &str) {
    let mut buf = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(5), sock.read(&mut buf))
        .await
        .unwrap_or_else(|_| panic!("{what}: the association stayed open"));
    assert!(
        matches!(read, Ok(0) | Err(_)),
        "{what}: the node kept the association alive",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_malformed_frame_tears_down_its_association_not_the_node() {
    // The §2.13 requirement over real wire bytes, which the simulator cannot carry:
    // its frames are structured, so only the payload is ever encoded (§2.12.2).
    // Two peers handshake for real and then write what no writer of this
    // protocol would: a payload that does not decode, and a length past the
    // 16 MiB cap. Each association must die alone, with the node serving its
    // other one before and after.
    let (la, addr_a) = support::bind_local().await;
    let (lb, addr_b) = support::bind_local().await;
    let node_a = NodeId::new(1);
    let node_b = NodeId::new(2);
    let peers: BTreeMap<NodeId, SocketAddr> = BTreeMap::from([(node_a, addr_a), (node_b, addr_b)]);
    let sys_a = support::start_node(support::tcp_config(node_a, peers.clone()), la);
    let sys_b = support::start_node(support::tcp_config(node_b, peers), lb);
    sys_a.add_member(node_b);
    sys_b.add_member(node_a);

    // The association that must survive: B calls a greeter on A.
    let greeter = sys_a.spawn(Greeter::<TcpCluster>::new("Hi"));
    let remote = sys_b.resolve::<Greeter<TcpCluster>>(greeter.id().clone());
    let greet = Greet {
        name: "before".into(),
    };
    let before = remote.ask(greet).await;
    assert_eq!(before, Ok("Hi, before!".to_string()));

    // A frame whose payload is not a `Wire` message at all.
    let mut bad = handshake_raw(addr_a, 3).await;
    write_record(&mut bad, b"not a frame at all").await;
    assert_torn_down(&mut bad, "an undecodable frame").await;

    // A length prefix past the cap, refused before a byte of it is allocated.
    let mut huge = handshake_raw(addr_a, 4).await;
    huge.write_all(&(32u32 * 1024 * 1024).to_be_bytes())
        .await
        .unwrap();
    huge.flush().await.unwrap();
    assert_torn_down(&mut huge, "an oversized length prefix").await;

    // The node itself never wavered: the other association still serves.
    let greet = Greet {
        name: "after".into(),
    };
    let after = remote.ask(greet).await;
    assert_eq!(after, Ok("Hi, after!".to_string()));
}

// --- regression: a dial to a refused address is Unreachable, never a hang -----

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_dial_is_unreachable_not_a_hang() {
    let (la, addr_a) = support::bind_local().await;
    let (dead, dead_addr) = support::bind_local().await;
    drop(dead); // free the port; connects will be refused

    let node_a = NodeId::new(1);
    let ghost = NodeId::new(2);
    let peers: BTreeMap<NodeId, SocketAddr> =
        BTreeMap::from([(node_a, addr_a), (ghost, dead_addr)]);
    let sys_a = support::start_node(support::tcp_config(node_a, peers), la);

    let id = ActorId::new(ghost, Path::new("/user/ghost"), 1);
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        sys_a
            .resolve::<Greeter<TcpCluster>>(id)
            .ask(Greet { name: "x".into() }),
    )
    .await
    .expect("a refused dial must fail fast, not hang");
    assert_eq!(outcome, Err(CallError::Unreachable));
}
