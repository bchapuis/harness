//! Integration: a real two-node cluster talking over the TCP transport on
//! loopback (spec §7). Remote `ask`/`tell` round-trip across the wire, an
//! unreachable peer surfaces as `CallError::Unreachable` rather than hanging,
//! and a node reaches a peer whose address it learns only by gossip (spec §9.3).

mod support;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use actor_core::ActorId;
use actor_core::ActorSystem;
use actor_core::CallError;
use actor_core::NodeId;
use actor_core::Path;
use actor_runtime::TcpCluster;

use support::Counter;
use support::Get;
use support::Greet;
use support::Greeter;
use support::Inc;

#[tokio::test]
async fn remote_ask_round_trips_over_tcp() {
    let (sys_a, sys_b) = support::two_nodes().await;

    // Greeter lives on B; A resolves it by id and asks over the wire.
    let greeter = sys_b.spawn(Greeter::<TcpCluster>::new("Hello"));
    let remote = sys_a.resolve::<Greeter<TcpCluster>>(greeter.id().clone());

    let reply = remote
        .ask(Greet {
            name: "world".into(),
        })
        .await;
    assert_eq!(reply, Ok("Hello, world!".to_string()));
}

#[tokio::test]
async fn remote_tell_then_ask_observes_the_effect() {
    let (sys_a, sys_b) = support::two_nodes().await;

    let counter = sys_b.spawn(Counter::<TcpCluster>::new());
    let remote = sys_a.resolve::<Counter<TcpCluster>>(counter.id().clone());

    // Per-directed-pair FIFO: the tell is observed before the ask's reply.
    remote.tell(Inc).await.unwrap();
    remote.tell(Inc).await.unwrap();
    let count = remote.ask(Get).await;
    assert_eq!(count, Ok(2));
}

#[tokio::test]
async fn ask_to_an_unknown_peer_is_unreachable_not_a_hang() {
    let (sys_a, _sys_b) = support::two_nodes().await;

    // Node 9 is not in the address book; resolving and asking an actor there
    // must fail fast with Unreachable.
    let ghost = ActorId::new(NodeId::new(9), Path::new("/user/ghost"), 1);
    let remote = sys_a.resolve::<Greeter<TcpCluster>>(ghost);

    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        remote.ask(Greet { name: "x".into() }),
    )
    .await
    .expect("ask must not hang");
    assert_eq!(outcome, Err(CallError::Unreachable));
}

#[tokio::test]
async fn a_codec_disagreement_is_rejected_at_the_handshake() {
    // Two ends that name different codecs must not associate (spec §5 #2, §7.1),
    // even though both happen to encode JSON — the handshake compares codec names
    // before any traffic, so the call fails fast with Unreachable.
    let make = |name: &'static str| {
        move |node, peers: BTreeMap<NodeId, SocketAddr>| {
            let mut cfg = support::tcp_config(node, peers);
            cfg.codec = std::sync::Arc::new(support::NamedJson(name));
            cfg
        }
    };
    let (sys_a, sys_b) = support::two_nodes_with(make("json"), make("json-v2")).await;

    let greeter = sys_b.spawn(Greeter::<TcpCluster>::new("Hello"));
    let remote = sys_a.resolve::<Greeter<TcpCluster>>(greeter.id().clone());
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        remote.ask(Greet { name: "x".into() }),
    )
    .await
    .expect("ask must not hang");
    assert_eq!(outcome, Err(CallError::Unreachable));
}

#[tokio::test]
async fn a_node_reaches_a_peer_it_learns_only_via_gossip() {
    // Three nodes. The hub (1) is seeded with everyone; B (2) and C (3) are
    // seeded only with the hub — they do not know each other's address.
    let (la, addr_a) = support::bind_local().await;
    let (lb, addr_b) = support::bind_local().await;
    let (lc, addr_c) = support::bind_local().await;
    let (hub, node_b, node_c) = (NodeId::new(1), NodeId::new(2), NodeId::new(3));

    let all: BTreeMap<NodeId, SocketAddr> =
        BTreeMap::from([(hub, addr_a), (node_b, addr_b), (node_c, addr_c)]);
    let only_hub = |me: NodeId, my_addr: SocketAddr| BTreeMap::from([(hub, addr_a), (me, my_addr)]);

    let sys_a = support::start_node(support::tcp_config(hub, all), la);
    let sys_b = support::start_node(support::tcp_config(node_b, only_hub(node_b, addr_b)), lb);
    let sys_c = support::start_node(support::tcp_config(node_c, only_hub(node_c, addr_c)), lc);
    // Keep the hub alive for the duration of the test.
    let _sys_a = sys_a;

    // The greeter lives on C; B holds its id but, at first, no route to C.
    let greeter = sys_c.spawn(Greeter::<TcpCluster>::new("Hello"));
    let remote = sys_b.resolve::<Greeter<TcpCluster>>(greeter.id().clone());

    // The hub gossips its endpoint table to B and C, so B learns C's address
    // (and C learns B's, needed for the reply). Retry until discovery converges;
    // before that the ask fails fast with Unreachable rather than hanging.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match remote
            .ask(Greet {
                name: "world".into(),
            })
            .await
        {
            Ok(reply) => {
                assert_eq!(reply, "Hello, world!");
                break;
            }
            Err(CallError::Unreachable) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            other => panic!("expected to reach C via gossip, got {other:?}"),
        }
    }
}
