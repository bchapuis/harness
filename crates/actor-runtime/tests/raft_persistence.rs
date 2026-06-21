//! Integration: durable Raft state across a real node restart (spec §9.4.3
//! item 2).
//!
//! A three-voter leader-mode cluster runs over real TCP on loopback, with each
//! voter's Raft state in a [`FileRaftWAL`] directory. A committed
//! transition must be on a stopped voter's disk; the voter must come back —
//! same identity, same directory, same port — with that state, and the cluster
//! must keep committing transitions with the restarted voter participating.
//! This ties real cluster traffic → bytes on disk → reload, end to end; the
//! storage format itself (recovery, truncation, torn tails) is covered by the
//! unit tests in `actor-runtime/src/storage.rs`.

mod support;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::EntryPayload;
use actor_cluster::GroupId;
use actor_cluster::MemberStatus;
use actor_cluster::MembershipCommand;
use actor_cluster::RaftConfig;
use actor_cluster::RaftWAL;
use actor_cluster::SwimConfig;
use actor_core::NodeId;
use actor_runtime::FileRaftWAL;
use actor_runtime::TcpCluster;
use support::bind_local;
use support::start_node_leader;
use support::tcp_config;
use tokio::net::TcpListener;

const A: NodeId = NodeId::new(1);
const B: NodeId = NodeId::new(2);
const C: NodeId = NodeId::new(3);

fn swim() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(80),
        suspect_timeout: Duration::from_millis(400),
        indirect_count: 2,
    }
}

/// Poll `cond` until it holds or `timeout` elapses (real time; this test runs
/// the production runtime, not the simulator).
async fn wait_until(what: &str, timeout: Duration, mut cond: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + timeout;
    while !cond() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}",
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Rebind `addr` after the previous owner released it, retrying briefly: the
/// old listener closes asynchronously on shutdown.
async fn bind_retry(addr: SocketAddr) -> TcpListener {
    for _ in 0..100 {
        match TcpListener::bind(addr).await {
            Ok(listener) => return listener,
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    panic!("could not rebind {addr}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_voter_recovers_its_persisted_raft_state() {
    let data_dir = tempfile::tempdir().unwrap();

    let mut peers: BTreeMap<NodeId, SocketAddr> = BTreeMap::new();
    let mut listeners = Vec::new();
    for node in [A, B, C] {
        let (listener, addr) = bind_local().await;
        peers.insert(node, addr);
        listeners.push((node, listener));
    }

    let mut raft = RaftConfig::new(vec![A, B, C]);
    raft.election_timeout = Duration::from_millis(500);
    raft.heartbeat_interval = Duration::from_millis(100);
    raft.storage = FileRaftWAL::factory(data_dir.path().to_path_buf());

    let nodes: Vec<TcpCluster> = listeners
        .into_iter()
        .map(|(node, listener)| {
            start_node_leader(
                tcp_config(node, peers.clone()),
                listener,
                swim(),
                raft.clone(),
                DowningPolicy::Conservative,
            )
        })
        .collect();
    for system in &nodes {
        for &peer in peers.keys() {
            if peer != system.node() {
                system.add_member(peer);
            }
        }
    }
    let (a, b, c) = (nodes[0].clone(), nodes[1].clone(), nodes[2].clone());

    // A leader emerges and every node agrees on it.
    wait_until("an agreed leader", Duration::from_secs(15), || {
        a.leader().is_some() && a.leader() == b.leader() && a.leader() == c.leader()
    })
    .await;

    // Commit a transition: drain C, proposed from A (forwarded if A is not the
    // leader), and observed cluster-wide.
    assert!(a.drain(C).await, "the drain commits through the leader");
    wait_until(
        "the drain visible everywhere",
        Duration::from_secs(10),
        || {
            a.membership().status(C) == Some(MemberStatus::Draining)
                && b.membership().status(C) == Some(MemberStatus::Draining)
                && c.membership().self_status() == MemberStatus::Draining
        },
    )
    .await;

    // Stop voter B — whichever role it held; a leader restart just adds a
    // failover to the scenario — and give its loops a beat to wind down.
    b.shutdown();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The committed transition is on B's disk: a real term and the Drain entry,
    // exactly what §9.4.3 item 2 requires to survive.
    // Storage is namespaced per (group, node); the membership log is the control
    // group's.
    let b_dir = data_dir
        .path()
        .join(GroupId::CONTROL.to_string())
        .join(B.to_string());
    {
        let persisted = FileRaftWAL::open(&b_dir).unwrap().load();
        assert!(
            persisted.term > 0,
            "B persisted the term it participated in"
        );
        assert!(
            persisted.log.iter().any(|e| matches!(
                &e.payload,
                EntryPayload::App(bytes)
                    if MembershipCommand::decode(bytes) == Some(MembershipCommand::Drain(C))
            )),
            "B persisted the committed Drain entry: {:?}",
            persisted.log,
        );
    }

    // Restart B: same identity, same storage directory, same port.
    let listener = bind_retry(peers[&B]).await;
    let b2 = start_node_leader(
        tcp_config(B, peers.clone()),
        listener,
        swim(),
        raft.clone(),
        DowningPolicy::Conservative,
    );
    for &peer in peers.keys() {
        if peer != B {
            b2.add_member(peer);
        }
    }

    // The restarted voter reconverges on the committed state (its own log plus
    // the leader's replication) and rejoins the quorum.
    wait_until(
        "B2 sees the committed drain",
        Duration::from_secs(15),
        || b2.membership().status(C) == Some(MemberStatus::Draining),
    )
    .await;

    // New transitions still commit, proposed *by the restarted voter*: resume C
    // and watch the whole cluster — B2 included — converge.
    let mut resumed = b2.resume(C).await;
    if !resumed {
        // One retry: a leader election may still be settling if B led before.
        resumed = b2.resume(C).await;
    }
    assert!(resumed, "the restarted voter's proposal commits");
    wait_until(
        "the resume visible everywhere",
        Duration::from_secs(10),
        || {
            a.membership().status(C) == Some(MemberStatus::Up)
                && b2.membership().status(C) == Some(MemberStatus::Up)
                && c.membership().self_status() == MemberStatus::Up
        },
    )
    .await;
}
