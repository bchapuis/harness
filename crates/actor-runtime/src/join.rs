//! The composed production node bring-up: [`join`], the counterpart of the
//! simulator's `SimNetwork::join` (spec §18.2). One call takes a deployment's
//! genuine choices and absorbs the assembly the simulator absorbs on its side:
//! the dial map, the allowlist, the bind, the transport, the system, and the
//! membership seeding.
//!
//! The roster is computed once and handed to the caller's
//! [`JoinOptions::membership`] closure, so Raft's voters and the `add_member`
//! loop cannot drift apart.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use actor_cluster::ClusterConfig;
use actor_cluster::ClusterSystem;
use actor_cluster::MembershipMode;
use actor_core::EventSink;
use actor_core::NodeId;
use actor_serialization::Codec;

use crate::TcpCluster;
use crate::clock::TokioClock;
use crate::entropy::OsEntropy;
use crate::spawner::TokioSpawner;
use crate::transport::Encryption;
use crate::transport::TcpConfig;
use crate::transport::TcpTransport;
use crate::transport::node_addr;

/// A deployment's genuine choices when a process joins a cluster, everything
/// else about bring-up is [`join`]'s job. Ports follow the one address plan:
/// node or client `i`'s transport port is `port_base + i - 1`.
pub struct JoinOptions {
    /// This process's id. A **voter** when inside `1..=voters`; a non-voting
    /// **client** (an edge tier) otherwise.
    pub node: u64,
    /// The voter roster size: voters are `1..=voters`, identical on every node.
    pub voters: u64,
    /// Per-id reachable host overrides for the dial map; loopback when unset.
    pub peer_hosts: BTreeMap<u64, String>,
    /// Non-voting clients this node admits (id → host). A host that does not
    /// resolve yet is admitted anyway (the client dials in and gossip flows
    /// over its connection), so a not-yet-scheduled edge pod never blocks a
    /// node's startup.
    pub clients: BTreeMap<u64, String>,
    /// The interface this process binds its transport on; `0.0.0.0` in a
    /// container.
    pub bind_host: String,
    /// The host peers dial back, when it differs from the natural one: a
    /// voter's default is its own `peer_hosts` entry (else loopback), a
    /// client's is `bind_host`. They differ behind a `0.0.0.0` wildcard bind.
    pub advertise_host: Option<String>,
    /// Base of the port plan.
    pub port_base: u16,
    /// The cluster secret guarding the transport handshake (spec §15).
    pub secret: String,
    /// The **one** codec value for the three places that must agree: the
    /// transport's framing, the message layer, and any store stamp derived
    /// from it (entity §7.4).
    pub codec: Arc<dyn Codec>,
    /// The observability sink (spec §16).
    pub events: Arc<dyn EventSink>,
    /// Transport encryption. Stated rather than defaulted, because the choice
    /// carries a threat model: `PlaintextTrusted` is valid only on a network
    /// the operator controls; the secret crosses the wire in the handshake.
    pub encryption: Encryption,
    /// Build the membership mode from the computed voter roster: the one
    /// place the roster reaches configuration, so Raft's voters and the
    /// admitted member set cannot drift. A voter names
    /// [`MembershipMode::Leader`] (seeding `RaftConfig` from the given
    /// roster); a client names a non-participating mode.
    #[allow(clippy::type_complexity)] // one closure type, named nowhere else
    pub membership: Box<dyn FnOnce(&[NodeId]) -> MembershipMode>,
}

/// Bring one process into a cluster: resolve the address plan, bind, start the
/// TCP transport, start the [`ClusterSystem`] over it, and seed the membership
/// view with every admitted peer. The inbound frame channel never surfaces:
/// transport and system are paired here, so a mismatched pairing cannot be
/// expressed.
pub async fn join(opts: JoinOptions) -> Result<TcpCluster, String> {
    // A client id inside the voter roster would collide with a voter's identity
    // and port. Refused here, where the roster is computed, so no deployment
    // binary needs to restate the rule.
    if let Some(id) = opts.clients.keys().find(|id| (1..=opts.voters).contains(id)) {
        return Err(format!(
            "client id {id} collides with the voter roster 1..={}: a client must be a \
             non-voting id outside it",
            opts.voters
        ));
    }
    let node = NodeId::new(opts.node);
    let roster: Vec<NodeId> = (1..=opts.voters).map(NodeId::new).collect();
    let is_voter = roster.contains(&node);
    let host_of = |id: u64| -> &str {
        opts.peer_hosts
            .get(&id)
            .map(String::as_str)
            .unwrap_or("127.0.0.1")
    };

    // Dial map: every voter at its host.
    let mut peers: BTreeMap<NodeId, SocketAddr> = roster
        .iter()
        .map(|peer| {
            Ok((
                *peer,
                node_addr(host_of(peer.uid()), opts.port_base, peer.uid())?,
            ))
        })
        .collect::<Result<_, String>>()?;

    // Clients are admitted by id; their dial address is best-effort (they can
    // always dial in).
    let client_ids: BTreeSet<NodeId> = opts.clients.keys().map(|id| NodeId::new(*id)).collect();
    for (id, host) in &opts.clients {
        match node_addr(host, opts.port_base, *id) {
            Ok(addr) => {
                peers.insert(NodeId::new(*id), addr);
            }
            Err(e) => eprintln!(
                "[{node}] client {id}={host} does not resolve yet ({e}); admitting it anyway: \
                 it will dial in"
            ),
        }
    }

    // What peers dial back: the stated override, else a voter's dial-map entry,
    // else (a client) the bind host.
    let advertised = match (&opts.advertise_host, is_voter) {
        (Some(host), _) => node_addr(host, opts.port_base, opts.node)?,
        (None, true) => peers[&node],
        (None, false) => node_addr(&opts.bind_host, opts.port_base, opts.node)?,
    };
    peers.insert(node, advertised);

    // The allowlist admits the voters, the clients, and self; only these ids
    // may complete the transport handshake (spec §15).
    let admitted: BTreeSet<NodeId> = peers
        .keys()
        .copied()
        .chain(client_ids.iter().copied())
        .collect();

    let bind = node_addr(&opts.bind_host, opts.port_base, opts.node)?;
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|e| format!("bind transport {bind}: {e}"))?;

    let membership = (opts.membership)(&roster);
    let mut tcp = TcpConfig::new(
        node,
        advertised,
        peers,
        Arc::clone(&opts.codec),
        opts.secret,
        opts.encryption,
    );
    tcp.allowlist = Some(admitted.clone());
    let (transport, inbound) = TcpTransport::start(tcp, listener);
    let system: TcpCluster = ClusterSystem::start(
        node,
        TokioClock::new(),
        OsEntropy::new(),
        TokioSpawner::current(),
        transport,
        inbound,
        ClusterConfig {
            codec: opts.codec,
            events: opts.events,
            membership,
            ..ClusterConfig::default()
        },
    );

    // Seed the membership view with every admitted peer. A client here is a
    // member of the view, never of the Raft roster.
    for peer in admitted {
        if peer != node {
            system.add_member(peer);
        }
    }
    Ok(system)
}

/// Hold startup open until the cluster has converged enough to serve: every
/// peer is in the membership view and the control group has elected a leader
/// (so shard groups can elect too). A convenience, not a correctness
/// requirement: the layers above absorb a command issued before convergence
/// (e.g. entity's bounded redirect, E13).
///
/// `expected` is the cluster size; `members()` reports peers only (never this
/// node) plus any admitted client, so the bar is the peer quorum
/// `expected - 1` (a client joining only raises the count). Counted polls
/// rather than a wall-clock deadline: reading the host clock directly is what
/// the `Clock` seam exists to prevent (spec §4.6), and the elapsed figure is
/// derived from the count. Size `budget` to cover peer discovery plus the
/// first election; exhausting it then means a peer never appeared or a group
/// cannot elect, which is reported with the member and leader state, and the
/// node serves anyway.
pub async fn wait_for_cluster(system: &TcpCluster, expected: usize, budget: Duration) {
    const POLL: Duration = Duration::from_millis(100);
    let peers = expected.saturating_sub(1);
    let attempts = (budget.as_millis() / POLL.as_millis()).max(1) as u32;
    for attempt in 0..attempts {
        if system.membership().members().len() >= peers && system.leader().is_some() {
            eprintln!(
                "[{}] cluster ready (leader elected) after {:.1}s",
                system.node(),
                (POLL * attempt).as_secs_f64()
            );
            return;
        }
        tokio::time::sleep(POLL).await;
    }
    eprintln!(
        "[{}] warning: cluster not ready after {:.0}s; serving anyway. \
         Members {}/{peers}, leader {:?}",
        system.node(),
        budget.as_secs_f64(),
        system.membership().members().len(),
        system.leader(),
    );
}
