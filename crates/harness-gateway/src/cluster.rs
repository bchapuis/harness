//! Joining the cluster as a non-voting, non-hosting **client**.
//!
//! The gateway builds a [`TcpCluster`] in [`MembershipMode::Static`]: it speaks
//! the cluster secret, dials the nodes, and is brought into the membership view
//! (`add_member`) so it receives the receptionist gossip — but it runs no Raft
//! engine, so it never votes, and it never calls `granary_named`, so it never
//! hosts a grain. Its node id is OUTSIDE the voter roster `1..=nodes`, and each
//! node must admit it with `--client <id>=<host>`.
//!
//! The transport is plaintext, guarded by the cluster secret, exactly as the node
//! is; a deployment crossing untrusted links would provision a transport cert on
//! both sides and set `TlsConfig` here and there.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use actor_cluster::ClusterConfig;
use actor_cluster::ClusterSystem;
use actor_cluster::MembershipMode;
use actor_core::Event;
use actor_core::EventSink;
use actor_core::NodeId;
use actor_runtime::DEFAULT_CONNECT_TIMEOUT;
use actor_runtime::DEFAULT_HANDSHAKE_TIMEOUT;
use actor_runtime::DEFAULT_OUTBOUND_CAPACITY;
use actor_runtime::OsEntropy;
use actor_runtime::TcpCluster;
use actor_runtime::TcpConfig;
use actor_runtime::TcpTransport;
use actor_runtime::TokioClock;
use actor_runtime::TokioSpawner;
use actor_serialization::JsonCodec;

/// How the gateway joins the transport: its own (non-voting) id, the roster it
/// dials, and the secret it presents.
#[derive(Clone, Debug)]
pub struct ClusterOptions {
    /// The gateway's node id — OUTSIDE `1..=nodes`, so it never votes or hosts.
    pub node_id: u64,
    /// The voter roster size; the nodes are ids `1..=nodes`.
    pub nodes: u64,
    /// The interface the transport binds. `0.0.0.0` in a container; loopback by
    /// default.
    pub bind_host: String,
    /// The host nodes dial the gateway back at. Defaults to `bind_host`; a
    /// container behind a wildcard bind sets it to the routable name. Must match
    /// the `<host>` the nodes pass in `--client <id>=<host>`.
    pub advertise_host: Option<String>,
    /// Each node's reachable host (`--peer <id>=<host>`); loopback if unset.
    pub peer_hosts: BTreeMap<u64, String>,
    /// Node/client `i`'s transport port is `port_base + i - 1`; must match the
    /// nodes' `--port-base`.
    pub port_base: u16,
    /// The cluster secret (core spec §15); must match the nodes' `--secret`.
    pub secret: String,
}

/// Build the cluster client and bring the nodes into its membership view. The
/// returned system hosts nothing; pass it to [`crate::connect`] to discover the
/// host gateways and build the [`Gateway`](crate::Gateway).
pub async fn join(opts: ClusterOptions) -> Result<TcpCluster, String> {
    if opts.node_id >= 1 && opts.node_id <= opts.nodes {
        return Err(format!(
            "--node-id {} collides with the voter roster 1..={}: the gateway must use a \
             non-voting id outside it",
            opts.node_id, opts.nodes
        ));
    }
    let node = NodeId::new(opts.node_id);
    let roster: Vec<NodeId> = (1..=opts.nodes).map(NodeId::new).collect();
    let host_of = |id: u64| -> &str {
        opts.peer_hosts
            .get(&id)
            .map(String::as_str)
            .unwrap_or("127.0.0.1")
    };
    // Dial map: every voter at its host, plus this gateway at its advertised host.
    let mut peers: BTreeMap<NodeId, SocketAddr> = roster
        .iter()
        .map(|peer| {
            Ok((
                *peer,
                resolve(host_of(peer.uid()), opts.port_base, peer.uid())?,
            ))
        })
        .collect::<Result<_, String>>()?;
    let advertise_host = opts.advertise_host.as_deref().unwrap_or(&opts.bind_host);
    let advertised = resolve(advertise_host, opts.port_base, opts.node_id)?;
    peers.insert(node, advertised);
    // The allowlist admits the voters and self; only these may complete the
    // handshake (core spec §15).
    let admitted: BTreeSet<NodeId> = peers.keys().copied().collect();
    let bind = resolve(&opts.bind_host, opts.port_base, opts.node_id)?;
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|e| format!("bind transport {bind}: {e}"))?;
    let (transport, inbound) = TcpTransport::start(
        TcpConfig {
            node,
            advertised,
            peers,
            endpoint_gossip_interval: Duration::from_secs(1),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            outbound_capacity: DEFAULT_OUTBOUND_CAPACITY,
            codec: Arc::new(JsonCodec),
            cluster_secret: opts.secret.clone(),
            allowlist: Some(admitted),
            tls: None,
        },
        listener,
    );
    let system: TcpCluster = ClusterSystem::start(
        node,
        TokioClock::new(),
        OsEntropy::new(),
        TokioSpawner::current(),
        transport,
        inbound,
        ClusterConfig {
            events: Arc::new(GatewayEvents { node }),
            // No Raft, no SWIM voting: a client observes membership but does not
            // drive it. `Static` without a detector is the non-participating mode
            // (system.rs) — the gateway never hosts, so it has no shard groups to
            // elect and nothing to down.
            membership: MembershipMode::Static { detector: None },
            ..ClusterConfig::default()
        },
    );
    // Bring the voters into the membership view so their receptionist gossip — the
    // host gateway refs the client routes through — reaches us. We are never added
    // to their Raft roster, so we never vote or host.
    for peer in &roster {
        system.add_member(*peer);
    }
    eprintln!(
        "[gateway {node}] joined the cluster (client of nodes 1..={})",
        opts.nodes
    );
    Ok(system)
}

/// Resolve id `id`'s address on `host` at port `base + id - 1` — the same
/// derivation the nodes use, so a client id maps to one transport port.
fn resolve(host: &str, base: u16, id: u64) -> Result<SocketAddr, String> {
    let port = base + (id - 1) as u16;
    (host, port)
        .to_socket_addrs()
        .map_err(|e| format!("resolve {host}:{port}: {e}"))?
        .next()
        .ok_or_else(|| format!("resolve {host}:{port}: no address"))
}

/// Membership and reachability transitions on stderr — enough for an operator to
/// see the gateway join and the nodes come and go.
struct GatewayEvents {
    node: NodeId,
}

impl EventSink for GatewayEvents {
    fn emit(&self, event: Event) {
        match &event {
            Event::Suspected { .. }
            | Event::Unreachable { .. }
            | Event::Reachable { .. }
            | Event::NodeDown { .. }
            | Event::MemberJoining { .. }
            | Event::MemberUp { .. }
            | Event::MemberDraining { .. }
            | Event::MemberResumed { .. } => eprintln!("[gateway {}] {event:?}", self.node),
            _ => {}
        }
    }
}
