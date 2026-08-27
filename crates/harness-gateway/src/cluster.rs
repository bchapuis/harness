//! Joining the cluster as a non-voting, non-hosting **client**.
//!
//! The gateway builds a [`TcpCluster`] in [`MembershipMode::Static`]: it speaks
//! the cluster secret, dials the nodes, and is brought into the membership view
//! (`add_member`) so it receives the receptionist gossip — but it runs no Raft
//! engine, so it never votes, and it never calls `granary_named`, so it never
//! hosts a grain. Its node id is OUTSIDE the voter roster `1..=nodes`, and each
//! node must admit it with `--client <id>=<host>`.
//!
//! The transport is plaintext, guarded by the cluster secret; a deployment
//! crossing untrusted links must provision a transport cert on both sides and set
//! `TlsConfig`.

use std::collections::BTreeMap;
use std::sync::Arc;

use actor_cluster::MembershipMode;
use actor_core::Event;
use actor_core::EventSink;
use actor_core::JsonCodec;
use actor_core::NodeId;
use actor_runtime::Encryption;
use actor_runtime::JoinOptions;
use actor_runtime::TcpCluster;

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
    let system: TcpCluster = actor_runtime::join(JoinOptions {
        node: opts.node_id,
        voters: opts.nodes,
        peer_hosts: opts.peer_hosts.clone(),
        clients: BTreeMap::new(),
        bind_host: opts.bind_host.clone(),
        advertise_host: opts.advertise_host.clone(),
        port_base: opts.port_base,
        secret: opts.secret.clone(),
        codec: Arc::new(JsonCodec),
        events: Arc::new(GatewayEvents { node }),
        // The gateway joins inside the cluster's trust boundary (see
        // `docs/multi-tenant-edge.md`); the same plaintext caveat applies.
        encryption: Encryption::PlaintextTrusted,
        // No Raft, no SWIM voting: a client observes membership but does not
        // drive it. `Static` without a detector is the non-participating mode
        // (system.rs) — the gateway never hosts, so it has no shard groups to
        // elect and nothing to down.
        membership: Box::new(|_| MembershipMode::Static { detector: None }),
    })
    .await?;
    eprintln!(
        "[gateway {node}] joined the cluster (client of nodes 1..={})",
        opts.nodes
    );
    Ok(system)
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
