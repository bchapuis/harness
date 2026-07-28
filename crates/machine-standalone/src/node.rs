//! One node of the standalone machine deployment: the production runtime wired
//! to the machine grain (machine spec).
//!
//! A node hosts `machine` grains, votes in Raft, and runs an **in-process SSH
//! front door** (machine §5.1). The front door is in-process rather than a
//! separate tier — the shape `harness-gateway` takes for sessions — for one
//! reason: a bridged channel ends at a node-local vsock socket, and the
//! cross-node relay that would let a detached door reach another node's guest
//! is machine §8's deferred work. So each node's door serves the machines that
//! node currently leads, and a client reconnects (machine §6) when leadership
//! moves.
//!
//! Every node is identical: same grain type, same seams, same VM binding. A
//! machine is a grain, so durability, placement, and the single-writer fence
//! are granary's; this file only chooses the bindings and opens the ports.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use actor_cluster::ClusterConfig;
use actor_cluster::ClusterSystem;
use actor_cluster::DowningPolicy;
use actor_cluster::LeaderMode;
use actor_cluster::MembershipMode;
use actor_cluster::RaftConfig;
use actor_cluster::SwimConfig;
use actor_core::ActorSystem;
use actor_core::Event;
use actor_core::EventSink;
use actor_core::NodeId;
use actor_runtime::DEFAULT_CONNECT_TIMEOUT;
use actor_runtime::DEFAULT_HANDSHAKE_TIMEOUT;
use actor_runtime::DEFAULT_OUTBOUND_CAPACITY;
use actor_runtime::FileRaftWAL;
use actor_runtime::OsEntropy;
use actor_runtime::TcpCluster;
use actor_runtime::TcpConfig;
use actor_runtime::TcpTransport;
use actor_runtime::TokioClock;
use actor_runtime::TokioSpawner;
use actor_serialization::PostcardCodec;
use granary::AlarmIndex;
use granary::FileGrainStore;
use granary::GrainName;
use granary::Granary;
use granary::GranaryConfig;
use granary::GranaryExt;
use machine::MACHINE_TYPE;
use machine::Machine;
use machine::fake::FakeVmProvider;
use machine_frontdoor::serve_connection;

use crate::authority::FrontDoor;
use crate::authority::GrainAuthority;
use crate::authority::NodeMachine;
use crate::backend::LocalVsockBackend;
use crate::provider::NodeVmProvider;

/// Which VM binding the node runs (see [`NodeVmProvider`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VmMode {
    Fake,
    Firecracker,
}

/// Everything `node` takes from the command line.
#[derive(Clone, Debug)]
pub struct NodeOptions {
    /// This node's id, `1..=nodes`.
    pub id: u64,
    /// Roster size; every node must agree on it.
    pub nodes: u64,
    /// This node's own data directory (journal, disk-facet images, workspaces).
    /// Nodes share nothing on disk: the journal replicates over the transport.
    pub data: PathBuf,
    /// The local interface the transport port binds.
    pub bind_host: String,
    /// Each node's reachable host, from `--peer <id>=<host>`.
    pub peer_hosts: BTreeMap<u64, String>,
    /// Where this node serves the admin socket the CLI uses (`--admin
    /// <addr>`), or `None` for a node that takes no admin traffic. Loopback:
    /// it carries no authentication (see `crate::admin`).
    pub admin: Option<String>,
    /// Node `i`'s transport port is `port_base + i - 1`.
    pub port_base: u16,
    /// The cluster secret peers must present (core §15).
    pub secret: String,
    /// Shards the machine grain type is spread over; every node must agree, and
    /// so must any cluster client (a name must hash to the same shard).
    pub shards: usize,
    /// The VM binding. No default: an operator who does not say gets told,
    /// rather than silently getting the fake guest.
    pub vm: Option<VmMode>,
    /// The firecracker executable, for `--vm firecracker`.
    pub fc_binary: String,
    /// The vmlinux kernel, for `--vm firecracker`.
    pub fc_kernel: String,
    /// SSH front doors to open: `port → machine name`, from `--door
    /// <port>=<machine>`. One machine per port, because SSH fixes the host key
    /// at KEX — before a username exists to name a machine with.
    pub doors: BTreeMap<u16, String>,
}

impl Default for NodeOptions {
    fn default() -> Self {
        NodeOptions {
            id: 0,
            nodes: 3,
            data: PathBuf::from("./machine-data"),
            bind_host: "127.0.0.1".to_string(),
            peer_hosts: BTreeMap::new(),
            admin: None,
            port_base: 7601,
            secret: "machine-standalone".to_string(),
            shards: GranaryConfig::default().shards,
            vm: None,
            fc_binary: "firecracker".to_string(),
            fc_kernel: String::new(),
            doors: BTreeMap::new(),
        }
    }
}

/// How long a front-door command waits on the machine's leader.
const DOOR_TIMEOUT: Duration = Duration::from_secs(30);

/// Boot the node, open its doors, and host machines forever.
pub async fn run(opts: NodeOptions) -> Result<(), String> {
    if opts.id < 1 || opts.id > opts.nodes {
        return Err(format!(
            "--id must be in 1..={}, got {}",
            opts.nodes, opts.id
        ));
    }
    // Resolved before any port is bound: a missing binding is a configuration
    // error the operator fixes, not a half-booted node.
    let vm_mode = opts.vm.ok_or(
        "--vm is required: `firecracker` (a real microVM per machine; Linux + /dev/kvm) or \
         `fake` (no guest — durability only, machine §7)",
    )?;
    let node = NodeId::new(opts.id);
    let roster: Vec<NodeId> = (1..=opts.nodes).map(NodeId::new).collect();
    let host_of = |id: u64| -> &str {
        opts.peer_hosts
            .get(&id)
            .map(String::as_str)
            .unwrap_or("127.0.0.1")
    };
    let peers: BTreeMap<NodeId, SocketAddr> = roster
        .iter()
        .map(|peer| {
            Ok((
                *peer,
                resolve(host_of(peer.uid()), opts.port_base, peer.uid())?,
            ))
        })
        .collect::<Result<_, String>>()?;
    let admitted: BTreeSet<NodeId> = peers.keys().copied().collect();
    let advertised = peers[&node];
    let bind = resolve(&opts.bind_host, opts.port_base, opts.id)?;
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|e| format!("bind transport {bind}: {e}"))?;
    let (transport, inbound) = TcpTransport::start(
        TcpConfig {
            node,
            advertised,
            peers: peers.clone(),
            endpoint_gossip_interval: Duration::from_secs(1),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            outbound_capacity: DEFAULT_OUTBOUND_CAPACITY,
            // Binary, not the library's JSON default: a machine's disk moves as
            // 1 MiB blocks through the blob path, and a payload is encoded
            // twice — once as the message, again as the frame field carrying
            // it. Under JSON that pair costs ~10.7 MiB and over a second of CPU
            // per copy, so a block's replication misses the quorum timeout on
            // an unremarkable host and provisioning fails as `Unavailable`.
            // Nothing on this system's wire needs a self-describing format.
            codec: Arc::new(PostcardCodec),
            cluster_secret: opts.secret.clone(),
            allowlist: Some(admitted),
            // Plaintext, guarded by the cluster secret: fine on loopback or a
            // trusted cluster network. Note what this is *not* protecting —
            // the SSH connection is terminated at the door and bridged over a
            // node-local socket, so no session bytes cross this transport.
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
            // The message layer, matching the transport's above: a blob's bytes
            // are encoded here first and framed there second, so both have to
            // be binary for either to help. This is also the codec granary
            // encodes a grain's records and snapshots with, so a deployment
            // that changes it cannot read journals written under the old one.
            codec: Arc::new(PostcardCodec),
            events: Arc::new(StderrEvents { node }),
            membership: MembershipMode::Leader(LeaderMode {
                // Deliberately more patient than the library defaults (1s
                // election, 250ms heartbeat, 3s suspect). A machine's shard
                // leader is where its microVM runs, so a *spurious* election
                // is not free here the way it is for a stateless service: it
                // resigns the activation that owns the guest. Three debug
                // builds sharing one host's CPU miss those defaults often
                // enough to churn leadership continuously. Real deployments
                // on separate hosts can tighten these back down; failure
                // detection stays well inside the machine's lease (M5).
                swim: SwimConfig {
                    probe_interval: Duration::from_secs(2),
                    rtt: Duration::from_millis(500),
                    suspect_timeout: Duration::from_secs(10),
                    indirect_count: 2,
                },
                raft: {
                    let mut raft = RaftConfig::new(roster.clone());
                    raft.storage = FileRaftWAL::factory(opts.data.join("raft"));
                    raft.election_timeout = Duration::from_secs(4);
                    raft.heartbeat_interval = Duration::from_secs(1);
                    raft
                },
                downing: DowningPolicy::Conservative,
            }),
            ..ClusterConfig::default()
        },
    );
    for peer in &roster {
        if *peer != node {
            system.add_member(*peer);
        }
    }

    let provider = Arc::new(vm_provider(&opts, vm_mode, &system)?);
    let grain_store = FileGrainStore::factory(opts.data.join("grains"));
    let config = GranaryConfig {
        shards: opts.shards,
        grain_store: Some(grain_store.clone()),
        // Where the disk facet materializes each machine's image and the
        // workspace facet its files (grain §7.11, §7.15) — under --data, so a
        // restarted node finds its own.
        data_dir: Some(opts.data.join("facets")),
        ..GranaryConfig::default()
    };
    // The shared alarm index (grain §16). The machine's checkpoint alarm is
    // also its session lease (machine §4, M5), and a lease that only fired
    // while the grain happened to be awake would not bound anything: the index
    // is what re-activates a due machine after hibernation or failover.
    let alarms: Granary<AlarmIndex<TcpCluster>> = system.granary(GranaryConfig {
        grain_store: Some(grain_store),
        shards: opts.shards,
        ..GranaryConfig::default()
    });
    let machines: Granary<NodeMachine> = system.granary_named_with_alarms(
        MACHINE_TYPE,
        config,
        Arc::new(move || Machine::new(Arc::clone(&provider))),
        alarms,
    );

    eprintln!(
        "[{node}] transport {advertised}, data {}, vm {vm_mode:?}",
        opts.data.display()
    );
    wait_for_hosts(&system, opts.nodes as usize).await;

    if let Some(addr) = &opts.admin {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| format!("bind admin {addr}: {e}"))?;
        eprintln!("[{node}] admin socket on {addr}");
        tokio::spawn(crate::admin::serve(listener, machines.clone()));
    }

    // One front-door actor for this node: every attachment it takes names it,
    // so a machine death-watches it and reaps the attachment if this process
    // dies without detaching (machine §5.1).
    let door = system.spawn(FrontDoor).id().clone();
    let authority = Arc::new(GrainAuthority::new(machines, door, DOOR_TIMEOUT));
    for (port, name) in &opts.doors {
        let addr: SocketAddr = format!("{}:{port}", opts.bind_host)
            .parse()
            .map_err(|e| format!("door {port}={name}: {e}"))?;
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| format!("bind door {addr}: {e}"))?;
        let machine = GrainName::new(MACHINE_TYPE, name.clone());
        eprintln!("[{node}] ssh front door for {machine} on {addr}");
        tokio::spawn(serve_door(
            listener,
            machine,
            Arc::clone(&authority),
            node,
            vm_mode,
        ));
    }
    if opts.doors.is_empty() {
        eprintln!("[{node}] no --door given: hosting machines with no ingress");
    }
    std::future::pending::<()>().await;
    Ok(())
}

/// Accept SSH connections for one machine until the process ends. Each
/// connection is independent: `serve_connection` terminates SSH, authenticates
/// against the machine's journaled keys, attaches, and bridges (machine §5.1).
async fn serve_door(
    listener: tokio::net::TcpListener,
    machine: GrainName,
    authority: Arc<GrainAuthority>,
    node: NodeId,
    vm_mode: VmMode,
) {
    let backend = Arc::new(LocalVsockBackend::new(vm_mode == VmMode::Firecracker));
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                eprintln!("[{node}] door {machine}: accept: {e}");
                continue;
            }
        };
        let machine = machine.clone();
        let authority = Arc::clone(&authority);
        let backend = Arc::clone(&backend);
        tokio::spawn(async move {
            if let Err(e) = serve_connection(stream, machine.clone(), authority, backend).await {
                // Reported, never masked (machine §6): a refused key, a
                // machine led by another node, a severed session.
                eprintln!("[{node}] door {machine} from {peer}: {e}");
            }
        });
    }
}

/// Build the node's VM binding from `--vm` and its assets.
fn vm_provider(
    opts: &NodeOptions,
    mode: VmMode,
    system: &TcpCluster,
) -> Result<NodeVmProvider, String> {
    match mode {
        VmMode::Fake => Ok(NodeVmProvider::Fake(FakeVmProvider::new(
            system.clone(),
            Duration::from_millis(50),
        ))),
        #[cfg(feature = "firecracker")]
        VmMode::Firecracker => {
            if opts.fc_kernel.is_empty() {
                return Err(
                    "--vm firecracker requires --fc-kernel (guest/machine-rootfs/build.sh \
                            produces one)"
                        .to_string(),
                );
            }
            Ok(NodeVmProvider::Firecracker(
                machine::firecracker::FirecrackerMachineProvider::new(
                    machine::firecracker::FirecrackerMachineConfig::new(
                        &opts.fc_binary,
                        &opts.fc_kernel,
                    ),
                ),
            ))
        }
        #[cfg(not(feature = "firecracker"))]
        VmMode::Firecracker => {
            Err("this binary was built without the `firecracker` feature".to_string())
        }
    }
}

/// Resolve node `id`'s address on `host` at port `base + id - 1`.
fn resolve(host: &str, base: u16, id: u64) -> Result<SocketAddr, String> {
    let port = base + (id - 1) as u16;
    (host, port)
        .to_socket_addrs()
        .map_err(|e| format!("resolve {host}:{port}: {e}"))?
        .next()
        .ok_or_else(|| format!("resolve {host}:{port}: no address"))
}

/// Hold startup open until the cluster has converged enough to serve. A
/// convenience: granary's bounded redirect absorbs a command issued before the
/// shard map converges (G13).
async fn wait_for_hosts(system: &TcpCluster, expected: usize) {
    let peers = expected.saturating_sub(1);
    for _ in 0..150 {
        if system.membership().members().len() >= peers && system.leader().is_some() {
            eprintln!("[{}] cluster ready (leader elected)", system.node());
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    eprintln!(
        "[{}] warning: cluster not ready after 15s; serving anyway",
        system.node()
    );
}

/// The observability stream on stderr: membership and reachability
/// transitions. Dispatch-level core events are swallowed as noise.
struct StderrEvents {
    node: NodeId,
}

impl EventSink for StderrEvents {
    fn emit(&self, event: Event) {
        match &event {
            Event::Suspected { .. }
            | Event::Unreachable { .. }
            | Event::Reachable { .. }
            | Event::NodeDown { .. }
            | Event::MemberJoining { .. }
            | Event::MemberUp { .. }
            | Event::MemberDraining { .. }
            | Event::MemberResumed { .. } => eprintln!("[{}] {event:?}", self.node),
            other => {
                if std::env::var_os("MACHINE_TRACE").is_some() {
                    eprintln!("[{}] {other:?}", self.node);
                }
            }
        }
    }
}
