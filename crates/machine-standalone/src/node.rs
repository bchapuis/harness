//! One node of the standalone machine deployment: the production runtime wired
//! to the machine grain (machine spec).
//!
//! A node hosts machine grains, votes in Raft, and runs an **in-process SSH
//! front door** (machine §5.1). The front door is in-process rather than a
//! separate tier — the shape `harness-gateway` takes for sessions — for one
//! reason: a bridged channel ends at a node-local vsock socket, and the
//! cross-node relay that would let a detached door reach another node's guest
//! is machine §8's deferred work. So each node's door serves the machines that
//! node currently leads, and a client reconnects (machine §6) when leadership
//! moves.
//!
//! Every node is identical: same grain type, same seams, same runtime binding. A
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
use actor_runtime::Encryption;
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
use machine_frontdoor::serve_connection;
use machine_grain::MACHINE_TYPE;
use machine_grain::Machine;
use machine_grain::fake::FakeRuntimeProvider;

use crate::authority::FrontDoor;
use crate::authority::GrainAuthority;
use crate::authority::NodeMachine;
use crate::backend::LocalBackend;
use crate::provider::NodeRuntimeProvider;

/// Which kind of machine this node runs (see [`NodeRuntimeProvider`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MachineKind {
    Fake,
    Firecracker,
    Docker,
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
    /// The kind of machine this node runs. No default: an operator who does not
    /// say gets told, rather than silently getting the fake guest.
    pub machine: Option<MachineKind>,
    /// The firecracker executable, for `--machine firecracker`.
    pub fc_binary: String,
    /// The vmlinux kernel, for `--machine firecracker`.
    pub fc_kernel: String,
    /// The container CLI, for `--machine docker`. Podman's compatible CLI works
    /// too.
    pub docker_cli: String,
    /// The runner image `guest/machine-docker/build.sh` builds, for `--machine
    /// docker`.
    pub docker_image: String,
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
            machine: None,
            fc_binary: "firecracker".to_string(),
            fc_kernel: String::new(),
            docker_cli: "docker".to_string(),
            docker_image: "harness-machine-runner:1".to_string(),
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
    let machine_kind = opts.machine.ok_or(
        "--machine is required: `firecracker` (a real microVM per machine; Linux + /dev/kvm), \
         `docker` (the rootfs in a privileged container; any host with docker, shared-kernel \
         isolation), or `fake` (no guest — durability only, machine §7)",
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
            // See the note in `harness-standalone`: plaintext is a stated choice,
            // valid only on a network the operator controls.
            encryption: Encryption::PlaintextTrusted,
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
                    // Patient enough to outlast a *capture*, which is the one thing
                    // this deployment does that stalls a node for seconds: the disk
                    // facet scans the whole image synchronously — 512 MiB here, read
                    // and hashed block by block on the runtime — so the node serves no
                    // heartbeat while it runs. Measured at 7-14s on one laptop, which
                    // sailed past a 4s timeout: the checkpoint elected a new leader for
                    // the very shard whose machine it was capturing, deposing the
                    // activation and killing the guest under a live SSH session. Every
                    // group inherits this timeout (`RaftEngine::create_group`), shard
                    // groups included, so it is the knob that governs that race.
                    //
                    // The scan belongs off the executor; until it is, a demo's timings
                    // have to cover it. A real deployment with a non-blocking capture
                    // tightens these back down.
                    raft.election_timeout = Duration::from_secs(20);
                    raft.heartbeat_interval = Duration::from_secs(4);
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

    // The mechanism this node holds guests with, built from `--machine` before
    // anything durable starts: a mechanism the operator named but this build or
    // host cannot supply is a configuration error, not a half-booted node.
    let host = machine_host(&opts, machine_kind)?;
    let provider = Arc::new(runtime_provider(&host, node, &system));
    let grain_store = FileGrainStore::factory(opts.data.join("grains"));
    // One I/O pool for the node (granary §7.4). A machine's disk facet writes whole
    // 1 MiB image blocks, so this is the deployment where an inline fsync would stall
    // the executor hardest — and the node is also running Raft heartbeats on it.
    let blocking_io: Arc<dyn granary::BlockingIo> =
        Arc::new(granary::ThreadPoolIo::sized_for_host());
    let metrics = Arc::new(granary::AtomicGrainMetrics::new());
    let config = GranaryConfig {
        shards: opts.shards,
        grain_store: Some(grain_store.clone()),
        blocking_io: Some(Arc::clone(&blocking_io)),
        metrics: Some(metrics.clone()),
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
        blocking_io: Some(blocking_io),
        metrics: Some(metrics),
        shards: opts.shards,
        ..GranaryConfig::default()
    });
    let machines: Granary<NodeMachine> = system.granary_named_with_alarms(
        MACHINE_TYPE,
        config,
        Arc::new(move || Machine::new(Arc::clone(&provider))),
        alarms,
    );

    // The grade, not just the flag: what confinement a machine actually gets is
    // the one thing about the mechanism an operator should read at startup
    // (sandbox §3.4 — the two grades are not interchangeable).
    let grade = match &host {
        None => " (no guest: durability only)".to_string(),
        Some(mechanism) => format!(" ({} grade)", mechanism.isolation()),
    };
    eprintln!(
        "[{node}] transport {advertised}, data {}, machine {machine_kind:?}{grade}",
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
            host.clone(),
            node.to_string(),
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
    host: Option<Arc<dyn machine_host::MachineHost>>,
    scope: String,
) {
    let backend = Arc::new(LocalBackend::new(host, scope));
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

/// Bind the grain factory's provider to what this node holds guests with.
fn runtime_provider(
    host: &Option<Arc<dyn machine_host::MachineHost>>,
    node: NodeId,
    system: &TcpCluster,
) -> NodeRuntimeProvider {
    match host {
        None => NodeRuntimeProvider::Fake(FakeRuntimeProvider::new(
            system.clone(),
            FAKE_WRITE_INTERVAL,
        )),
        #[cfg(feature = "host")]
        Some(mechanism) => {
            NodeRuntimeProvider::Hosted(machine_grain::hosted::HostedRuntimeProvider::new(
                Arc::clone(mechanism),
                machine_grain::hosted::HostedRuntimeConfig::new(node.to_string()),
            ))
        }
        // Unreachable: `machine_host` answers `None` for every mode this build
        // can serve.
        #[cfg(not(feature = "host"))]
        Some(_) => {
            let _ = node;
            unreachable!("no mechanism is built without the `host` feature")
        }
    }
}

/// The same, for a build without the binding: only the fake guest remains, and
/// naming any other mechanism is a configuration error rather than a silent
/// downgrade to it.
#[cfg(not(feature = "host"))]
fn machine_host(
    _opts: &NodeOptions,
    mode: MachineKind,
) -> Result<Option<Arc<dyn machine_host::MachineHost>>, String> {
    match mode {
        MachineKind::Fake => Ok(None),
        other => Err(format!(
            "--machine {other:?} needs the `host` feature; this binary was built without it"
        )),
    }
}

/// How fast the fake guest dirties blocks: fast enough that a demo's captures
/// have real content, slow enough not to swamp a debug-build cluster.
const FAKE_WRITE_INTERVAL: Duration = Duration::from_millis(50);

/// Build the mechanism this node holds guests with, from `--machine` and its assets.
/// `None` is `--machine fake`: no mechanism, and no guest to reach.
#[cfg(feature = "host")]
fn machine_host(
    opts: &NodeOptions,
    mode: MachineKind,
) -> Result<Option<Arc<dyn machine_host::MachineHost>>, String> {
    let mechanism = match mode {
        MachineKind::Fake => return Ok(None),
        MachineKind::Firecracker => {
            if opts.fc_kernel.is_empty() {
                return Err(
                    "--machine firecracker requires --fc-kernel (guest/machine-rootfs/build.sh \
                     produces one)"
                        .to_string(),
                );
            }
            machine_grain::hosted::microvm_host(&opts.fc_binary, &opts.fc_kernel)
        }
        MachineKind::Docker => {
            machine_grain::hosted::container_host(&opts.docker_cli, &opts.docker_image)
        }
    };
    Ok(Some(mechanism))
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
