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
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::LeaderMode;
use actor_cluster::MembershipMode;
use actor_cluster::RaftConfig;
use actor_cluster::SwimConfig;
use actor_core::ActorSystem;
use actor_core::Event;
use actor_core::EventSink;
use actor_core::NodeId;
use actor_runtime::Encryption;
use actor_runtime::FileRaftWAL;
use actor_runtime::JoinOptions;
use actor_runtime::TcpCluster;
use actor_serialization::Codec;
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

/// The Raft election timeout every group on this node inherits.
///
/// This was 20 s, and the reason was not network latency: the disk facet's capture and
/// import scanned a whole image *on the async worker*, so a node served no heartbeat
/// for the 7-14 s that took. A 4 s timeout sailed past that, and the resulting election
/// deposed the activation owning the guest — killing a live SSH session by way of a
/// checkpoint. The timeout was covering for that stall.
///
/// The stall is gone: both scans now run on the node's blocking-I/O pool (granary
/// §7.4, `disk.rs`), so a capture no longer costs the node its heartbeats. What remains
/// to cover is ordinary scheduling jitter on a laptop running three debug nodes, which
/// is what the library's own defaults are sized for. Every group inherits this
/// (`RaftEngine::create_group`), shard groups included.
const RAFT_ELECTION_TIMEOUT: Duration = Duration::from_secs(2);

/// How often a leader heartbeats, and — because the driver checks election deadlines on
/// the same tick — the granularity of every election decision on this node.
///
/// Well under the election timeout, so a healthy leader is never mistaken for a stopped
/// one, and small enough that a cold start is not quantized into multi-second steps.
const RAFT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(250);

/// How long [`actor_runtime::wait_for_cluster`] waits for the cluster to be
/// ready before serving anyway.
///
/// Derived, not chosen. This was a flat 15 s against a 20 s election timeout, so a cold
/// start could not possibly satisfy it: every boot printed "cluster not ready after
/// 15s", opened its door on a cluster that had no leader, and left the first caller to
/// absorb the rest of the wait. The warning was not describing a fault, it was
/// describing the constant sitting below the one it was racing.
///
/// Readiness waits on two things, so the budget has to cover both: SWIM has to discover
/// the peers (`probe_interval` below), and the control group has to elect (a pristine
/// group campaigns a fraction of [`RAFT_ELECTION_TIMEOUT`] after it is built, actor
/// §9.4.3). Several times the larger of the two, so a slow start is absorbed rather
/// than reported, and derived so retuning either does not silently reintroduce a
/// warning that describes the constant rather than the cluster.
const READY_TIMEOUT: Duration = Duration::from_secs(10);

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
    // One codec value for the three places that must agree: the transport's
    // framing, the message layer granary encodes records and snapshots with, and
    // the grain store's stamp naming that codec (granary §7.4). Binary, not the
    // library's JSON default: a machine's disk moves as 1 MiB blocks through the
    // blob path, and a payload is encoded twice — once as the message, again as
    // the frame field carrying it. Under JSON that pair costs ~10.7 MiB and over
    // a second of CPU per copy, so a block's replication misses the quorum
    // timeout on an unremarkable host and provisioning fails as `Unavailable`.
    // Nothing on this system's wire needs a self-describing format.
    let codec: Arc<dyn Codec> = Arc::new(PostcardCodec);
    let raft_dir = opts.data.join("raft");
    let system: TcpCluster = actor_runtime::join(JoinOptions {
        node: opts.id,
        voters: opts.nodes,
        peer_hosts: opts.peer_hosts.clone(),
        clients: BTreeMap::new(),
        bind_host: opts.bind_host.clone(),
        advertise_host: None,
        port_base: opts.port_base,
        secret: opts.secret.clone(),
        codec: Arc::clone(&codec),
        events: Arc::new(StderrEvents { node }),
        // Plaintext, guarded by the cluster secret: fine on loopback or a
        // trusted cluster network. Note what this is *not* protecting — the SSH
        // connection is terminated at the door and bridged over a node-local
        // socket, so no session bytes cross this transport. See the note in
        // `harness-standalone`: plaintext is a stated choice, valid only on a
        // network the operator controls.
        encryption: Encryption::PlaintextTrusted,
        membership: Box::new(move |roster| {
            MembershipMode::Leader(LeaderMode {
                // A machine's shard leader is where its microVM runs, so a
                // *spurious* election is not free here the way it is for a
                // stateless service: it resigns the activation that owns the
                // guest. That is why the SWIM side below stays more patient
                // than the library defaults (3 s suspect) — three debug builds
                // sharing one host's CPU miss the tighter ones often enough to
                // churn membership.
                //
                // The Raft side is *not* patient any more, and the difference is
                // worth reading: it used to be an order of magnitude slower than
                // the defaults, but only to survive a capture that blocked the
                // executor. That stall is gone (the scans run on the blocking-I/O
                // pool, granary §7.4), so the timings answer to the network again.
                // Failure detection stays well inside the machine's lease (M5).
                swim: SwimConfig {
                    probe_interval: Duration::from_secs(2),
                    rtt: Duration::from_millis(500),
                    suspect_timeout: Duration::from_secs(10),
                    indirect_count: 2,
                },
                raft: {
                    let mut raft = RaftConfig::new(roster.to_vec());
                    raft.storage = FileRaftWAL::factory(raft_dir);
                    // See the constants: these were an order of magnitude more
                    // patient to survive a capture that blocked the executor,
                    // which it no longer does.
                    raft.election_timeout = RAFT_ELECTION_TIMEOUT;
                    raft.heartbeat_interval = RAFT_HEARTBEAT_INTERVAL;
                    raft
                },
                downing: DowningPolicy::Conservative,
            })
        }),
    })
    .await?;

    // The mechanism this node holds guests with, built from `--machine` before
    // anything durable starts: a mechanism the operator named but this build or
    // host cannot supply is a configuration error, not a half-booted node.
    let host = machine_host(&opts, machine_kind)?;
    let provider = Arc::new(runtime_provider(&host, node, &system));
    let grain_store = FileGrainStore::factory(opts.data.join("grains"), codec.as_ref());
    // This node's shared capabilities, set once and used to host every type below
    // (granary §7.4, §13). The I/O pool matters most in this deployment: a machine's
    // disk facet writes whole 1 MiB image blocks, so an inline fsync would stall the
    // executor hardest here — and the node is also running Raft heartbeats on it.
    let granary_node = system
        .granary_node()
        .blocking_io(Arc::new(granary::ThreadPoolIo::sized_for_host()))
        .metrics(Arc::new(granary::AtomicGrainMetrics::new()));
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
    let alarms: Granary<AlarmIndex<TcpCluster>> = granary_node.granary(GranaryConfig {
        grain_store: Some(grain_store),
        shards: opts.shards,
        ..GranaryConfig::default()
    });
    let machines: Granary<NodeMachine> = granary_node.granary_named_with_alarms(
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
        "[{node}] joined (voter {}/{}), data {}, machine {machine_kind:?}{grade}",
        opts.id,
        opts.nodes,
        opts.data.display()
    );
    actor_runtime::wait_for_cluster(&system, opts.nodes as usize, READY_TIMEOUT).await;

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
) {
    let backend = Arc::new(LocalBackend::new(host, node.to_string()));
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
