//! One node (silo) of the standalone deployment: the production runtime wired to
//! the harness. A node hosts grains and votes in Raft; it has no client-facing
//! listener. The public edge is `harness-gateway`, a trusted cluster *client*
//! that joins this transport as a non-voting member and addresses the grains
//! directly (design: `docs/multi-tenant-acp-design.md`).
//!
//! Every node is identical (harness spec §7.1): same kinds, same seams. A
//! session is a grain, so durability, placement, and the single-writer fence are
//! granary's: each node hosts every kind's grain type, joining its shards' Raft
//! groups and registering its gateway (§5.3). Membership is a static roster with
//! the SWIM detector observe-only (core spec §9.4.1) — reachability drives the
//! shard map's reallocation and the gateway gossip nodes route activations
//! through. A `--client` (the gateway) is admitted to that membership so the same
//! gossip reaches it, but never to the Raft roster, so it never votes or hosts.

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
use actor_serialization::JsonCodec;
use granary::Granary;
use granary::GranaryExt;
use granary::fs::Fs;
use harness::Budget;
use harness::FileGrainStore;
use harness::GrainStoreFactory;
use harness::GranaryConfig;
use harness::Harness;
use harness::HarnessConfig;
use harness::HarnessEvent;
use harness::Kind;
use harness::Kinds;
use harness::Model;
use harness::ModelParams;
use harness::SandboxProvider;
use harness::Tier;
use harness_anthropic::AnthropicModel;

use tenancy::Directory;

use crate::http::HttpsPost;
use crate::sandbox::LocalSandboxes;

/// Which sandbox provider the node runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SandboxMode {
    /// `LocalSandboxes`: an unconfined `/bin/sh` per session directory,
    /// trusted-input only (sandbox spec §3.4).
    Local,
    /// `harness-sandbox`'s container-backed `Native` tier: `shell` runs
    /// inside a per-session OCI container, workspace bind-mounted, no
    /// network — shared-kernel confinement (sandbox spec §3.5's development
    /// fallback).
    Docker,
    /// `harness-sandbox`'s microVM-backed `Native` tier: `shell` runs
    /// inside a per-session Firecracker VM, workspace synced over vsock, no
    /// network device — hardware-virtualization confinement (sandbox spec
    /// §3.4's stronger grade). Linux with `/dev/kvm` only.
    Firecracker,
    /// `harness-sandbox`'s `DurableWorkspaces`: the `Workspace` tier backed by a
    /// durable filesystem grain (granary §7.10), so a session's workspace survives
    /// hibernation, migration, and node loss. Offers the typed file tools
    /// (read/write/list/remove), no guest shell; excluded trees stay in an ephemeral
    /// overlay (harness §5.5 reversal).
    Durable,
}

/// Everything `node` takes from the command line. The defaults make
/// `node --id N --sandbox <mode>` enough for the three-node local
/// deployment — the sandbox is the one choice without a default, because
/// the unconfined mode must never be reached by omission.
#[derive(Clone, Debug)]
pub struct NodeOptions {
    /// This node's id, `1..=nodes`. Required.
    pub id: u64,
    /// Roster size; every node must agree on it.
    pub nodes: u64,
    /// This node's own data directory (journal + workspaces). Each node keeps
    /// its own: the journal replicates over the transport (a quorum append per
    /// grain, §7.2), so nodes never share a directory or a filesystem.
    pub data: PathBuf,
    /// The local interface the transport port binds. `0.0.0.0` binds every
    /// interface, which is what a container or pod needs; the `127.0.0.1`
    /// default keeps a single-host cluster on loopback.
    pub bind_host: String,
    /// Each node's reachable host, from `--peer <id>=<host>`. A node advertises
    /// its own entry to peers and dials the others at theirs. Empty leaves every
    /// node at `127.0.0.1` (single host); supplying the roster's hosts — pod DNS
    /// names, say — is the whole of what makes the cluster multi-host.
    pub peer_hosts: BTreeMap<u64, String>,
    /// Clients to admit, from `--client <id>=<host>` (the HTTP gateway). Each id
    /// is outside `1..=nodes`: the client joins the transport and membership as a
    /// non-voting, non-hosting participant — so it receives the receptionist
    /// gossip that carries the gateway refs it routes through — but is never added
    /// to the Raft roster, so it never votes or hosts a grain.
    pub clients: BTreeMap<u64, String>,
    /// Node `i`'s transport port is `port_base + i - 1`. A `--client <id>`'s
    /// transport port is derived the same way, so the gateway must agree on
    /// `--port-base`.
    pub port_base: u16,
    /// The Anthropic model id every kind runs on.
    pub model: String,
    /// The cluster secret peers must present (core spec §15).
    pub secret: String,
    /// The Messages API base; `http://…` points at a local fake.
    pub api_url: String,
    /// The sandbox provider; every node must agree (the kind digest covers
    /// the tool declarations and profile this choice selects). `None` fails
    /// at startup: the operator names the confinement story explicitly, so
    /// the unconfined `Local` mode cannot be selected by omission.
    pub sandbox: Option<SandboxMode>,
    /// Container image for `--sandbox docker`; required there, and digest-
    /// covered like `--model`.
    pub sandbox_image: String,
    /// The container CLI binary for `--sandbox docker`.
    pub container_cli: String,
    /// The firecracker executable for `--sandbox firecracker`.
    pub fc_binary: String,
    /// The vmlinux kernel for `--sandbox firecracker`; required there, and
    /// digest-covered like `--model` via the kind digest's profile.
    pub fc_kernel: String,
    /// The base rootfs (ext4, containing `/sbin/fc-agent`) for `--sandbox
    /// firecracker`; required there, digest-covered as the profile image.
    pub fc_rootfs: String,
}

impl Default for NodeOptions {
    fn default() -> Self {
        NodeOptions {
            id: 0,
            nodes: 3,
            data: PathBuf::from("./harness-data"),
            bind_host: "127.0.0.1".to_string(),
            peer_hosts: BTreeMap::new(),
            clients: BTreeMap::new(),
            port_base: 7401,
            model: ModelParams::default().model,
            secret: "harness-standalone".to_string(),
            api_url: "https://api.anthropic.com".to_string(),
            sandbox: None,
            sandbox_image: String::new(),
            container_cli: "docker".to_string(),
            fc_binary: "firecracker".to_string(),
            fc_kernel: String::new(),
            fc_rootfs: String::new(),
        }
    }
}

/// Boot the node and host its grains forever.
pub async fn run(opts: NodeOptions, api_key: String) -> Result<(), String> {
    if opts.id < 1 || opts.id > opts.nodes {
        return Err(format!(
            "--id must be in 1..={}, got {}",
            opts.nodes, opts.id
        ));
    }
    // A client id must fall outside the voter roster: it is admitted to the
    // transport and membership, never to Raft, so it never votes or hosts.
    for id in opts.clients.keys() {
        if *id >= 1 && *id <= opts.nodes {
            return Err(format!(
                "--client id {id} collides with the node roster 1..={}: a client must be a \
                 non-voting id outside it",
                opts.nodes
            ));
        }
    }
    // Resolved before any port is bound: a missing confinement choice is a
    // configuration error the operator fixes, not a half-booted node. The
    // unconfined mode is reachable only by typing it, and says so loudly.
    let sandbox_mode = opts.sandbox.ok_or(
        "--sandbox is required: `docker` or `firecracker` (confined), or `local` \
         (UNCONFINED /bin/sh — trusted-input only, sandbox spec §3.4)",
    )?;
    let node = NodeId::new(opts.id);
    let roster: Vec<NodeId> = (1..=opts.nodes).map(NodeId::new).collect();
    // Each node's reachable host: its `--peer` entry, or loopback if unset. With
    // no `--peer` flags every host is 127.0.0.1 and the cluster is single-host,
    // exactly as before.
    let host_of = |id: u64| -> &str {
        opts.peer_hosts
            .get(&id)
            .map(String::as_str)
            .unwrap_or("127.0.0.1")
    };
    // The transport's dial map: every voter at its host. A client's port derives
    // from its id like a node's, so the gateway must bind its transport on
    // `port_base + node_id - 1`.
    let mut peers: BTreeMap<NodeId, SocketAddr> = roster
        .iter()
        .map(|peer| {
            Ok((
                *peer,
                resolve(host_of(peer.uid()), opts.port_base, peer.uid())?,
            ))
        })
        .collect::<Result<_, String>>()?;
    let client_ids: BTreeSet<NodeId> = opts.clients.keys().map(|id| NodeId::new(*id)).collect();
    // A client's dial address is best-effort: if its host resolves now, add it so
    // the node can also initiate to the client; if not (e.g. the gateway pod is
    // not up yet when the node boots), the client still dials in and gossip flows
    // over that connection — so an unresolvable client host never blocks startup.
    for (id, host) in &opts.clients {
        match resolve(host, opts.port_base, *id) {
            Ok(addr) => {
                peers.insert(NodeId::new(*id), addr);
            }
            Err(e) => eprintln!(
                "[{node}] --client {id}={host} does not resolve yet ({e}); admitting it anyway \
                 — it will dial in"
            ),
        }
    }
    // The allowlist admits the voters and the clients (by id, address or not);
    // only these ids may complete the transport handshake (core spec §15).
    let admitted: BTreeSet<NodeId> = peers
        .keys()
        .copied()
        .chain(client_ids.iter().copied())
        .collect();
    // Advertise the routable host (what peers dial back), but bind the local
    // interface — they differ when bound to the 0.0.0.0 wildcard in a container.
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
            codec: Arc::new(JsonCodec),
            cluster_secret: opts.secret.clone(),
            allowlist: Some(admitted),
            // Plaintext, guarded by the cluster secret. Fine on loopback or a
            // trusted cluster network; a deployment crossing untrusted links
            // would provision certificates and set `TlsConfig` here.
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
            events: Arc::new(StderrEvents { node }),
            // Granary's sharded journal rides Raft, so the node must run the
            // consensus engine: leader-based membership is the only mode that
            // builds one (system.rs). Voters are the full roster; the shard-map
            // and per-shard groups seed from these. Disk-backed storage under
            // --data keeps a restarted node's term/vote, so killing a node and
            // re-attaching stays Raft-safe (the demo's `:retry` story).
            membership: MembershipMode::Leader(LeaderMode {
                swim: SwimConfig::default(),
                raft: {
                    let mut raft = RaftConfig::new(roster.clone());
                    raft.storage = FileRaftWAL::factory(opts.data.join("raft"));
                    raft
                },
                downing: DowningPolicy::Conservative,
            }),
            ..ClusterConfig::default()
        },
    );
    // Bring every voter and every admitted client into the membership view. A
    // client is added here but never to the Raft roster above, so it receives the
    // gossip (and the gateway refs it carries) without ever voting or hosting.
    for peer in &roster {
        if *peer != node {
            system.add_member(*peer);
        }
    }
    for id in &client_ids {
        system.add_member(*id);
    }

    let http = Arc::new(HttpsPost::new(&opts.api_url)?);
    let model: Arc<dyn Model> = Arc::new(AnthropicModel::new(
        http,
        api_key,
        TokioClock::new(),
        Arc::new(OsEntropy::new()),
    ));
    if sandbox_mode == SandboxMode::Local {
        eprintln!(
            "[{node}] WARNING: --sandbox local is UNCONFINED: `shell` runs as this process's \
             user with all its permissions; only the working directory is per-session. \
             Trusted-input only (sandbox spec §3.4) — anything that feeds a real model \
             untrusted content belongs in --sandbox docker or firecracker."
        );
    }
    // One durable grain store under --data (§7.4), shared by the session kinds, the
    // tenancy directory, and — in `--sandbox durable` — the workspace filesystem grain.
    // Its factory caches per node, so every grain type shares one on-disk store keyed by
    // (shard, grain), the grain analogue of the Raft WAL.
    let grain_store = FileGrainStore::factory(opts.data.join("grains"));
    let sandboxes: Arc<dyn SandboxProvider> = match sandbox_mode {
        SandboxMode::Local => Arc::new(LocalSandboxes::new(opts.data.join("workspaces"))),
        SandboxMode::Docker => {
            if opts.sandbox_image.is_empty() {
                return Err("--sandbox docker requires --sandbox-image".to_string());
            }
            Arc::new(
                harness_sandbox::TieredSandboxes::new(opts.data.join("workspaces"))
                    .map_err(|e| format!("workspaces root: {e}"))?
                    .with_container_cli(opts.container_cli.clone())
                    // The hermetic JS surface, so `run_js` runs without any
                    // language runtime in the container (sandbox spec §3.2).
                    .with_quickjs(),
            )
        }
        SandboxMode::Firecracker => {
            if opts.fc_kernel.is_empty() || opts.fc_rootfs.is_empty() {
                return Err(
                    "--sandbox firecracker requires --fc-kernel and --fc-rootfs \
                     (guest/fc-rootfs/build.sh produces both)"
                        .to_string(),
                );
            }
            Arc::new(
                harness_sandbox::TieredSandboxes::new(opts.data.join("workspaces"))
                    .map_err(|e| format!("workspaces root: {e}"))?
                    .with_firecracker(harness_sandbox::FirecrackerConfig::new(
                        &opts.fc_binary,
                        &opts.fc_kernel,
                        &opts.fc_rootfs,
                    ))
                    // The hermetic JS surface, alongside the microVM shell.
                    .with_quickjs(),
            )
        }
        SandboxMode::Durable => {
            // The Workspace tier is a durable filesystem grain (granary §7.10): host
            // `Fs` on this node's system over the shared grain store, then back the
            // provider with it. A workspace survives hibernation, migration, and node
            // loss; the harness re-binds the same grain by session id each activation.
            let workspaces: Granary<Fs<TcpCluster>> = system.granary(GranaryConfig {
                grain_store: Some(grain_store.clone()),
                ..GranaryConfig::default()
            });
            Arc::new(
                harness_sandbox::DurableWorkspaces::new(
                    workspaces,
                    opts.data.join("durable-overlay"),
                )
                .map_err(|e| format!("durable overlay root: {e}"))?,
            )
        }
    };
    // Hosting the kinds is the point; the handle is bound for the node's life (it
    // never returns) to keep the gateway actors alive, like `_directory` below.
    let node_kinds = kinds(&opts, sandbox_mode, grain_store.clone());
    let _harness = Harness::builder(system.clone(), &node_kinds)
        .config(harness_config())
        .host_all(model, sandboxes)
        .build();
    // Host the tenancy ownership-index grain type (one grain per principal) so the
    // gateway's client `Granary<Directory>` can route `Record`/`List` to it. The
    // node only *hosts* it now — the recording on each prompt happens at the
    // gateway edge. Bound for the node's life to keep the handle (and thus the
    // gateway actor's keepalive) alive alongside the kinds the harness holds.
    let _directory: Granary<Directory<TcpCluster>> = system.granary(GranaryConfig {
        grain_store: Some(grain_store),
        ..GranaryConfig::default()
    });

    eprintln!(
        "[{node}] transport {advertised}, data {}, model {}",
        opts.data.display(),
        opts.model
    );
    wait_for_hosts(&system, opts.nodes as usize).await;
    eprintln!(
        "[{node}] hosting grains; the public edge is harness-gateway (a cluster client). \
         No client-facing listener on this node."
    );
    // The node has no listener of its own: it hosts grains and serves the cluster
    // over the transport. Park forever; the process exits on a signal.
    std::future::pending::<()>().await;
    Ok(())
}

/// Resolve node `id`'s address on `host` at port `base + id - 1`. An IP literal
/// (the `127.0.0.1` default) passes straight through; a hostname — a container
/// or pod DNS name like `harness-0.harness` — resolves through the system
/// resolver, which is what lets the roster span machines instead of loopback.
fn resolve(host: &str, base: u16, id: u64) -> Result<SocketAddr, String> {
    let port = base + (id - 1) as u16;
    (host, port)
        .to_socket_addrs()
        .map_err(|e| format!("resolve {host}:{port}: {e}"))?
        .next()
        .ok_or_else(|| format!("resolve {host}:{port}: no address"))
}

/// The cluster-wide kind map (harness spec §7.1). One pure function of the
/// node options that shape kinds (`--model`, `--sandbox`, `--sandbox-image`):
/// every node must register byte-identical kinds — the digest is pinned by
/// `SessionCreated` — so all nodes must run with the same values for those
/// flags.
fn kinds(opts: &NodeOptions, sandbox_mode: SandboxMode, grain_store: GrainStoreFactory) -> Kinds {
    let params = ModelParams {
        model: opts.model.to_string(),
        max_tokens: 4096,
    };
    // The sandbox tools per mode: the same `shell` name and shape, but
    // distinct declarations (and a profile image in docker/firecracker
    // mode), so the digests differ — a mixed-mode cluster fails to agree
    // instead of silently splitting confinement. The `TieredSandboxes`
    // modes additionally offer `run_js` (the hermetic QuickJS Compute tier,
    // sandbox spec §3.2): JavaScript without any runtime in the shell
    // environment. The unconfined `LocalSandboxes` is Native-only, so it has
    // no Compute engine and offers `shell` alone.
    let tools = |kind: Kind| -> Kind {
        match sandbox_mode {
            SandboxMode::Local => {
                let description = "Run a POSIX shell command (`/bin/sh -c`) in the session's \
                                   private workspace directory. Returns exit_code, stdout, and \
                                   stderr.";
                let schema = serde_json::json!({
                    "type": "object",
                    "properties": {
                        "command": {
                            "type": "string",
                            "description": "The shell command to run."
                        }
                    },
                    "required": ["command"]
                });
                kind.sandboxed("shell", description, &schema, Tier::Native)
            }
            SandboxMode::Docker => kind
                .tool(harness_sandbox::shell_tool())
                .tool(harness_sandbox::run_js_tool())
                .sandbox(harness::SandboxProfile::image(&opts.sandbox_image)),
            // The microVM shell declaration differs from the docker one (sync
            // semantics are model-visible), and the profile image carries
            // the rootfs path — both digest-covered, so a cluster mixing
            // realizations fails to agree instead of splitting confinement.
            SandboxMode::Firecracker => kind
                .tool(harness_sandbox::fc_shell_tool())
                .tool(harness_sandbox::run_js_tool())
                .sandbox(harness::SandboxProfile::image(&opts.fc_rootfs)),
            // The durable Workspace tier: the four typed file tools over the durable
            // filesystem grain (granary §7.10), no guest shell or compute.
            SandboxMode::Durable => harness_sandbox::workspace_tools()
                .into_iter()
                .fold(kind, |kind, tool| kind.tool(tool)),
        }
    };
    // Durable grain storage under --data (§7.4): a session is a grain, so its
    // journal must outlive a process restart for a cold-restarted cluster to recover
    // the conversation. Without this, each node's grain store is in-memory and a full
    // restart loses every session (the records do NOT ride the Raft log — that log
    // only carries leader election and the shard map, §7.1). The factory is built by
    // the caller and shared with the tenancy directory: it caches per node, so every
    // grain type shares one on-disk store keyed by (shard, grain) — the grain analogue
    // of the Raft WAL one line up.
    let grain = |kind: Kind| -> Kind {
        kind.grain(GranaryConfig {
            grain_store: Some(grain_store.clone()),
            ..GranaryConfig::default()
        })
    };
    // The Compute tier exists only behind TieredSandboxes; steer toward it
    // for JavaScript exactly where it is offered, and nowhere it is not.
    let js_hint = if matches!(sandbox_mode, SandboxMode::Local | SandboxMode::Durable) {
        ""
    } else {
        " To run JavaScript, use the `run_js` tool rather than reaching for a \
          `node` binary through `shell`: it runs hermetically (QuickJS) and \
          needs no runtime installed in the environment."
    };
    let assistant = tools(
        Kind::new(format!(
            "You are the assistant agent of a small local cluster. Use the `shell` tool for \
             anything you need to inspect, compute, or build; it runs in your session's private \
             workspace directory, which persists across your turns.{js_hint} You may delegate a \
             self-contained subtask to the `worker` kind with the `delegate` tool."
        ))
        .model(params.clone()),
    )
    .delegates_to(&["worker"])
    .budget(Budget::new(200_000, 50));
    let assistant = grain(assistant);
    let worker = tools(
        Kind::new(format!(
            "You are a worker agent. Complete the task you were delegated using the `shell` \
             tool in your private workspace, then reply with a concise result.{js_hint}"
        ))
        .model(params),
    )
    .budget(Budget::new(100_000, 25));
    let worker = grain(worker);
    Kinds::new()
        .register("assistant", assistant)
        .register("worker", worker)
}

/// Harness tuning for interactive local use: deadlines sized for real model
/// calls and multi-step tool runs rather than the library defaults.
fn harness_config() -> HarnessConfig {
    HarnessConfig {
        submit_deadline: Duration::from_secs(600),
        tool_timeout: Duration::from_secs(120),
        ..HarnessConfig::default()
    }
}

/// Hold startup open until the cluster has converged enough to serve: every peer
/// is in the membership view and the control group has elected a leader (so
/// granary's shard groups can elect too). granary's bounded redirect absorbs a
/// prompt issued before the shard map converges (invariant G13), so this is a UX
/// nicety, not a correctness requirement: it makes the first prompt of a fresh
/// cluster prompt rather than bouncing off a still-electing shard.
///
/// `expected` is the cluster size; `members()` reports peers only (never this
/// node, membership.rs) and now also any admitted client, so the bar is the peer
/// quorum `expected - 1` (a client joining only raises the count).
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
        "[{}] warning: cluster not ready after 15s (no leader or peers missing); serving anyway",
        system.node()
    );
}

/// The observability stream on stderr (harness spec §10.4): membership and
/// reachability transitions — the narration of the kill-a-node demo — and
/// every harness event. Dispatch-level core events are swallowed as noise.
struct StderrEvents {
    node: NodeId,
}

impl EventSink for StderrEvents {
    fn emit(&self, event: Event) {
        if let Some(harness_event) = event.as_app::<HarnessEvent>() {
            eprintln!("[{}] {harness_event:?}", self.node);
            return;
        }
        match &event {
            Event::Suspected { .. }
            | Event::Unreachable { .. }
            | Event::Reachable { .. }
            | Event::NodeDown { .. }
            | Event::MemberJoining { .. }
            | Event::MemberUp { .. }
            | Event::MemberDraining { .. }
            | Event::MemberResumed { .. } => eprintln!("[{}] {event:?}", self.node),
            _ => {}
        }
    }
}
