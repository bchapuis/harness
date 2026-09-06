//! One node (silo) of the standalone deployment: the production runtime wired to
//! the harness. A node hosts entities and votes in Raft; it has no client-facing
//! listener. The public edge is `agent-gateway`, a trusted cluster *client*
//! that joins this transport as a non-voting member and addresses the entities
//! directly (design: `docs/multi-tenant-edge.md`).
//!
//! Every node is identical (agent spec §7.1): same kinds, same seams. A
//! session is an entity, so durability, placement, and the single-writer fence are
//! entity's: each node hosts every kind's entity type, joining its shards' Raft
//! groups and registering its gateway (§5.3). Membership is a static roster with
//! the SWIM detector observe-only (core spec §9.4.1) — reachability drives the
//! shard map's reallocation and the gateway gossip nodes route activations
//! through. A `--client` (the gateway) is admitted to that membership so the same
//! gossip reaches it, but never to the Raft roster, so it never votes or hosts.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use actor_cluster::DowningPolicy;
use actor_cluster::LeaderMode;
use actor_cluster::MembershipMode;
use actor_cluster::RaftConfig;
use actor_cluster::SwimConfig;
use actor_core::Codec;
use actor_core::Event;
use actor_core::EventSink;
use actor_core::JsonCodec;
use actor_core::NodeId;
use actor_runtime::Encryption;
use actor_runtime::FileRaftWAL;
use actor_runtime::JoinOptions;
use actor_runtime::OsEntropy;
use actor_runtime::TcpCluster;
use actor_runtime::TokioClock;
use entity::Entities;
use entity::EntityExt;
use agent::Budget;
use agent::FileEntityStore;
use agent::EntityStoreFactory;
use agent::EntityConfig;
use agent::Harness;
use agent::HarnessConfig;
use agent::AgentEvent;
use agent::Kind;
use agent::Kinds;
use agent::Model;
use agent::ModelParams;
use agent::SandboxProvider;
use agent_anthropic::AnthropicModel;

use tenancy::Directory;

use crate::http::HttpsPost;

/// Which sandbox provider the node runs. Every mode's workspace is the agent
/// entity's own facet directory (entity §7.11): tool-call deltas are captured
/// into the session journal, so the workspace survives hibernation, migration,
/// and node loss in every mode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SandboxMode {
    /// `agent-sandbox`'s container-backed `Native` tier: `shell` runs inside
    /// a per-session OCI container (the facet's workspace directory
    /// bind-mounted, no network — shared-kernel confinement, sandbox spec
    /// §3.5's development fallback). Each variant carries exactly the
    /// configuration its mode needs, so a flag for the wrong mode is a parse
    /// error rather than a silently ignored field.
    Docker {
        /// Container image; digest-covered like `--model`.
        image: String,
        /// The container CLI binary.
        cli: String,
    },
    /// `agent-sandbox`'s microVM-backed `Native` tier: `shell` runs inside a
    /// per-session Firecracker VM (the workspace synced over vsock, no network
    /// device — hardware-virtualization confinement, sandbox spec §3.4's
    /// stronger grade; Linux with `/dev/kvm` only).
    Firecracker {
        /// The firecracker executable.
        binary: String,
        /// The vmlinux kernel; digest-covered via the kind digest's profile.
        kernel: String,
        /// The base rootfs (ext4, containing `/sbin/fc-agent`); digest-covered
        /// as the profile image.
        rootfs: String,
    },
    /// The typed file tools alone over the facet's workspace directory — no
    /// guest shell or compute.
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
    /// entity, §7.2), so nodes never share a directory or a filesystem.
    pub data: PathBuf,
    /// The local interface the transport port binds. `0.0.0.0` binds every
    /// interface, which is what a container or pod needs; the `127.0.0.1`
    /// default keeps a single-host cluster on loopback.
    pub bind_host: String,
    /// Each node's reachable host, from `--peer <id>=<host>`. A node advertises
    /// its own entry to peers and dials the others at theirs. Empty leaves every
    /// node at `127.0.0.1` (single host); supplying the roster's hosts (pod DNS
    /// names, say) is what makes the cluster multi-host.
    pub peer_hosts: BTreeMap<u64, String>,
    /// Clients to admit, from `--client <id>=<host>` (the HTTP gateway). Each id
    /// is outside `1..=nodes`: the client joins the transport and membership as a
    /// non-voting, non-hosting participant — so it receives the receptionist
    /// gossip that carries the gateway refs it routes through — but is never added
    /// to the Raft roster, so it never votes or hosts an entity.
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
    /// The sandbox provider, with its mode-scoped configuration (image and
    /// CLI for docker, binary/kernel/rootfs for firecracker); every node must
    /// agree (the kind digest covers the tool declarations and profile this
    /// choice selects). `None` fails at startup (see [`NodeOptions`]).
    pub sandbox: Option<SandboxMode>,
}

impl Default for NodeOptions {
    fn default() -> Self {
        NodeOptions {
            id: 0,
            nodes: 3,
            data: PathBuf::from("./agent-data"),
            bind_host: "127.0.0.1".to_string(),
            peer_hosts: BTreeMap::new(),
            clients: BTreeMap::new(),
            port_base: 7401,
            // The deployment's choice, stated here beside the Anthropic adapter
            // it is wired to; the neutral crate deliberately defaults to no
            // real id.
            model: "claude-sonnet-4-6".to_string(),
            secret: "agent-standalone".to_string(),
            api_url: "https://api.anthropic.com".to_string(),
            sandbox: None,
        }
    }
}

/// The most a stop waits on the shard handoff. The handoff is bounded on its
/// own (entity's `hand_off_leadership`: a fixed number of rounds, each one
/// transfer round trip per led shard), so this is the backstop against a
/// transport that stops answering, sized well inside the unit's 120 s stop
/// budget (`deploy/systemd`).
const HANDOFF_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a stop waits after announcing `Leaving` before releasing the
/// transport: two SWIM rounds at the default probe interval, so the gossip
/// carries the departure to peers and the control leader commits it.
const LEAVE_GRACE: Duration = Duration::from_secs(2);

/// Boot the node and host its entities until SIGTERM or SIGINT, then stop
/// gracefully: hand off every shard this node leads (entity spec §8.3),
/// announce the departure, and release the transport.
pub async fn run(opts: NodeOptions, api_key: String) -> Result<(), String> {
    if opts.id < 1 || opts.id > opts.nodes {
        return Err(format!(
            "--id must be in 1..={}, got {}",
            opts.nodes, opts.id
        ));
    }
    // Resolved before any port is bound: a missing confinement choice is a
    // configuration error the operator fixes, not a half-booted node.
    let sandbox_mode = opts.sandbox.clone().ok_or(
        "--sandbox is required: `docker` or `firecracker` (confined shell, durable \
         workspace), or `durable` (typed file tools, no shell)",
    )?;
    let node = NodeId::new(opts.id);
    // One codec value for this node, used in all three places that need to agree:
    // the transport's framing, the message layer entity encodes an entity's records
    // and snapshots with, and the entity store's stamp naming that codec (entity
    // §7.4). The store refuses to open under a codec other than the one that wrote
    // it, so a stamp taken from a *different* value than the message layer's would
    // be worse than none — it would enforce agreement with the wrong thing.
    let codec: Arc<dyn Codec> = Arc::new(JsonCodec);
    let raft_dir = opts.data.join("raft");
    let system: TcpCluster = actor_runtime::join(JoinOptions {
        node: opts.id,
        voters: opts.nodes,
        peer_hosts: opts.peer_hosts.clone(),
        clients: opts.clients.clone(),
        bind_host: opts.bind_host.clone(),
        advertise_host: None,
        port_base: opts.port_base,
        secret: opts.secret.clone(),
        codec: Arc::clone(&codec),
        events: Arc::new(StderrEvents { node }),
        // Plaintext, guarded by the cluster secret. Fine on loopback or a
        // trusted cluster network; a deployment crossing untrusted links
        // provisions certificates and names `Encryption::MutualTls` here.
        // Stated rather than defaulted: the cluster secret crosses the wire in
        // the handshake, so anyone who can watch this traffic can join.
        encryption: Encryption::PlaintextTrusted,
        // Entities's sharded journal rides Raft, so the node must run the
        // consensus engine: leader-based membership is the only mode that
        // builds one (system.rs). Voters are the roster `join` hands in; the
        // shard-map and per-shard groups seed from these. Disk-backed storage
        // under --data keeps a restarted node's term/vote, so killing a node
        // and re-attaching stays Raft-safe.
        membership: Box::new(move |roster| {
            MembershipMode::Leader(LeaderMode {
                swim: SwimConfig::default(),
                raft: {
                    let mut raft = RaftConfig::new(roster.to_vec());
                    raft.storage = FileRaftWAL::factory(raft_dir);
                    raft
                },
                downing: DowningPolicy::Conservative,
            })
        }),
    })
    .await?;

    let http = Arc::new(HttpsPost::new(&opts.api_url)?);
    let model: Arc<dyn Model> = Arc::new(AnthropicModel::new(
        http,
        api_key,
        TokioClock::new(),
        Arc::new(OsEntropy::new()),
    ));
    // One durable entity store under --data (§7.4), shared by the session kinds and
    // the tenancy directory. Its factory caches per node, so every entity type shares
    // one on-disk store keyed by (shard, entity), the entity analogue of the Raft WAL.
    let entity_store = FileEntityStore::factory(opts.data.join("entities"), codec.as_ref());
    // This node's shared entity capabilities, set once here and used for every entity
    // type it hosts (entity §7.4, §13).
    //
    // The I/O pool: without it every fsync runs on the tokio worker that is also
    // driving this node's Raft heartbeats and its shards' quorum waits, so a slow disk
    // becomes spurious elections and a rehydration storm rather than just slow writes.
    // The metrics registry: commit latency by outcome, rehydration cost, and the
    // resident entity count — the signals that say whether this node's shards are
    // healthy. Distinct from the event stream, which is shaped for the simulator's
    // checkers, not for an operator.
    let entity_node = system
        .entity_node()
        .blocking_io(Arc::new(entity::ThreadPoolIo::sized_for_host()))
        .metrics(Arc::new(entity::AtomicEntityMetrics::new()));
    // Every mode runs over the agent entity's own workspace facet (see `SandboxMode`);
    // the provider just opens the supplied directory.
    let sandboxes: Arc<dyn SandboxProvider> = match &sandbox_mode {
        SandboxMode::Docker { cli, .. } => Arc::new(
            agent_sandbox::TieredSandboxes::new()
                .with_container_cli(cli.clone())
                // The hermetic JS surface, so `run_js` runs without any
                // language runtime in the container (sandbox spec §3.2).
                .with_quickjs(),
        ),
        SandboxMode::Firecracker { binary, kernel, .. } => Arc::new(
            agent_sandbox::TieredSandboxes::new()
                .with_firecracker(agent_sandbox::FirecrackerConfig::new(binary, kernel))
                // The hermetic JS surface, alongside the microVM shell.
                .with_quickjs(),
        ),
        // No guest shell or compute: the typed file tools alone, over the
        // facet-owned workspace directory.
        SandboxMode::Durable => Arc::new(agent_sandbox::TieredSandboxes::new()),
    };
    // The handle is bound for the node's life to keep the gateway actors alive,
    // like `directory` below; both hand off their shards at the stop.
    let node_kinds = kinds(&opts, &sandbox_mode, entity_store.clone());
    let harness = Harness::builder(system.clone(), &node_kinds)
        .entity_node(entity_node.clone())
        .config(harness_config())
        .host_all(model, sandboxes)
        .build();
    // Host the tenancy ownership-index entity type (one entity per principal) so the
    // gateway's client `Entities<Directory>` can route `Record`/`List` to it. The
    // node only hosts it; the recording on each prompt happens at the gateway edge.
    let directory: Entities<Directory<TcpCluster>> = entity_node.entities(EntityConfig {
        entity_store: Some(entity_store),
        ..EntityConfig::default()
    });

    eprintln!(
        "[{node}] joined (voter {}/{}), data {}, model {}",
        opts.id,
        opts.nodes,
        opts.data.display(),
        opts.model
    );
    actor_runtime::wait_for_cluster(&system, opts.nodes as usize, Duration::from_secs(15)).await;
    eprintln!(
        "[{node}] hosting entities; the public edge is agent-gateway (a cluster client). \
         No client-facing listener on this node."
    );
    shutdown_signal().await;
    // The graceful stop the systemd unit relies on (`deploy/systemd`): hand off
    // every led shard while this node is still a full member, so the transfers
    // are accepted, then announce the departure and release the transport in the
    // order `leave`/`shutdown` document. Skipping the handoff is safe but turns a
    // rolling restart into one failover per led shard.
    eprintln!("[{node}] stop signal received; handing off shard leadership");
    let outstanding = tokio::time::timeout(HANDOFF_TIMEOUT, async {
        harness.hand_off_leadership().await + directory.hand_off_leadership().await
    })
    .await;
    match outstanding {
        Ok(0) => eprintln!("[{node}] handed off every led shard; leaving the cluster"),
        Ok(n) => eprintln!(
            "[{node}] {n} shard(s) kept their leader (no caught-up replica); leaving the cluster"
        ),
        Err(_) => eprintln!(
            "[{node}] handoff still running after {HANDOFF_TIMEOUT:?}; leaving the cluster"
        ),
    }
    system.leave();
    tokio::time::sleep(LEAVE_GRACE).await;
    system.shutdown();
    Ok(())
}

/// Resolve on SIGTERM or SIGINT (the unit's stop signal, and ctrl-c at a
/// terminal). Without a SIGTERM handler the default disposition kills the
/// process outright, and nothing above gets to run.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::SignalKind;
        use tokio::signal::unix::signal;
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                term.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

/// The cluster-wide kind map (agent spec §7.1). One pure function of the
/// node options that shape kinds (`--model`, `--sandbox`, `--sandbox-image`):
/// every node must register byte-identical kinds — the digest is pinned by
/// `SessionCreated` — so all nodes must run with the same values for those
/// flags.
fn kinds(opts: &NodeOptions, sandbox_mode: &SandboxMode, entity_store: EntityStoreFactory) -> Kinds {
    let params = ModelParams {
        model: opts.model.to_string(),
        max_tokens: 4096,
    };
    // The sandbox tools per mode: the same `shell` name and shape, but distinct
    // declarations (and a profile image in docker/firecracker mode), so the digests
    // differ — a mixed-mode cluster fails to agree instead of silently splitting
    // confinement. Every mode offers the typed `Workspace` file tools over the
    // durable entity-backed workspace (§7.10); the shell-capable modes add `shell`
    // (the confined Native tier) and `run_js` (the hermetic QuickJS Compute tier,
    // sandbox spec §3.2) on top, over the same workspace.
    let with_file_tools = |kind: Kind| -> Kind {
        agent_sandbox::workspace_tools()
            .into_iter()
            .fold(kind, |kind, tool| kind.tool(tool))
    };
    let tools = |kind: Kind| -> Kind {
        match sandbox_mode {
            SandboxMode::Docker { image, .. } => with_file_tools(
                kind.tool(agent_sandbox::shell_tool())
                    .tool(agent_sandbox::run_js_tool())
                    .sandbox(agent::SandboxProfile::image(image)),
            ),
            // The microVM shell declaration differs from the docker one (sync
            // semantics are model-visible), and the profile image carries the
            // rootfs path; both are digest-covered.
            SandboxMode::Firecracker { rootfs, .. } => with_file_tools(
                kind.tool(agent_sandbox::fc_shell_tool())
                    .tool(agent_sandbox::run_js_tool())
                    .sandbox(agent::SandboxProfile::image(rootfs)),
            ),
            // No guest shell or compute: the typed file tools alone, over the durable
            // filesystem entity (entity §7.10).
            SandboxMode::Durable => with_file_tools(kind),
        }
    };
    // Durable entity storage under --data (§7.4): a session is an entity, so its
    // journal must outlive a process restart for a cold-restarted cluster to recover
    // the conversation. Without this, each node's entity store is in-memory and a full
    // restart loses every session (the records do NOT ride the Raft log — that log
    // only carries leader election and the shard map, §7.1). `data_dir` places the
    // workspace facet's per-session directory materializations (entity §7.11) under
    // --data as well.
    let data_dir = opts.data.join("workspaces");
    let entity = move |kind: Kind| -> Kind {
        kind.entity(EntityConfig {
            entity_store: Some(entity_store.clone()),
            data_dir: Some(data_dir.clone()),
            ..EntityConfig::default()
        })
    };
    // Mode-aware tool guidance: `durable` has no shell and no Compute tier, so its
    // guidance must not point at either.
    let tool_guidance = if matches!(sandbox_mode, SandboxMode::Durable) {
        "Use the typed file tools (`read_file`, `write_file`, `edit_file`, `list_dir`, `remove`) to work \
         with files in your session's private workspace, which persists across your turns. \
         There is no shell in this environment."
    } else {
        "Use the `shell` tool for anything you need to inspect, compute, or build; it runs in \
         your session's private workspace directory, which persists across your turns. To \
         create, read, or edit a file you can also use the typed file tools (`read_file`, \
         `write_file`, `edit_file`, `list_dir`, `remove`) directly, without spinning up a \
         container. To run \
         JavaScript, use the `run_js` tool (hermetic QuickJS, no runtime needed) rather than a \
         `node` binary through `shell`."
    };
    let assistant = tools(
        Kind::new(format!(
            "You are the assistant agent of a small local cluster. {tool_guidance} You may \
             delegate a self-contained subtask to the `worker` kind with the `delegate` tool."
        ))
        .model(params.clone()),
    )
    .delegates_to(&["worker"])
    .budget(Budget::new(200_000, 50));
    let assistant = entity(assistant);
    let worker = tools(
        Kind::new(format!(
            "You are a worker agent. Complete the task you were delegated, then reply with a \
             concise result. {tool_guidance}"
        ))
        .model(params),
    )
    .budget(Budget::new(100_000, 25));
    let worker = entity(worker);
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

/// The observability stream on stderr (agent spec §10.4): membership and
/// reachability transitions, and every harness event. Dispatch-level core events
/// are swallowed as noise.
struct StderrEvents {
    node: NodeId,
}

impl EventSink for StderrEvents {
    fn emit(&self, event: Event) {
        if let Some(harness_event) = event.as_app::<AgentEvent>() {
            eprintln!("[{}] {harness_event:?}", self.node);
        } else if event.is_membership() {
            eprintln!("[{}] {event:?}", self.node);
        }
    }
}
