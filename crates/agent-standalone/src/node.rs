//! One node (silo) of the standalone deployment: the production runtime wired to
//! the agent runtime. A node hosts entities and votes in Raft; it has no client-facing
//! listener. The public edge is `agent-gateway`, a trusted cluster *client*
//! that joins this transport as a non-voting member and addresses the entities
//! directly (design: `docs/multi-tenant-edge.md`).
//!
//! Every node is identical (agent spec §7.1): same kinds, same seams. A
//! session is an entity, so durability, placement, and the single-writer fence are
//! entity's: each node hosts every kind's entity type, joining its shards' Raft
//! groups and registering its gateway (§5.3). Membership is a static roster with
//! the SWIM detector observe-only (core spec §9.4.1): reachability drives the
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
use entity::cluster::EntityNode;
use agent::Budget;
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
use agent::ToolDecl;
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
    /// bind-mounted, no network; shared-kernel confinement, sandbox spec
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
    /// device; hardware-virtualization confinement, sandbox spec §3.4's
    /// stronger grade; Linux with `/dev/kvm` only).
    Firecracker {
        /// The firecracker executable.
        binary: String,
        /// The vmlinux kernel: a node-local VMM asset, not digest-covered;
        /// only the rootfs travels in the profile as `image`.
        kernel: String,
        /// The base rootfs (ext4, containing `/sbin/fc-agent`); digest-covered
        /// as the profile image.
        rootfs: String,
    },
    /// The typed file tools alone over the facet's workspace directory, no
    /// guest shell or compute.
    Durable,
}

/// Everything `node` takes from the command line. The defaults make
/// `node --id N --sandbox <mode>` enough for the three-node local
/// deployment: the sandbox is the one choice without a default, so that a
/// deployment names its confinement grade explicitly and every node agrees
/// on the digest.
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
    /// non-voting, non-hosting participant (so it receives the receptionist
    /// gossip that carries the gateway refs it routes through), but is never added
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

/// Two SWIM rounds at the default probe interval, the departure's gossip
/// window; see [`actor_runtime::depart`].
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
    // be worse than none: it would enforce agreement with the wrong thing.
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
    // One durable node under --data (entity §7.4, §13): the store the session kinds
    // and the tenancy directory share, the I/O pool, and the metrics registry.
    let (entity_node, entity_store) =
        EntityNode::durable(system.clone(), &opts.data);
    // Every mode runs over the agent entity's own workspace facet (see `SandboxMode`);
    // the provider just opens the supplied directory. The tool declarations the
    // kinds carry are the provider's answer, so a kind cannot declare an
    // environment this node does not have.
    // One match: the mode decides the provider and the profile image its kinds
    // carry, and every other question about the environment is the provider's to
    // answer (its tool declarations, and whether it serves a shell).
    let (sandboxes, profile) = match &sandbox_mode {
        SandboxMode::Docker { cli, image } => (
            agent_sandbox::TieredSandboxes::new()
                .with_container_cli(cli.clone())
                // The hermetic JS surface, so `run_js` runs without any
                // language runtime in the container (sandbox spec §3.2).
                .with_quickjs(),
            Some(agent::SandboxProfile::image(image)),
        ),
        SandboxMode::Firecracker {
            binary,
            kernel,
            rootfs,
        } => (
            agent_sandbox::TieredSandboxes::new()
                .with_firecracker(agent_sandbox::FirecrackerConfig::new(binary, kernel))
                // The hermetic JS surface, alongside the microVM shell.
                .with_quickjs(),
            // The profile image carries the rootfs path, digest-covered like the
            // container image it stands in for.
            Some(agent::SandboxProfile::image(rootfs)),
        ),
        // No guest shell or compute: the typed file tools alone, over the
        // facet-owned workspace directory.
        SandboxMode::Durable => (agent_sandbox::TieredSandboxes::new(), None),
    };
    let tools = sandboxes.tools();
    let has_shell = sandboxes.has_shell();
    let sandboxes: Arc<dyn SandboxProvider> = Arc::new(sandboxes);
    // The handle is bound for the node's life to keep the gateway actors alive,
    // like `directory` below; both hand off their shards at the stop.
    let node_kinds = kinds(&opts, &tools, profile, has_shell, entity_store.clone());
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
    actor_runtime::shutdown_signal().await;
    actor_runtime::depart(
        &system,
        async { harness.hand_off_leadership().await + directory.hand_off_leadership().await },
        actor_runtime::HANDOFF_TIMEOUT,
        LEAVE_GRACE,
    )
    .await;
    Ok(())
}

/// The cluster-wide kind map (agent spec §7.1). One pure function of the
/// node options that shape kinds (`--model`, `--sandbox`, `--sandbox-image`)
/// and the tools the configured provider answers: every node must register
/// byte-identical kinds (the digest is pinned by `SessionCreated`), so all
/// nodes must run with the same values for those flags.
fn kinds(
    opts: &NodeOptions,
    tools: &[ToolDecl],
    profile: Option<agent::SandboxProfile>,
    has_shell: bool,
    entity_store: EntityStoreFactory,
) -> Kinds {
    let params = ModelParams {
        model: opts.model.to_string(),
        max_tokens: 4096,
    };
    // The provider's declarations, plus the profile image the shell-capable
    // modes run under: the one per-mode fact the digest must cover beyond the
    // declarations themselves, so a mixed-mode cluster fails to agree instead
    // of silently splitting confinement.
    let equip = |kind: Kind| -> Kind {
        let kind = tools.iter().cloned().fold(kind, Kind::tool);
        match &profile {
            Some(profile) => kind.sandbox(profile.clone()),
            None => kind,
        }
    };
    // Durable entity storage under --data (§7.4): a session is an entity, so its
    // journal must outlive a process restart for a cold-restarted cluster to recover
    // the conversation. Without this, each node's entity store is in-memory and a full
    // restart loses every session (the records do NOT ride the Raft log: that log
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
    // Tool guidance follows the declarations: without a `shell` there is no
    // Compute tier either, so the guidance must point at neither.
    let tool_guidance = if !has_shell {
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
    let assistant = equip(
        Kind::new(format!(
            "You are the assistant agent of a small local cluster. {tool_guidance} You may \
             delegate a self-contained subtask to the `worker` kind with the `delegate` tool."
        ))
        .model(params.clone()),
    )
    .delegates_to(&["worker"])
    .budget(Budget::new(200_000, 50));
    let assistant = entity(assistant);
    let worker = equip(
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
/// reachability transitions, and every agent event. Dispatch-level core events
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
