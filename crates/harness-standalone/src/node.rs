//! One node of the standalone deployment: the production runtime wired to
//! the harness, plus the control listener a REPL attaches to.
//!
//! Every node is identical (harness spec §7.1): same kinds, same seams. A
//! session is a grain, so durability, placement, and the single-writer fence are
//! granary's: each node hosts every kind's grain type, joining its shards' Raft
//! groups and registering its gateway (§5.3). Membership is a static roster with
//! the SWIM detector observe-only (core spec §9.4.1) — reachability drives the
//! shard map's reallocation and the gateway gossip nodes route activations
//! through.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::SocketAddr;
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
use harness::Budget;
use harness::Harness;
use harness::HarnessConfig;
use harness::HarnessEvent;
use harness::Kind;
use harness::Kinds;
use harness::Model;
use harness::ModelParams;
use harness::SandboxProvider;
use harness::Seq;
use harness::SessionId;
use harness::Tier;
use harness::Turn;
use harness::TurnId;
use harness_anthropic::AnthropicModel;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use crate::http::HttpsPost;
use crate::proto::Op;
use crate::proto::Reply;
use crate::proto::Request;
use crate::proto::Response;
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
    /// The shared data directory (journal + workspaces).
    pub data: PathBuf,
    /// Node `i` binds its transport on `127.0.0.1:(port_base + i - 1)`.
    pub port_base: u16,
    /// Node `i` binds its control port on `127.0.0.1:(control_base + i - 1)`.
    pub control_base: u16,
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
            port_base: 7401,
            control_base: 7501,
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

/// Boot the node and serve its control port forever.
pub async fn run(opts: NodeOptions, api_key: String) -> Result<(), String> {
    if opts.id < 1 || opts.id > opts.nodes {
        return Err(format!(
            "--id must be in 1..={}, got {}",
            opts.nodes, opts.id
        ));
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
    let peers: BTreeMap<NodeId, SocketAddr> = roster
        .iter()
        .map(|peer| (*peer, loopback(opts.port_base, peer.uid())))
        .collect();
    let advertised = peers[&node];

    let listener = TcpListener::bind(advertised)
        .await
        .map_err(|e| format!("bind transport {advertised}: {e}"))?;
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
            allowlist: Some(roster.iter().copied().collect::<BTreeSet<_>>()),
            // Plaintext is acceptable on loopback; a multi-host deployment
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
    for peer in &roster {
        if *peer != node {
            system.add_member(*peer);
        }
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
    };
    let harness = Harness::with_config(
        system.clone(),
        kinds(&opts, sandbox_mode),
        model,
        sandboxes,
        harness_config(),
    );

    eprintln!(
        "[{node}] transport {advertised}, data {}, model {}",
        opts.data.display(),
        opts.model
    );
    wait_for_hosts(&system, opts.nodes as usize).await;

    let control = loopback(opts.control_base, opts.id);
    let listener = TcpListener::bind(control)
        .await
        .map_err(|e| format!("bind control {control}: {e}"))?;
    eprintln!(
        "[{node}] control listening on {control} — attach with: harness-standalone repl {control}"
    );
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(|e| format!("accept on {control}: {e}"))?;
        tokio::spawn(serve_connection(harness.clone(), stream));
    }
}

fn loopback(base: u16, id: u64) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], base + (id - 1) as u16))
}

/// The cluster-wide kind map (harness spec §7.1). One pure function of the
/// node options that shape kinds (`--model`, `--sandbox`, `--sandbox-image`):
/// every node must register byte-identical kinds — the digest is pinned by
/// `SessionCreated` — so all nodes must run with the same values for those
/// flags.
fn kinds(opts: &NodeOptions, sandbox_mode: SandboxMode) -> Kinds {
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
        }
    };
    // The Compute tier exists only behind TieredSandboxes; steer toward it
    // for JavaScript exactly where it is offered, and nowhere it is not.
    let js_hint = if sandbox_mode == SandboxMode::Local {
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
    let worker = tools(
        Kind::new(format!(
            "You are a worker agent. Complete the task you were delegated using the `shell` \
             tool in your private workspace, then reply with a concise result.{js_hint}"
        ))
        .model(params),
    )
    .budget(Budget::new(100_000, 25));
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

/// Hold the control port closed until the cluster has converged enough to serve:
/// every peer is in the membership view and the control group has elected a
/// leader (so granary's shard groups can elect too). granary's bounded redirect
/// absorbs a prompt issued before the shard map converges (invariant G13), so
/// this is a UX nicety, not a correctness requirement: it makes the first prompt
/// of a fresh cluster prompt rather than bouncing off a still-electing shard.
///
/// `expected` is the cluster size; `members()` reports peers only (never this
/// node, membership.rs), so the peer quorum is `expected - 1`.
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

/// One control connection: requests handled concurrently (a parked prompt
/// must not block a tail or cancel), responses serialized by a writer task.
async fn serve_connection(harness: Harness<TcpCluster>, stream: tokio::net::TcpStream) {
    let (read, mut write) = stream.into_split();
    let (tx, mut rx) = mpsc::channel::<Response>(64);
    let writer = tokio::spawn(async move {
        while let Some(response) = rx.recv().await {
            let Ok(mut line) = serde_json::to_vec(&response) else {
                continue;
            };
            line.push(b'\n');
            if write.write_all(&line).await.is_err() {
                break;
            }
        }
    });
    let mut lines = BufReader::new(read).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        let request: Request = match serde_json::from_str(&line) {
            Ok(request) => request,
            Err(e) => {
                let _ = tx
                    .send(Response {
                        id: 0,
                        body: Reply::Error {
                            message: format!("bad request: {e}"),
                        },
                    })
                    .await;
                continue;
            }
        };
        let harness = harness.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let body = handle(harness, request.op).await;
            let _ = tx
                .send(Response {
                    id: request.id,
                    body,
                })
                .await;
        });
    }
    drop(tx);
    let _ = writer.await;
}

async fn handle(harness: Harness<TcpCluster>, op: Op) -> Reply {
    match op {
        Op::Prompt {
            kind,
            session,
            turn,
            content,
            within_secs,
        } => {
            let session = harness.session(&kind, SessionId::new(session));
            let turn = Turn::new(TurnId::new(turn), content);
            match session
                .prompt_within(turn, Duration::from_secs(within_secs))
                .await
            {
                Ok(outcome) => Reply::Outcome { outcome },
                Err(e) => Reply::Error {
                    message: format!("{e:?}"),
                },
            }
        }
        Op::Tail {
            kind,
            session,
            from,
            limit,
        } => {
            let session = harness.session(&kind, SessionId::new(session));
            match session.tail(Seq::new(from), limit).await {
                Ok(records) => Reply::Records { records },
                Err(e) => Reply::Error {
                    message: format!("{e:?}"),
                },
            }
        }
        Op::Cancel {
            kind,
            session,
            turn,
        } => {
            let session = harness.session(&kind, SessionId::new(session));
            match session.cancel(&TurnId::new(turn)).await {
                Ok(()) => Reply::Cancelled,
                Err(e) => Reply::Error {
                    message: format!("{e:?}"),
                },
            }
        }
    }
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
