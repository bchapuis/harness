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
use harness::Budget;
use harness::FileGrainStore;
use harness::GranaryConfig;
use harness::Harness;
use harness::HarnessConfig;
use harness::HarnessEvent;
use harness::Kind;
use harness::Kinds;
use harness::Model;
use harness::ModelParams;
use harness::RecordBody;
use harness::SandboxProvider;
use harness::Seq;
use harness::SessionId;
use harness::SessionRef;
use harness::Tier;
use harness::Turn;
use harness::TurnId;
use harness_anthropic::AnthropicModel;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
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
    /// This node's own data directory (journal + workspaces). Each node keeps
    /// its own: the journal replicates over the transport (a quorum append per
    /// grain, §7.2), so nodes never share a directory or a filesystem.
    pub data: PathBuf,
    /// The local interface the transport and control ports bind. `0.0.0.0`
    /// binds every interface, which is what a container or pod needs; the
    /// `127.0.0.1` default keeps a single-host cluster on loopback.
    pub bind_host: String,
    /// Each node's reachable host, from `--peer <id>=<host>`. A node advertises
    /// its own entry to peers and dials the others at theirs. Empty leaves every
    /// node at `127.0.0.1` (single host); supplying the roster's hosts — pod DNS
    /// names, say — is the whole of what makes the cluster multi-host.
    pub peer_hosts: BTreeMap<u64, String>,
    /// Node `i`'s transport port is `port_base + i - 1`.
    pub port_base: u16,
    /// Node `i`'s control port is `control_base + i - 1`.
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
            bind_host: "127.0.0.1".to_string(),
            peer_hosts: BTreeMap::new(),
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
    // Each node's reachable host: its `--peer` entry, or loopback if unset. With
    // no `--peer` flags every host is 127.0.0.1 and the cluster is single-host,
    // exactly as before.
    let host_of = |id: u64| -> &str {
        opts.peer_hosts.get(&id).map(String::as_str).unwrap_or("127.0.0.1")
    };
    let peers: BTreeMap<NodeId, SocketAddr> = roster
        .iter()
        .map(|peer| Ok((*peer, resolve(host_of(peer.uid()), opts.port_base, peer.uid())?)))
        .collect::<Result<_, String>>()?;
    // Advertise the routable host (what peers dial back), but bind the local
    // interface — they differ when bound to the 0.0.0.0 wildcard in a container.
    let advertised = peers[&node];
    let bind = resolve(&opts.bind_host, opts.port_base, opts.id)?;
    let listener = TcpListener::bind(bind)
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
            allowlist: Some(roster.iter().copied().collect::<BTreeSet<_>>()),
            // Plaintext, guarded by the cluster secret. Fine on loopback or a
            // trusted cluster network; a deployment crossing untrusted links
            // would provision certificates and set `TlsConfig` here.
            tls: None,
        },
        listener,
    );
    // The harness-event fan-out behind `Op::Watch`: every `HarnessEvent` the
    // sink sees is republished here, and each watch subscribes to learn when a
    // run committed new records (the event carries no content, so the watch
    // still tails — but only when woken, never on a blind interval). A bounded
    // buffer; a lagged watch catches up by tailing, so drops cost only latency.
    let (runs_tx, _) = broadcast::channel::<HarnessEvent>(1024);

    let system: TcpCluster = ClusterSystem::start(
        node,
        TokioClock::new(),
        OsEntropy::new(),
        TokioSpawner::current(),
        transport,
        inbound,
        ClusterConfig {
            events: Arc::new(NodeEvents {
                node,
                runs: runs_tx.clone(),
            }),
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

    let control_bind = resolve(&opts.bind_host, opts.control_base, opts.id)?;
    let control_addr = resolve(host_of(opts.id), opts.control_base, opts.id)?;
    let listener = TcpListener::bind(control_bind)
        .await
        .map_err(|e| format!("bind control {control_bind}: {e}"))?;
    eprintln!(
        "[{node}] control listening on {control_bind} — attach with: harness-standalone repl {control_addr}"
    );
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(|e| format!("accept on {control_bind}: {e}"))?;
        tokio::spawn(serve_connection(harness.clone(), stream, runs_tx.clone()));
    }
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
    // Durable grain storage under --data (§7.4): a session is a grain, so its
    // journal must outlive a process restart for a cold-restarted cluster to recover
    // the conversation. Without this, each node's grain store is in-memory and a full
    // restart loses every session (the records do NOT ride the Raft log — that log
    // only carries leader election and the shard map, §7.1). One factory, shared by
    // both kinds: it caches per node, so a node's two grain types share one on-disk
    // store keyed by (shard, grain) — the grain analogue of the Raft WAL one line up.
    let grain_store = FileGrainStore::factory(opts.data.join("grains"));
    let grain = |kind: Kind| -> Kind {
        kind.grain(GranaryConfig {
            grain_store: Some(grain_store.clone()),
            ..GranaryConfig::default()
        })
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
async fn serve_connection(
    harness: Harness<TcpCluster>,
    stream: tokio::net::TcpStream,
    runs: broadcast::Sender<HarnessEvent>,
) {
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
        // `Watch` streams many replies on one id over the run's life; the other
        // ops resolve to a single reply. Subscribe before spawning so no event
        // committed after this point is missed (the watch then tails for it).
        if let Op::Watch {
            kind,
            session,
            turn,
            from,
        } = request.op
        {
            let events = runs.subscribe();
            tokio::spawn(watch(harness, tx, request.id, kind, session, turn, from, events));
            continue;
        }
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
        // Streamed over many replies; intercepted in `serve_connection` before
        // ever reaching the single-reply path.
        Op::Watch { .. } => unreachable!("Op::Watch is dispatched by serve_connection"),
    }
}

/// Journal page size for a watch's catch-up tails.
const WATCH_PAGE: u32 = 500;

/// One subscription (`Op::Watch`): stream a run's records as they commit. The
/// harness-event stream is the wake signal — events carry no content (§10.4),
/// so each wake re-tails — and the run's `RunEnded` record (not the event) is
/// the authoritative stop, so a terminal event missed before subscription is
/// still caught by the initial drain.
#[allow(clippy::too_many_arguments)]
async fn watch(
    harness: Harness<TcpCluster>,
    tx: mpsc::Sender<Response>,
    id: u64,
    kind: String,
    session: String,
    turn: String,
    from: u64,
    mut events: broadcast::Receiver<HarnessEvent>,
) {
    let session_ref = harness.session(&kind, SessionId::new(session.clone()));
    let mut from = Seq::new(from);
    loop {
        match drain(&session_ref, &mut from, &tx, id, &turn).await {
            WatchState::Ended => {
                let _ = tx.send(Response {
                    id,
                    body: Reply::WatchEnded,
                })
                .await;
                return;
            }
            WatchState::Closed => return,
            WatchState::Live => {}
        }
        // Wait for the next wake: any event for our session, or a lag (drop)
        // that we recover from by tailing anyway. A closed channel means the
        // node is shutting down.
        loop {
            match events.recv().await {
                Ok(event) if event_targets(&event, &session) => break,
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Lagged(_)) => break,
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    }
}

/// Where a `drain` left the watch.
enum WatchState {
    /// Streamed (or found nothing); the run continues.
    Live,
    /// The run's terminal record was streamed; the watch is done.
    Ended,
    /// The client hung up mid-stream.
    Closed,
}

/// Tail and stream every record after `*from`, advancing it, until the journal
/// is exhausted or the watched turn's `RunEnded` is reached.
async fn drain(
    session_ref: &SessionRef<TcpCluster>,
    from: &mut Seq,
    tx: &mpsc::Sender<Response>,
    id: u64,
    turn: &str,
) -> WatchState {
    loop {
        let records = match session_ref.tail(*from, WATCH_PAGE).await {
            Ok(records) => records,
            // A transient grain error (redirect, no leader): report it and keep
            // the watch alive — the next wake re-tails. Mirrors `Tail`.
            Err(e) => {
                let _ = tx.send(Response {
                    id,
                    body: Reply::Error {
                        message: format!("{e:?}"),
                    },
                })
                .await;
                return WatchState::Live;
            }
        };
        if records.is_empty() {
            return WatchState::Live;
        }
        let len = records.len();
        let ended = records.iter().any(|(_, r)| {
            matches!(&r.body, RecordBody::RunEnded { turn: t, .. } if t.as_str() == turn)
        });
        if let Some((seq, _)) = records.last() {
            *from = *seq;
        }
        if tx
            .send(Response {
                id,
                body: Reply::Records { records },
            })
            .await
            .is_err()
        {
            return WatchState::Closed;
        }
        if ended {
            return WatchState::Ended;
        }
        if len < WATCH_PAGE as usize {
            return WatchState::Live;
        }
    }
}

/// Whether a harness event signals possible new records for `session` (a model
/// call committed, a turn started, or a run ended). The sandbox bind/release
/// pair carries no transcript record, so it never wakes a watch.
fn event_targets(event: &HarnessEvent, session: &str) -> bool {
    match event {
        HarnessEvent::RunStarted { session: s, .. }
        | HarnessEvent::ModelCompleted { session: s, .. }
        | HarnessEvent::ToolCompleted { session: s, .. }
        | HarnessEvent::RunEnded { session: s, .. } => s.as_str() == session,
        HarnessEvent::SandboxBound { .. } | HarnessEvent::SandboxReleased { .. } => false,
    }
}

/// The observability stream on stderr (harness spec §10.4): membership and
/// reachability transitions — the narration of the kill-a-node demo — and
/// every harness event. Dispatch-level core events are swallowed as noise.
///
/// It also republishes every `HarnessEvent` onto `runs`, the fan-out behind
/// `Op::Watch`: a watch subscribes there to learn when a run committed records,
/// then tails for the content (the event carries none, §10.4).
struct NodeEvents {
    node: NodeId,
    runs: broadcast::Sender<HarnessEvent>,
}

impl EventSink for NodeEvents {
    fn emit(&self, event: Event) {
        if let Some(harness_event) = event.as_app::<HarnessEvent>() {
            eprintln!("[{}] {harness_event:?}", self.node);
            // Wake any watches; an error just means none are subscribed.
            let _ = self.runs.send(harness_event.clone());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watch_wakes_only_for_its_own_sessions_content_events() {
        let mine = SessionId::new("demo");
        let other = SessionId::new("else");
        let turn = TurnId::new("t-1");
        // A content-bearing event for our session wakes the watch.
        assert!(event_targets(
            &HarnessEvent::ModelCompleted {
                session: mine.clone(),
                turn: turn.clone(),
                node: NodeId::new(1),
                usage: 10,
            },
            "demo"
        ));
        assert!(event_targets(
            &HarnessEvent::RunEnded {
                session: mine.clone(),
                turn: turn.clone(),
                outcome: "ok",
            },
            "demo"
        ));
        // A tool outcome committing is a wake too — the whole point of (1).
        assert!(event_targets(
            &HarnessEvent::ToolCompleted {
                session: mine.clone(),
                turn,
                node: NodeId::new(1),
            },
            "demo"
        ));
        // Another session's event, or a sandbox bind/release (no record), does not.
        assert!(!event_targets(
            &HarnessEvent::ModelCompleted {
                session: other,
                turn: TurnId::new("t-1"),
                node: NodeId::new(1),
                usage: 10,
            },
            "demo"
        ));
        assert!(!event_targets(
            &HarnessEvent::SandboxBound {
                session: mine,
                node: NodeId::new(1),
            },
            "demo"
        ));
    }
}
