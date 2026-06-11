//! One node of the standalone deployment: the production runtime wired to
//! the harness, plus the control listener a REPL attaches to.
//!
//! Every node is identical (harness spec §7.1): same kinds, same seams, one
//! shared journal directory. Membership is a static roster with the SWIM
//! detector observe-only (core spec §9.4.1) — reachability events route
//! placement around a killed node and carry the receptionist gossip that
//! lets nodes discover each other's hosts.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use actor_cluster::ClusterConfig;
use actor_cluster::ClusterSystem;
use actor_cluster::MembershipMode;
use actor_cluster::SwimConfig;
use actor_core::ActorSystem;
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
use harness::Budget;
use harness::Harness;
use harness::HarnessConfig;
use harness::HarnessEvent;
use harness::Kind;
use harness::Kinds;
use harness::Model;
use harness::ModelParams;
use harness::SandboxProvider;
use harness::SeqNo;
use harness::SessionId;
use harness::Turn;
use harness::TurnId;
use harness::host_key;
use harness_anthropic::AnthropicModel;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use crate::http::HttpsPost;
use crate::journal::FileJournal;
use crate::proto::Op;
use crate::proto::Reply;
use crate::proto::Request;
use crate::proto::Response;
use crate::sandbox::LocalSandboxes;

/// Everything `node` takes from the command line. The defaults make
/// `node --id N` enough for the three-node local deployment.
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
            // The detector must run (observe-only): it carries reachability
            // for placement and the gossip that spreads host registrations.
            membership: MembershipMode::Static {
                detector: Some(SwimConfig::default()),
            },
            ..ClusterConfig::default()
        },
    );
    for peer in &roster {
        if *peer != node {
            system.add_member(*peer);
        }
    }

    let journal = Arc::new(FileJournal::new(opts.data.join("journal"), node));
    let http = Arc::new(HttpsPost::new(&opts.api_url)?);
    let model: Arc<dyn Model> = Arc::new(AnthropicModel::new(
        http,
        api_key,
        TokioClock::new(),
        Arc::new(OsEntropy::new()),
    ));
    let sandboxes: Arc<dyn SandboxProvider> =
        Arc::new(LocalSandboxes::new(opts.data.join("workspaces")));
    let harness = Harness::with_config(
        system.clone(),
        kinds(&opts.model),
        journal,
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
/// model id: every node must register byte-identical kinds — the digest is
/// pinned by `SessionCreated` — so all nodes must run with the same
/// `--model`.
fn kinds(model: &str) -> Kinds {
    let params = ModelParams {
        model: model.to_string(),
        max_tokens: 4096,
    };
    let shell_description = "Run a POSIX shell command (`/bin/sh -c`) in the session's private \
                             workspace directory. Returns exit_code, stdout, and stderr.";
    let shell_schema = serde_json::json!({
        "type": "object",
        "properties": {
            "command": {
                "type": "string",
                "description": "The shell command to run."
            }
        },
        "required": ["command"]
    });
    let assistant = Kind::new(
        "You are the assistant agent of a small local cluster. Use the `shell` tool for \
         anything you need to inspect, compute, or build; it runs in your session's private \
         workspace directory, which persists across your turns. You may delegate a \
         self-contained subtask to the `worker` kind with the `delegate` tool.",
    )
    .model(params.clone())
    .sandboxed("shell", shell_description, &shell_schema)
    .delegates_to(&["worker"])
    .budget(Budget::new(200_000, 50));
    let worker = Kind::new(
        "You are a worker agent. Complete the task you were delegated using the `shell` tool \
         in your private workspace, then reply with a concise result.",
    )
    .model(params)
    .sandboxed("shell", shell_description, &shell_schema)
    .budget(Budget::new(100_000, 25));
    Kinds::new()
        .register("assistant", assistant)
        .register("worker", worker)
}

/// Harness tuning for interactive local use: deadlines sized for real model
/// calls and multi-step tool runs rather than the library defaults.
fn harness_config() -> HarnessConfig {
    HarnessConfig {
        idle_timeout: Duration::from_secs(300),
        submit_deadline: Duration::from_secs(600),
        tool_timeout: Duration::from_secs(120),
        ..HarnessConfig::default()
    }
}

/// Hold the control port closed until every peer's host shows up in the
/// receptionist listing (or a bounded wait elapses): a `Submit` routed
/// before discovery would fail fast with `DeadLetter` anyway, but waiting
/// makes the first prompt of a fresh cluster reliable.
async fn wait_for_hosts(system: &TcpCluster, expected: usize) {
    for _ in 0..150 {
        let listed = system.receptionist().lookup(host_key::<TcpCluster>()).len();
        if listed >= expected {
            eprintln!("[{}] all {expected} hosts discovered", system.node());
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    eprintln!(
        "[{}] warning: not all hosts discovered after 15s; serving anyway",
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
            match session.tail(SeqNo(from), limit).await {
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
