//! `agent-gateway`: the public HTTP/SSE edge, a cluster client of a harness
//! cluster.
//!
//! It joins the actor transport as a non-voting, non-hosting member (its
//! `--node-id` is outside the nodes' `1..=--nodes` roster, and each node admits
//! it with `--client`), terminates tenant auth (bearer token → principal), and
//! drives each caller's session by addressing its entity directly. Run one or many
//! replicas behind a load balancer, each with its own `--node-id`. Public TLS is
//! expected to terminate at an ingress/LB; the transport link is plaintext,
//! guarded by the cluster `--secret`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use actor_cluster::MembershipMode;
use actor_core::Event;
use actor_core::EventSink;
use actor_core::JsonCodec;
use actor_core::NodeId;
use actor_runtime::Encryption;
use actor_runtime::JoinOptions;
use actor_runtime::TcpCluster;
use agent::HarnessSystem;
use agent_gateway::Gateway;
use agent_gateway::Limits;
use agent_gateway::auth::InsecureTokens;
use agent_gateway::auth::StaticTokens;
use agent_gateway::auth::TokenVerifier;
use agent_gateway::auth::bind_is_loopback;

const USAGE: &str = "\
usage:
  agent-gateway [options]

options (defaults in parentheses):
  --bind <host:port>    public HTTP bind                    (127.0.0.1:8080)
  --secret <s>          cluster secret; must match the nodes' --secret
                                                            (agent-standalone)
  --node-id <n>         this gateway's node id, OUTSIDE 1..=--nodes; each node
                        must admit it with --client <n>=<host>   (100)
  --nodes <n>           the voter roster size (nodes are 1..=n)   (3)
  --peer <id>=<host>    a node's reachable host; repeat for the roster. Omit for
                        a single-host loopback cluster.     (all 127.0.0.1)
  --bind-host <addr>    interface the transport binds; 0.0.0.0 in a container
                                                            (127.0.0.1)
  --advertise-host <a>  host the nodes dial the gateway back at (default
                        --bind-host); must match the nodes' --client <host>
  --port-base <p>       node/client i's transport port = p+i-1     (7401)
  --kind <name>         a kind the nodes host; repeat per kind. MUST match the
                        nodes' deployment: an unknown name fails discovery.
                        (assistant worker, matching agent-standalone's default
                        kinds)
  --auth-tokens <path>  tenants file (`<principal> <token>` per line). Without it
                        the gateway runs INSECURE: the bearer token IS the tenant.
                        Reloaded on SIGHUP (rotate tokens without a restart).
  --max-within-secs <n> ceiling on a prompt's within_secs                 (3600)
  --max-body-bytes <n>  max request body size in bytes                 (1048576)

Each request carries `Authorization: Bearer <tenant-token>`. The public side is
plaintext; terminate TLS at an ingress/LB. SIGTERM/SIGINT drains gracefully.";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if matches!(
        args.first().map(String::as_str),
        Some("--help") | Some("-h")
    ) {
        println!("{USAGE}");
        return;
    }
    if let Err(message) = run(&args).await {
        eprintln!("error: {message}\n\n{USAGE}");
        std::process::exit(1);
    }
}

async fn run(args: &[String]) -> Result<(), String> {
    let mut bind = "127.0.0.1:8080".to_string();
    let mut secret = "agent-standalone".to_string();
    let mut node_id: u64 = 100;
    let mut nodes: u64 = 3;
    let mut peer_hosts: BTreeMap<u64, String> = BTreeMap::new();
    let mut bind_host = "127.0.0.1".to_string();
    let mut advertise_host: Option<String> = None;
    let mut port_base: u16 = 7401;
    let mut auth_tokens: Option<PathBuf> = None;
    let mut kinds: Vec<String> = Vec::new();
    let mut limits = Limits::default();

    let mut i = 0;
    while i < args.len() {
        let flag = args[i].as_str();
        let value = args
            .get(i + 1)
            .ok_or_else(|| format!("{flag} requires a value"))?;
        match flag {
            "--bind" => bind = value.clone(),
            "--secret" => secret = value.clone(),
            "--node-id" => node_id = parse(flag, value)?,
            "--nodes" => nodes = parse(flag, value)?,
            "--peer" => {
                let (id, host) = value
                    .split_once('=')
                    .ok_or_else(|| format!("--peer expects <id>=<host>, got {value}"))?;
                peer_hosts.insert(parse("--peer <id>", id)?, host.to_string());
            }
            "--bind-host" => bind_host = value.clone(),
            "--advertise-host" => advertise_host = Some(value.clone()),
            "--port-base" => port_base = parse(flag, value)?,
            "--auth-tokens" => auth_tokens = Some(PathBuf::from(value)),
            "--kind" => {
                if value.is_empty() {
                    return Err("--kind expects a kind name".to_string());
                }
                kinds.push(value.clone());
            }
            "--max-within-secs" => limits.max_within_secs = parse(flag, value)?,
            "--max-body-bytes" => limits.max_body_bytes = parse(flag, value)?,
            other => return Err(format!("unknown flag: {other}")),
        }
        i += 2;
    }
    // A voter's id is already a node's, so the gateway would collide with it on
    // the transport.
    if (1..=nodes).contains(&node_id) {
        return Err(format!(
            "--node-id {node_id} collides with the voter roster 1..={nodes}: the gateway must \
             use a non-voting id outside it"
        ));
    }

    let tokens: Arc<dyn TokenVerifier> = match &auth_tokens {
        Some(path) => {
            let text = std::fs::read_to_string(path)
                .map_err(|e| format!("read --auth-tokens {}: {e}", path.display()))?;
            Arc::new(
                StaticTokens::parse(&text)
                    .map_err(|e| format!("--auth-tokens {}: {e}", path.display()))?,
            )
        }
        None => {
            // The bearer token is taken as the tenant, unverified. Confine it to a
            // loopback public bind, so an unauthenticated multi-tenant edge can
            // never face the network.
            if !bind_is_loopback(&bind) {
                return Err(format!(
                    "--auth-tokens is required when --bind is {bind} (not loopback): an \
                     unauthenticated multi-tenant edge must not face the network"
                ));
            }
            eprintln!(
                "WARNING: no --auth-tokens: the gateway is INSECURE; the bearer token is taken \
                 as the tenant, unverified. Loopback only; provide --auth-tokens to authenticate."
            );
            Arc::new(InsecureTokens)
        }
    };

    // Join as a client: brought into the membership view so the receptionist
    // gossip (the host gateway refs) reaches it, but never a voter and never a
    // host, since it builds no Raft engine and hosts no entity type.
    let node = NodeId::new(node_id);
    let system: TcpCluster = actor_runtime::join(JoinOptions {
        node: node_id,
        voters: nodes,
        peer_hosts,
        clients: BTreeMap::new(),
        bind_host,
        advertise_host,
        port_base,
        secret,
        codec: Arc::new(JsonCodec),
        events: Arc::new(GatewayEvents { node }),
        // The gateway joins inside the cluster's trust boundary (see
        // `docs/multi-tenant-edge.md`); the same plaintext caveat applies.
        encryption: Encryption::PlaintextTrusted,
        // `Static` without a detector is the non-participating mode: a client
        // observes membership but does not drive it, and with no shard groups it
        // has nothing to elect and nothing to down.
        membership: Box::new(|_| MembershipMode::Static { detector: None }),
    })
    .await?;
    eprintln!("[gateway {node}] joined the cluster (client of nodes 1..={nodes})");

    // Discover the host gateways and assemble the routing-only state.
    if kinds.is_empty() {
        // The dev default, matching `agent-standalone`'s default kinds; a
        // real deployment states its kinds explicitly, next to the nodes'.
        kinds = vec!["assistant".into(), "worker".into()];
    }
    let gateway = agent_gateway::connect(
        system,
        &kinds,
        tokens,
        limits,
        Duration::from_secs(30),
    )
    .await?;

    // Rotate tenant tokens on SIGHUP without a restart (only in the file-backed
    // secure mode; the insecure loopback mode has no file to reload).
    if let Some(path) = auth_tokens.clone() {
        spawn_token_reload(gateway.clone(), path);
    }

    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .map_err(|e| format!("bind {bind}: {e}"))?;
    eprintln!("gateway listening on {bind}");
    axum::serve(listener, agent_gateway::http::router(gateway.clone()))
        .with_graceful_shutdown(shutdown_signal(gateway))
        .await
        .map_err(|e| format!("serve: {e}"))
}

/// Membership and reachability transitions on stderr: enough for an operator to
/// see the gateway join and the nodes come and go.
struct GatewayEvents {
    node: NodeId,
}

impl EventSink for GatewayEvents {
    fn emit(&self, event: Event) {
        if event.is_membership() {
            eprintln!("[gateway {}] {event:?}", self.node);
        }
    }
}

/// Resolve once a shutdown signal (SIGTERM or SIGINT) arrives, then flip the
/// gateway to *not ready* and pause briefly so a load balancer deregisters the
/// pod (via `/readyz`) before axum drains the in-flight requests. In-flight
/// prompts are durable in the journal, so dropping their background tasks is
/// safe: a client reconnects and folds the session back out of the journal.
async fn shutdown_signal<S: HarnessSystem>(gateway: Arc<Gateway<S>>) {
    actor_runtime::shutdown_signal().await;
    eprintln!("[gateway] shutdown signal received; draining");
    gateway.set_ready(false);
    tokio::time::sleep(Duration::from_secs(3)).await;
}

/// Spawn a task that re-reads and hot-swaps the `--auth-tokens` file on each
/// SIGHUP. A parse failure keeps the current tokens (a bad edit never locks the
/// tenants out); success swaps atomically behind the gateway's lock.
#[cfg(unix)]
fn spawn_token_reload<S: HarnessSystem>(gateway: Arc<Gateway<S>>, path: PathBuf) {
    tokio::spawn(async move {
        use tokio::signal::unix::SignalKind;
        use tokio::signal::unix::signal;
        let mut hup = match signal(SignalKind::hangup()) {
            Ok(hup) => hup,
            Err(e) => {
                eprintln!("[gateway] cannot install SIGHUP handler; token reload disabled: {e}");
                return;
            }
        };
        while hup.recv().await.is_some() {
            match std::fs::read_to_string(&path)
                .map_err(|e| e.to_string())
                .and_then(|text| StaticTokens::parse(&text))
            {
                Ok(parsed) => {
                    gateway.reload_tokens(Arc::new(parsed));
                    eprintln!("[gateway] reloaded --auth-tokens {}", path.display());
                }
                Err(e) => eprintln!(
                    "[gateway] reload of --auth-tokens {} failed, keeping current: {e}",
                    path.display()
                ),
            }
        }
    });
}

/// On non-unix targets there is no SIGHUP; token reload is a no-op.
#[cfg(not(unix))]
fn spawn_token_reload<S: HarnessSystem>(_gateway: Arc<Gateway<S>>, _path: PathBuf) {}

fn parse<T: std::str::FromStr>(flag: &str, value: &str) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    value.parse().map_err(|e| format!("{flag} {value}: {e}"))
}
