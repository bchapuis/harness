//! `harness-gateway`: the public HTTP/SSE edge, a cluster client of a harness
//! cluster.
//!
//! It joins the actor transport as a non-voting, non-hosting member (its
//! `--node-id` is outside the nodes' `1..=--nodes` roster, and each node admits
//! it with `--client`), terminates tenant auth (bearer token → principal), and
//! drives each caller's session by addressing its grain directly. Run one or many
//! replicas behind a load balancer, each with its own `--node-id`. Public TLS is
//! expected to terminate at an ingress/LB; the transport link is plaintext,
//! guarded by the cluster `--secret`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use harness::GranaryConfig;
use harness_gateway::auth::InsecureTokens;
use harness_gateway::auth::StaticTokens;
use harness_gateway::auth::TokenVerifier;
use harness_gateway::auth::is_loopback;
use harness_gateway::cluster::ClusterOptions;

const USAGE: &str = "\
usage:
  harness-gateway [options]

options (defaults in parentheses):
  --bind <host:port>    public HTTP bind                    (127.0.0.1:8080)
  --secret <s>          cluster secret; must match the nodes' --secret
                                                            (harness-standalone)
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
  --auth-tokens <path>  tenants file (`<principal> <token>` per line). Without it
                        the gateway runs INSECURE: the bearer token IS the tenant.

Each request carries `Authorization: Bearer <tenant-token>`. The public side is
plaintext; terminate TLS at an ingress/LB.";

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
    let mut secret = "harness-standalone".to_string();
    let mut node_id: u64 = 100;
    let mut nodes: u64 = 3;
    let mut peer_hosts: BTreeMap<u64, String> = BTreeMap::new();
    let mut bind_host = "127.0.0.1".to_string();
    let mut advertise_host: Option<String> = None;
    let mut port_base: u16 = 7401;
    let mut auth_tokens: Option<PathBuf> = None;

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
            other => return Err(format!("unknown flag: {other}")),
        }
        i += 2;
    }

    let tokens: Box<dyn TokenVerifier> = match &auth_tokens {
        Some(path) => {
            let text = std::fs::read_to_string(path)
                .map_err(|e| format!("read --auth-tokens {}: {e}", path.display()))?;
            Box::new(
                StaticTokens::parse(&text)
                    .map_err(|e| format!("--auth-tokens {}: {e}", path.display()))?,
            )
        }
        None => {
            // The bearer token is taken as the tenant, unverified. Confine it to a
            // loopback public bind, so an unauthenticated multi-tenant edge can
            // never face the network.
            let host = bind.rsplit_once(':').map(|(h, _)| h).unwrap_or(&bind);
            if !is_loopback(host) {
                return Err(format!(
                    "--auth-tokens is required when --bind is {bind} (not loopback): an \
                     unauthenticated multi-tenant edge must not face the network"
                ));
            }
            eprintln!(
                "WARNING: no --auth-tokens: the gateway is INSECURE — the bearer token is taken \
                 as the tenant, unverified. Loopback only; provide --auth-tokens to authenticate."
            );
            Box::new(InsecureTokens)
        }
    };

    let system = harness_gateway::cluster::join(ClusterOptions {
        node_id,
        nodes,
        bind_host,
        advertise_host,
        peer_hosts,
        port_base,
        secret,
    })
    .await?;

    // Discover the host gateways and assemble the routing-only state. The
    // directory shards must match the nodes' `Directory` shards (the default).
    let gateway = harness_gateway::connect(
        system,
        harness_gateway::client_kinds(),
        GranaryConfig::default().shards,
        tokens,
        Duration::from_secs(30),
    )
    .await?;

    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .map_err(|e| format!("bind {bind}: {e}"))?;
    eprintln!("gateway listening on {bind}");
    axum::serve(listener, harness_gateway::http::router(gateway))
        .await
        .map_err(|e| format!("serve: {e}"))
}

fn parse<T: std::str::FromStr>(flag: &str, value: &str) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    value.parse().map_err(|e| format!("{flag} {value}: {e}"))
}
