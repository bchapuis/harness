//! `harness-standalone`: a runnable deployment of the agentic harness.
//!
//! Two subcommands:
//! - `node` boots one cluster node — production runtime (tokio, TCP),
//!   shared file journal, the Anthropic model, a workspace sandbox — and
//!   serves a loopback control port.
//! - `repl` attaches to any node's control port and drives sessions.
//!
//! See `docs/standalone-deployment.md` for the three-node walkthrough.

mod http;
mod ids;
mod journal;
mod node;
mod proto;
mod repl;
mod sandbox;

use std::path::PathBuf;

const USAGE: &str = "\
usage:
  harness-standalone node --id <n> [options]   run one cluster node
  harness-standalone repl [host:port]          attach a REPL to a node's control port
                                               (default 127.0.0.1:7501)

node options (defaults in parentheses; every node must agree on all of them):
  --id <n>             this node's id, 1..=--nodes        (required)
  --nodes <n>          roster size                        (3)
  --data <dir>         shared data directory              (./harness-data)
  --port-base <p>      node i's transport port = p+i-1    (7401)
  --control-base <p>   node i's control port = p+i-1      (7501)
  --model <id>         Anthropic model id                 (claude-sonnet-4-6)
  --secret <s>         cluster secret                     (harness-standalone)
  --api-url <url>      Messages API base                  (https://api.anthropic.com)
  --sandbox <mode>     sandbox provider, REQUIRED — no default:
                         docker | firecracker   confined `shell`
                         local                  UNCONFINED /bin/sh as this user;
                                                trusted-input only
  --sandbox-image <r>  container image for --sandbox docker (required there)
  --container-cli <c>  container CLI binary               (docker)
  --fc-binary <path>   firecracker executable, --sandbox firecracker (firecracker)
  --fc-kernel <path>   vmlinux for --sandbox firecracker  (required there)
  --fc-rootfs <path>   base rootfs ext4 with /sbin/fc-agent (required there;
                       guest/fc-rootfs/build.sh produces both assets)

environment:
  ANTHROPIC_API_KEY    required by `node`";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("node") => run_node(&args[1..]).await,
        Some("repl") => run_repl(&args[1..]).await,
        Some("--help") | Some("-h") | None => {
            println!("{USAGE}");
            return;
        }
        Some(other) => Err(format!("unknown command: {other}")),
    };
    if let Err(message) = result {
        eprintln!("error: {message}\n\n{USAGE}");
        std::process::exit(1);
    }
}

async fn run_node(args: &[String]) -> Result<(), String> {
    let mut opts = node::NodeOptions::default();
    let mut i = 0;
    while i < args.len() {
        let flag = &args[i];
        let value = args
            .get(i + 1)
            .ok_or_else(|| format!("{flag} requires a value"))?;
        match flag.as_str() {
            "--id" => opts.id = parse(flag, value)?,
            "--nodes" => opts.nodes = parse(flag, value)?,
            "--data" => opts.data = PathBuf::from(value),
            "--port-base" => opts.port_base = parse(flag, value)?,
            "--control-base" => opts.control_base = parse(flag, value)?,
            "--model" => opts.model = value.clone(),
            "--secret" => opts.secret = value.clone(),
            "--api-url" => opts.api_url = value.clone(),
            "--sandbox" => {
                opts.sandbox = Some(match value.as_str() {
                    "local" => node::SandboxMode::Local,
                    "docker" => node::SandboxMode::Docker,
                    "firecracker" => node::SandboxMode::Firecracker,
                    other => {
                        return Err(format!(
                            "--sandbox must be `local`, `docker`, or `firecracker`, got {other}"
                        ));
                    }
                })
            }
            "--sandbox-image" => opts.sandbox_image = value.clone(),
            "--container-cli" => opts.container_cli = value.clone(),
            "--fc-binary" => opts.fc_binary = value.clone(),
            "--fc-kernel" => opts.fc_kernel = value.clone(),
            "--fc-rootfs" => opts.fc_rootfs = value.clone(),
            other => return Err(format!("unknown flag: {other}")),
        }
        i += 2;
    }
    if opts.id == 0 {
        return Err("--id is required".to_string());
    }
    let api_key = std::env::var("ANTHROPIC_API_KEY")
        .map_err(|_| "ANTHROPIC_API_KEY is not set; the node needs it for the model seam")?;
    node::run(opts, api_key).await
}

async fn run_repl(args: &[String]) -> Result<(), String> {
    match args {
        [] => repl::run("127.0.0.1:7501").await,
        [addr] => repl::run(addr).await,
        _ => Err("repl takes at most one address".to_string()),
    }
}

fn parse<T: std::str::FromStr>(flag: &str, value: &str) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    value.parse().map_err(|e| format!("{flag} {value}: {e}"))
}
