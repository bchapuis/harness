//! `harness-standalone`: a runnable deployment of the agentic harness.
//!
//! One subcommand, `node`, boots one cluster node (silo): the production runtime
//! (tokio, TCP), its own file journal replicated to peers, the Anthropic model,
//! and a workspace sandbox. A node hosts entities and votes in Raft; it has no
//! client-facing listener. The public edge is the separate `harness-gateway`
//! binary.
//!
//! See `docs/standalone-deployment.md` for the walkthrough.

use std::path::PathBuf;

use harness_standalone::node;

const USAGE: &str = "\
usage:
  harness-standalone node --id <n> [options]   run one cluster node (silo)

node options (defaults in parentheses; every node must agree on all of them):
  --id <n>             this node's id, 1..=--nodes        (required)
  --nodes <n>          roster size                        (3)
  --data <dir>         this node's data directory         (./harness-data)
  --bind-host <addr>   interface the transport binds; 0.0.0.0 in a container
                                                          (127.0.0.1)
  --peer <id>=<host>   a node's reachable host; repeat for the roster. Omit for
                       a single-host loopback cluster.    (all 127.0.0.1)
  --client <id>=<host> admit a non-voting cluster client (the gateway): an id
                       OUTSIDE 1..=--nodes, reachable at <host>. Repeatable.
  --port-base <p>      node/client i's transport port = p+i-1 (7401)
  --model <id>         Anthropic model id                 (claude-sonnet-4-6)
  --secret <s>         cluster secret                     (harness-standalone)
  --api-url <url>      Messages API base                  (https://api.anthropic.com)
  --sandbox <mode>     sandbox provider, REQUIRED — no default:
                         docker | firecracker   confined `shell`, durable workspace
                         durable                typed file tools, no shell
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
    let mut sandbox: Option<String> = None;
    let mut sandbox_image: Option<String> = None;
    let mut container_cli = "docker".to_string();
    let mut fc_binary = "firecracker".to_string();
    let mut fc_kernel: Option<String> = None;
    let mut fc_rootfs: Option<String> = None;
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
            "--bind-host" => opts.bind_host = value.clone(),
            "--peer" => {
                let (id, host) = value
                    .split_once('=')
                    .ok_or_else(|| format!("--peer expects <id>=<host>, got {value}"))?;
                opts.peer_hosts
                    .insert(parse("--peer <id>", id)?, host.to_string());
            }
            "--client" => {
                let (id, host) = value
                    .split_once('=')
                    .ok_or_else(|| format!("--client expects <id>=<host>, got {value}"))?;
                opts.clients
                    .insert(parse("--client <id>", id)?, host.to_string());
            }
            "--port-base" => opts.port_base = parse(flag, value)?,
            "--model" => opts.model = value.clone(),
            "--secret" => opts.secret = value.clone(),
            "--api-url" => opts.api_url = value.clone(),
            "--sandbox" => match value.as_str() {
                "docker" | "firecracker" | "durable" => sandbox = Some(value.clone()),
                other => {
                    return Err(format!(
                        "--sandbox must be `docker`, `firecracker`, or `durable`, got {other}"
                    ));
                }
            },
            "--sandbox-image" => sandbox_image = Some(value.clone()),
            "--container-cli" => container_cli = value.clone(),
            "--fc-binary" => fc_binary = value.clone(),
            "--fc-kernel" => fc_kernel = Some(value.clone()),
            "--fc-rootfs" => fc_rootfs = Some(value.clone()),
            other => return Err(format!("unknown flag: {other}")),
        }
        i += 2;
    }
    if opts.id == 0 {
        return Err("--id is required".to_string());
    }
    // Assemble the sandbox mode from its flags once all are seen: each variant
    // carries exactly its own configuration, so a flag for the wrong mode is
    // rejected here instead of being silently ignored at boot.
    opts.sandbox = match sandbox.as_deref() {
        Some("docker") => {
            if fc_kernel.is_some() || fc_rootfs.is_some() {
                return Err("--fc-kernel/--fc-rootfs apply only to --sandbox firecracker".into());
            }
            Some(node::SandboxMode::Docker {
                image: sandbox_image.ok_or("--sandbox docker requires --sandbox-image")?,
                cli: container_cli,
            })
        }
        Some("firecracker") => {
            if sandbox_image.is_some() {
                return Err("--sandbox-image applies only to --sandbox docker".into());
            }
            Some(node::SandboxMode::Firecracker {
                binary: fc_binary,
                kernel: fc_kernel.ok_or(
                    "--sandbox firecracker requires --fc-kernel and --fc-rootfs \
                     (guest/fc-rootfs/build.sh produces both)",
                )?,
                rootfs: fc_rootfs.ok_or(
                    "--sandbox firecracker requires --fc-kernel and --fc-rootfs \
                     (guest/fc-rootfs/build.sh produces both)",
                )?,
            })
        }
        Some("durable") => {
            if sandbox_image.is_some() || fc_kernel.is_some() || fc_rootfs.is_some() {
                return Err(
                    "--sandbox durable takes no --sandbox-image/--fc-kernel/--fc-rootfs".into(),
                );
            }
            Some(node::SandboxMode::Durable)
        }
        _ => None,
    };
    let api_key = std::env::var("ANTHROPIC_API_KEY")
        .map_err(|_| "ANTHROPIC_API_KEY is not set; the node needs it for the model seam")?;
    node::run(opts, api_key).await
}

fn parse<T: std::str::FromStr>(flag: &str, value: &str) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    value.parse().map_err(|e| format!("{flag} {value}: {e}"))
}
