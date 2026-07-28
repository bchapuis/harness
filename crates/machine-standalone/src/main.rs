//! A standalone multi-process deployment of persistent machines (machine spec):
//! lightweight VMs you address by name, reach over SSH, and cannot lose.
//!
//! ```text
//! machine-standalone node   --id 1 --vm firecracker --fc-kernel … \
//!                           --door 2222=dev-box --admin 127.0.0.1:7701
//! machine-standalone create dev-box --admin 127.0.0.1:7701 --base-image … \
//!                           --key ~/.ssh/id_ed25519.pub
//! machine-standalone status dev-box --admin 127.0.0.1:7701
//! machine-standalone key    dev-box --admin 127.0.0.1:7701 --add ~/.ssh/other.pub
//! ```
//!
//! `node` hosts machine grains and opens SSH front doors. The rest are
//! one-shot commands issued through a node's admin socket (see [`admin`] for
//! why they are not cluster members themselves); each becomes an ordinary
//! journaled grain command, because provisioning and access policy *are*
//! journal events (machine §3), not an administrative side channel.

mod admin;
mod authority;
mod backend;
mod node;
mod provider;

use std::collections::BTreeMap;

use crate::admin::AdminReply;
use crate::admin::AdminRequest;
use crate::node::NodeOptions;
use crate::node::VmMode;

const USAGE: &str = "\
machine-standalone — persistent lightweight VMs as grains (machine spec)

  node    --id <n> --vm <firecracker|fake> [--nodes 3] [--data DIR]
          [--fc-binary PATH] [--fc-kernel PATH] [--door <port>=<machine>]…
          [--admin ADDR] [--peer <id>=<host>]… [--bind-host H]
          [--port-base 7601] [--secret S] [--shards N]

  create  <machine> --base-image PATH [--key PUBKEY]… [--owner WHO]
          [--vcpus 1] [--mem-mib 512] [--checkpoint-secs 30] [--lease-secs 10]
  status  <machine>
  key     <machine> [--add PUBKEY]… [--revoke FINGERPRINT]…

Every client subcommand takes one or more --admin ADDR (a node's admin socket);
they are tried in turn, so a command still works after one node is killed.
";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("node") => run_node(&args[1..]).await,
        Some("create") => run_create(&args[1..]).await,
        Some("status") => run_status(&args[1..]).await,
        Some("key") => run_key(&args[1..]).await,
        Some("--help") | Some("-h") | None => {
            print!("{USAGE}");
            return;
        }
        Some(other) => Err(format!("unknown subcommand `{other}`\n\n{USAGE}")),
    };
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run_node(args: &[String]) -> Result<(), String> {
    let mut opts = NodeOptions::default();
    let mut rest = args.iter();
    while let Some(flag) = rest.next() {
        match flag.as_str() {
            "--id" => opts.id = parse(flag, &next(&mut rest, flag)?)?,
            "--nodes" => opts.nodes = parse(flag, &next(&mut rest, flag)?)?,
            "--data" => opts.data = next(&mut rest, flag)?.into(),
            "--bind-host" => opts.bind_host = next(&mut rest, flag)?,
            "--port-base" => opts.port_base = parse(flag, &next(&mut rest, flag)?)?,
            "--secret" => opts.secret = next(&mut rest, flag)?,
            "--shards" => opts.shards = parse(flag, &next(&mut rest, flag)?)?,
            "--fc-binary" => opts.fc_binary = next(&mut rest, flag)?,
            "--fc-kernel" => opts.fc_kernel = next(&mut rest, flag)?,
            "--admin" => opts.admin = Some(next(&mut rest, flag)?),
            "--vm" => {
                let value = next(&mut rest, flag)?;
                opts.vm = Some(match value.as_str() {
                    "firecracker" => VmMode::Firecracker,
                    "fake" => VmMode::Fake,
                    other => return Err(format!("--vm {other}: expected firecracker|fake")),
                });
            }
            "--peer" => {
                let (id, host) = split_pair(&next(&mut rest, flag)?, flag)?;
                opts.peer_hosts.insert(parse(flag, &id)?, host);
            }
            "--door" => {
                let (port, name) = split_pair(&next(&mut rest, flag)?, flag)?;
                opts.doors.insert(parse(flag, &port)?, name);
            }
            other => return Err(format!("unknown flag `{other}`\n\n{USAGE}")),
        }
    }
    node::run(opts).await
}

/// A client subcommand's parsed arguments: the machine it names, the admin
/// sockets to try, and the subcommand's own flags left untouched.
struct Common {
    machine: String,
    admin: Vec<String>,
    rest: Vec<(String, String)>,
}

fn common(args: &[String]) -> Result<Common, String> {
    let mut machine = String::new();
    let mut admin = Vec::new();
    let mut rest = Vec::new();
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if !arg.starts_with("--") {
            if machine.is_empty() {
                machine = arg.clone();
                continue;
            }
            return Err(format!("unexpected argument `{arg}`"));
        }
        let value = next(&mut iter, arg)?;
        match arg.as_str() {
            "--admin" => admin.push(value),
            _ => rest.push((arg.clone(), value)),
        }
    }
    if machine.is_empty() {
        return Err("a machine name is required".to_string());
    }
    if admin.is_empty() {
        return Err("--admin ADDR is required (a node's admin socket)".to_string());
    }
    Ok(Common {
        machine,
        admin,
        rest,
    })
}

async fn run_create(args: &[String]) -> Result<(), String> {
    let common = common(args)?;
    let mut base_image = String::new();
    let mut owner = std::env::var("USER").unwrap_or_else(|_| "operator".to_string());
    let mut vcpus: u8 = 1;
    let mut mem_mib: u32 = 512;
    let mut checkpoint_secs: u64 = 30;
    let mut lease_secs: u64 = 10;
    let mut authorized_keys = BTreeMap::new();
    for (flag, value) in &common.rest {
        match flag.as_str() {
            "--base-image" => base_image = value.clone(),
            "--owner" => owner.clone_from(value),
            "--vcpus" => vcpus = parse(flag, value)?,
            "--mem-mib" => mem_mib = parse(flag, value)?,
            "--checkpoint-secs" => checkpoint_secs = parse(flag, value)?,
            "--lease-secs" => lease_secs = parse(flag, value)?,
            "--key" => {
                let (fingerprint, line) = read_public_key(value)?;
                authorized_keys.insert(fingerprint, line);
            }
            other => return Err(format!("create: unknown flag `{other}`")),
        }
    }
    if base_image.is_empty() {
        return Err(
            "--base-image is required: the rootfs a fresh machine's disk starts from \
                    (guest/machine-rootfs/build.sh produces machine.ext4)"
                .to_string(),
        );
    }
    // Absolute, because the path is opened by whichever node leads the machine:
    // every node must see the base image at the same place.
    let base_image = std::path::Path::new(&base_image)
        .canonicalize()
        .map_err(|e| format!("--base-image {base_image}: {e}"))?
        .to_string_lossy()
        .into_owned();
    let reply = admin::call(
        &common.admin,
        AdminRequest::Provision {
            machine: common.machine.clone(),
            owner,
            base_image,
            vcpus,
            mem_mib,
            checkpoint_secs,
            lease_secs,
            authorized_keys,
        },
    )
    .await
    .map_err(|e| format!("provision {}: {e}", common.machine))?;
    match reply {
        AdminReply::Done => println!("machine `{}` provisioned", common.machine),
        // A machine is a durable, named thing (machine §1): asking for one that
        // exists is asking for the one that exists.
        AdminReply::AlreadyProvisioned => {
            println!(
                "machine `{}` already exists — leaving it as it is",
                common.machine
            );
        }
        other => return Err(format!("provision {}: {}", common.machine, describe(other))),
    }
    Ok(())
}

async fn run_status(args: &[String]) -> Result<(), String> {
    let common = common(args)?;
    if let Some((flag, _)) = common.rest.first() {
        return Err(format!("status: unknown flag `{flag}`"));
    }
    let reply = admin::call(
        &common.admin,
        AdminRequest::Status {
            machine: common.machine.clone(),
        },
    )
    .await
    .map_err(|e| format!("status {}: {e}", common.machine))?;
    let AdminReply::Status(status) = reply else {
        return Err(format!("status {}: {}", common.machine, describe(reply)));
    };
    println!("machine        {}", common.machine);
    println!("provisioned    {}", status.provisioned);
    println!("owner          {}", status.owner);
    println!("egress         {:?}", status.egress);
    println!("vm running     {}", status.vm_running);
    println!("image bytes    {}", status.image_bytes);
    println!(
        "image digest   {}",
        status
            .image_digest
            .map(|d| d.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    // The capture cadence is the durability grain (machine §4, M3): the last
    // capture is the point a crash rewinds to.
    println!("captures       {}", status.captures);
    println!(
        "ws captures    {} ({} skipped)",
        status.ws_captures, status.ws_capture_skips
    );
    if status.attachments.is_empty() {
        println!("attachments    none");
    } else {
        for (id, principal) in &status.attachments {
            println!("attachment {id:<3} {principal}");
        }
    }
    Ok(())
}

async fn run_key(args: &[String]) -> Result<(), String> {
    let common = common(args)?;
    let mut changed = 0;
    for (flag, value) in &common.rest {
        let request = match flag.as_str() {
            "--add" => {
                let (fingerprint, key) = read_public_key(value)?;
                AdminRequest::AddKey {
                    machine: common.machine.clone(),
                    fingerprint,
                    key,
                }
            }
            "--revoke" => AdminRequest::RevokeKey {
                machine: common.machine.clone(),
                fingerprint: value.clone(),
            },
            other => return Err(format!("key: unknown flag `{other}`")),
        };
        match admin::call(&common.admin, request).await? {
            AdminReply::Done => {}
            other => return Err(describe(other)),
        }
        match flag.as_str() {
            "--add" => println!("authorized {}", read_public_key(value)?.0),
            // A revoked key stops authorizing the *next* attach; it does not
            // tear down a live connection (machine §3).
            _ => println!("revoked {value} (live connections are unaffected)"),
        }
        changed += 1;
    }
    if changed == 0 {
        return Err("key: nothing to do — pass --add PUBKEY or --revoke FINGERPRINT".to_string());
    }
    Ok(())
}

fn describe(reply: AdminReply) -> String {
    match reply {
        AdminReply::Error(e) => e,
        AdminReply::Done => "unexpected reply: done".to_string(),
        AdminReply::AlreadyProvisioned => "machine already provisioned".to_string(),
        AdminReply::Status(_) => "unexpected reply: status".to_string(),
    }
}

/// Read an OpenSSH public key file into the `(fingerprint, key line)` pair the
/// machine journals (machine §3). Parsed here so a malformed key fails at the
/// CLI rather than silently authorizing nobody.
fn read_public_key(path: &str) -> Result<(String, String), String> {
    let line = std::fs::read_to_string(path).map_err(|e| format!("read {path}: {e}"))?;
    let line = line.trim().to_string();
    let key = russh::keys::PublicKey::from_openssh(&line)
        .map_err(|e| format!("{path}: not an OpenSSH public key: {e}"))?;
    Ok((
        key.fingerprint(russh::keys::HashAlg::Sha256).to_string(),
        line,
    ))
}

fn next<'a>(iter: &mut impl Iterator<Item = &'a String>, flag: &str) -> Result<String, String> {
    iter.next()
        .cloned()
        .ok_or_else(|| format!("{flag} needs a value"))
}

/// Split a `<key>=<value>` flag argument.
fn split_pair(value: &str, flag: &str) -> Result<(String, String), String> {
    value
        .split_once('=')
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .ok_or_else(|| format!("{flag} expects <key>=<value>, got `{value}`"))
}

fn parse<T: std::str::FromStr>(flag: &str, value: &str) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    value.parse().map_err(|e| format!("{flag} `{value}`: {e}"))
}
