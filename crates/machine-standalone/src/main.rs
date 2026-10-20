//! A standalone multi-process deployment of persistent machines: lightweight
//! VMs you address by name, reach over SSH, and cannot lose. The runnable
//! walkthrough is `./demo-machine.sh` at the repository root; this binary is
//! what it drives.
//!
//! ```text
//! machine-standalone node   --id 1 --machine firecracker --fc-kernel … \
//!                           --door 2222=dev-box --admin 127.0.0.1:7701
//! machine-standalone create dev-box --admin 127.0.0.1:7701 --base-image … \
//!                           --key ~/.ssh/id_ed25519.pub
//! machine-standalone status dev-box --admin 127.0.0.1:7701
//! machine-standalone key    dev-box --admin 127.0.0.1:7701 --add ~/.ssh/other.pub
//! machine-standalone egress dev-box --admin 127.0.0.1:7701 --allow 93.184.216.0/24
//! ```
//!
//! `node` hosts machine entities on a TCP cluster and opens SSH front doors;
//! `--help` lists every flag. The rest are one-shot commands issued through a
//! node's admin socket (see [`admin`] for why they are not cluster members
//! themselves); each becomes an ordinary journaled entity command, because
//! provisioning and access policy *are* journal events (machine §4), not an
//! administrative side channel.
//!
//! The deployment supplies every plug point the machine crates leave open:
//! the runtime binding (`--machine firecracker|docker|fake` picks the
//! `machine_host::MachineHost` behind the hosted binding, or the fake), the
//! front door's two halves ([`authority`] answers `MachineAuthority` with the
//! machine entity's commands on its leader and refuses machines this node does
//! not lead; [`backend`] answers `ChannelBackend` from the node's own guests),
//! and the node's `EgressConfig` from the `--egress-*` flags (feature `net`).
//! [`node`] wires them into one cluster member with an in-process door.
//!
//! Specified by `docs/machine-spec.md`.

mod admin;
mod authority;
mod backend;
mod node;

use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use machine_entity::EgressConfig;
use machine_entity::EgressPolicy;
use machine_entity::MachineError;

use crate::admin::AdminReply;
use crate::admin::AdminRequest;
use crate::node::MachineKind;
use crate::node::NodeOptions;

const USAGE: &str = "\
machine-standalone: persistent lightweight VMs as entities (machine spec)

  node    --id <n> --machine <firecracker|docker|fake> [--nodes 3] [--data DIR]
          [--fc-binary PATH] [--fc-kernel PATH]
          [--docker-cli docker] [--docker-image harness-machine-runner:1]
          [--door <port>=<machine>]…
          [--egress-uplink IFACE --egress-pool CIDR [--cluster-cidr CIDR]…]
          [--admin ADDR] [--peer <id>=<host>]… [--bind-host H]
          [--port-base 7601] [--secret S] [--shards N]

  create  <machine> --base-image PATH [--key PUBKEY]… [--owner WHO]
          [--vcpus 1] [--mem-mib 512] [--checkpoint-secs 30] [--lease-secs 10]
  status  <machine>
  key     <machine> [--add PUBKEY]… [--revoke FINGERPRINT]…
  egress  <machine> (--policy open|none | --allow CIDR…)

Every client subcommand takes one or more --admin ADDR (a node's admin socket);
they are tried in turn, so a command still works after one node is killed.

Egress (the `net` feature; Linux with CAP_NET_ADMIN; firecracker only) NATs
each machine's tap out IFACE, giving each running machine one /30 of the pool;
--cluster-cidr adds the cluster's own ranges to what a guest may never reach.
A machine's `egress` policy takes effect at its next boot.
";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("node") => run_node(&args[1..]).await,
        Some("create") => run_create(&args[1..]).await,
        Some("status") => run_status(&args[1..]).await,
        Some("key") => run_key(&args[1..]).await,
        Some("egress") => run_egress(&args[1..]).await,
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
    let mut egress_uplink = None;
    let mut egress_pool = None;
    let mut cluster_cidrs = Vec::new();
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
            "--docker-cli" => opts.docker_cli = next(&mut rest, flag)?,
            "--docker-image" => opts.docker_image = next(&mut rest, flag)?,
            "--admin" => opts.admin = Some(next(&mut rest, flag)?),
            "--egress-uplink" => egress_uplink = Some(next(&mut rest, flag)?),
            "--egress-pool" => egress_pool = Some(next(&mut rest, flag)?),
            "--cluster-cidr" => cluster_cidrs.push(next(&mut rest, flag)?),
            "--machine" => {
                let value = next(&mut rest, flag)?;
                opts.machine = Some(match value.as_str() {
                    "firecracker" => MachineKind::Firecracker,
                    "docker" => MachineKind::Docker,
                    "fake" => MachineKind::Fake,
                    other => {
                        return Err(format!(
                            "--machine {other}: expected firecracker|docker|fake"
                        ));
                    }
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
    opts.egress = egress_config(egress_uplink, egress_pool, cluster_cidrs)?;
    #[cfg(not(feature = "net"))]
    if opts.egress.is_some() {
        return Err(
            "--egress-uplink needs the `net` feature; this binary was built without it".to_string(),
        );
    }
    node::run(opts).await
}

/// The node's egress configuration (machine §5.2) from its three flags, or
/// `None` when egress is off. An uplink turns it on and needs a pool beside it:
/// nodes sharing a host must not carve guest addresses from one pool, so there
/// is no default.
fn egress_config(
    uplink: Option<String>,
    pool: Option<String>,
    cluster_cidrs: Vec<String>,
) -> Result<Option<EgressConfig>, String> {
    let (uplink, pool) = match (uplink, pool) {
        (None, None) if cluster_cidrs.is_empty() => return Ok(None),
        (Some(uplink), Some(pool)) => (uplink, pool),
        _ => {
            return Err(
                "egress needs --egress-uplink and --egress-pool together; --cluster-cidr \
                 only extends them"
                    .to_string(),
            );
        }
    };
    // Both are written into the nftables ruleset verbatim, so only an interface
    // name and IPv4 ranges get that far.
    let is_name_char = |c: char| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.');
    if uplink.is_empty() || uplink.len() > 15 || !uplink.chars().all(is_name_char) {
        return Err(format!("--egress-uplink `{uplink}`: not an interface name"));
    }
    for cidr in &cluster_cidrs {
        ipv4_cidr("--cluster-cidr", cidr)?;
    }
    let (base, prefix) = ipv4_cidr("--egress-pool", &pool)?;
    // A running machine takes one /30: network, tap (its gateway), guest,
    // broadcast.
    if prefix > 30 {
        return Err(format!(
            "--egress-pool `{pool}`: narrower than the /30 one machine takes"
        ));
    }
    if u32::from(base) & (u32::MAX >> prefix) != 0 {
        return Err(format!(
            "--egress-pool `{pool}`: {base} is not the range's network address"
        ));
    }
    Ok(Some(EgressConfig {
        cluster_cidrs,
        uplink,
        guest_pool_base: base,
        guest_pool_slots: 1 << (30 - prefix),
    }))
}

/// An IPv4 address or `address/prefix`, the forms nftables' `ip daddr` takes.
fn ipv4_cidr(flag: &str, value: &str) -> Result<(Ipv4Addr, u8), String> {
    let (addr, prefix) = value.split_once('/').unwrap_or((value, "32"));
    let addr: Ipv4Addr = parse(flag, addr)?;
    let prefix: u8 = parse(flag, prefix)?;
    if prefix > 32 {
        return Err(format!("{flag} `{value}`: an IPv4 prefix is at most /32"));
    }
    Ok((addr, prefix))
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
        // A machine is a durable, named thing (machine §2): asking for one that
        // exists is asking for the one that exists. Reported rather than failed,
        // so a re-run of a demo, or a retried command whose first landing was
        // lost, is a no-op instead of an error.
        AdminReply::Refused(MachineError::AlreadyProvisioned) => {
            println!(
                "machine `{}` already exists, leaving it as it is",
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
    println!("running        {}", status.running);
    println!("image bytes    {}", status.image_bytes);
    println!(
        "image digest   {}",
        status
            .image_digest
            .map(|d| d.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    // The capture cadence is the durability grain (machine §6, M3): the last
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
        let (request, done) = match flag.as_str() {
            "--add" => {
                let (fingerprint, key) = read_public_key(value)?;
                let done = format!("authorized {fingerprint}");
                (
                    AdminRequest::AddKey {
                        machine: common.machine.clone(),
                        fingerprint,
                        key,
                    },
                    done,
                )
            }
            "--revoke" => (
                AdminRequest::RevokeKey {
                    machine: common.machine.clone(),
                    fingerprint: value.clone(),
                },
                // A revoked key stops authorizing the *next* attach; it does
                // not tear down a live connection (machine §4).
                format!("revoked {value} (live connections are unaffected)"),
            ),
            other => return Err(format!("key: unknown flag `{other}`")),
        };
        match admin::call(&common.admin, request).await? {
            AdminReply::Done => {}
            other => return Err(format!("key {}: {}", common.machine, describe(other))),
        }
        println!("{done}");
        changed += 1;
    }
    if changed == 0 {
        return Err("key: nothing to do; pass --add PUBKEY or --revoke FINGERPRINT".to_string());
    }
    Ok(())
}

async fn run_egress(args: &[String]) -> Result<(), String> {
    let common = common(args)?;
    let mut policy = None;
    let mut allowed = Vec::new();
    for (flag, value) in &common.rest {
        match flag.as_str() {
            "--policy" => {
                policy = Some(match value.as_str() {
                    "open" => EgressPolicy::Open,
                    "none" => EgressPolicy::None,
                    other => return Err(format!("--policy {other}: expected open|none")),
                });
            }
            "--allow" => {
                ipv4_cidr(flag, value)?;
                allowed.push(value.clone());
            }
            other => return Err(format!("egress: unknown flag `{other}`")),
        }
    }
    let policy = match (policy, allowed.is_empty()) {
        (Some(policy), true) => policy,
        (None, false) => EgressPolicy::Allowlist(allowed),
        _ => {
            return Err(
                "egress: pass either --policy open|none or one or more --allow CIDR".to_string(),
            );
        }
    };
    let shown = format!("{policy:?}");
    let reply = admin::call(
        &common.admin,
        AdminRequest::SetEgress {
            machine: common.machine.clone(),
            policy,
        },
    )
    .await
    .map_err(|e| format!("egress {}: {e}", common.machine))?;
    match reply {
        // Journaled now, realized when the machine next boots (machine §4).
        AdminReply::Done => println!(
            "machine `{}` egress set to {shown}, effective from its next boot",
            common.machine
        ),
        other => return Err(format!("egress {}: {}", common.machine, describe(other))),
    }
    Ok(())
}

fn describe(reply: AdminReply) -> String {
    match reply {
        AdminReply::Refused(reason) => format!("refused: {reason}"),
        AdminReply::Unavailable(why) => why,
        AdminReply::Done => "unexpected reply: done".to_string(),
        AdminReply::Status(_) => "unexpected reply: status".to_string(),
    }
}

/// Read an OpenSSH public key file into the `(fingerprint, key line)` pair the
/// machine journals (machine §4). Parsed here so a malformed key fails at the
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

#[cfg(test)]
mod tests {
    use super::*;

    fn flags(
        uplink: Option<&str>,
        pool: Option<&str>,
        cidrs: &[&str],
    ) -> Result<Option<EgressConfig>, String> {
        egress_config(
            uplink.map(str::to_string),
            pool.map(str::to_string),
            cidrs.iter().map(|c| c.to_string()).collect(),
        )
    }

    #[test]
    fn no_egress_flag_leaves_egress_off() {
        assert!(flags(None, None, &[]).unwrap().is_none());
    }

    #[test]
    fn the_pool_cidr_sets_the_base_and_one_slot_per_thirty() {
        let egress = flags(Some("eth0"), Some("172.31.0.0/24"), &["10.100.0.0/16"])
            .unwrap()
            .expect("egress on");
        assert_eq!(egress.uplink, "eth0");
        assert_eq!(egress.guest_pool_base, Ipv4Addr::new(172, 31, 0, 0));
        assert_eq!(egress.guest_pool_slots, 64, "a /24 holds 64 /30s");
        assert_eq!(egress.cluster_cidrs, ["10.100.0.0/16"]);

        let one = flags(Some("eth0"), Some("172.31.0.4/30"), &[]).unwrap();
        assert_eq!(one.expect("egress on").guest_pool_slots, 1);
    }

    #[test]
    fn an_uplink_and_a_pool_come_together() {
        assert!(flags(Some("eth0"), None, &[]).is_err());
        assert!(flags(None, Some("172.31.0.0/24"), &[]).is_err());
        assert!(flags(None, None, &["10.100.0.0/16"]).is_err());
    }

    #[test]
    fn what_would_reach_the_ruleset_unchecked_is_refused() {
        for (uplink, pool, cidrs) in [
            ("eth0\" accept", "172.31.0.0/24", &[][..]),
            ("an-interface-name-too-long", "172.31.0.0/24", &[][..]),
            ("eth0", "172.31.0.0/31", &[][..]),
            ("eth0", "172.31.0.1/24", &[][..]),
            ("eth0", "172.31.0.0/33", &[][..]),
            ("eth0", "172.31.0.0", &[][..]),
            ("eth0", "172.31.0.0/24", &["10.0.0.0/8; accept"][..]),
            ("eth0", "172.31.0.0/24", &["fd00::/8"][..]),
        ] {
            assert!(
                flags(Some(uplink), Some(pool), cidrs).is_err(),
                "{uplink} {pool} {cidrs:?} must be refused",
            );
        }
    }
}
