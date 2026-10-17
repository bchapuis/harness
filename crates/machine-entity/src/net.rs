//! Policy-bound, attributable, isolated egress (machine spec §5.2, M6).
//!
//! A machine's guest gets an outbound path, unlike the agent sandbox, whose
//! egress is default-deny per tool call. Three properties bind (M6):
//!
//! - **Policy-bound.** The machine's journaled [`EgressPolicy`] grants egress;
//!   nothing is ambient. `Open` is the fresh machine's default.
//! - **Attributable.** A per-machine tap device (named by the entity hash) NATs
//!   through the node, and per-chain counters attribute flows to the machine,
//!   joining the attachment journal to answer *who was attached while this
//!   machine reached what*.
//! - **Isolated: no lateral movement.** Whatever the policy grants toward the
//!   internet, the path MUST NOT reach another machine's guest, an
//!   agent-sandbox environment, the host's own services, or the cluster's
//!   control plane and node-internal addresses (link-local and metadata
//!   endpoints included).
//!
//! [`nft_ruleset`] is a **pure function**: the mechanism M6 is verified
//! against. Applying it (the tap device, the node NAT, invoking `nft`) is the
//! Linux-only, `CAP_NET_ADMIN` realization behind [`feature = "net"`](apply).

use std::collections::BTreeSet;
use std::net::Ipv4Addr;

use crate::entity::EgressPolicy;
use entity::EntityName;
use entity::facet::BlobId;

/// The interface name for a machine's tap device: `hm-` plus 8 hex of the
/// entity-name hash. Stable per machine: the attribution key (M6), and short
/// enough for the 15-char Linux interface-name limit.
pub(crate) fn tap_name(machine: &EntityName) -> String {
    let hash = BlobId::of(machine.to_string().as_bytes());
    format!("hm-{}", &hash.to_string()[..8])
}

/// The nftables table (and chain-name prefix) for a machine's egress rules.
/// Derived from [`tap_name`] so install and teardown cannot drift apart.
fn egress_table(machine: &EntityName) -> String {
    format!("egress_{}", tap_name(machine).replace('-', "_"))
}

/// The destination classes egress MUST NOT reach (M6's no-lateral-movement),
/// beyond the cluster's own CIDRs the caller supplies: RFC1918 private ranges
/// (other taps, host services, node-internal addresses all live here),
/// link-local, and the cloud metadata endpoint.
const FORBIDDEN_V4: &[&str] = &[
    "10.0.0.0/8",
    "172.16.0.0/12",
    "192.168.0.0/16",
    "169.254.0.0/16", // link-local, incl. 169.254.169.254 (metadata)
    "127.0.0.0/8",
];
const FORBIDDEN_V6: &[&str] = &[
    "fe80::/10", // link-local
    "fc00::/7",  // unique-local
    "::1/128",   // loopback
];

/// Generate the nftables ruleset for one machine's egress (machine §5.2). One
/// chain per machine (named by [`tap_name`]): the forbidden-destination drops
/// first, then the policy, then NAT masquerade out the node uplink with a
/// counter for attribution.
///
/// - `cluster_cidrs`: the cluster's own control-plane and node-internal ranges,
///   dropped in addition to the RFC1918/link-local/metadata classes.
/// - `uplink`: the node's egress interface for the masquerade.
pub(crate) fn nft_ruleset(
    machine: &EntityName,
    policy: &EgressPolicy,
    cluster_cidrs: &[&str],
    uplink: &str,
) -> String {
    let tap = tap_name(machine);
    let chain = egress_table(machine);
    let mut out = String::new();
    out.push_str(&format!("table inet {chain} {{\n"));
    // The base chain accepts by default and decides only this machine's
    // tap-originated packets, which `_out` always ends with a terminal
    // verdict. A drop in any base chain is final in nftables, so a drop
    // policy here would also drop the uplink's reply packets (no handshake
    // could complete under any policy) and every other machine's packets,
    // which their own tables decide.
    out.push_str("  chain forward {\n");
    out.push_str("    type filter hook forward priority 0; policy accept;\n");
    out.push_str(&format!("    iifname \"{tap}\" jump {chain}_out\n"));
    out.push_str("  }\n");
    out.push_str(&format!("  chain {chain}_out {{\n"));

    // Isolation first (M6): no lateral movement, whatever the policy grants.
    // The cluster's own ranges and the universal private/link-local/metadata
    // classes drop before the policy is consulted.
    for cidr in cluster_cidrs {
        out.push_str(&format!(
            "    ip daddr {cidr} counter drop comment \"cluster-internal\"\n"
        ));
    }
    for cidr in FORBIDDEN_V4 {
        out.push_str(&format!("    ip daddr {cidr} counter drop\n"));
    }
    for cidr in FORBIDDEN_V6 {
        out.push_str(&format!("    ip6 daddr {cidr} counter drop\n"));
    }

    // Policy (M6): what the machine's journaled policy grants toward the
    // internet, everything else denied (the chain's drop default).
    match policy {
        EgressPolicy::Open => {
            out.push_str("    counter accept comment \"policy: open\"\n");
        }
        EgressPolicy::Allowlist(dests) => {
            for dest in dests {
                out.push_str(&format!(
                    "    ip daddr {dest} counter accept comment \"policy: allow\"\n"
                ));
            }
            // Anything not allowed falls through to the chain's drop.
            out.push_str("    counter drop comment \"policy: allowlist default\"\n");
        }
        EgressPolicy::None => {
            out.push_str("    counter drop comment \"policy: none\"\n");
        }
    }
    out.push_str("  }\n");

    // NAT masquerade out the node uplink, attributed by a per-machine counter
    // (M6).
    out.push_str(&format!("  chain {chain}_nat {{\n"));
    out.push_str("    type nat hook postrouting priority 100; policy accept;\n");
    out.push_str(&format!(
        "    iifname \"{tap}\" oifname \"{uplink}\" counter masquerade\n"
    ));
    out.push_str("  }\n");
    out.push_str("}\n");
    out
}

/// Deployment-level egress configuration for a node (machine §5.2): the ranges
/// the ruleset must fence off, the uplink it masquerades out, and the address
/// pool per-machine guest /30s are carved from. The per-machine half (which
/// slot, which policy) is decided per boot. Its presence is what turns egress
/// on: a provider built without it boots machines with no NIC, so a deployment
/// that has not provisioned an uplink stays functional.
#[derive(Clone, Debug)]
pub struct EgressConfig {
    /// The cluster's own control-plane and node-internal CIDRs, dropped by the
    /// ruleset in addition to the universal private/link-local/metadata classes
    /// (M6 no lateral movement).
    pub cluster_cidrs: Vec<String>,
    /// The node's egress interface, the masquerade's `oifname` (M6 attribution).
    pub uplink: String,
    /// The base of the node-local pool guest /30s are carved from (e.g.
    /// `172.31.0.0`). Each machine takes the next free /30: `.0` network,
    /// `.1` the tap (the guest's gateway), `.2` the guest, `.3` broadcast.
    pub guest_pool_base: Ipv4Addr,
    /// How many machines the pool addresses concurrently; a boot past this
    /// count degrades to no NIC rather than colliding addresses.
    pub guest_pool_slots: u32,
}

/// One machine's realized guest addressing (machine §5.2), carved from the
/// node's [`EgressConfig`] pool: the tap the guest's virtio-net binds to, the
/// two addresses of its /30, and the guest's MAC.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GuestNet {
    /// The tap device name: the stable per-machine attribution key ([`tap_name`]).
    pub tap: String,
    /// The tap's host-side address, e.g. `172.31.0.1`: what [`apply::install`]
    /// assigns to the tap (with the /30 prefix) and the guest's default gateway.
    pub host: Ipv4Addr,
    /// The guest's own address, e.g. `172.31.0.2`.
    pub guest: Ipv4Addr,
    /// A stable, locally-administered MAC for the guest NIC (derived from the
    /// machine name, so it survives reboots: attribution stability, M6).
    pub guest_mac: String,
}

/// A stable, locally-administered unicast MAC for a machine's guest NIC,
/// derived from the entity-name hash (the `0x02` first octet marks it
/// locally-administered). Stable per machine across reboots (M6 attribution).
pub(crate) fn guest_mac(machine: &EntityName) -> String {
    let hash = BlobId::of(machine.to_string().as_bytes());
    let b = hash.as_bytes();
    format!(
        "02:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        b[0], b[1], b[2], b[3], b[4]
    )
}

/// Carve the `index`-th guest /30 out of the pool based at `pool_base`
/// (machine §5.2). Pure: the same machine + index always maps to the same
/// addresses, so a crash-and-reboot that reallocates the same slot lands the
/// same guest address.
pub(crate) fn guest_net(machine: &EntityName, pool_base: Ipv4Addr, index: u32) -> GuestNet {
    // Each /30 is four addresses; `index * 4` steps to this machine's block.
    let network = u32::from(pool_base).wrapping_add(index.wrapping_mul(4));
    GuestNet {
        tap: tap_name(machine),
        host: Ipv4Addr::from(network.wrapping_add(1)),
        guest: Ipv4Addr::from(network.wrapping_add(2)),
        guest_mac: guest_mac(machine),
    }
}

/// The `ip=` kernel argument that brings the guest's `eth0` up before init via
/// Linux IP autoconfiguration (`CONFIG_IP_PNP`), so the base image needs no
/// DHCP client: `ip=<guest>::<gateway>:<netmask>::eth0:off`. Appended to the
/// boot args when a machine boots with a NIC.
pub(crate) fn guest_ip_boot_arg(net: &GuestNet) -> String {
    format!("ip={}::{}:255.255.255.252::eth0:off", net.guest, net.host)
}

/// A node-local allocator for per-machine guest /30 slots (machine §5.2). One
/// per node; a boot takes the lowest free slot and a kill returns it, so a
/// machine that comes and goes never exhausts the pool.
#[derive(Debug)]
pub(crate) struct GuestPool {
    slots: u32,
    used: BTreeSet<u32>,
}

impl GuestPool {
    pub(crate) fn new(slots: u32) -> GuestPool {
        GuestPool {
            slots,
            used: BTreeSet::new(),
        }
    }

    /// Take the lowest free slot's index, or `None` when the pool is full.
    pub(crate) fn allocate(&mut self) -> Option<u32> {
        let index = (0..self.slots).find(|i| !self.used.contains(i))?;
        self.used.insert(index);
        Some(index)
    }

    /// Return a slot to the pool. Idempotent: freeing an already-free slot is a
    /// no-op, so a doubled teardown never disturbs a slot since reallocated.
    pub(crate) fn free(&mut self, index: u32) {
        self.used.remove(&index);
    }
}

/// Apply a machine's egress plumbing (machine §5.2): create the tap device,
/// address it, enable forwarding, and load the nftables ruleset. Linux +
/// `CAP_NET_ADMIN` only; shells out to `ip`, `sysctl`, and `nft`. Returns an
/// error the caller logs and degrades on, booting the machine without a NIC
/// rather than failing it.
#[cfg(all(feature = "net", target_os = "linux"))]
pub mod apply {
    use super::*;
    use std::process::Command;

    /// Bring up the machine's tap at `host/30` and load `ruleset` via
    /// `nft -f -`. Forwarding is enabled node-wide: without it the kernel
    /// never consults the forward hook and no guest packet leaves the tap.
    pub fn install(
        machine: &EntityName,
        ruleset: &str,
        host: Ipv4Addr,
    ) -> Result<(), std::io::Error> {
        let tap = tap_name(machine);
        run(Command::new("ip").args(["tuntap", "add", "dev", &tap, "mode", "tap"]))?;
        run(Command::new("ip").args(["addr", "add", &format!("{host}/30"), "dev", &tap]))?;
        run(Command::new("ip").args(["link", "set", &tap, "up"]))?;
        run(Command::new("sysctl").args(["-w", "net.ipv4.ip_forward=1"]))?;
        use std::io::Write;
        let mut child = Command::new("nft")
            .args(["-f", "-"])
            .stdin(std::process::Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .expect("piped stdin")
            .write_all(ruleset.as_bytes())?;
        let status = child.wait()?;
        if !status.success() {
            return Err(std::io::Error::other(format!("nft failed: {status}")));
        }
        Ok(())
    }

    /// Tear down a machine's tap and ruleset on deactivation.
    pub fn remove(machine: &EntityName) {
        let tap = tap_name(machine);
        let chain = egress_table(machine);
        let _ = run(Command::new("nft").args(["delete", "table", "inet", &chain]));
        let _ = run(Command::new("ip").args(["link", "del", &tap]));
    }

    fn run(command: &mut Command) -> Result<(), std::io::Error> {
        let status = command.status()?;
        if status.success() {
            Ok(())
        } else {
            Err(std::io::Error::other(format!(
                "{command:?} failed: {status}"
            )))
        }
    }
}

/// The same seam where the plumbing cannot be realized (no `feature = "net"`,
/// or not Linux): every install fails, so a configured egress degrades to the
/// documented "booting without a NIC" path, and there is nothing to remove.
#[cfg(not(all(feature = "net", target_os = "linux")))]
pub mod apply {
    use super::*;

    pub fn install(
        _machine: &EntityName,
        _ruleset: &str,
        _host: Ipv4Addr,
    ) -> Result<(), std::io::Error> {
        Err(std::io::Error::other("egress needs feature `net` on linux"))
    }

    pub fn remove(_machine: &EntityName) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn machine() -> EntityName {
        EntityName::new("machine", "dev-box")
    }

    const CLUSTER: &[&str] = &["10.100.0.0/16"];

    #[test]
    fn tap_name_is_stable_and_within_the_interface_limit() {
        let name = tap_name(&machine());
        assert!(name.starts_with("hm-"));
        assert_eq!(name.len(), 11, "hm- + 8 hex fits the 15-char limit");
        assert_eq!(name, tap_name(&machine()), "stable per machine");
        assert_ne!(
            name,
            tap_name(&EntityName::new("machine", "other")),
            "distinct per machine (the attribution key)"
        );
    }

    #[test]
    fn every_forbidden_class_is_dropped_regardless_of_policy() {
        // M6: isolation holds under the most permissive policy. Each class is
        // its own terminal drop rule, not a drop somewhere in the ruleset.
        let rules = nft_ruleset(&machine(), &EgressPolicy::Open, CLUSTER, "eth0");
        for forbidden in [
            "10.0.0.0/8", // RFC1918 (other taps, host services)
            "172.16.0.0/12",
            "192.168.0.0/16",
            "169.254.0.0/16", // link-local + metadata 169.254.169.254
            "127.0.0.0/8",    // host loopback
            "10.100.0.0/16",  // the cluster's own range
        ] {
            assert!(
                rules.contains(&format!("\n    ip daddr {forbidden} counter drop")),
                "egress must drop {forbidden} (M6 no lateral movement)",
            );
        }
        for forbidden in ["fe80::/10", "fc00::/7", "::1/128"] {
            assert!(
                rules.contains(&format!("\n    ip6 daddr {forbidden} counter drop")),
                "egress must drop {forbidden} (M6 no lateral movement)",
            );
        }
    }

    #[test]
    fn the_base_chain_accepts_and_only_the_tap_is_judged() {
        // The base chain must not drop by policy: a base-chain drop is final,
        // so it would drop the uplink's replies (no connection could complete)
        // and every other machine's packets. Isolation comes from the tap's
        // own chain, whose every path ends in a terminal verdict.
        let rules = nft_ruleset(&machine(), &EgressPolicy::Open, CLUSTER, "eth0");
        let tap = tap_name(&machine());
        assert!(rules.contains("type filter hook forward priority 0; policy accept;"));
        let chain = tap.replace('-', "_");
        assert!(rules.contains(&format!("iifname \"{tap}\" jump egress_{chain}_out")));
        for policy in [
            EgressPolicy::Open,
            EgressPolicy::Allowlist(vec!["93.184.216.0/24".into()]),
            EgressPolicy::None,
        ] {
            let rules = nft_ruleset(&machine(), &policy, CLUSTER, "eth0");
            let out = rules.split("_out {\n").nth(1).expect("the tap's chain");
            let body = out.split("\n  }").next().expect("chain body");
            let last = body.lines().last().expect("a rule");
            assert!(
                last.contains("counter accept") || last.contains("counter drop"),
                "the tap's chain must end in a terminal verdict, got {last:?}",
            );
        }
    }

    #[test]
    fn open_policy_accepts_the_internet_after_the_drops() {
        let rules = nft_ruleset(&machine(), &EgressPolicy::Open, CLUSTER, "eth0");
        // The accept must come AFTER the forbidden drops.
        let accept = rules.find("policy: open").expect("open accept");
        let cluster_drop = rules.find("cluster-internal").expect("cluster drop");
        assert!(cluster_drop < accept, "drops precede the open accept");
        assert!(rules.contains("masquerade"), "NAT out the uplink");
    }

    #[test]
    fn allowlist_accepts_only_listed_destinations() {
        let policy = EgressPolicy::Allowlist(vec!["93.184.216.0/24".into()]);
        let rules = nft_ruleset(&machine(), &policy, CLUSTER, "eth0");
        assert!(rules.contains("ip daddr 93.184.216.0/24 counter accept"));
        assert!(
            rules.contains("policy: allowlist default") && rules.contains("drop"),
            "an allowlist denies everything not listed",
        );
        assert!(!rules.contains("policy: open"));
    }

    #[test]
    fn none_policy_drops_all_egress() {
        let rules = nft_ruleset(&machine(), &EgressPolicy::None, CLUSTER, "eth0");
        assert!(rules.contains("policy: none"));
        assert!(!rules.contains("accept comment \"policy"));
    }

    #[test]
    fn guest_net_carves_a_distinct_thirty_from_the_pool_per_slot() {
        let base: Ipv4Addr = "172.31.0.0".parse().unwrap();
        let a = guest_net(&machine(), base, 0);
        assert_eq!(a.host, Ipv4Addr::new(172, 31, 0, 1));
        assert_eq!(a.guest, Ipv4Addr::new(172, 31, 0, 2));
        assert_eq!(
            a.tap,
            tap_name(&machine()),
            "the tap is the attribution key"
        );

        // The next slot is the next /30: four addresses on.
        let b = guest_net(&machine(), base, 1);
        assert_eq!(b.host, Ipv4Addr::new(172, 31, 0, 5));
        assert_eq!(b.guest, Ipv4Addr::new(172, 31, 0, 6));

        // Same machine + slot is stable; the MAC is stable regardless of slot.
        assert_eq!(a, guest_net(&machine(), base, 0));
        assert_eq!(a.guest_mac, b.guest_mac, "MAC is per-machine, not per-slot");
        assert!(a.guest_mac.starts_with("02:"), "locally administered");
        assert_ne!(
            a.guest_mac,
            guest_mac(&EntityName::new("machine", "other")),
            "distinct per machine",
        );
    }

    #[test]
    fn the_guest_boot_arg_configures_eth0_from_the_pool() {
        let net = guest_net(&machine(), "172.31.0.0".parse().unwrap(), 0);
        assert_eq!(
            guest_ip_boot_arg(&net),
            "ip=172.31.0.2::172.31.0.1:255.255.255.252::eth0:off",
        );
    }

    #[test]
    fn the_pool_hands_out_the_lowest_free_slot_and_reclaims_it() {
        let mut pool = GuestPool::new(2);
        assert_eq!(pool.allocate(), Some(0));
        assert_eq!(pool.allocate(), Some(1));
        assert_eq!(pool.allocate(), None, "full pool degrades, not collides");
        pool.free(0);
        assert_eq!(pool.allocate(), Some(0), "a freed slot is reused");
        pool.free(1);
        pool.free(1); // idempotent: a doubled teardown is safe.
        assert_eq!(pool.allocate(), Some(1));
    }

    #[test]
    fn the_masquerade_counter_attributes_flows_per_machine() {
        // M6 attributable: the tap name is the key the NAT counter joins to.
        let rules = nft_ruleset(&machine(), &EgressPolicy::Open, CLUSTER, "wan0");
        let tap = tap_name(&machine());
        assert!(rules.contains(&format!(
            "iifname \"{tap}\" oifname \"wan0\" counter masquerade"
        )));
    }
}
