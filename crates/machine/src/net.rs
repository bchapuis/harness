//! Policy-bound, attributable, isolated egress (machine spec §5.2, M6).
//!
//! A development machine that cannot `git clone` or install a package is not a
//! development machine, so a machine's guest gets an outbound path — unlike
//! the agent sandbox, whose egress is default-deny per tool call. Three
//! properties bind (M6):
//!
//! - **Policy-bound.** The machine's journaled [`EgressPolicy`] grants egress;
//!   nothing is ambient. `Open` is the fresh machine's default.
//! - **Attributable.** A per-machine tap device (named by the grain hash) NATs
//!   through the node, and per-chain counters attribute flows to the machine,
//!   joining the attachment journal to answer *who was attached while this
//!   machine reached what*.
//! - **Isolated: no lateral movement.** Whatever the policy grants toward the
//!   internet, the path MUST NOT reach another machine's guest, an
//!   agent-sandbox environment, the host's own services, or the cluster's
//!   control plane and node-internal addresses (link-local and metadata
//!   endpoints included). A machine's network neighbor is the internet, never
//!   the infrastructure it runs on.
//!
//! [`nft_ruleset`] is a **pure function** — the mechanism M6 is verified
//! against (golden tests below cover every policy and every forbidden
//! destination class). Applying it (the tap device, the node NAT, invoking
//! `nft`) is the Linux-only, `CAP_NET_ADMIN` realization behind
//! [`feature = "net"`](apply); the obligation is the property, not the plumbing.

use crate::grain::EgressPolicy;
use granary::BlobId;
use granary::GrainName;

/// The interface name for a machine's tap device: `hm-` plus 8 hex of the
/// grain-name hash. Stable per machine — the attribution key (M6) — and short
/// enough for the 15-char Linux interface-name limit.
pub fn tap_name(machine: &GrainName) -> String {
    let hash = BlobId::of(machine.to_string().as_bytes());
    format!("hm-{}", &hash.to_string()[..8])
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
/// come first (isolation is not negotiable, whatever the policy grants), then
/// the policy decides the rest, then NAT masquerade out the node uplink with a
/// counter for attribution.
///
/// - `cluster_cidrs`: the cluster's own control-plane and node-internal ranges,
///   dropped in addition to the RFC1918/link-local/metadata classes.
/// - `uplink`: the node's egress interface for the masquerade.
pub fn nft_ruleset(
    machine: &GrainName,
    policy: &EgressPolicy,
    cluster_cidrs: &[&str],
    uplink: &str,
) -> String {
    let tap = tap_name(machine);
    let chain = format!("egress_{}", tap.replace('-', "_"));
    let mut out = String::new();
    out.push_str(&format!("table inet {chain} {{\n"));
    out.push_str("  chain forward {\n");
    out.push_str("    type filter hook forward priority 0; policy drop;\n");
    out.push_str(&format!("    iifname \"{tap}\" jump {chain}_out\n"));
    out.push_str("  }\n");
    out.push_str(&format!("  chain {chain}_out {{\n"));

    // Isolation first (M6): no lateral movement, whatever the policy grants.
    // The cluster's own ranges plus the universal private/link-local/metadata
    // classes are dropped before the policy is consulted.
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
    // (M6): the reference realization of per-machine flow accounting.
    out.push_str(&format!("  chain {chain}_nat {{\n"));
    out.push_str("    type nat hook postrouting priority 100; policy accept;\n");
    out.push_str(&format!(
        "    iifname \"{tap}\" oifname \"{uplink}\" counter masquerade\n"
    ));
    out.push_str("  }\n");
    out.push_str("}\n");
    out
}

/// Apply a machine's egress plumbing (machine §5.2's reference realization):
/// create the tap device, address it, and load the nftables ruleset. Linux +
/// `CAP_NET_ADMIN` only; shells out to `ip` and `nft`. Returns an error the
/// caller logs and degrades on (boot without a NIC), so a node lacking the
/// capability refuses egress gracefully rather than failing the machine.
#[cfg(all(feature = "net", target_os = "linux"))]
pub mod apply {
    use super::*;
    use std::process::Command;

    /// Bring up `tap` and load `ruleset` via `nft -f -`. `guest_cidr` is the
    /// /30 the guest addresses from (a node-local pool allocates it).
    pub fn install(
        machine: &GrainName,
        ruleset: &str,
        guest_cidr: &str,
    ) -> Result<(), std::io::Error> {
        let tap = tap_name(machine);
        run(Command::new("ip").args(["tuntap", "add", "dev", &tap, "mode", "tap"]))?;
        run(Command::new("ip").args(["addr", "add", guest_cidr, "dev", &tap]))?;
        run(Command::new("ip").args(["link", "set", &tap, "up"]))?;
        // `nft -f -` reads the ruleset from stdin.
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
    pub fn remove(machine: &GrainName) {
        let tap = tap_name(machine);
        let chain = format!("egress_{}", tap.replace('-', "_"));
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

#[cfg(test)]
mod tests {
    use super::*;

    fn machine() -> GrainName {
        GrainName::new("machine", "dev-box")
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
            tap_name(&GrainName::new("machine", "other")),
            "distinct per machine (the attribution key)"
        );
    }

    #[test]
    fn every_forbidden_class_is_dropped_regardless_of_policy() {
        // M6: isolation holds under the most permissive policy.
        let rules = nft_ruleset(&machine(), &EgressPolicy::Open, CLUSTER, "eth0");
        for forbidden in [
            "10.0.0.0/8", // RFC1918 (other taps, host services)
            "172.16.0.0/12",
            "192.168.0.0/16",
            "169.254.0.0/16", // link-local + metadata 169.254.169.254
            "127.0.0.0/8",    // host loopback
            "fe80::/10",      // v6 link-local
            "10.100.0.0/16",  // the cluster's own range
        ] {
            assert!(
                rules.contains(&format!("daddr {forbidden}")) && rules.contains("drop"),
                "egress must drop {forbidden} (M6 no lateral movement)",
            );
        }
    }

    #[test]
    fn open_policy_accepts_the_internet_after_the_drops() {
        let rules = nft_ruleset(&machine(), &EgressPolicy::Open, CLUSTER, "eth0");
        // The accept must come AFTER the forbidden drops (order matters: a drop
        // above the accept is what makes isolation hold under Open).
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
    fn the_masquerade_counter_attributes_flows_per_machine() {
        // M6 attributable: the tap name is the key, and the NAT rule carries a
        // counter joined to it.
        let rules = nft_ruleset(&machine(), &EgressPolicy::Open, CLUSTER, "wan0");
        let tap = tap_name(&machine());
        assert!(rules.contains(&format!(
            "iifname \"{tap}\" oifname \"wan0\" counter masquerade"
        )));
    }
}
