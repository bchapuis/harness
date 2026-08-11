//! The machine-readable M-catalogue, M1–M6 (machine spec §7).
//!
//! The machine's analogue of the core, utilities, harness, sandbox, and granary
//! catalogues: one table linking each invariant to the code that verifies it,
//! guarded by the drift test in `conformance_catalogue.rs`. `invariant: n` reads
//! as "Mn".
//!
//! The machine's verification is spread across three crates — the grain's own
//! suites here, the SSH front door's in `machine-frontdoor`, and the egress rule
//! generator's unit tests in this crate's `src/net.rs` — so a pointer containing a
//! `/` is resolved relative to `crates/` and a bare filename relative to this
//! crate's `tests/`. Without that, M4 and M6 would have to be recorded as prose,
//! which is the form that rots.

#![allow(dead_code)]

use actor_simulation::CatalogueEntry;
use actor_simulation::Verify;

/// The machine invariant catalogue, M1–M6 (machine spec §7).
pub fn m_catalogue() -> &'static [CatalogueEntry] {
    M_CATALOGUE
}

const M_CATALOGUE: &[CatalogueEntry] = &[
    CatalogueEntry {
        invariant: 1,
        spec: "machine §4, §6; granary §8, §7.15",
        property: "Single disk, never forked: only the shard leader appends, term-fenced (G1), and each capture is one atomic record (G19), so two activations never both commit disk state",
        verify: &[Verify::TestFn(
            "the_machine_survives_the_attach_crash_partition_failover_reconnect_story, \
             machine_invariants_hold_under_the_cluster_swarm",
        )],
    },
    CatalogueEntry {
        invariant: 2,
        spec: "machine §4, §6",
        property: "Lossless across graceful boundaries: a machine hibernated when idle, or migrated cooperatively, re-activates with the disk as of its final capture command; no write preceding a graceful boundary is lost",
        verify: &[Verify::TestFn(
            "attach_capture_detach_hibernate_reattach_round_trips, \
             workspace_files_survive_boot_capture_and_hibernation",
        )],
    },
    CatalogueEntry {
        invariant: 3,
        spec: "machine §4, §6",
        property: "Bounded crash window: on ungraceful node loss the disk rewinds to the last durable capture, never a fork and never a torn image; the window is the capture cadence",
        verify: &[Verify::TestFn(
            "the_machine_survives_the_attach_crash_partition_failover_reconnect_story, \
             machine_invariants_hold_under_the_cluster_swarm",
        )],
    },
    CatalogueEntry {
        invariant: 4,
        spec: "machine §5.1",
        property: "Authenticated, attributable, isolated ingress: a session is bridged only on possession of a key the machine's journaled policy authorizes; each attachment and detachment is journaled with its principal; a bridged session reaches only its own machine's guest",
        verify: &[Verify::TestFn(
            "an_authorized_key_attaches_and_bridges_an_exec_channel, \
             an_unauthorized_key_is_rejected, \
             each_machine_draws_its_own_host_key_and_keeps_it_across_hibernation, \
             front_door_death_folds_detached_and_releases_the_pin",
        )],
    },
    CatalogueEntry {
        invariant: 5,
        spec: "machine §4, §6",
        property: "A deposed activation self-fences within the lease: while attached it commits a fenced append once per lease interval, and failing that stops the microVM and severs its attachments",
        verify: &[Verify::TestFn(
            "the_machine_survives_the_attach_crash_partition_failover_reconnect_story, \
             machine_invariants_hold_under_the_cluster_swarm",
        )],
    },
    CatalogueEntry {
        invariant: 6,
        spec: "machine §5.2",
        property: "Policy-bound, attributable, isolated egress: a guest's outbound path is exactly what the journaled policy grants, attributable per machine, and can never reach another machine's guest, a sandbox environment, the host's services, or cluster-internal addresses",
        verify: &[Verify::TestFn(
            "every_forbidden_class_is_dropped_regardless_of_policy, \
             open_policy_accepts_the_internet_after_the_drops, \
             allowlist_accepts_only_listed_destinations, none_policy_drops_all_egress, \
             the_masquerade_counter_attributes_flows_per_machine",
        )],
    },
];
