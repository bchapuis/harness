//! The machine-readable D-catalogue, D1–D7 (drive spec §9).
//!
//! The drive analogue of the core, entity, and harness catalogues: one table
//! linking each invariant to the code that verifies it, guarded by the drift test
//! in `conformance_catalogue.rs`. `invariant: n` reads as "Dn".
//!
//! `SimTest` pointers name files under this crate's `tests/` directory. The
//! `Checker` entries name `drive-cycle`, the one continuous checker this crate
//! ships (`drive::testing`), and are held equal to its declared coverage per
//! `(checker, invariant)` pair.

#![allow(dead_code)]

use actor_simulation::CatalogueEntry;
use actor_simulation::Verify;

/// The drive invariant catalogue, D1–D7 (drive spec §9).
pub fn d_catalogue() -> &'static [CatalogueEntry] {
    D_CATALOGUE
}

const D_CATALOGUE: &[CatalogueEntry] = &[
    CatalogueEntry {
        invariant: 1,
        spec: "drive §3.1",
        property: "Desired-set purity: pending is a pure, deterministic function of committed state; two activations folding the same journal prefix compute the same desired set, and no ephemeral state is reachable from it",
        verify: &[
            Verify::Differential("conformance_cycle.rs, conformance_resume.rs"),
            Verify::SimTest("drive_swarm.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 2,
        spec: "drive §3.2, §3.4, §6",
        property: "Launch coverage: once faults cease and an activation is live, every member of the desired set is in flight, settled, or waiting out its backoff; a member at the cap is at rest and is re-driven by the next activation a caller or alarm reaches",
        verify: &[
            Verify::Checker("drive-cycle"),
            Verify::SimTest("conformance_cycle.rs, drive_swarm.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 3,
        spec: "drive §3.3",
        property: "Launch-once per claim: an activation launches each key at most once per claim and a re-drive never relaunches an effect in flight; across activations re-launch is permitted and the settled record is the durable fact",
        verify: &[
            Verify::Checker("drive-cycle"),
            Verify::SimTest("conformance_dangling.rs, drive_swarm.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 4,
        spec: "drive §5",
        property: "Intent before effect: an effect is launched only after the records that placed it in the desired set have committed",
        verify: &[Verify::SimTest("conformance_cycle.rs, drive_swarm.rs")],
    },
    CatalogueEntry {
        invariant: 5,
        spec: "drive §4.5, §3.1",
        property: "Straggler rejection: an outcome whose key is absent from the current desired set is discarded, never settled, and writes no record; exact because keys are unique over the journal",
        verify: &[
            Verify::Checker("drive-cycle"),
            Verify::SimTest("conformance_straggler.rs, drive_swarm.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 6,
        spec: "drive §4.4",
        property: "Dangling resolution: after rehydration every member of the first drive's desired set is re-launched or settled Interrupted per its declared policy; none is dropped and none is resolved twice",
        verify: &[
            Verify::SimTest("conformance_dangling.rs, drive_swarm.rs"),
            Verify::Differential("conformance_resume.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 7,
        spec: "drive §6, §3.4",
        property: "Quiescence: an activation permits hibernation exactly when nothing is in flight and every member of its desired set is at rest; launchable work vetoes eviction until it settles or backs off to the cap, and an entity hibernated while quiescent re-derives the same set on activation",
        verify: &[
            Verify::Checker("drive-cycle"),
            Verify::SimTest("conformance_cycle.rs, drive_swarm.rs"),
        ],
    },
];
