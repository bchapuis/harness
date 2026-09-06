//! The machine-readable W-catalogue, W1–W7 (workflow spec §9).
//!
//! The workflow analogue of the core, entity, and agent catalogue: one table
//! linking each invariant to the code that verifies it, guarded by the drift test
//! in `conformance_catalogue.rs`. `invariant: n` reads as "Wn".
//!
//! `SimTest` pointers name files under this crate's `tests/` directory. The
//! `Checker` entries name `workflow-cycle`, the one continuous checker this crate
//! ships (`workflow::testing`), and are held equal to its declared coverage per
//! `(checker, invariant)` pair.

#![allow(dead_code)]

use actor_simulation::CatalogueEntry;
use actor_simulation::Verify;

/// The workflow invariant catalogue, W1–W7 (workflow spec §9).
pub fn w_catalogue() -> &'static [CatalogueEntry] {
    W_CATALOGUE
}

const W_CATALOGUE: &[CatalogueEntry] = &[
    CatalogueEntry {
        invariant: 1,
        spec: "workflow §3.1",
        property: "Desired-set purity: pending is a pure, deterministic function of committed state; two activations folding the same journal prefix compute the same desired set, and no ephemeral state is reachable from it",
        verify: &[
            Verify::Differential("conformance_cycle.rs, conformance_resume.rs"),
            Verify::SimTest("workflow_swarm.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 2,
        spec: "workflow §3.2, §3.4, §6",
        property: "Launch coverage: once faults cease and an activation is live, every member of the desired set is in flight, settled, or waiting out its backoff; a member at the cap is at rest and is re-driven by the next activation a caller or alarm reaches",
        verify: &[
            Verify::Checker("workflow-cycle"),
            Verify::SimTest("conformance_cycle.rs, workflow_swarm.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 3,
        spec: "workflow §3.3",
        property: "Launch-once per claim: an activation launches each key at most once per claim and a later tick never relaunches an effect in flight; across activations re-launch is permitted and the settled record is the durable fact",
        verify: &[
            Verify::Checker("workflow-cycle"),
            Verify::SimTest("conformance_orphan.rs, workflow_swarm.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 4,
        spec: "workflow §5",
        property: "Intent before effect: an effect is launched only after the records that placed it in the desired set have committed",
        verify: &[Verify::SimTest("conformance_cycle.rs, workflow_swarm.rs")],
    },
    CatalogueEntry {
        invariant: 5,
        spec: "workflow §4.5, §3.1",
        property: "Straggler rejection: an outcome whose key is absent from the current desired set is discarded, never settled, and writes no record; exact because keys are unique over the journal",
        verify: &[
            Verify::Checker("workflow-cycle"),
            Verify::SimTest("conformance_straggler.rs, workflow_swarm.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 6,
        spec: "workflow §4.4",
        property: "Orphan resolution: after rehydration every member of the first tick's desired set is re-launched or settled Interrupted per its declared policy; none is dropped and none is resolved twice",
        verify: &[
            Verify::SimTest("conformance_orphan.rs, workflow_swarm.rs"),
            Verify::Differential("conformance_resume.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 7,
        spec: "workflow §6, §3.4",
        property: "Quiescence: an activation permits hibernation exactly when nothing is in flight and every member of its desired set is at rest; launchable work vetoes eviction until it settles or backs off to the cap, and an entity hibernated while quiescent re-derives the same set on activation",
        verify: &[
            Verify::Checker("workflow-cycle"),
            Verify::SimTest("conformance_cycle.rs, workflow_swarm.rs"),
        ],
    },
];
