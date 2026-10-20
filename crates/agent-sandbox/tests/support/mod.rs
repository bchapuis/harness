//! Test support: the machine-readable S-catalogue (sandbox spec §7),
//! mirroring the core, utilities, and agent catalogues in form and guarded
//! by the same drift-test pattern (`conformance_catalogue.rs`).

#![allow(dead_code)]

use actor_simulation::CatalogueEntry;
use actor_simulation::Verify;

/// The sandbox-tier invariant catalogue, S1–S5 (sandbox spec §7): machine
/// readable alongside this crate's conformance suite. `invariant: n` reads
/// as "Sn".
///
/// S4 is deliberately verified in the *agent* suite
/// (`agent/tests/conformance_sandbox.rs`): the acquisition discipline is
/// loop conduct, observed through the journal; a write-ahead-order audit at
/// quiescence (agent §4.3, E5), never a stream checker (agent spec §5.5
/// item 6).
///
/// Each `verify` entry names suites and nothing else, as `Verify::SimTest`
/// specifies. What each suite contributes is the spec row's to say, and §7 says
/// it: keeping the explanation in one of the two copies rather than both is what
/// lets the other be checked.
pub fn s_catalogue() -> &'static [CatalogueEntry] {
    S_CATALOGUE
}

const S_CATALOGUE: &[CatalogueEntry] = &[
    CatalogueEntry {
        invariant: 1,
        spec: "sandbox §5.1, §5.2",
        property: "Workspace confinement: at every tier, no path outside the session's workspace is representable to a tool or a guest; confinement is by capability handle, never by sanitization of a representable escape",
        verify: &[
            Verify::CompileTime(
                "the cap-std Dir handle is the only filesystem capability the provider holds, \
                 and workspace contents are reached only through it; the crate's ambient \
                 calls are the open, the loss stat §6 requires, the mount and tar host \
                 path, and the microVM's assets",
            ),
            Verify::SimTest("workspace.rs, quickjs.rs, native.rs, firecracker.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 2,
        spec: "sandbox §5.2",
        property: "Compute hermeticity: a Compute environment takes no ambient clock, entropy, network, or OS identity; its outputs are a function of the call, the workspace, and the injected seed",
        verify: &[
            Verify::CompileTime(
                "the wasmtime linker defines only the `harness` host module and deterministic \
                 WASI stubs; any other import fails instantiation",
            ),
            Verify::Differential("compute.rs, quickjs.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 3,
        spec: "sandbox §5.3, §4.3",
        property: "Granted egress only: a Network environment's egress reaches only hosts on the allowlist its journaled acquisition granted; default-deny; per-session attributable",
        verify: &[Verify::Deferred(
            "no Network tier ships in this provider, so nothing exercises the allowlist; the \
             profile's egress allowlist is digest-covered now (agent §6.2), which pins the \
             contract ahead of the dataplane that will have to honour it",
        )],
    },
    CatalogueEntry {
        invariant: 4,
        spec: "sandbox §4.3, §6; agent §5.5, §4.3",
        property: "Journaled, monotone, capped acquisition: a TierAcquired record precedes the first effect at its tier within the activation; the held set only grows within an activation, never crosses an activation boundary, and never leaves the kind's cap",
        verify: &[Verify::SimTest("agent/tests/conformance_sandbox.rs")],
    },
    CatalogueEntry {
        invariant: 5,
        spec: "sandbox §4.3; agent §3.3",
        property: "Per-tier release: release tears down every provisioned tier's environment and is idempotent; deactivation releases all held tiers (the per-tier face of agent A7)",
        verify: &[Verify::SimTest(
            "workspace.rs, compute.rs, native.rs, firecracker.rs",
        )],
    },
];
