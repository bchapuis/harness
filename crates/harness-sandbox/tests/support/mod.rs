//! Test support: the machine-readable S-catalogue (sandbox spec §6),
//! mirroring the core, utilities, and harness catalogues in form and guarded
//! by the same drift-test pattern (`conformance_catalogue.rs`).

#![allow(dead_code)]

use actor_simulation::CatalogueEntry;
use actor_simulation::Verify;

/// The sandbox-tier invariant catalogue, S1–S5 (sandbox spec §6): machine
/// readable alongside this crate's conformance suite. `invariant: n` reads
/// as "Sn".
///
/// S4 is deliberately verified in the *harness* suite
/// (`harness/tests/conformance_sandbox.rs`): the acquisition discipline is
/// loop conduct, observed through the journal — a journal audit at
/// quiescence, H2-style, never a stream checker (harness spec §5.6 item 6).
pub fn s_catalogue() -> &'static [CatalogueEntry] {
    S_CATALOGUE
}

const S_CATALOGUE: &[CatalogueEntry] = &[
    CatalogueEntry {
        invariant: 1,
        spec: "sandbox §3.1, §3.2",
        property: "Workspace confinement: at every tier, no path outside the session's workspace is representable to a tool or a guest — confinement is by capability handle, never by sanitization of a representable escape",
        verify: &[
            Verify::CompileTime(
                "the cap-std Dir handle is the only filesystem capability the provider holds; \
                 the crate's one ambient-authority call opens the root",
            ),
            Verify::SimTest(
                "harness-sandbox/tests/workspace.rs (adversarial traversal); tests/native.rs \
                 (container confinement smoke: only the workspace mount reaches the host \
                 filesystem, no network); tests/firecracker.rs (microVM confinement smoke: \
                 only the synced workspace reaches the host, no network device, absolute \
                 symlinks dropped at the pull)",
            ),
        ],
    },
    CatalogueEntry {
        invariant: 2,
        spec: "sandbox §3.2",
        property: "Compute hermeticity: a Compute environment takes no ambient clock, entropy, network, or OS identity; its outputs are a function of the call, the workspace, and the injected seed",
        verify: &[
            Verify::CompileTime(
                "the wasmtime linker defines only the `harness` host module and deterministic \
                 WASI stubs; any other import fails instantiation",
            ),
            Verify::Differential(
                "harness-sandbox/tests/compute.rs and tests/quickjs.rs (same call + workspace + \
                 seed, twice); the QuickJS runner's imports are pinned to the host surface",
            ),
        ],
    },
    CatalogueEntry {
        invariant: 3,
        spec: "sandbox §3.3, §2.3",
        property: "Granted egress only: a Network environment's egress reaches only hosts on the allowlist its journaled acquisition granted; default-deny; per-session attributable",
        verify: &[Verify::SimTest(
            "future: no Network tier ships in this provider; the profile's egress allowlist is \
             digest-covered now (harness §7.1) so the contract is pinned before the dataplane",
        )],
    },
    CatalogueEntry {
        invariant: 4,
        spec: "sandbox §2.3, §4; harness §5.6, §6.4",
        property: "Journaled, monotone, capped acquisition: a TierAcquired record precedes the first effect at its tier within the activation; the held set only grows within an activation, never crosses an activation boundary, and never leaves the kind's cap",
        verify: &[Verify::SimTest(
            "harness/tests/conformance_sandbox.rs (tier journal audit at quiescence, \
             registration-time cap panic)",
        )],
    },
    CatalogueEntry {
        invariant: 5,
        spec: "sandbox §2.3; harness §5.3",
        property: "Per-tier release: release tears down every provisioned tier's environment and is idempotent; deactivation releases all held tiers (the per-tier face of harness H8)",
        verify: &[Verify::SimTest(
            "harness-sandbox/tests/workspace.rs (open/release accounting); tests/native.rs \
             (container removed on release, idempotent); tests/firecracker.rs (VM killed and \
             control directory removed on release, idempotent); the harness's existing H8 \
             checker covers the binding window",
        )],
    },
];
