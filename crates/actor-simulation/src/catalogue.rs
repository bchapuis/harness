//! The §18.5 invariant catalogue (spec §17, §18.5, §18.6).
//!
//! The conformance-traceability concern, kept apart from the invariant
//! *mechanism* in [`crate::invariant`]: a machine-readable table linking each of
//! the 21 §18.5 invariants to *how* it is verified. The `conformance_catalogue`
//! integration test asserts it stays consistent with
//! [`default_invariants`](crate::default_invariants), so a checker added in code
//! but not recorded here (or vice versa) fails the build.

/// How a §18.5 invariant is verified — the machine-readable form of the §17
/// conformance table's "Verified by" column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verify {
    /// A continuous [`Invariant`](crate::Invariant) in
    /// [`default_invariants`](crate::default_invariants), named by its
    /// [`Invariant::name`](crate::Invariant::name). Cross-checked against the
    /// live checker set.
    Checker(&'static str),
    /// One or more example/conformance tests (a human-readable file pointer;
    /// not machine-verified to exist).
    SimTest(&'static str),
    /// A `trybuild` compile-fail case asserting invalid code is rejected (#20).
    CompileFail(&'static str),
    /// A local-vs-remote differential test (#21).
    Differential(&'static str),
    /// Enforced at compile time by a trait bound or exhaustive enum — no runtime
    /// test is possible or needed.
    CompileTime(&'static str),
}

/// One row of the §18.5 invariant catalogue: the invariant number, the spec
/// sections that define it, a one-line property, and how it is verified.
#[derive(Clone, Copy, Debug)]
pub struct CatalogueEntry {
    pub invariant: u8,
    pub spec: &'static str,
    pub property: &'static str,
    pub verify: &'static [Verify],
}

/// The §18.5 invariant catalogue (#1–#21): the single source of truth linking
/// each invariant to the code that verifies it (spec §17, §18.5). Kept
/// consistent with [`default_invariants`](crate::default_invariants) by the
/// `conformance_catalogue` test.
pub fn catalogue() -> &'static [CatalogueEntry] {
    CATALOGUE
}

const CATALOGUE: &[CatalogueEntry] = &[
    CatalogueEntry {
        invariant: 1,
        spec: "§7.2, §14",
        property: "No silent loss: every ask reaches exactly one outcome; none pending at quiescence",
        verify: &[
            Verify::Checker("no-silent-loss"),
            Verify::SimTest("swarm.rs, conformance_messaging.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 2,
        spec: "§7.2, §10",
        property: "An ask to a downed node completes with Unreachable, never hangs",
        verify: &[Verify::SimTest("failure.rs, conformance_faults.rs")],
    },
    CatalogueEntry {
        invariant: 3,
        spec: "§6",
        property: "Per-pair FIFO: messages from one sender to one recipient observed in send order",
        verify: &[Verify::SimTest("actor.rs, cluster.rs, conformance_faults.rs")],
    },
    CatalogueEntry {
        invariant: 4,
        spec: "§6",
        property: "Serial, non-reentrant execution: an actor never dispatches two messages at once",
        verify: &[
            Verify::Checker("serial-execution"),
            Verify::SimTest("actor.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 5,
        spec: "§6",
        property: "Bounded, non-dropping mailbox: a full mailbox blocks or returns MailboxFull",
        verify: &[Verify::SimTest("actor.rs, conformance_messaging.rs")],
    },
    CatalogueEntry {
        invariant: 6,
        spec: "§4.2",
        property: "Lifecycle order and exactly-once: assign_id → actor_ready → resign_id",
        verify: &[
            Verify::Checker("lifecycle-exactly-once"),
            Verify::SimTest("conformance_lifecycle.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 7,
        spec: "§4.3",
        property: "resolve classifies locality with no network round-trip",
        verify: &[Verify::SimTest("conformance_lifecycle.rs")],
    },
    CatalogueEntry {
        invariant: 8,
        spec: "§4.4, §5, §15",
        property: "Manifest dispatch and allowlist: unregistered (type, manifest) → Unhandled",
        verify: &[Verify::SimTest("conformance_serialization.rs, wire.rs")],
    },
    CatalogueEntry {
        invariant: 9,
        spec: "§4.3, §4.4",
        property: "Local sends skip serialization, with a result identical to the remote path",
        verify: &[Verify::SimTest("cluster.rs")],
    },
    CatalogueEntry {
        invariant: 10,
        spec: "§4.4",
        property: "An ActorRef in a message/reply is rebound to the receiving system on decode",
        verify: &[Verify::SimTest("conformance_messaging.rs")],
    },
    CatalogueEntry {
        invariant: 11,
        spec: "§12",
        property: "Death-watch exactly-once, including NodeDown",
        verify: &[Verify::SimTest("conformance_deathwatch.rs, watch.rs")],
    },
    CatalogueEntry {
        invariant: 12,
        spec: "§12",
        property: "Watching an already-terminated actor yields Terminated immediately",
        verify: &[Verify::SimTest("watch.rs")],
    },
    CatalogueEntry {
        invariant: 13,
        spec: "§12",
        property: "Signal ordering: Terminated delivered through the mailbox in serial order",
        verify: &[
            Verify::Checker("signal-in-band"),
            Verify::SimTest("conformance_deathwatch.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 14,
        spec: "§9.2",
        property: "Membership convergence once faults cease and partitions heal",
        verify: &[Verify::SimTest("gossip.rs")],
    },
    CatalogueEntry {
        invariant: 15,
        spec: "§9.1",
        property: "down is terminal: a node observed down never reappears up at the same incarnation",
        verify: &[
            Verify::Checker("down-is-terminal"),
            Verify::SimTest("failure.rs, conformance_join.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 16,
        spec: "§9.2",
        property: "Partition tolerance: under the default policy a partition alone never downs a member",
        verify: &[Verify::SimTest("failure.rs, conformance_membership.rs")],
    },
    CatalogueEntry {
        invariant: 17,
        spec: "§10",
        property: "SWIM refutation: a suspected node refutes via a higher incarnation",
        verify: &[Verify::SimTest("gossip.rs, conformance_membership.rs")],
    },
    CatalogueEntry {
        invariant: 18,
        spec: "§11",
        property: "Supervision containment: a panic never crashes the node; default Stop; restarts back off",
        verify: &[Verify::SimTest("supervision.rs, escalation.rs")],
    },
    CatalogueEntry {
        invariant: 19,
        spec: "§13",
        property: "Receptionist consistency: pruned on node down; subscribe delivers snapshot then changes",
        verify: &[Verify::SimTest("receptionist.rs, conformance_receptionist.rs")],
    },
    CatalogueEntry {
        invariant: 20,
        spec: "§3.3",
        property: "Type-safety: an ask/tell of a message the actor has no Handler for does not compile",
        verify: &[Verify::CompileFail("actor-core/tests/compile_fail")],
    },
    CatalogueEntry {
        invariant: 21,
        spec: "§3.3",
        property: "Location transparency: local vs remote target produce identical replies and ordering",
        verify: &[Verify::Differential("cluster.rs")],
    },
];
