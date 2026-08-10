//! The §18.5 invariant catalogue (spec §17, §18.5, §18.6).
//!
//! A machine-readable table linking each of the 22 §18.5 invariants to *how* it
//! is verified; the invariant *mechanism* is [`crate::invariant`]. The
//! `conformance_catalogue` integration test asserts this table stays consistent
//! with [`default_invariants`](crate::default_invariants), so a checker added in
//! code but not recorded here, or the reverse, fails the build.

/// How a §18.5 invariant is verified — the machine-readable form of the §17
/// conformance table's "Verified by" column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verify {
    /// A continuous [`Invariant`](crate::Invariant) in
    /// [`default_invariants`](crate::default_invariants), named by its
    /// [`Invariant::name`](crate::Invariant::name). Cross-checked against the
    /// live checker set.
    Checker(&'static str),
    /// One or more example/conformance tests, named as comma-separated files
    /// under this crate's `tests/` directory — or, where the suite that decides an
    /// invariant lives in another crate, as a path relative to `crates/`. The
    /// `conformance_catalogue` test machine-verifies each named file exists, so a
    /// renamed or deleted test fails the build.
    ///
    /// Files and nothing else. What a given suite contributes belongs in the spec
    /// row, which has the room for it; a payload that mixes the two cannot be
    /// checked, which is the whole point of writing it here as well.
    SimTest(&'static str),
    /// A `trybuild` compile-fail case asserting invalid code is rejected (#20).
    CompileFail(&'static str),
    /// A local-vs-remote differential test (#21).
    Differential(&'static str),
    /// Enforced at compile time by a trait bound or exhaustive enum — no runtime
    /// test is possible or needed.
    CompileTime(&'static str),
    /// Nothing verifies this yet, and the string says why not.
    ///
    /// Distinct from the others because it is the one entry that names no test,
    /// no checker, and no guarantee. It was a `SimTest` whose payload was a
    /// sentence beginning "future:", which reads as verification to anything that
    /// does not parse the prose — including the drift tests, which found a file
    /// list with no files in it and had nothing to say. A deferral is a real state
    /// for an invariant to be in (sandbox §7 keeps a list of them); it just has to
    /// be able to announce itself.
    Deferred(&'static str),
}

impl Verify {
    /// The text this entry carries, whatever it names.
    ///
    /// Every variant wraps one string — a checker's name, a comma-separated file
    /// list, a trybuild path, a sentence explaining a compile-time guarantee or a
    /// deferral. The spec↔catalogue comparison
    /// ([`spec_xref::catalogue`](https://docs.rs/spec-xref)) takes them all and
    /// picks out the suite names itself, so a caller need not sort by variant to
    /// hand it the pointers.
    pub const fn text(&self) -> &'static str {
        match self {
            Self::Checker(t)
            | Self::SimTest(t)
            | Self::CompileFail(t)
            | Self::Differential(t)
            | Self::CompileTime(t)
            | Self::Deferred(t) => t,
        }
    }
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

/// Which table an invariant number belongs to. The two are numbered
/// independently — core #2 and utilities U2 are unrelated claims — so a
/// coverage row naming a bare number has to say which.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Catalogue {
    /// The core catalogue, #1–#22 ([`core_catalogue`]).
    Core,
    /// The cluster-utilities catalogue, U1, U2, … ([`utilities_catalogue`]).
    Utilities,
}

/// One catalogued invariant that one continuous checker enforces — the pairing
/// a `Verify::Checker` row asserts, stated from the checker's side.
///
/// Both sides are needed because neither is derivable from the other: the
/// catalogue is what the spec's "Verified by" column mirrors, and the coverage
/// table is what an edit to an `observe` body invalidates. The
/// `conformance_catalogue` test holds them equal
/// (see [`checker_coverage`](crate::checker_coverage)).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CheckerCoverage {
    /// The checker's [`Invariant::name`](crate::Invariant::name).
    pub checker: &'static str,
    pub catalogue: Catalogue,
    /// The invariant's number within [`catalogue`](CheckerCoverage::catalogue).
    pub invariant: u8,
}

impl CheckerCoverage {
    /// `checker` enforces core invariant `#invariant`.
    pub const fn core(checker: &'static str, invariant: u8) -> CheckerCoverage {
        CheckerCoverage {
            checker,
            catalogue: Catalogue::Core,
            invariant,
        }
    }

    /// `checker` enforces utilities invariant `U<invariant>`.
    pub const fn utilities(checker: &'static str, invariant: u8) -> CheckerCoverage {
        CheckerCoverage {
            checker,
            catalogue: Catalogue::Utilities,
            invariant,
        }
    }
}

/// The §18.5 invariant catalogue (#1–#22): the single source of truth linking
/// each invariant to the code that verifies it (spec §17, §18.5). Kept
/// consistent with [`default_invariants`](crate::default_invariants) by the
/// `conformance_catalogue` test.
pub fn core_catalogue() -> &'static [CatalogueEntry] {
    CATALOGUE
}

/// The cluster-utilities invariant catalogue (utilities spec §6): U1, U2, …,
/// numbered apart from the core #1–#22 because the utilities are specified in
/// their own document (`cluster-utilities-spec.md`). `invariant: n` here reads
/// as "Un". Guarded by the same `conformance_catalogue` drift test as the core
/// table.
pub fn utilities_catalogue() -> &'static [CatalogueEntry] {
    UTILITIES_CATALOGUE
}

const CATALOGUE: &[CatalogueEntry] = &[
    CatalogueEntry {
        invariant: 1,
        spec: "§7.2, §14",
        property: "No silent loss: every ask reaches exactly one outcome; none pending at quiescence",
        verify: &[
            Verify::Checker("no-silent-loss"),
            Verify::SimTest("conformance_swarm.rs, conformance_messaging.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 2,
        // §9.4 is where `down` is declared, and the MUST is stated against `down`;
        // §10's confirmed `unreachable` only MAY complete the ask early (§18.5 #2).
        spec: "§7.2, §9.4",
        property: "An ask to a downed node completes with Unreachable, never hangs",
        verify: &[Verify::SimTest(
            "conformance_faults.rs, conformance_membership.rs",
        )],
    },
    CatalogueEntry {
        invariant: 3,
        spec: "§6",
        property: "Per-pair FIFO: messages from one sender to one recipient observed in send order",
        verify: &[Verify::SimTest(
            "conformance_messaging.rs, conformance_faults.rs",
        )],
    },
    CatalogueEntry {
        invariant: 4,
        spec: "§6",
        property: "Serial, non-reentrant execution: an actor never dispatches two messages at once",
        verify: &[
            Verify::Checker("serial-execution"),
            Verify::SimTest("conformance_messaging.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 5,
        spec: "§6",
        property: "Bounded, non-dropping mailbox: a full mailbox blocks or returns MailboxFull",
        verify: &[Verify::SimTest("conformance_messaging.rs")],
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
        verify: &[Verify::SimTest("conformance_serialization.rs")],
    },
    CatalogueEntry {
        invariant: 9,
        spec: "§4.3, §4.4",
        property: "Local sends skip serialization, with a result identical to the remote path",
        verify: &[Verify::SimTest("conformance_messaging.rs")],
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
        verify: &[Verify::SimTest("conformance_deathwatch.rs")],
    },
    CatalogueEntry {
        invariant: 12,
        spec: "§12",
        property: "Watching an already-terminated actor yields Terminated immediately",
        verify: &[Verify::SimTest("conformance_deathwatch.rs")],
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
        spec: "§9.2, §9.4",
        property: "Membership convergence once faults cease and partitions heal — by anti-entropy (gossip-based), registry sync (registry-based), or log replication (leader-based)",
        verify: &[Verify::SimTest(
            "conformance_membership.rs, conformance_registry.rs, conformance_leader.rs",
        )],
    },
    CatalogueEntry {
        invariant: 15,
        spec: "§9.1",
        property: "down is terminal: a node observed down never reappears up at the same incarnation",
        verify: &[
            Verify::Checker("down-is-terminal"),
            Verify::SimTest("conformance_membership.rs, conformance_join.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 16,
        spec: "§9.4",
        property: "Partition tolerance: under the default policy a partition alone never downs a member — unconditionally in registry-based mode (observe-only detector), and on any quorum-less side in leader-based mode (#22)",
        verify: &[Verify::SimTest(
            "conformance_membership.rs, conformance_registry.rs, conformance_leader.rs",
        )],
    },
    CatalogueEntry {
        invariant: 17,
        spec: "§10",
        property: "SWIM refutation: a suspected node refutes via a higher incarnation",
        verify: &[Verify::SimTest("conformance_membership.rs")],
    },
    CatalogueEntry {
        invariant: 18,
        spec: "§11",
        property: "Supervision containment: a panic never crashes the node; default Stop; restarts back off",
        verify: &[Verify::SimTest("conformance_supervision.rs")],
    },
    CatalogueEntry {
        invariant: 19,
        spec: "§13",
        property: "Receptionist consistency: pruned on node down; subscribe delivers snapshot then changes",
        verify: &[Verify::SimTest("conformance_receptionist.rs")],
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
        verify: &[Verify::Differential("conformance_messaging.rs")],
    },
    CatalogueEntry {
        invariant: 22,
        spec: "§9.4.3",
        property: "Quorum-gated control plane: every transition is a quorum-committed log entry applied in log order; at most one leader per term; a minority never evicts the majority",
        verify: &[
            Verify::Checker("one-leader-per-term"),
            Verify::SimTest("conformance_leader.rs, conformance_restart.rs, conformance_swarm.rs"),
        ],
    },
];

const UTILITIES_CATALOGUE: &[CatalogueEntry] = &[
    CatalogueEntry {
        invariant: 1, // U1
        spec: "utilities §2",
        property: "Deterministic placement: a pure, version-stable function of the serving set and key — identical serving sets compute identical owners, and a single-member change reassigns only the keys that member owned or now owns",
        // No continuous checker: placement is a pure function off the event stream;
        // per-decision events would flood the stream without enabling any check the
        // property tests do not already perform (utilities spec §5).
        verify: &[Verify::SimTest("conformance_placement.rs")],
    },
    CatalogueEntry {
        invariant: 2, // U2
        spec: "utilities §4",
        property: "Singleton activation discipline: a node never runs two live activations of one name concurrently; a healed, converged cluster runs exactly one per name; an anchor failure re-activates within bounded logical time",
        verify: &[
            Verify::Checker("singleton-at-most-one-per-node"),
            Verify::SimTest("conformance_singleton.rs, conformance_swarm.rs"),
        ],
    },
];
