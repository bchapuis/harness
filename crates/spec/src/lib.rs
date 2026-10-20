//! The specifications, read mechanically, so the prose fails when the tree moves.
//!
//! `docs/` describes a tree that keeps changing under it. Prose does not fail on
//! its own, so a description that has come loose reads exactly like one that has
//! not: the edit that broke it was local and the sentences it broke were not.
//! This crate reads the specifications the way a compiler reads a source file and
//! reports the places they no longer describe anything, one module per way the
//! prose comes loose:
//!
//! - [`citations`]: a `§` reference resolves to a section that exists. `entity
//!   §7.12` after §7 is renumbered is the ordinary break, and the specs are
//!   layered so that each cites the ones beneath it, which makes the citation the
//!   load-bearing part of the layering rather than a convenience.
//! - [`identifiers`]: a backticked `EntityHandler` or `load_snapshot()` still
//!   names something the tree declares, and so does every name a rust fence uses
//!   without declaring it. The same failure one level down: a rename moves the
//!   code and leaves the prose behind.
//! - [`catalogue`]: a spec's invariant table and the Rust copy beside the suite
//!   that verifies it agree on which invariants exist, where they are defined, and
//!   what verifies them. Two hand-maintained copies of one fact, previously
//!   compared by nothing.
//! - [`ranges`]: a spec naming a sibling's catalogue by its extent, `E1–E20`,
//!   still names the whole of it. The third way one document falls behind another:
//!   the citation resolves, the identifiers resolve, and the extent is a release
//!   out of date because the catalogue it names grew a row.
//!
//! All four read [`DOCUMENTS`], the table of what the specification set contains.
//! A spec absent from that table is checked by nothing, which is why
//! `tests/citations.rs` holds the table equal to `docs/`.
//!
//! Nothing here is used at run time. Whatever a gate can decide from `docs/`
//! alone runs from this crate's own `tests/`, which is all of the first two, all
//! of the fourth, and the half of the third that holds each catalogue site
//! parsing. The comparison against a Rust catalogue runs from the owning crate's
//! `conformance_catalogue.rs` instead, because a Rust catalogue lives in a
//! `tests/` module only its own crate can import.

pub mod architecture;
pub mod catalogue;
pub mod citations;
pub mod identifiers;
mod markdown;
pub mod ranges;
pub mod retarget;

use std::path::Path;
use std::path::PathBuf;

/// The workspace root, from a crate's `CARGO_MANIFEST_DIR`.
///
/// Every member sits at `crates/<name>`, so the root is two levels up. Tests read
/// `docs/` relative to it: `spec::workspace_root(env!("CARGO_MANIFEST_DIR"))`.
///
/// # Panics
///
/// If `manifest_dir` is not two levels below a root, which for a workspace member
/// means the layout changed and every caller's paths are wrong anyway.
pub fn workspace_root(manifest_dir: &str) -> PathBuf {
    Path::new(manifest_dir)
        .ancestors()
        .nth(2)
        .expect("a workspace member sits two levels below the root")
        .to_path_buf()
}

/// How a document's *unprefixed* citations are resolved.
///
/// Every document declares one. The declaration is the convention that document
/// actually follows, so a citation resolving nowhere is a defect in the prose
/// rather than a gap in this table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unprefixed {
    /// The document numbers its own sections and a bare `§N` means its own §N.
    /// The ordinary case: every layered spec that cites outward with a prefix.
    Own,
    /// The document numbers its own sections and also cites others without a
    /// prefix. Its own sections are tried first, then the named documents in
    /// order.
    OwnThen(&'static [&'static str]),
    /// The document numbers nothing of its own; every bare `§N` belongs to one of
    /// the named documents.
    Other(&'static [&'static str]),
}

/// A document in the specification set: where it lives, what prefixes name it,
/// and how its own unprefixed citations resolve.
#[derive(Debug, Clone, Copy)]
pub struct DocSpec {
    /// Short stable handle, used in failure messages and by [`Unprefixed`].
    pub id: &'static str,
    /// Path relative to the repository root. Its file name also resolves a
    /// markdown-link prefix, so `[entity](entity-spec.md) §7.12` finds it.
    pub path: &'static str,
    /// The prefixes other documents use to name this one. Matched
    /// case-insensitively, so a sentence may open with `Actor §7`.
    pub aliases: &'static [&'static str],
    /// How this document's own unprefixed citations resolve.
    pub unprefixed: Unprefixed,
    /// Whether [`identifiers`] holds its backticked names to the tree. Off for the
    /// research notes, which describe other systems (`storage.setAlarm()` is
    /// correct there and absent here by nature), and for `docs/README.md`, which
    /// describes this crate, whose own names the index leaves out.
    pub scanned: bool,
}

/// Every document the gates read: the citation graph, the identifiers scanned out
/// of the prose, and the specs the catalogues live in.
///
/// A spec absent from this table is checked by nothing, so `tests/citations.rs`
/// asserts the table covers all of `docs/`: adding a spec without registering it
/// fails the build, the way adding an invariant without cataloguing it does.
pub const DOCUMENTS: &[DocSpec] = &[
    DocSpec {
        id: "actor",
        path: "docs/distributed-actor-spec.md",
        aliases: &["actor", "core"],
        unprefixed: Unprefixed::Own,
        scanned: true,
    },
    // The clustered actor system: transport, membership, failure detection,
    // consensus groups, and authorization, on the actor model above.
    DocSpec {
        id: "cluster",
        path: "docs/cluster-spec.md",
        aliases: &["cluster"],
        unprefixed: Unprefixed::Own,
        scanned: true,
    },
    DocSpec {
        id: "utilities",
        path: "docs/cluster-utilities-spec.md",
        aliases: &["utilities", "util", "cluster-utilities"],
        unprefixed: Unprefixed::Own,
        scanned: true,
    },
    DocSpec {
        id: "entity",
        path: "docs/entity-spec.md",
        aliases: &["entity"],
        unprefixed: Unprefixed::Own,
        scanned: true,
    },
    // The entity's storage half: journal and store seams, fence, replication,
    // shards, snapshots, and blobs.
    DocSpec {
        id: "entity-storage",
        path: "docs/entity-storage-spec.md",
        aliases: &["storage", "entity-storage"],
        unprefixed: Unprefixed::Own,
        scanned: true,
    },
    DocSpec {
        id: "entity-facets",
        path: "docs/entity-facets-spec.md",
        aliases: &["facets", "entity-facets"],
        unprefixed: Unprefixed::Own,
        scanned: true,
    },
    // The autonomous entity (the `workflow` crate). Cites the agent runtime: the
    // layer it generalizes, and the one that now sits on it.
    DocSpec {
        id: "workflow",
        path: "docs/workflow-spec.md",
        aliases: &["workflow"],
        unprefixed: Unprefixed::Own,
        scanned: true,
    },
    DocSpec {
        id: "agent",
        path: "docs/agent-spec.md",
        aliases: &["agent"],
        unprefixed: Unprefixed::Own,
        scanned: true,
    },
    DocSpec {
        id: "sandbox",
        path: "docs/sandbox-spec.md",
        aliases: &["sandbox"],
        unprefixed: Unprefixed::Own,
        scanned: true,
    },
    DocSpec {
        id: "machine",
        path: "docs/machine-spec.md",
        aliases: &["machine"],
        unprefixed: Unprefixed::Own,
        scanned: true,
    },
    DocSpec {
        id: "compatibility",
        path: "docs/compatibility-spec.md",
        aliases: &["compatibility", "compat"],
        unprefixed: Unprefixed::Own,
        scanned: true,
    },
    DocSpec {
        id: "hw",
        path: "docs/hardware-envelope.md",
        aliases: &["hw", "hardware-envelope"],
        unprefixed: Unprefixed::Own,
        scanned: true,
    },
    // Numbers nothing of its own, and is written against the actor spec's §16–§18
    // (see its opening paragraph): every bare `§N` in it is an actor citation. It
    // cites entity as well, and says so with a prefix each time: this list stays
    // one document long so that remains true rather than becoming a convention a
    // reader has to infer per sentence.
    DocSpec {
        id: "simulation",
        path: "docs/simulation-testing.md",
        aliases: &["simulation-testing"],
        unprefixed: Unprefixed::Other(&["actor"]),
        scanned: true,
    },
    // A deployment guide for the agent runtime; its bare `spec §6.1` is the agent spec.
    DocSpec {
        id: "standalone",
        path: "docs/standalone-deployment.md",
        aliases: &["standalone-deployment"],
        unprefixed: Unprefixed::Other(&["agent"]),
        scanned: true,
    },
    DocSpec {
        id: "edge",
        path: "docs/edge-spec.md",
        aliases: &["edge", "multi-tenant-edge"],
        unprefixed: Unprefixed::Own,
        scanned: true,
    },
    // What the implementation does not yet meet, per spec; it cites every spec with
    // a prefix, and no spec cites it.
    DocSpec {
        id: "status",
        path: "docs/status.md",
        aliases: &["status"],
        unprefixed: Unprefixed::Own,
        scanned: true,
    },
    // The one-page map of components and seams; it cites every spec with a prefix.
    DocSpec {
        id: "architecture",
        path: "docs/architecture.md",
        aliases: &["architecture"],
        unprefixed: Unprefixed::Own,
        scanned: true,
    },
    DocSpec {
        id: "docs-index",
        path: "docs/README.md",
        aliases: &[],
        unprefixed: Unprefixed::Other(&["actor"]),
        scanned: false,
    },
    // The rubric the specs were written against; its bare citations are the actor
    // spec's, which is the document its examples are drawn from.
    DocSpec {
        id: "principles",
        path: "design-principles.md",
        aliases: &["design-principles"],
        unprefixed: Unprefixed::Other(&["actor"]),
        scanned: true,
    },
    // A background note on Durable Objects that maps them onto this tree, so it
    // cites the actor spec's control-plane modes without a prefix.
    DocSpec {
        id: "DO",
        path: "research/durable-objects.md",
        aliases: &["DO"],
        unprefixed: Unprefixed::OwnThen(&["actor"]),
        scanned: false,
    },
    DocSpec {
        id: "durable-sqlite",
        path: "research/durable-sqlite-and-filesystem.md",
        aliases: &["durable-sqlite-and-filesystem"],
        unprefixed: Unprefixed::OwnThen(&["entity"]),
        scanned: false,
    },
];

/// A crate whose sources the citation gate reads, and the specifications its bare
/// `§N` citations name.
#[derive(Debug, Clone, Copy)]
pub struct CrateSpec {
    /// The directory under `crates/`.
    pub name: &'static str,
    /// The [`DocSpec::id`]s a bare citation is tried against, in order.
    pub cites: &'static [&'static str],
}

/// Every crate the code half of the citation gate reads.
///
/// `tests/code_citations.rs` holds this table equal to `crates/` less
/// [`UNSCANNED_CRATES`], so a new crate is checked from its first comment.
pub const CRATES: &[CrateSpec] = &[
    CrateSpec {
        name: "actor-core",
        cites: &["actor"],
    },
    CrateSpec {
        name: "actor-serialization",
        cites: &["actor"],
    },
    CrateSpec {
        name: "actor-cluster",
        cites: &["actor"],
    },
    CrateSpec {
        name: "actor-runtime",
        cites: &["actor"],
    },
    CrateSpec {
        name: "actor-simulation",
        cites: &["actor"],
    },
    // Specified by its own rustdoc: a bare citation there has no document to name.
    CrateSpec {
        name: "wal",
        cites: &[],
    },
    CrateSpec {
        name: "compat",
        cites: &["compatibility"],
    },
    CrateSpec {
        name: "entity",
        cites: &["entity"],
    },
    CrateSpec {
        name: "workflow",
        cites: &["workflow"],
    },
    CrateSpec {
        name: "tenancy",
        cites: &["entity"],
    },
    CrateSpec {
        name: "agent",
        cites: &["agent"],
    },
    CrateSpec {
        name: "agent-anthropic",
        cites: &["agent"],
    },
    CrateSpec {
        name: "agent-openai",
        cites: &["agent"],
    },
    CrateSpec {
        name: "agent-http",
        cites: &["agent"],
    },
    CrateSpec {
        name: "agent-gateway",
        cites: &["agent"],
    },
    CrateSpec {
        name: "agent-standalone",
        cites: &["agent"],
    },
    CrateSpec {
        name: "agent-tui",
        cites: &["agent"],
    },
    CrateSpec {
        name: "agent-sandbox",
        cites: &["sandbox", "agent"],
    },
    CrateSpec {
        name: "machine-entity",
        cites: &["machine"],
    },
    CrateSpec {
        name: "machine-host",
        cites: &["machine"],
    },
    CrateSpec {
        name: "machine-proto",
        cites: &["machine"],
    },
    CrateSpec {
        name: "machine-frontdoor",
        cites: &["machine"],
    },
    CrateSpec {
        name: "machine-standalone",
        cites: &["machine"],
    },
];

/// The crates the citation gate does not read, each with the reason.
pub const UNSCANNED_CRATES: &[(&str, &str)] = &[(
    "spec",
    "its comments quote citations as examples of what the gates parse",
)];
