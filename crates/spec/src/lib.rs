//! The specifications, read mechanically, so the prose fails when the tree moves.
//!
//! `docs/` describes a tree that keeps changing under it. Prose does not fail on
//! its own, so a description that has come loose reads exactly like one that has
//! not — the edit that broke it was local and the sentences it broke were not.
//! This crate reads the specifications the way a compiler reads a source file and
//! reports the places they no longer describe anything, one module per way the
//! prose comes loose:
//!
//! - [`citations`] — a `§` reference resolves to a section that exists. `grain
//!   §7.12` after §7 is renumbered is the ordinary break, and the specs are
//!   layered so that each cites the ones beneath it, which makes the citation the
//!   load-bearing part of the layering rather than a convenience.
//! - [`identifiers`] — a backticked `GrainHandler` or `load_snapshot()` still
//!   names something the tree declares. The same failure one level down: a rename
//!   moves the code and leaves the prose behind.
//! - [`catalogue`] — a spec's invariant table and the Rust copy beside the suite
//!   that verifies it agree on which invariants exist, where they are defined, and
//!   what verifies them. Two hand-maintained copies of one fact, previously
//!   compared by nothing.
//! - [`ranges`] — a spec naming a sibling's catalogue by its extent, `G1–G20`,
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

pub mod catalogue;
pub mod citations;
pub mod identifiers;
pub mod ranges;

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
    /// markdown-link prefix, so `[granary](granary-spec.md) §7.12` finds it.
    pub path: &'static str,
    /// The prefixes other documents use to name this one. Matched
    /// case-insensitively, so a sentence may open with `Actor §7`.
    pub aliases: &'static [&'static str],
    /// How this document's own unprefixed citations resolve.
    pub unprefixed: Unprefixed,
}

/// Every document the gates read: the citation graph, the identifiers scanned out
/// of the prose, and the specs the catalogues live in.
///
/// A spec absent from this table is checked by nothing, so `tests/citations.rs`
/// asserts the table covers all of `docs/` — adding a spec without registering it
/// fails the build, the way adding an invariant without cataloguing it does.
pub const DOCUMENTS: &[DocSpec] = &[
    DocSpec {
        id: "actor",
        path: "docs/distributed-actor-spec.md",
        aliases: &["actor", "core"],
        unprefixed: Unprefixed::Own,
    },
    DocSpec {
        id: "utilities",
        path: "docs/cluster-utilities-spec.md",
        aliases: &["utilities", "util", "cluster-utilities"],
        unprefixed: Unprefixed::Own,
    },
    DocSpec {
        id: "wal",
        path: "docs/wal-spec.md",
        aliases: &["wal"],
        unprefixed: Unprefixed::Own,
    },
    DocSpec {
        id: "granary",
        path: "docs/granary-spec.md",
        aliases: &["granary", "grain"],
        unprefixed: Unprefixed::Own,
    },
    DocSpec {
        id: "blob",
        path: "docs/blob-store-spec.md",
        aliases: &["blob", "blob-store"],
        unprefixed: Unprefixed::Own,
    },
    DocSpec {
        id: "harness",
        path: "docs/agentic-harness-spec.md",
        aliases: &["harness"],
        unprefixed: Unprefixed::Own,
    },
    // §5 relates the tier model to the harness's own §5, and cites into it bare.
    DocSpec {
        id: "sandbox",
        path: "docs/sandbox-spec.md",
        aliases: &["sandbox"],
        unprefixed: Unprefixed::OwnThen(&["harness"]),
    },
    DocSpec {
        id: "machine",
        path: "docs/machine-spec.md",
        aliases: &["machine"],
        unprefixed: Unprefixed::Own,
    },
    DocSpec {
        id: "compatibility",
        path: "docs/compatibility-spec.md",
        aliases: &["compatibility", "compat"],
        unprefixed: Unprefixed::Own,
    },
    DocSpec {
        id: "hw",
        path: "docs/hardware-envelope.md",
        aliases: &["hw", "hardware-envelope"],
        unprefixed: Unprefixed::Own,
    },
    // Numbers nothing of its own, and is written against the actor spec's §16–§18
    // (see its opening paragraph): every bare `§N` in it is an actor citation. It
    // cites granary as well, and says so with a prefix each time — this list stays
    // one document long so that remains true rather than becoming a convention a
    // reader has to infer per sentence.
    DocSpec {
        id: "simulation",
        path: "docs/simulation-testing.md",
        aliases: &["simulation-testing"],
        unprefixed: Unprefixed::Other(&["actor"]),
    },
    // A deployment guide for the harness; its bare `spec §6.1` is the harness spec.
    DocSpec {
        id: "standalone",
        path: "docs/standalone-deployment.md",
        aliases: &["standalone-deployment"],
        unprefixed: Unprefixed::Other(&["harness"]),
    },
    DocSpec {
        id: "edge",
        path: "docs/multi-tenant-edge.md",
        aliases: &["multi-tenant-edge"],
        unprefixed: Unprefixed::Other(&["harness"]),
    },
    DocSpec {
        id: "docs-index",
        path: "docs/README.md",
        aliases: &[],
        unprefixed: Unprefixed::Other(&["actor"]),
    },
    // The rubric the specs were written against; its bare citations are the actor
    // spec's, which is the document its examples are drawn from.
    DocSpec {
        id: "principles",
        path: "design-principles.md",
        aliases: &["design-principles"],
        unprefixed: Unprefixed::Other(&["actor"]),
    },
    // A background note on Durable Objects that maps them onto this tree, so it
    // cites the actor spec's control-plane modes without a prefix.
    DocSpec {
        id: "DO",
        path: "research/durable-objects.md",
        aliases: &["DO"],
        unprefixed: Unprefixed::OwnThen(&["actor"]),
    },
    DocSpec {
        id: "durable-sqlite",
        path: "research/durable-sqlite-and-filesystem.md",
        aliases: &["durable-sqlite-and-filesystem"],
        unprefixed: Unprefixed::OwnThen(&["granary"]),
    },
];
