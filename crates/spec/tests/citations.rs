//! The spec↔spec drift gate: every `§` citation in the documentation set resolves
//! to a section that exists.
//!
//! The `docs/` analogue of the per-crate catalogue drift tests. Those keep an
//! invariant's "Verified by" column pointing at a real test; this keeps a spec's
//! `grain §7.12` pointing at a real section. Both fail for the same reason —
//! renaming or renumbering is a local edit whose citations are not local, and
//! prose does not fail on its own.
//!
//! A failure here is fixed in one of two places. Usually the citation is stale and
//! follows the section that moved. Occasionally the *convention* moved: a document
//! started citing a spec it had not cited before, and its entry in `DOCUMENTS`
//! needs to say so.

use std::path::Path;
use std::path::PathBuf;

use spec::DOCUMENTS;
use spec::Unprefixed;
use spec::citations::Registry;

/// The workspace root: this crate sits at `crates/spec`.
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/spec has a workspace root two levels up")
        .to_path_buf()
}

fn registry() -> Registry {
    Registry::load(&root()).expect("every document in DOCUMENTS is readable")
}

#[test]
fn every_citation_resolves() {
    let registry = registry();
    let unresolved = registry.unresolved();
    assert!(
        unresolved.is_empty(),
        "{} of {} citations resolve to no section:\n\n{}\n",
        unresolved.len(),
        registry.citation_count(),
        unresolved
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n\n"),
    );
}

/// A document absent from `DOCUMENTS` is checked by nothing and cites into the
/// dark. Adding a spec must therefore register it, the way adding an invariant
/// must catalogue it.
#[test]
fn every_document_in_docs_is_registered() {
    let root = root();
    let mut unregistered: Vec<String> = std::fs::read_dir(root.join("docs"))
        .expect("docs/ exists")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "md"))
        .filter_map(|p| {
            let relative = format!("docs/{}", p.file_name()?.to_str()?);
            (!DOCUMENTS.iter().any(|d| d.path == relative)).then_some(relative)
        })
        .collect();
    unregistered.sort();
    assert!(
        unregistered.is_empty(),
        "these documents are not in DOCUMENTS, so nothing checks their citations: {unregistered:?}",
    );
}

#[test]
fn every_registered_document_is_distinct_and_its_aliases_are_unique() {
    let mut ids: Vec<&str> = DOCUMENTS.iter().map(|d| d.id).collect();
    let count = ids.len();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), count, "two entries share an id");

    let mut aliases: Vec<String> = DOCUMENTS
        .iter()
        .flat_map(|d| d.aliases.iter().map(|a| a.to_ascii_lowercase()))
        .collect();
    let count = aliases.len();
    aliases.sort();
    aliases.dedup();
    assert_eq!(aliases.len(), count, "two documents answer to one prefix");
}

/// Every `Unprefixed` target names a document that exists, so a fallback cannot
/// point at a document that was renamed out of the table.
#[test]
fn every_unprefixed_fallback_names_a_registered_document() {
    for doc in DOCUMENTS {
        let fallbacks = match doc.unprefixed {
            Unprefixed::Own => continue,
            Unprefixed::OwnThen(ids) | Unprefixed::Other(ids) => ids,
        };
        assert!(!fallbacks.is_empty(), "{}'s fallback list is empty", doc.id);
        for target in fallbacks {
            assert!(
                DOCUMENTS.iter().any(|d| d.id == *target),
                "{}'s unprefixed citations resolve against {target:?}, which is not registered",
                doc.id,
            );
        }
    }
}

/// A parser that silently stops recognizing citations would make this suite pass
/// by finding nothing to check. The floors are well under the counts at the time
/// of writing (2,100-odd citations; every spec numbers dozens of sections), and
/// exist to catch a regression to zero rather than to pin an exact figure.
#[test]
fn the_gate_still_has_something_to_check() {
    let registry = registry();
    assert!(
        registry.citation_count() > 1_500,
        "only {} citations found across the set; the scanner has regressed",
        registry.citation_count(),
    );
    for doc in registry.documents() {
        if matches!(doc.spec.unprefixed, Unprefixed::Own) {
            assert!(
                doc.anchors.len() >= 5,
                "{} offers only {} anchors; its numbering is declared `Own`",
                doc.spec.id,
                doc.anchors.len(),
            );
        }
    }
}
