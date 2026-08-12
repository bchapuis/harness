//! The invariant ranges the specs cite of each other resolve against the
//! catalogues they name.
//!
//! `grain §N` (its invariants as **G1–G20**) is a citation: it points at granary's
//! catalogue and says how far it runs. Nothing resolved it until now, and a
//! catalogue that grows leaves every sibling stating its old extent describing a
//! shorter document than the one on disk — each sentence still reading exactly
//! right. `spec::ranges` documents why the two checks below are the pair that
//! holds, and why the two more obvious ones do not.

use std::path::Path;
use std::path::PathBuf;

use spec::catalogue::CATALOGUES;
use spec::ranges::Owner;
use spec::ranges::extents;
use spec::ranges::label_of;
use spec::ranges::resolve;
use spec::ranges::scan;
use spec::ranges::unknown_labels;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/spec has a workspace root two levels up")
        .to_path_buf()
}

/// Every label a document writes is one `LABELS` knows.
///
/// A new catalogue whose ranges resolve nowhere would otherwise be skipped in
/// silence, which is the state this gate exists to end.
#[test]
fn every_range_names_a_known_catalogue() {
    let scanned = scan(&root()).expect("scanning the document set");
    let unknown = unknown_labels(&scanned);
    assert!(
        unknown.is_empty(),
        "invariant ranges naming no catalogue in `spec::ranges::LABELS`:\n{}",
        unknown
            .iter()
            .map(|u| format!("  {}: {} ({})", u.path, u.range, u.range.label))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}

/// No citation names an invariant past the end of the catalogue it names.
///
/// The span may skip a number — the harness catalogue has no H2 and `H1–H8` is
/// still how its extent is written — but it may not run off the end, whatever the
/// surrounding sentence meant by it.
#[test]
fn no_range_runs_past_its_catalogue() {
    let root = root();
    let extents = extents(&root).expect("reading the catalogues");
    let mut wrong = Vec::new();

    for (path, range) in scan(&root).expect("scanning the document set") {
        let Some(Owner::Catalogue(id)) = resolve(&range.label) else {
            continue;
        };
        let (low, high) = extents[id];
        if range.low < low || range.high > high {
            wrong.push(format!(
                "  {path}:{}: {range} names an invariant outside {}{low}–{}{high}",
                range.line, range.label, range.label,
            ));
        }
    }

    assert!(
        wrong.is_empty(),
        "invariant ranges reaching past the catalogue they name:\n{}",
        wrong.join("\n"),
    );
}

/// Some document names each catalogue's last invariant.
///
/// One spec's sub-span is another's full extent, so no single sentence can be held
/// to state the whole range — but the union of them must reach the end, or the
/// catalogue has grown past every description of it. This is the direction that
/// catches a *new* invariant, and the one that catches a catalogue the index
/// documents never mention at all.
#[test]
fn every_catalogues_last_invariant_is_named_somewhere() {
    let root = root();
    let extents = extents(&root).expect("reading the catalogues");
    let scanned = scan(&root).expect("scanning the document set");

    let mut behind = Vec::new();
    for site in CATALOGUES {
        let label = label_of(site.id).expect("a registered catalogue has a label");
        let (_, high) = extents[site.id];
        let reached = scanned
            .iter()
            .filter(|(_, r)| r.label == label)
            .map(|(_, r)| r.high)
            .max();
        if reached != Some(high) {
            behind.push(format!(
                "  {}: the catalogue runs to {label}{high}, the document set reaches {}",
                site.id,
                reached.map_or("no range at all".to_string(), |r| format!("{label}{r}")),
            ));
        }
    }

    assert!(
        behind.is_empty(),
        "catalogues the documents describing them have fallen behind, or never \
         named at all:\n{}",
        behind.join("\n"),
    );
}

/// The unregistered labels are unregistered for a stated reason, and the reason is
/// not empty. `F` and `V` name tables the catalogue gates cannot read; saying so
/// here is what keeps them from looking checked.
#[test]
fn every_unregistered_label_says_why() {
    for (label, owner) in spec::ranges::LABELS {
        if let Owner::Unregistered(reason) = owner {
            assert!(
                reason.len() > 40,
                "{label}: unregistered with no reason worth reading",
            );
        }
    }
}

/// The gate has something to check. A scanner that stopped recognizing ranges
/// would pass all three assertions above by finding nothing.
#[test]
fn the_gate_still_has_something_to_check() {
    let scanned = scan(&root()).expect("scanning the document set");
    assert!(
        scanned.len() >= 15,
        "only {} invariant ranges found across the set; the scanner has regressed",
        scanned.len(),
    );
}
