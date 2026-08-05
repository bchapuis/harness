//! The corpus's completeness gate: every boundary the registry names has
//! checked-in bytes, and every checked-in fixture belongs to a registered boundary
//! (compatibility spec §3, §4).
//!
//! The corpus itself is distributed — a fixture's *decoder* lives with the format
//! that owns it, because `compat` owns no format and reads no file on any live path
//! (§2). What can only be checked centrally is the part a per-crate test cannot
//! see: that a **new** boundary did not ship without a corpus at all. §3 says a new
//! durable or wire format MUST appear in the registry; this makes the sentence
//! after it — that it must also be decodable by a later build — mechanical rather
//! than remembered.
//!
//! So this test reads the registry table out of the specification and holds the
//! tree to it, in both directions. The spec is the source of truth (§3 owns the
//! registry); the files answer to it.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;

/// The workspace root: `crates/compat/../..`.
fn workspace() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("the compat crate sits two levels below the workspace root")
        .to_path_buf()
}

/// The registry of §3, as `boundary -> revision`.
///
/// A boundary whose `Rev` column is `—` carries no revision at all — it is an
/// identity field (`wal.checksum`) or a reserved one (`wal.reserved`), matched
/// rather than compared — so it has nothing for a corpus to pin and is left out.
fn registry() -> BTreeMap<String, u16> {
    let spec = std::fs::read_to_string(workspace().join("docs/compatibility-spec.md"))
        .expect("read the compatibility specification");

    let section = spec
        .split_once("## 3. The boundary registry")
        .expect("the specification must carry the §3 registry")
        .1;
    let section = section.split("\n## ").next().expect("a section ends");

    let mut found = BTreeMap::new();
    for line in section.lines() {
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        // A table row is `| name | rev | ... |`, so the split yields a leading and
        // a trailing empty cell around it. Anything else is prose or the header.
        if cells.len() < 4 {
            continue;
        }
        let Some(name) = cells[1].strip_prefix('`').and_then(|c| c.strip_suffix('`')) else {
            continue;
        };
        if let Ok(revision) = cells[2].parse::<u16>() {
            found.insert(name.to_string(), revision);
        }
    }
    assert!(
        found.len() >= 6,
        "parsed only {} rows out of the §3 registry — the table's shape changed and \
         this gate is no longer reading it",
        found.len(),
    );
    found
}

/// Every `crates/*/corpus/<boundary>/` directory in the tree, as
/// `boundary -> the revisions checked in for it`.
fn corpora() -> BTreeMap<String, BTreeSet<u16>> {
    let mut found: BTreeMap<String, BTreeSet<u16>> = BTreeMap::new();
    let crates = workspace().join("crates");
    for crate_dir in std::fs::read_dir(&crates).expect("read crates/") {
        let corpus = crate_dir.expect("a crates/ entry").path().join("corpus");
        if !corpus.is_dir() {
            continue;
        }
        for boundary in std::fs::read_dir(&corpus).expect("read a corpus directory") {
            let boundary = boundary.expect("a corpus entry").path();
            let name = boundary
                .file_name()
                .expect("a boundary directory has a name")
                .to_string_lossy()
                .into_owned();
            let revisions = found.entry(name).or_default();
            for fixture in std::fs::read_dir(&boundary).expect("read a boundary directory") {
                let fixture = fixture.expect("a fixture entry").path();
                let stem = fixture
                    .file_stem()
                    .expect("a fixture has a name")
                    .to_string_lossy()
                    .into_owned();
                let revision = stem
                    .strip_prefix('v')
                    .and_then(|n| n.parse::<u16>().ok())
                    .unwrap_or_else(|| {
                        panic!(
                            "corpus fixture {} is not named v<revision>",
                            fixture.display()
                        )
                    });
                revisions.insert(revision);
            }
        }
    }
    found
}

#[test]
fn every_registered_boundary_has_a_corpus_at_its_current_revision() {
    let corpora = corpora();
    for (boundary, revision) in registry() {
        let revisions = corpora.get(&boundary).unwrap_or_else(|| {
            panic!(
                "boundary {boundary:?} is in the §3 registry but has no corpus. A format \
                 with no checked-in bytes cannot be shown to still decode, so V4 and V5 \
                 are unenforceable for it (§4). Add crates/<owner>/corpus/{boundary}/ and \
                 a test beside the format that decodes it."
            )
        });
        assert!(
            revisions.contains(&revision),
            "boundary {boundary:?} is at revision {revision} in the §3 registry, but its \
             corpus holds only {revisions:?}. A revision nothing has decoded since it \
             shipped is a revision nothing is holding.",
        );
    }
}

#[test]
fn every_corpus_belongs_to_a_registered_boundary() {
    // The other direction: a fixture directory for a boundary the registry does not
    // name is either a boundary that was never registered (§3 requires it) or one
    // that was removed while its bytes stayed behind, still passing a test that no
    // longer means anything.
    let registry = registry();
    for boundary in corpora().keys() {
        assert!(
            registry.contains_key(boundary),
            "corpus directory {boundary:?} names no boundary in the §3 registry — \
             register the format, or delete the fixtures with it",
        );
    }
}

#[test]
fn a_corpus_fixture_is_never_empty() {
    // A zero-byte fixture decodes to nothing and asserts nothing, which is the one
    // way a corpus can be present and still be worthless.
    let crates = workspace().join("crates");
    for crate_dir in std::fs::read_dir(&crates).expect("read crates/") {
        let corpus = crate_dir.expect("a crates/ entry").path().join("corpus");
        if !corpus.is_dir() {
            continue;
        }
        for boundary in std::fs::read_dir(&corpus).expect("read a corpus directory") {
            for fixture in
                std::fs::read_dir(boundary.expect("a corpus entry").path()).expect("read fixtures")
            {
                let path = fixture.expect("a fixture entry").path();
                let len = std::fs::metadata(&path).expect("stat a fixture").len();
                assert!(len > 0, "corpus fixture {} is empty", path.display());
            }
        }
    }
}
