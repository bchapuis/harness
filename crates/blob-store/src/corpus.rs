//! The golden corpus's fixture discipline, for this crate's one durable boundary
//! (compatibility spec §4).
//!
//! `blob.tombstone` keeps checked-in bytes from every revision it accepts, and a
//! test beside the format that decodes them with the current build.
//!
//! The corpus matters more here than the size of the format suggests, because this
//! crate has no in-tree consumer: nothing else in the workspace depends on it, so a
//! format break has nothing downstream to fail. Every other boundary in the tree
//! would eventually surface a mistake as a grain that would not activate or a node
//! that could not rejoin. Here the fixture is the only thing that would notice.
//!
//! **The files are evidence, not output.** A fixture for a revision that has
//! shipped records what those bytes meant; regenerating it destroys the only thing
//! holding **V4**/**V5** up, and converts a caught format break into a green run.
//! So [`golden`] writes only a file that is *absent* — the case of adding a
//! revision — and never rewrites one that exists.
//!
//! When a fixture stops decoding, the corpus has done its job. The fix is the
//! compatibility spec's, not the fixture's: widen the window to keep reading the
//! old revision, leave its decoder in place, and add the new revision's bytes
//! beside it (**V4**, read-new first).
//!
//! This is the fourth copy of the helper, one per crate that owns a boundary,
//! because `corpus_dir` resolves against the *owning* crate's manifest. Hoisting it
//! into `compat` would cost that crate the property its position below `wal` rests
//! on — it owns no format, reads no file, and depends on nothing in the tree — and
//! a feature flag would not keep the promise under cargo's feature unification. If
//! a fifth owner appears, the move is a dev-only crate exporting a `macro_rules!`
//! so `CARGO_MANIFEST_DIR` still expands at the call site, not a `compat` feature.

use std::path::Path;
use std::path::PathBuf;

/// The crate's corpus directory: `corpus/<boundary>/v<revision>.bin`.
fn corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus")
}

/// The checked-in bytes for `boundary` at `revision`.
///
/// Writes them from `produce` only when the file is absent *and* `GOLDEN_UPDATE`
/// is set; an existing fixture is read and never rewritten.
pub(crate) fn golden(boundary: &str, revision: u16, produce: impl FnOnce() -> Vec<u8>) -> Vec<u8> {
    let path = corpus_dir().join(boundary).join(format!("v{revision}.bin"));
    if path.exists() {
        return std::fs::read(&path).expect("read a checked-in corpus fixture");
    }
    assert!(
        std::env::var_os("GOLDEN_UPDATE").is_some(),
        "no corpus fixture at {}. If this revision is new, create it with \
         GOLDEN_UPDATE=1 cargo test -p blob-store. If it is not, the file was \
         deleted — restore it from git rather than regenerating it, or the corpus \
         stops being evidence of what the old bytes meant.",
        path.display(),
    );
    let bytes = produce();
    std::fs::create_dir_all(path.parent().expect("fixture path has a parent"))
        .expect("create the corpus directory");
    std::fs::write(&path, &bytes).expect("write a new corpus fixture");
    bytes
}
