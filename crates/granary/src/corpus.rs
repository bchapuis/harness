//! The golden corpus's fixture discipline, shared by this crate's four durable
//! boundaries (compatibility spec §4).
//!
//! Each boundary keeps checked-in bytes from every revision it accepts, and a test
//! beside the format that decodes them with the current build. That is the check a
//! type system cannot make: `postcard` is positional, so adding a field to a
//! record, a manifest entry, or a snapshot body compiles cleanly and breaks every
//! stored copy of it at once. Nothing else in the tree would notice until a real
//! grain failed to activate.
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
//! The tests deliberately do **not** assert that this build re-encodes a fixture
//! byte-for-byte. Under **V4** a build reads revisions it no longer writes, so
//! byte equality would fail on exactly the upgrade the policy prescribes. Decoding
//! old bytes to the right *value* is the property; reproducing them is not.

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
         GOLDEN_UPDATE=1 cargo test -p granary. If it is not, the file was deleted \
         — restore it from git rather than regenerating it, or the corpus stops \
         being evidence of what the old bytes meant.",
        path.display(),
    );
    let bytes = produce();
    std::fs::create_dir_all(path.parent().expect("fixture path has a parent"))
        .expect("create the corpus directory");
    std::fs::write(&path, &bytes).expect("write a new corpus fixture");
    bytes
}
