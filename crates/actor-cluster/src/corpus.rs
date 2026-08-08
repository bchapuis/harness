//! The golden corpus's fixture discipline, for this crate's one boundary
//! (compatibility spec §4).
//!
//! `actor.membership` is a hand-rolled nine-byte form — a tag and a little-endian
//! node uid — inside the control group's replicated log. Nothing in the type system
//! notices a tag renumbered or a field widened: both compile, both round-trip
//! against themselves, and both make a build read every transition its peers
//! committed as a *different* transition. That is a member set diverging silently,
//! which is the one failure the control plane exists to prevent, so the checked-in
//! bytes are what stands between the encoding and a change nobody meant to make.
//!
//! **The files are evidence, not output.** A fixture for a revision that has
//! shipped records what those bytes meant; regenerating it destroys the only thing
//! holding **V4**/**V5** up. [`golden`] therefore writes only a file that is
//! *absent* — the case of adding a revision — and never rewrites one that exists.
//! When a fixture stops decoding, the corpus has done its job: widen the window,
//! keep the old decoder, and add the new revision's bytes beside it.

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
         GOLDEN_UPDATE=1 cargo test -p actor-cluster. If it is not, the file was \
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
