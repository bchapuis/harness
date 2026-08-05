//! Golden corpus for `wal.frame` (compatibility spec §4).
//!
//! Checked-in bytes from every revision this build accepts, opened with the
//! current build. This is the check a type system cannot make: adding a field to a
//! `postcard` record, or moving a byte in the header, compiles cleanly and breaks
//! every stored copy of the format at once. Nothing else in the tree would notice
//! until a real log failed to open.
//!
//! **The files are evidence, not output.** A fixture for a revision that has
//! shipped records what those bytes meant, so regenerating it destroys the only
//! thing holding **V4**/**V5** up. `GOLDEN_UPDATE=1` therefore writes only files
//! that are *absent* — the case of adding a revision — and refuses to touch one
//! that exists. A format change that makes an existing fixture unreadable is the
//! corpus working, not a stale fixture: widen the window, keep the old decoder,
//! and add the new revision's bytes beside it.
//!
//! What is deliberately *not* asserted is that this build re-encodes the fixture
//! byte-for-byte. A log records which digest wrote it and reads both (`wal` §2.1),
//! so changing `Digest::WRITES` legitimately changes the bytes without changing
//! what they mean. Decoding old bytes to the right value is the property;
//! reproducing them is not.

use std::path::Path;
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;
use wal::Wal;

/// The corpus record type, and it must never change.
///
/// It is part of what the checked-in bytes *mean*: the fixture is a log of these,
/// so editing this struct silently redefines every fixture rather than testing
/// against it. A new shape belongs to a new revision, with its own type beside
/// this one.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct Rec {
    index: u64,
    data: Vec<u8>,
}

fn rec(index: u64, data: &[u8]) -> Rec {
    Rec {
        index,
        data: data.to_vec(),
    }
}

/// One frame under the bound, one empty payload, and one long enough to cross a
/// length prefix that a narrower encoding would have truncated.
fn corpus_records() -> Vec<Rec> {
    vec![rec(1, b"a"), rec(2, b""), rec(3, &[0xAB; 300])]
}

const MAX: u32 = 1 << 20;

/// The window the corpus is written and read under. `wal.test` rather than a live
/// caller's boundary: this file exercises the *frame* layout and the header, which
/// is what `wal.frame` versions — the callers' record schemas are their own
/// boundaries and their own fixtures.
const RECORDS: compat::Window = compat::Window::at("wal.test", 1);

fn corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus")
}

/// The checked-in bytes for `boundary` at `revision`, writing them from `produce`
/// only when the file is absent and `GOLDEN_UPDATE` is set.
fn golden(boundary: &str, revision: u16, produce: impl FnOnce() -> Vec<u8>) -> Vec<u8> {
    let path = corpus_dir().join(boundary).join(format!("v{revision}.bin"));
    if path.exists() {
        return std::fs::read(&path).expect("read a checked-in corpus fixture");
    }
    assert!(
        std::env::var_os("GOLDEN_UPDATE").is_some(),
        "no corpus fixture at {}. If this revision is new, create it with \
         GOLDEN_UPDATE=1 cargo test -p wal --test corpus. If it is not, the file \
         was deleted — restore it from git rather than regenerating it, or the \
         corpus stops being evidence of what the old bytes meant.",
        path.display(),
    );
    let bytes = produce();
    std::fs::create_dir_all(path.parent().expect("fixture path has a parent"))
        .expect("create the corpus directory");
    std::fs::write(&path, &bytes).expect("write a new corpus fixture");
    bytes
}

/// Write a log holding [`corpus_records`] and return its bytes.
fn produce_frame_log() -> Vec<u8> {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("log");
    let (mut wal, _) = Wal::<Rec>::open(&path, MAX, &RECORDS).expect("open a fresh log");
    wal.append_batch(&corpus_records()).expect("append");
    drop(wal);
    std::fs::read(&path).expect("read back the produced log")
}

#[test]
fn wal_frame_v1_still_opens_and_recovers_its_records() {
    let bytes = golden("wal.frame", 1, produce_frame_log);

    // `open` truncates a torn tail and stamps a fresh header, so it must never run
    // against the checked-in file itself — a build that could not read the fixture
    // would rewrite it and erase the failure.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("log");
    std::fs::write(&path, &bytes).expect("stage the fixture");

    let (wal, recovered) = Wal::<Rec>::open(&path, MAX, &RECORDS)
        .expect("this build must open a wal.frame v1 log it accepts");
    assert_eq!(
        recovered,
        corpus_records(),
        "wal.frame v1 bytes decoded to different records than they were written from",
    );
    assert_eq!(wal.len(), corpus_records().len());

    // The fixture must survive the round trip unchanged: an `open` that silently
    // truncated a frame it could not parse would still return the prefix it liked.
    assert_eq!(
        std::fs::read(&path).expect("re-read"),
        bytes,
        "opening the fixture rewrote it — a frame was discarded as torn",
    );
}

#[test]
fn a_wal_frame_revision_this_build_does_not_accept_is_refused_by_name() {
    // The other half of the corpus's job: bytes from *outside* the window must be
    // refused rather than parsed. Built by hand rather than checked in, since it is
    // a revision no build ever wrote.
    let mut bytes = golden("wal.frame", 1, produce_frame_log);
    let head = b"\x89WAL\r\n\x1a\n".len();
    bytes[head..head + 2].copy_from_slice(&9u16.to_le_bytes());

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("log");
    std::fs::write(&path, &bytes).expect("stage");

    let err = Wal::<Rec>::open(&path, MAX, &RECORDS)
        .err()
        .expect("a wal.frame v9 log must be refused");
    let msg = err.into_io().to_string();
    assert!(
        msg.contains("wal.frame") && msg.contains("v9"),
        "the refusal must name the boundary and what it found, got: {msg}",
    );
}
