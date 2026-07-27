//! Recovery from every possible crash point (wal spec: the torn-tail rule).
//!
//! A write-ahead log's whole promise is what survives a process that died
//! mid-write, and the only interesting question is *where* it died. The unit
//! tests in `src/lib.rs` check a handful of hand-picked truncations; this file
//! checks **all of them** — every byte offset from empty to the full file — plus
//! every single-byte corruption of a header, payload, and checksum.
//!
//! Exhaustive rather than seeded on purpose. Elsewhere in this tree a property is
//! sampled across seeds because its state space is a distributed interleaving and
//! cannot be enumerated (docs/simulation-testing.md). Here it can: a log of a few
//! records is a few hundred bytes, so "the recovered records are always a prefix
//! of what was appended" is *decided*, not estimated, and there is no seed to
//! record if it fails. A sweep would be strictly weaker and slower.
//!
//! The property, at every crash point:
//!
//! 1. **Prefix.** Recovery yields some prefix of the appended records — never a
//!    record that was never written, never one out of order, never a partial one
//!    deserialized into a whole.
//! 2. **Monotone.** Truncating further never recovers *more*. This is what rules
//!    out a scan that resynchronizes on a byte pattern and finds a "record" in the
//!    middle of a payload.
//! 3. **Durable.** Recovery rewrites the valid prefix to disk, so reopening the
//!    recovered file yields exactly the same records — a torn tail is dropped
//!    once, not re-examined forever.
//! 4. **Usable.** A recovered log still appends, and the appended record comes
//!    back on the next open. Recovery leaves the file in a writable state, with
//!    the write landing after the surviving prefix rather than over it.
//! 5. **Refused, not recovered.** Damage to the *file header* is the one case that
//!    is not a truncated recovery: the log's identity is what was damaged, so
//!    nothing about the frames can be trusted and `open` refuses. Every one of its
//!    sixteen bytes is covered, which is also what proves the header validates all
//!    of itself rather than only its magic.

use std::fs;
use std::path::Path;
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;
use wal::Wal;

/// Bounds one record; a length header above this is corruption by definition.
const MAX_RECORD: u32 = 1 << 16;

/// This suite's record schema. The header carries it; these tests never change it.
const RECORDS: compat::Window = compat::Window::at("wal.crash_points", 1);

/// Records of deliberately varied width, so truncation lands inside headers,
/// payloads, and checksums rather than always at a tidy boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Entry {
    seq: u64,
    body: String,
}

fn entries(n: u64) -> Vec<Entry> {
    (0..n)
        .map(|seq| Entry {
            seq,
            // Lengths cycle 1, 8, 27, 64 … so record sizes differ.
            body: "x".repeat(((seq % 4) as usize + 1).pow(3)),
        })
        .collect()
}

/// Write `records` to a fresh log and return the file's bytes.
fn written(dir: &Path, records: &[Entry]) -> Vec<u8> {
    let path = dir.join("full.log");
    let (mut wal, recovered) = Wal::<Entry>::open(&path, MAX_RECORD, &RECORDS).expect("open");
    assert!(recovered.is_empty(), "a fresh log recovers nothing");
    wal.append_batch(records).expect("append");
    fs::read(&path).expect("read back")
}

/// Lay `bytes` down as a log file of its own and open it, returning what recovery
/// produced and the path (so the caller can reopen or append).
fn recover(dir: &Path, name: &str, bytes: &[u8]) -> (Vec<Entry>, PathBuf) {
    let path = dir.join(name);
    fs::write(&path, bytes).expect("write case");
    let (_wal, recovered) = Wal::<Entry>::open(&path, MAX_RECORD, &RECORDS).expect("open");
    (recovered, path)
}

#[test]
fn every_truncation_recovers_a_prefix_and_stays_recovered() {
    let dir = tempfile::tempdir().expect("tempdir");
    let records = entries(6);
    let full = written(dir.path(), &records);

    let mut previous = 0usize;
    for cut in 0..=full.len() {
        let (recovered, path) = recover(dir.path(), &format!("cut{cut}.log"), &full[..cut]);

        // 1. A prefix of what was appended.
        assert_eq!(
            recovered[..],
            records[..recovered.len()],
            "truncating at {cut} recovered something that is not a prefix of the \
             appended records",
        );

        // 2. Monotone in the truncation point.
        assert!(
            recovered.len() >= previous,
            "truncating at {cut} recovered {} records, fewer than the {previous} \
             recovered at {}: recovery is not monotone in how much of the file \
             survived",
            recovered.len(),
            cut - 1,
        );
        previous = recovered.len();

        // 3. The torn tail was dropped *on disk*, so a second open agrees.
        let again = Wal::<Entry>::open(&path, MAX_RECORD, &RECORDS).expect("reopen").1;
        assert_eq!(
            again, recovered,
            "reopening the log recovered at {cut} produced a different history — \
             the torn tail was not truncated durably",
        );

        // 4. The recovered log still takes writes, landing after the prefix.
        let mut wal = Wal::<Entry>::open(&path, MAX_RECORD, &RECORDS).expect("reopen").0;
        let extra = Entry {
            seq: 999,
            body: "after".into(),
        };
        wal.append(&extra).expect("append after recovery");
        drop(wal);
        let after = Wal::<Entry>::open(&path, MAX_RECORD, &RECORDS)
            .expect("reopen after append")
            .1;
        let mut expected = recovered.clone();
        expected.push(extra);
        assert_eq!(
            after, expected,
            "a log recovered at {cut} did not accept an append cleanly",
        );
    }

    // The exhaustive loop is only meaningful if it actually saw the log grow from
    // empty to whole — otherwise the file was empty and every case was trivial.
    assert_eq!(
        previous,
        records.len(),
        "the untruncated file did not recover every record",
    );
    assert!(full.len() > 50, "the corpus of crash points was too small");
}

#[test]
fn every_single_byte_corruption_drops_that_record_and_the_rest() {
    let dir = tempfile::tempdir().expect("tempdir");
    let records = entries(5);
    let full = written(dir.path(), &records);

    for pos in 0..full.len() {
        let mut corrupt = full.clone();
        // Flip the low bit: a one-bit change anywhere in the file header, a length
        // header, a payload, or a checksum.
        corrupt[pos] ^= 1;

        if pos < wal::HEADER_LEN {
            // Damage in the *file* header is a different outcome, and the stronger
            // one: the log's identity is what was damaged, so nothing about the
            // frames can be trusted and `open` refuses outright rather than
            // recovering a prefix. Every one of the sixteen bytes is covered — the
            // magic, the frame revision, the checksum kind, the record schema, and
            // the reserved field.
            let path = dir.path().join(format!("hdr{pos}.log"));
            fs::write(&path, &corrupt).expect("write case");
            assert!(
                Wal::<Entry>::open(&path, MAX_RECORD, &RECORDS).is_err(),
                "corrupting header byte {pos} was accepted; a log whose identity is \
                 damaged must be refused, not scanned",
            );
            continue;
        }

        let (recovered, _) = recover(dir.path(), &format!("bit{pos}.log"), &corrupt);

        assert!(
            recovered.len() <= records.len(),
            "corrupting byte {pos} recovered more records than were ever written",
        );
        assert_eq!(
            recovered[..],
            records[..recovered.len()],
            "corrupting byte {pos} recovered a history that is not a prefix of \
             the appended records — a damaged record was returned as good",
        );
    }
}

#[test]
fn a_length_header_claiming_more_than_the_file_holds_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let records = entries(3);
    let mut full = written(dir.path(), &records);

    // Rewrite the first record's length header as `MAX_RECORD + 1`. The scan must
    // treat an oversized length as corruption rather than trusting it and reading
    // past the end — the case a hostile or bit-rotted header produces. The frames
    // begin after the file header, which this must not disturb.
    full[wal::HEADER_LEN..wal::HEADER_LEN + 4].copy_from_slice(&(MAX_RECORD + 1).to_le_bytes());
    let (recovered, _) = recover(dir.path(), "oversized.log", &full);
    assert!(
        recovered.is_empty(),
        "an oversized length header was trusted; recovery returned {} records",
        recovered.len(),
    );
}
