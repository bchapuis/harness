//! Differential oracle for the [`GrainStore`] seam (granary §7.2, §7.4, §8, §9):
//! drive a seeded stream of every store operation through `FileGrainStore` and a
//! `MemoryGrainStore` mirror, reopening the file store from disk as it goes, and
//! require that the two agree on **every return value** at every step.
//!
//! `MemoryGrainStore` is the reference implementation, so this is the check that the
//! durable store means the same thing the reference does — including across a
//! restart, which is the half no in-process test reaches. It exists to make a
//! storage-engine replacement reviewable: a rewrite that preserves the seam's meaning
//! passes unchanged, and one that does not fails here rather than in a swarm suite
//! three layers up, where the symptom would be a lost commit with no obvious cause.
//!
//! **Why the returns and not just the final state.** A store that answered `Stale`
//! where the reference answered `Stored` would leave identical records on disk and
//! still be wrong: the ack is what the replicator counts toward a quorum (§7.2), so a
//! divergence there is a divergence in whether a write committed. Comparing reads
//! alone cannot see it.
//!
//! This drives the store directly and synchronously — no simulation, no `run_swarm`,
//! so no `*swarm.rs` naming obligation and no corpus key (simulation-testing §4).

use actor_serialization::Codec;
use actor_serialization::JsonCodec;
use std::collections::BTreeSet;

use granary::BlobId;
use granary::FileGrainStore;
use granary::GrainBlobStore;
use granary::GrainName;
use granary::GrainStore;
use granary::MemoryGrainStore;
use granary::ReadOutcome;
use granary::Seq;
use granary::StoreAck;
use granary::Term;
use granary::WriteKind;

/// Shards, so the per-shard fence and append bound are exercised as *shared* state
/// across grains and as *isolated* state across shards.
const SHARDS: [u32; 2] = [0, 1];
/// Grains per shard. Enough that a range seal/removal splits the set rather than
/// covering all or none of it.
const GRAINS: usize = 8;
/// Operations per seed.
const STEPS: usize = 80;
/// Seeds. Each is a distinct schedule; this is the knob to raise when hunting a
/// suspected divergence, at roughly half a second per seed.
const SEEDS: u64 = 100;

/// splitmix64 — a seeded stream in twenty lines, so this suite needs no `rand`
/// dependency and replays identically from a seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn chance(&mut self, one_in: u64) -> bool {
        self.below(one_in) == 0
    }
}

/// Read a store call's durable outcome. The seam completes the write before it
/// returns, so this only unwraps the value the store already made stable.
fn grain(i: u64) -> GrainName {
    GrainName::new("test.Grain", format!("g{i}"))
}

/// The grains this oracle touches, in a fixed order.
fn all_grains() -> Vec<GrainName> {
    (0..GRAINS as u64).map(grain).collect()
}

/// A blob payload from a small pool, so ids repeat and the dedup path (**B2**) is
/// exercised alongside distinct content.
fn blob_bytes(i: u64) -> Vec<u8> {
    vec![b'a' + (i % 8) as u8; 1 + (i % 5) as usize * 7]
}

/// One step's operation. Named rather than inlined so a failure reports which
/// operation diverged, which is most of the work of diagnosing one.
#[derive(Debug)]
enum Op {
    StoreRecord {
        after: Seq,
        term: Term,
        count: usize,
        kind: WriteKind,
    },
    StoreSnapshot {
        at: Seq,
        term: Term,
        kind: WriteKind,
    },
    Truncate {
        after: Seq,
        term: Term,
    },
    Prepare {
        term: Term,
    },
    Read,
    ReadFrom {
        from: Seq,
        limit: usize,
    },
    PutBlob {
        which: u64,
    },
    GetBlob {
        which: u64,
    },
    HasBlob {
        which: u64,
    },
    DeleteBlob {
        which: u64,
    },
    RetainBlobs {
        keep: Vec<u64>,
    },
    BlobIds,
    DeleteBlobs,
    SealRange {
        from: u64,
    },
    Unseal,
    RemoveGrain,
    RemoveRange {
        from: u64,
    },
    DropShard,
}

/// Draw an operation. `head` and `base` come from the mirror, so `after` can be aimed
/// at the boundaries where the two stores could plausibly disagree — the current
/// head (the ordinary append), one below it (a re-delivery, which must be idempotent
/// per slot), one above (a gap), zero, and just under the compacted base (a write the
/// snapshot already subsumes).
fn draw(rng: &mut Rng, head: Seq, base: Seq) -> Op {
    let term = Term::new(rng.below(4));
    let after = match rng.below(5) {
        0 => head,
        1 => Seq::new(head.value().saturating_sub(1)),
        2 => Seq::new(head.value() + 1),
        3 => Seq::ZERO,
        _ => Seq::new(base.value().saturating_sub(1)),
    };
    let kind = match rng.below(6) {
        0 => WriteKind::Repair,
        1 => WriteKind::Transfer,
        _ => WriteKind::Append,
    };
    // Weighted so the record path dominates and the destructive range operations
    // stay rare enough to leave state for the rest to act on.
    match rng.below(100) {
        0..=24 => Op::StoreRecord {
            after,
            term,
            count: 1 + rng.below(3) as usize,
            kind,
        },
        25..=34 => Op::StoreSnapshot {
            at: after,
            term,
            kind,
        },
        35..=39 => Op::Truncate { after, term },
        40..=47 => Op::Prepare { term },
        48..=53 => Op::Read,
        54..=59 => Op::ReadFrom {
            from: after,
            limit: 1 + rng.below(4) as usize,
        },
        60..=69 => Op::PutBlob {
            which: rng.below(6),
        },
        70..=73 => Op::GetBlob {
            which: rng.below(6),
        },
        74..=76 => Op::HasBlob {
            which: rng.below(6),
        },
        77..=79 => Op::DeleteBlob {
            which: rng.below(6),
        },
        80..=82 => Op::RetainBlobs {
            keep: (0..rng.below(4)).map(|_| rng.below(6)).collect(),
        },
        83..=85 => Op::BlobIds,
        86..=87 => Op::DeleteBlobs,
        88..=91 => Op::SealRange { from: rng.next() },
        92..=93 => Op::Unseal,
        94..=96 => Op::RemoveGrain,
        97..=98 => Op::RemoveRange { from: rng.next() },
        _ => Op::DropShard,
    }
}

/// Apply one operation to both stores and require identical returns. `where_` names
/// the seed and step so a failure is replayable.
fn apply(
    file: &FileGrainStore,
    mem: &MemoryGrainStore,
    shard: u32,
    g: &GrainName,
    op: &Op,
    where_: &str,
) {
    match op {
        Op::StoreRecord {
            after,
            term,
            count,
            kind,
        } => {
            let records: Vec<Vec<u8>> = (0..*count).map(|i| vec![b'r', i as u8]).collect();
            let f = file.store_record(shard, g, *after, *term, records.clone(), *kind);
            let m = mem.store_record(shard, g, *after, *term, records, *kind);
            assert_eq!(f, m, "{where_}: store_record ack diverged ({op:?})");
        }
        Op::StoreSnapshot { at, term, kind } => {
            let state = vec![b's', at.value() as u8];
            let f = file.store_snapshot(shard, g, *at, *term, state.clone(), *kind);
            let m = mem.store_snapshot(shard, g, *at, *term, state, *kind);
            assert_eq!(f, m, "{where_}: store_snapshot ack diverged ({op:?})");
        }
        Op::Truncate { after, term } => {
            file.truncate(shard, g, *after, *term);
            mem.truncate(shard, g, *after, *term);
        }
        Op::Prepare { term } => {
            let f = file.prepare(shard, g, *term);
            let m = mem.prepare(shard, g, *term);
            match (&f, &m) {
                (ReadOutcome::Prepared(a), ReadOutcome::Prepared(b)) => {
                    assert_eq!(a.slots, b.slots, "{where_}: prepare slots diverged");
                    assert_eq!(
                        a.snapshot, b.snapshot,
                        "{where_}: prepare snapshot diverged"
                    );
                }
                (ReadOutcome::Fenced(a), ReadOutcome::Fenced(b)) => {
                    assert_eq!(a, b, "{where_}: prepare fence term diverged")
                }
                _ => panic!("{where_}: prepare outcome diverged: {f:?} vs {m:?}"),
            }
        }
        Op::Read => {
            let f = file.read(shard, g);
            let m = mem.read(shard, g);
            assert_eq!(f.slots, m.slots, "{where_}: read slots diverged");
            assert_eq!(f.snapshot, m.snapshot, "{where_}: read snapshot diverged");
        }
        Op::ReadFrom { from, limit } => {
            let f = file.read_from(shard, g, *from, *limit);
            let m = mem.read_from(shard, g, *from, *limit);
            assert_eq!(f, m, "{where_}: read_from diverged ({op:?})");
        }
        Op::PutBlob { which } => {
            let bytes = blob_bytes(*which);
            let id = BlobId::of(&bytes);
            // The acks are compared like every other op's result: a store that
            // refuses a put the other accepted has diverged, and the two stores
            // agreeing on *outcomes* is what this oracle is for (**G18**).
            let f = file.put_blob(shard, g, id, bytes.clone());
            let m = mem.put_blob(shard, g, id, bytes);
            assert_eq!(f, m, "{where_}: put_blob diverged ({op:?})");
        }
        Op::GetBlob { which } => {
            let id = BlobId::of(&blob_bytes(*which));
            let f = file.get_blob(shard, g, id);
            let m = mem.get_blob(shard, g, id);
            assert_eq!(f, m, "{where_}: get_blob diverged ({op:?})");
        }
        Op::HasBlob { which } => {
            let id = BlobId::of(&blob_bytes(*which));
            let f = file.has_blob(shard, g, id);
            let m = mem.has_blob(shard, g, id);
            assert_eq!(f, m, "{where_}: has_blob diverged ({op:?})");
        }
        Op::DeleteBlob { which } => {
            let id = BlobId::of(&blob_bytes(*which));
            file.delete_blob(shard, g, id);
            mem.delete_blob(shard, g, id);
        }
        Op::RetainBlobs { keep } => {
            let retain: BTreeSet<BlobId> =
                keep.iter().map(|w| BlobId::of(&blob_bytes(*w))).collect();
            file.retain_blobs(shard, g, &retain);
            mem.retain_blobs(shard, g, &retain);
        }
        Op::BlobIds => {
            let mut f = file.blob_ids(shard, g);
            let mut m = mem.blob_ids(shard, g);
            f.sort();
            m.sort();
            assert_eq!(f, m, "{where_}: blob_ids diverged");
        }
        Op::DeleteBlobs => {
            file.delete_blobs(shard, g);
            mem.delete_blobs(shard, g);
        }
        Op::SealRange { from } => {
            file.seal_range(shard, *from);
            mem.seal_range(shard, *from);
        }
        Op::Unseal => {
            file.unseal(shard);
            mem.unseal(shard);
        }
        Op::RemoveGrain => {
            file.remove_grain(shard, g);
            mem.remove_grain(shard, g);
        }
        Op::RemoveRange { from } => {
            file.remove_range(shard, *from);
            mem.remove_range(shard, *from);
        }
        Op::DropShard => {
            file.drop_shard(shard);
            mem.drop_shard(shard);
        }
    }
}

/// Compare everything both stores expose, for every shard and grain.
fn compare_all(
    file: &FileGrainStore,
    mem: &MemoryGrainStore,
    root: &std::path::Path,
    where_: &str,
) {
    for shard in SHARDS {
        for g in all_grains() {
            let f = file.read(shard, &g);
            let m = mem.read(shard, &g);
            assert_eq!(f.slots, m.slots, "{where_}: final slots diverged for {g:?}");
            assert_eq!(
                f.snapshot, m.snapshot,
                "{where_}: final snapshot diverged for {g:?}"
            );
            assert_eq!(
                file.read_from(shard, &g, Seq::ZERO, usize::MAX),
                mem.read_from(shard, &g, Seq::ZERO, usize::MAX),
                "{where_}: final read_from diverged for {g:?}"
            );
            let mut fb = file.blob_ids(shard, &g);
            let mut mb = mem.blob_ids(shard, &g);
            fb.sort();
            mb.sort();
            assert_eq!(fb, mb, "{where_}: final blob_ids diverged for {g:?}");
        }
        let mut fg = file.grains(shard);
        let mut mg = mem.grains(shard);
        fg.sort();
        mg.sort();
        if fg != mg {
            // The listing is derived from what is on disk, so a divergence is most
            // quickly read there.
            eprintln!("--- on disk under {} ---", root.display());
            for sub in ["segments", "blobs"] {
                if let Ok(entries) = std::fs::read_dir(root.join(sub)) {
                    for e in entries.flatten() {
                        eprintln!("  {sub}/{:?}", e.file_name());
                    }
                }
            }
        }
        assert_eq!(fg, mg, "{where_}: grains({shard}) diverged");

        // `shard_bytes` is an estimate by contract (`GrainStore::shard_bytes`), and
        // the two stores measure different things: the mirror sums payload lengths,
        // the file store sums file sizes, which carry framing and a log header. So
        // the assertion is the relation that must hold rather than equality — the
        // durable footprint is never below the bytes it stores. Do not strengthen
        // this to `assert_eq!`; it would fail on the header alone.
        assert!(
            file.shard_bytes(shard) >= mem.shard_bytes(shard),
            "{where_}: file shard_bytes {} below mirror {} for shard {shard}",
            file.shard_bytes(shard),
            mem.shard_bytes(shard),
        );
    }
}

fn run(seed: u64) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut rng = Rng(seed.wrapping_mul(0x2545_F491_4F6C_DD1D) | 1);
    let mem = MemoryGrainStore::new();
    let mut file = FileGrainStore::open(dir.path(), JsonCodec.name()).expect("open");

    for step in 0..STEPS {
        let where_ = format!("seed {seed} step {step}");
        // Reopen from disk mid-stream: everything after this reads state the file
        // store recovered rather than state it still had in memory.
        if rng.chance(6) {
            drop(file);
            file = FileGrainStore::open(dir.path(), JsonCodec.name()).expect("reopen");
            compare_all(&file, &mem, dir.path(), &format!("{where_} (after reopen)"));
        }
        let shard = SHARDS[rng.below(SHARDS.len() as u64) as usize];
        let g = grain(rng.below(GRAINS as u64));
        let reply = mem.read(shard, &g);
        let head = reply
            .slots
            .last()
            .map(|(s, _, _)| *s)
            .or(reply.snapshot.as_ref().map(|(s, _, _)| *s))
            .unwrap_or(Seq::ZERO);
        let base = reply.snapshot.as_ref().map_or(Seq::ZERO, |(s, _, _)| *s);
        let op = draw(&mut rng, head, base);
        if std::env::var_os("ORACLE_TRACE").is_some() {
            eprintln!("{where_}: shard {shard} {g:?} {op:?}");
        }
        apply(&file, &mem, shard, &g, &op, &where_);
    }

    // The state must survive one more round trip through the filesystem.
    drop(file);
    let file = FileGrainStore::open(dir.path(), JsonCodec.name()).expect("final reopen");
    compare_all(&file, &mem, dir.path(), &format!("seed {seed} final"));
}

#[test]
fn file_store_matches_memory_store_under_a_seeded_op_stream() {
    for seed in 0..SEEDS {
        run(seed);
    }
}

/// Named regression for the first divergence the seeded stream found.
///
/// The manifest is append-only, so `remove_grain` leaves the grain's segment id
/// assigned — and `segment_id` kept answering with it, so opening the segment to
/// serve a *read* re-created the file that removal had just deleted. Nothing looked
/// wrong at the read (the reply was still empty), but the grain came back in
/// `grains`, and the split driver's GC enumerates that list.
#[test]
fn a_read_of_a_removed_grain_does_not_resurrect_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let g = grain(0);
    let store = FileGrainStore::open(dir.path(), JsonCodec.name()).expect("open");
    let seeded = store.store_record(
        0,
        &g,
        Seq::ZERO,
        Term::new(1),
        vec![b"a".to_vec()],
        WriteKind::Append,
    );
    assert!(
        matches!(seeded, StoreAck::Stored(_)),
        "the grain must exist before removal is worth testing"
    );
    store.remove_grain(0, &g);

    // The read that used to bring it back.
    assert!(store.read(0, &g).slots.is_empty());
    assert!(store.read_from(0, &g, Seq::ZERO, 10).records.is_empty());

    assert!(
        !store.grains(0).contains(&g),
        "a read resurrected the removed grain"
    );
    drop(store);
    let reopened = FileGrainStore::open(dir.path(), JsonCodec.name()).expect("reopen");
    assert!(
        !reopened.grains(0).contains(&g),
        "the resurrected grain survived a reopen"
    );
}

/// Named regression for the second divergence.
///
/// `truncate` created the grain's segment before dropping a tail it did not have, so
/// rolling back an append that never landed locally *created* the grain — which then
/// enumerated and migrated as if it held data.
#[test]
fn truncating_an_unseen_grain_does_not_create_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let g = grain(1);
    let store = FileGrainStore::open(dir.path(), JsonCodec.name()).expect("open");
    store.truncate(0, &g, Seq::ZERO, Term::new(1));
    assert!(
        store.grains(0).is_empty(),
        "truncating nothing created a grain: {:?}",
        store.grains(0)
    );
}

/// Named regression for the third divergence, and the only one the oracle above
/// structurally cannot reach: it drives the store from one thread, and this one needs
/// two.
///
/// A blob is named by its content, so two puts of the same bytes are two puts at one
/// path — and once the disk facet started pipelining its block puts, that stopped
/// being rare. An image of identical blocks (an all-zero base image is the ordinary
/// case, not a contrived one) hashes every block to a single id and issues a wave of
/// them at once. They collided on a shared scratch file: one truncated another's
/// half-written bytes, and every rename after the first found the path already gone.
/// `put_blob` reports that as `Failed`, which poisons the store store-wide and
/// one-way — so a create of a zero-filled image left a live node refusing every
/// subsequent write, for the rest of its life.
///
/// Asserting on `failure()` and not just the acks is the point: a store that answered
/// `Stored` while quietly poisoning itself would pass on the return values alone and
/// fail the next unrelated write.
#[test]
fn concurrent_puts_of_one_blob_neither_fail_nor_poison_the_store() {
    const PUTTERS: usize = 16;
    let dir = tempfile::tempdir().expect("tempdir");
    let store = FileGrainStore::open(dir.path(), JsonCodec.name()).expect("open");
    let g = grain(1);
    // A block's worth of one repeated byte — the zero-filled image, in miniature.
    let bytes = vec![0u8; 256 * 1024];
    let id = BlobId::of(&bytes);

    std::thread::scope(|scope| {
        for _ in 0..PUTTERS {
            scope.spawn(|| {
                assert_eq!(
                    store.put_blob(0, &g, id, bytes.clone()),
                    granary::BlobAck::Stored,
                    "a concurrent put of already-agreed content was refused"
                );
            });
        }
    });

    assert_eq!(store.failure(), None, "the store poisoned itself");
    assert_eq!(
        store.get_blob(0, &g, id),
        Some(bytes),
        "the blob that landed is not the bytes every putter wrote"
    );
    // The store still takes writes — what poisoning would have taken away.
    assert_eq!(
        store.put_blob(0, &g, BlobId::of(b"after"), b"after".to_vec()),
        granary::BlobAck::Stored,
    );
}
