//! The durable workspace filesystem grain on the single-node `Local` tier
//! (durable-workspace design; granary §7.10, §14).
//!
//! Drives [`Fs`] through its public command API: write and read files (whole and
//! ranged, including multi-block content), `mkdir -p` on nested writes, list and stat,
//! overwrite, truncate (shrink and zero-fill grow), remove, rename, destroy — and
//! that the workspace survives hibernation because the metadata is journaled and the
//! blocks live in the grain's colocated blob area (§7.10).

use std::time::Duration;

use actor_core::EventSink;
use actor_core::LocalSystemBuilder;
use actor_simulation::Recorder;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use granary::GrainEvent;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::fs::BLOCK_BYTES;
use granary::fs::Destroy;
use granary::fs::DirEntry;
use granary::fs::Fs;
use granary::fs::FsError;
use granary::fs::ListDir;
use granary::fs::Metadata;
use granary::fs::ReadFile;
use granary::fs::Remove;
use granary::fs::Rename;
use granary::fs::Stat;
use granary::fs::Truncate;
use granary::fs::WriteFile;

type Ws = Fs<SimSystem>;

fn local() -> (Simulation, SimSystem) {
    let sim = Simulation::new(7);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    (sim, system)
}

fn write(path: &str, content: &[u8]) -> WriteFile {
    WriteFile {
        path: path.into(),
        content: content.to_vec(),
    }
}

fn read(path: &str) -> ReadFile {
    ReadFile {
        path: path.into(),
        range: None,
    }
}

#[test]
fn write_then_read_round_trips_whole_and_ranged() {
    let (sim, system) = local();
    let ws = system.granary::<Ws>(GranaryConfig::default());
    let g = ws.grain("ws/0");
    sim.block_on(async move {
        let content = b"a workspace file, written through the grain".to_vec();
        assert_eq!(
            g.ask(write("notes.txt", &content)).await.unwrap(),
            Ok(content.len() as u64)
        );
        assert_eq!(g.ask(read("notes.txt")).await.unwrap(), Ok(content.clone()));
        // A ranged read slices the file.
        assert_eq!(
            g.ask(ReadFile {
                path: "notes.txt".into(),
                range: Some((2, 11))
            })
            .await
            .unwrap(),
            Ok(content[2..11].to_vec())
        );
        // A missing file is a typed application error, not wrong bytes.
        assert_eq!(
            g.ask(read("missing")).await.unwrap(),
            Err(FsError::NotFound)
        );
    });
}

#[test]
fn nested_write_creates_parent_dirs_and_lists() {
    let (sim, system) = local();
    let ws = system.granary::<Ws>(GranaryConfig::default());
    let g = ws.grain("ws/0");
    sim.block_on(async move {
        g.ask(write("src/app/main.rs", b"fn main() {}"))
            .await
            .unwrap()
            .unwrap();
        g.ask(write("src/app/lib.rs", b"// lib"))
            .await
            .unwrap()
            .unwrap();
        // mkdir -p created src and src/app; both are directories.
        assert_eq!(
            g.ask(Stat { path: "src".into() }).await.unwrap(),
            Ok(Metadata { dir: true, size: 0 })
        );
        // The directory lists its two files, name-sorted, with sizes.
        let listing = g
            .ask(ListDir {
                path: "src/app".into(),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            listing,
            vec![
                DirEntry {
                    name: "lib.rs".into(),
                    dir: false,
                    size: 6
                },
                DirEntry {
                    name: "main.rs".into(),
                    dir: false,
                    size: 12
                },
            ]
        );
    });
}

#[test]
fn overwrite_replaces_the_whole_file() {
    let (sim, system) = local();
    let ws = system.granary::<Ws>(GranaryConfig::default());
    let g = ws.grain("ws/0");
    sim.block_on(async move {
        g.ask(write("f", b"the original longer contents"))
            .await
            .unwrap()
            .unwrap();
        g.ask(write("f", b"short")).await.unwrap().unwrap();
        // The shorter overwrite fully replaces — no stale tail from the first write.
        assert_eq!(g.ask(read("f")).await.unwrap(), Ok(b"short".to_vec()));
        assert_eq!(
            g.ask(Stat { path: "f".into() }).await.unwrap(),
            Ok(Metadata {
                dir: false,
                size: 5
            })
        );
    });
}

#[test]
fn multi_block_content_round_trips_across_the_block_boundary() {
    let (sim, system) = local();
    let ws = system.granary::<Ws>(GranaryConfig::default());
    let g = ws.grain("ws/0");
    sim.block_on(async move {
        // Just over one block, so the write chunks into two blobs.
        let mut content = vec![0u8; BLOCK_BYTES + 1000];
        for (i, b) in content.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        g.ask(write("big.bin", &content)).await.unwrap().unwrap();
        assert_eq!(g.ask(read("big.bin")).await.unwrap(), Ok(content.clone()));
        // A range straddling the block boundary reassembles from both blocks.
        let (lo, hi) = (BLOCK_BYTES as u64 - 10, BLOCK_BYTES as u64 + 10);
        assert_eq!(
            g.ask(ReadFile {
                path: "big.bin".into(),
                range: Some((lo, hi))
            })
            .await
            .unwrap(),
            Ok(content[lo as usize..hi as usize].to_vec())
        );
    });
}

#[test]
fn truncate_shrinks_and_grows_with_zero_fill() {
    let (sim, system) = local();
    let ws = system.granary::<Ws>(GranaryConfig::default());
    let g = ws.grain("ws/0");
    sim.block_on(async move {
        g.ask(write("f", b"0123456789")).await.unwrap().unwrap();
        // Shrink: the read clamps to the new size.
        g.ask(Truncate {
            path: "f".into(),
            size: 4,
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(g.ask(read("f")).await.unwrap(), Ok(b"0123".to_vec()));
        // Grow: the gap reads back as zeros.
        g.ask(Truncate {
            path: "f".into(),
            size: 7,
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(g.ask(read("f")).await.unwrap(), Ok(b"0123\0\0\0".to_vec()));
    });
}

#[test]
fn remove_and_rename() {
    let (sim, system) = local();
    let ws = system.granary::<Ws>(GranaryConfig::default());
    let g = ws.grain("ws/0");
    sim.block_on(async move {
        g.ask(write("dir/a", b"a")).await.unwrap().unwrap();
        g.ask(write("dir/b", b"b")).await.unwrap().unwrap();
        // A non-recursive remove of a non-empty directory is refused.
        assert_eq!(
            g.ask(Remove {
                path: "dir".into(),
                recursive: false
            })
            .await
            .unwrap(),
            Err(FsError::NotEmpty)
        );
        // Remove one file; rename the other within the directory.
        g.ask(Remove {
            path: "dir/a".into(),
            recursive: false,
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(g.ask(read("dir/a")).await.unwrap(), Err(FsError::NotFound));
        g.ask(Rename {
            from: "dir/b".into(),
            to: "dir/c".into(),
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(g.ask(read("dir/c")).await.unwrap(), Ok(b"b".to_vec()));
        // Recursive remove clears the directory subtree.
        g.ask(Remove {
            path: "dir".into(),
            recursive: true,
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            g.ask(Stat { path: "dir".into() }).await.unwrap(),
            Err(FsError::NotFound)
        );
    });
}

#[test]
fn destroy_resets_the_workspace() {
    let (sim, system) = local();
    let ws = system.granary::<Ws>(GranaryConfig::default());
    let g = ws.grain("ws/0");
    sim.block_on(async move {
        g.ask(write("a", b"a")).await.unwrap().unwrap();
        g.ask(write("b/c", b"c")).await.unwrap().unwrap();
        g.ask(Destroy).await.unwrap().unwrap();
        assert_eq!(g.ask(read("a")).await.unwrap(), Err(FsError::NotFound));
        assert_eq!(
            g.ask(Stat { path: "b".into() }).await.unwrap(),
            Err(FsError::NotFound)
        );
        // The workspace is usable again after the reset.
        g.ask(write("fresh", b"new")).await.unwrap().unwrap();
        assert_eq!(g.ask(read("fresh")).await.unwrap(), Ok(b"new".to_vec()));
    });
}

#[test]
fn workspace_survives_hibernation() {
    let sim = Simulation::new(7);
    let recorder = Recorder::new();
    let sink: std::sync::Arc<dyn EventSink> = std::sync::Arc::new(recorder.clone());
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build();
    let ws = system.granary::<Ws>(GranaryConfig {
        idle_after: Duration::from_millis(10),
        snapshot_every: 1,
        ..GranaryConfig::default()
    });

    sim.block_on({
        let g = ws.grain("ws/0");
        async move {
            g.ask(write("keep/notes.txt", b"durable working state"))
                .await
                .unwrap()
                .unwrap();
        }
    });

    // Drive past the idle window: the grain snapshots its metadata and hibernates.
    sim.run();
    assert!(
        recorder.events().iter().any(|e| matches!(
            e.as_app::<GrainEvent>(),
            Some(GrainEvent::Passivated { .. })
        )),
        "the idle workspace must hibernate",
    );

    // A fresh activation rehydrates the metadata from the snapshot and the file blocks
    // are still in the colocated blob area — the workspace survives the eviction.
    let g = ws.grain("ws/0");
    let got = sim.block_on(async move { g.ask(read("keep/notes.txt")).await.unwrap() });
    assert_eq!(got, Ok(b"durable working state".to_vec()));
}

// --- Blob reclamation: the post-commit sweep (§7.10 GC) -------------------------

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use granary::BlobId;
use granary::Grain;
use granary::GrainName;
use granary::GrainStore;
use granary::GranarySystem;
use granary::GrainStoreFactory;
use granary::MemoryGrainStore;
use granary::ReadOutcome;
use granary::ReadReply;
use granary::Seq;
use granary::StoreAck;
use granary::Term;
use granary::WriteKind;
use granary::shard_for;

/// A workspace over a shared, inspectable store: the granary plus the handles the
/// blob-reclamation assertions need (the grain's shard index and name).
fn inspectable(
    system: &SimSystem,
    key: &str,
) -> (
    granary::Granary<Ws>,
    Arc<MemoryGrainStore>,
    u32,
    GrainName,
) {
    let store = Arc::new(MemoryGrainStore::new());
    let factory: GrainStoreFactory = {
        let store = Arc::clone(&store);
        Arc::new(move |_| Arc::clone(&store) as Arc<dyn GrainStore>)
    };
    let config = GranaryConfig {
        grain_store: Some(factory),
        ..GranaryConfig::default()
    };
    let shard = shard_for(<Ws as Grain>::GRAIN_TYPE, key, config.shards).index;
    let name = GrainName::new(<Ws as Grain>::GRAIN_TYPE, key);
    (system.granary::<Ws>(config), store, shard, name)
}

#[test]
fn overwrite_reclaims_the_old_files_blocks() {
    let (sim, system) = local();
    let (ws, store, shard, name) = inspectable(&system, "ws/0");
    let g = ws.grain("ws/0");
    let old_id = BlobId::of(b"version one");
    let new_id = BlobId::of(b"version two");
    sim.block_on(async move {
        g.ask(write("f", b"version one")).await.unwrap().unwrap();
        assert!(store.has_blob(shard, &name, old_id));
        g.ask(write("f", b"version two")).await.unwrap().unwrap();
        // Let the post-commit sweep the overwrite scheduled run.
        system.sleep(Duration::from_secs(1)).await;
        assert!(
            !store.has_blob(shard, &name, old_id),
            "the overwritten block must be reclaimed by the sweep"
        );
        assert!(store.has_blob(shard, &name, new_id));
        assert_eq!(g.ask(read("f")).await.unwrap(), Ok(b"version two".to_vec()));
    });
}

#[test]
fn remove_reclaims_the_files_blocks() {
    let (sim, system) = local();
    let (ws, store, shard, name) = inspectable(&system, "ws/0");
    let g = ws.grain("ws/0");
    let id = BlobId::of(b"doomed bytes");
    sim.block_on(async move {
        g.ask(write("dir/f", b"doomed bytes")).await.unwrap().unwrap();
        assert!(store.has_blob(shard, &name, id));
        g.ask(Remove {
            path: "dir".into(),
            recursive: true,
        })
        .await
        .unwrap()
        .unwrap();
        system.sleep(Duration::from_secs(1)).await;
        assert!(
            !store.has_blob(shard, &name, id),
            "a removed file's blocks must be reclaimed by the sweep"
        );
    });
}

#[test]
fn destroy_reclaims_the_blob_area_after_the_commit() {
    let (sim, system) = local();
    let (ws, store, shard, name) = inspectable(&system, "ws/0");
    let g = ws.grain("ws/0");
    let id = BlobId::of(b"workspace bytes");
    sim.block_on(async move {
        g.ask(write("f", b"workspace bytes")).await.unwrap().unwrap();
        g.ask(Destroy).await.unwrap().unwrap();
        system.sleep(Duration::from_secs(1)).await;
        assert!(
            !store.has_blob(shard, &name, id),
            "destroy must reclaim the whole blob area once the reset committed"
        );
    });
}

/// A [`GrainStore`] that refuses the next record append with a fence, driving the
/// `Local` journal to report the commit `Unavailable` — the §6 commit-failure
/// injection. Everything else delegates.
struct FailNextAppend {
    inner: MemoryGrainStore,
    fail: AtomicBool,
}

impl GrainStore for FailNextAppend {
    fn store_record(
        &self,
        shard: u32,
        grain: &GrainName,
        after: Seq,
        term: Term,
        records: Vec<Vec<u8>>,
        kind: WriteKind,
    ) -> StoreAck {
        if self.fail.swap(false, Ordering::SeqCst) {
            return StoreAck::Fenced(Term::new(u64::MAX));
        }
        self.inner
            .store_record(shard, grain, after, term, records, kind)
    }

    fn read(&self, shard: u32, grain: &GrainName) -> ReadReply {
        self.inner.read(shard, grain)
    }

    fn read_from(&self, shard: u32, grain: &GrainName, from: Seq, limit: usize) -> Vec<(Seq, Vec<u8>)> {
        self.inner.read_from(shard, grain, from, limit)
    }

    fn prepare(&self, shard: u32, grain: &GrainName, term: Term) -> ReadOutcome {
        self.inner.prepare(shard, grain, term)
    }

    fn store_snapshot(&self, shard: u32, grain: &GrainName, at: Seq, term: Term, state: Vec<u8>) -> StoreAck {
        self.inner.store_snapshot(shard, grain, at, term, state)
    }

    fn truncate(&self, shard: u32, grain: &GrainName, after: Seq, term: Term) {
        self.inner.truncate(shard, grain, after, term)
    }

    fn put_blob(&self, shard: u32, grain: &GrainName, id: BlobId, bytes: Vec<u8>) {
        self.inner.put_blob(shard, grain, id, bytes)
    }

    fn get_blob(&self, shard: u32, grain: &GrainName, id: BlobId) -> Option<Vec<u8>> {
        self.inner.get_blob(shard, grain, id)
    }

    fn has_blob(&self, shard: u32, grain: &GrainName, id: BlobId) -> bool {
        self.inner.has_blob(shard, grain, id)
    }

    fn delete_blob(&self, shard: u32, grain: &GrainName, id: BlobId) {
        self.inner.delete_blob(shard, grain, id)
    }

    fn delete_blobs(&self, shard: u32, grain: &GrainName) {
        self.inner.delete_blobs(shard, grain)
    }

    fn retain_blobs(&self, shard: u32, grain: &GrainName, retain: &std::collections::BTreeSet<BlobId>) {
        self.inner.retain_blobs(shard, grain, retain)
    }

    fn grains(&self, shard: u32) -> Vec<GrainName> {
        self.inner.grains(shard)
    }

    fn blob_ids(&self, shard: u32, grain: &GrainName) -> Vec<BlobId> {
        self.inner.blob_ids(shard, grain)
    }
}

#[test]
fn a_failed_destroy_commit_leaves_the_workspace_readable() {
    // The critical-ordering regression: `Destroy` must journal the reset BEFORE the
    // blob area is reclaimed. If the commit fails, the grain rehydrates the old tree
    // — which must still resolve every block. (Pre-fix, the decide phase deleted the
    // blobs first, leaving the workspace permanently unreadable after this exact
    // sequence.)
    let (sim, system) = local();
    let store = Arc::new(FailNextAppend {
        inner: MemoryGrainStore::new(),
        fail: AtomicBool::new(false),
    });
    let factory: GrainStoreFactory = {
        let store = Arc::clone(&store);
        Arc::new(move |_| Arc::clone(&store) as Arc<dyn GrainStore>)
    };
    let ws = system.granary::<Ws>(GranaryConfig {
        grain_store: Some(factory),
        ..GranaryConfig::default()
    });
    let g = ws.grain("ws/0");
    sim.block_on(async move {
        let content = b"must survive a failed destroy".to_vec();
        g.ask(write("f", &content)).await.unwrap().unwrap();

        // The Destroy's append fails: the reset never commits, the ask surfaces the
        // durability outcome, and the host steps down.
        store.fail.store(true, Ordering::SeqCst);
        assert!(g.ask(Destroy).await.is_err(), "the destroy must not commit");

        // Let the destroy-scheduled sweep run against the (unchanged) committed
        // tree — it must reclaim nothing the tree still references.
        system.sleep(Duration::from_secs(1)).await;

        // A fresh activation rehydrates the old tree; every block must still read.
        assert_eq!(g.ask(read("f")).await.unwrap(), Ok(content));
    });
}

#[test]
fn dot_dot_path_components_are_rejected_as_invalid() {
    // The tree keeps no parent links, so `..` cannot be resolved — and treating it
    // as a literal name used to mint a directory *called* `..`. Every path-taking
    // command must refuse it instead.
    let (sim, system) = local();
    let ws = system.granary::<Ws>(GranaryConfig::default());
    let g = ws.grain("ws/0");
    sim.block_on(async move {
        g.ask(write("a/f", b"data")).await.unwrap().unwrap();
        assert_eq!(
            g.ask(write("a/../etc/passwd", b"nope")).await.unwrap(),
            Err(FsError::InvalidPath)
        );
        assert_eq!(
            g.ask(read("a/../a/f")).await.unwrap(),
            Err(FsError::InvalidPath)
        );
        assert_eq!(
            g.ask(Stat { path: "..".into() }).await.unwrap(),
            Err(FsError::InvalidPath)
        );
        assert_eq!(
            g.ask(ListDir { path: "a/..".into() }).await.unwrap(),
            Err(FsError::InvalidPath)
        );
        assert_eq!(
            g.ask(Remove {
                path: "a/../a".into(),
                recursive: true
            })
            .await
            .unwrap(),
            Err(FsError::InvalidPath)
        );
        assert_eq!(
            g.ask(Rename {
                from: "a/f".into(),
                to: "a/../g".into()
            })
            .await
            .unwrap(),
            Err(FsError::InvalidPath)
        );
        assert_eq!(
            g.ask(Truncate {
                path: "../a/f".into(),
                size: 0
            })
            .await
            .unwrap(),
            Err(FsError::InvalidPath)
        );
        // No literal `..` entry was ever created, and `.`/`//` still normalize.
        assert_eq!(
            g.ask(ListDir { path: "/".into() }).await.unwrap().unwrap().len(),
            1
        );
        assert_eq!(g.ask(read("./a//f")).await.unwrap(), Ok(b"data".to_vec()));
    });
}
